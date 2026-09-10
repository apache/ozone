/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.freon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

/**
 * Verifies that {@code --reads-per-write} of {@link HadoopFsReadWriteValidator}
 * (dfsrw) controls how many reads the run issues per write. The workload only
 * needs a Hadoop {@code FileSystem}, so these run against the local one instead
 * of a cluster.
 */
public class TestHadoopFsReadWriteRatio {

  private static final int WRITES = 8;

  @TempDir
  private java.nio.file.Path tempDir;

  private String rootPath;

  @BeforeEach
  void setUp() {
    // toUri() rather than the path itself: on Windows the latter is not a valid
    // URI, it has backslashes and a drive letter where the authority goes
    String uri = tempDir.toUri().toString();
    rootPath = uri.endsWith("/") ? uri.substring(0, uri.length() - 1) : uri;
    CorruptingLocalFileSystem.corruptFromRead(Integer.MAX_VALUE);
  }

  /**
   * One read per write is the default, and it stays that way when the ratio is
   * given explicitly.
   */
  @Test
  void pairsOneReadWithEveryWriteByDefault() {
    CommandLine cmd = runValidator(2);

    assertEquals(WRITES, writeCount(cmd));
    assertEquals(WRITES, readCount(cmd));
  }

  /**
   * A whole number of reads per write holds however the writes are spread over
   * the threads, so this can afford more than one of them.
   */
  @ParameterizedTest
  @ValueSource(ints = {2, 3, 5})
  void issuesRequestedReadsPerWrite(int ratio) {
    CommandLine cmd =
        runValidator(2, "--reads-per-write", String.valueOf(ratio));

    assertEquals(WRITES, writeCount(cmd));
    assertEquals((long) WRITES * ratio, readCount(cmd));
  }

  /**
   * A ratio below 1 reads back only every n-th write, and the fraction it
   * leaves over is carried between writes rather than rounded away on each of
   * them, which would read back nothing at all. The leftover is per thread, so
   * only a single thread gives an exact count.
   */
  @Test
  void spreadsFractionalRatioOverWrites() {
    CommandLine cmd = runValidator(1, "--reads-per-write", "0.25");

    assertEquals(WRITES, writeCount(cmd));
    assertEquals(WRITES / 4, readCount(cmd));
  }

  /** A ratio with a whole and a fractional part mixes the two behaviours. */
  @Test
  void spreadsMixedRatioOverWrites() {
    CommandLine cmd = runValidator(1, "--reads-per-write", "1.5");

    assertEquals(WRITES, writeCount(cmd));
    assertEquals(WRITES * 3 / 2, readCount(cmd));
  }

  /**
   * The leftover of a fractional ratio is carried exactly, so the tenth write
   * of a run at 0.1 is read back. A ratio accumulated as a double drifts —
   * adding 0.1 ten times gives 0.9999999999999999 — which would put that read
   * off to an eleventh write a ten-write run never makes.
   */
  @Test
  void carriesFractionalRatioWithoutDrift() {
    CommandLine cmd = runValidator(1, 10, "--reads-per-write", "0.1");

    assertEquals(10, writeCount(cmd));
    assertEquals(1, readCount(cmd));
  }

  /**
   * Every read of a task validates, not only the first or the last one: a
   * single write read back three times fails the run wherever among the three
   * the content is corrupted.
   */
  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  void everyReadOfATaskValidatesContent(int corruptedRead) {
    CorruptingLocalFileSystem.corruptFromRead(corruptedRead);

    int exitCode = new Freon().getCmd().execute(
        "-D", "fs.file.impl=" + CorruptingLocalFileSystem.class.getName(),
        "dfsrw",
        "-r", rootPath,
        "-p", "dfsrw-corrupt",
        "-n", "1",
        "-t", "1",
        "-s", "1KB",
        "--buffer", "1024",
        "--copy-buffer", "1024",
        "--reads-per-write", "3");

    assertNotEquals(0, exitCode,
        "Corrupted content of read " + corruptedRead + " was not detected");
  }

  @Test
  void rejectsNonPositiveRatio() {
    assertThat(execute(new Freon().getCmd(), 1, WRITES, "--reads-per-write", "0"))
        .isNotZero();
    assertThat(execute(new Freon().getCmd(), 1, WRITES, "--reads-per-write", "-1"))
        .isNotZero();
  }

  private CommandLine runValidator(int threads, String... args) {
    return runValidator(threads, WRITES, args);
  }

  private CommandLine runValidator(int threads, int writes, String... args) {
    CommandLine cmd = new Freon().getCmd();
    assertEquals(0, execute(cmd, threads, writes, args),
        "Freon dfsrw command failed");
    return cmd;
  }

  private int execute(CommandLine cmd, int threads, int writes,
      String... args) {
    String[] fixed = {
        "dfsrw",
        "-r", rootPath,
        "-p", "dfsrw",
        "-n", String.valueOf(writes),
        "-t", String.valueOf(threads),
        "-s", "1KB",
        "--buffer", "1024",
        "--copy-buffer", "1024"};
    String[] argv = new String[fixed.length + args.length];
    System.arraycopy(fixed, 0, argv, 0, fixed.length);
    System.arraycopy(args, 0, argv, fixed.length, args.length);
    return cmd.execute(argv);
  }

  private static long writeCount(CommandLine cmd) {
    return timerCount(cmd, "file-write");
  }

  private static long readCount(CommandLine cmd) {
    return timerCount(cmd, "file-read-validate");
  }

  private static long timerCount(CommandLine cmd, String name) {
    BaseFreonGenerator subject = (BaseFreonGenerator)
        cmd.getParseResult().subcommand().commandSpec().userObject();
    return subject.getMetrics().timer(name).getCount();
  }

  /**
   * A {@link LocalFileSystem} that alters the content it hands out from the
   * n-th {@code open()} on. The bytes are changed above the checksum
   * verification of {@link org.apache.hadoop.fs.ChecksumFileSystem}, so it is
   * the workload, not the file system, that has to notice.
   */
  public static final class CorruptingLocalFileSystem extends LocalFileSystem {

    private static final AtomicInteger READS = new AtomicInteger();
    private static int corruptFrom = Integer.MAX_VALUE;

    static void corruptFromRead(int read) {
      READS.set(0);
      corruptFrom = read;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      FSDataInputStream input = super.open(f, bufferSize);
      // the hidden .crc companions are the checksum bookkeeping of the file
      // system itself, only the reads of the workload are to be counted
      if (f.getName().startsWith(".") || READS.incrementAndGet() < corruptFrom) {
        return input;
      }
      return new FSDataInputStream(new FlippingInputStream(input));
    }
  }

  /**
   * Passes the wrapped stream through, with the first byte of the file flipped.
   * {@link FSInputStream} supplies the positioned reads on top of seek and
   * read, so only those two and the plain reads are forwarded here.
   */
  private static final class FlippingInputStream extends FSInputStream {

    private final FSDataInputStream input;

    private FlippingInputStream(FSDataInputStream input) {
      this.input = input;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      long pos = input.getPos();
      int read = input.read(b, off, len);
      if (read > 0 && pos == 0) {
        b[off] ^= 0xff;
      }
      return read;
    }

    @Override
    public int read() throws IOException {
      long pos = input.getPos();
      int b = input.read();
      return b >= 0 && pos == 0 ? b ^ 0xff : b;
    }

    @Override
    public void close() throws IOException {
      input.close();
    }

    @Override
    public void seek(long pos) throws IOException {
      input.seek(pos);
    }

    @Override
    public long getPos() throws IOException {
      return input.getPos();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return input.seekToNewSource(targetPos);
    }
  }
}
