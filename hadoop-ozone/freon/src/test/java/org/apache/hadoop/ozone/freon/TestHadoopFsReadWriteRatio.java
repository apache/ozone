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
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

/**
 * Verifies that {@code --read-write-ratio} of {@link HadoopFsReadWriteValidator}
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
        runValidator(2, "--read-write-ratio", String.valueOf(ratio));

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
    CommandLine cmd = runValidator(1, "--read-write-ratio", "0.25");

    assertEquals(WRITES, writeCount(cmd));
    assertEquals(WRITES / 4, readCount(cmd));
  }

  /** A ratio with a whole and a fractional part mixes the two behaviours. */
  @Test
  void spreadsMixedRatioOverWrites() {
    CommandLine cmd = runValidator(1, "--read-write-ratio", "1.5");

    assertEquals(WRITES, writeCount(cmd));
    assertEquals(WRITES * 3 / 2, readCount(cmd));
  }

  /**
   * Every read of a task validates, not only the first one: a single write read
   * back three times fails the run when the content is corrupted before the
   * last of the three reads.
   */
  @Test
  void everyReadOfATaskValidatesContent() {
    CorruptingLocalFileSystem.corruptFromRead(3);

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
        "--read-write-ratio", "3");

    assertNotEquals(0, exitCode, "Corrupted content was not detected");
  }

  @Test
  void rejectsNonPositiveRatio() {
    assertThat(execute(new Freon().getCmd(), 1, "--read-write-ratio", "0"))
        .isNotZero();
    assertThat(execute(new Freon().getCmd(), 1, "--read-write-ratio", "-1"))
        .isNotZero();
  }

  private CommandLine runValidator(int threads, String... args) {
    CommandLine cmd = new Freon().getCmd();
    assertEquals(0, execute(cmd, threads, args), "Freon dfsrw command failed");
    return cmd;
  }

  private int execute(CommandLine cmd, int threads, String... args) {
    String[] fixed = {
        "dfsrw",
        "-r", rootPath,
        "-p", "dfsrw",
        "-n", String.valueOf(WRITES),
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
   */
  private static final class FlippingInputStream extends InputStream
      implements Seekable, PositionedReadable {

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

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
        throws IOException {
      return input.read(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
        throws IOException {
      input.readFully(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      input.readFully(position, buffer);
    }
  }
}
