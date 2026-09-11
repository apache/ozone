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
import static org.assertj.core.api.Assertions.within;
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
 * Verifies that {@code --read-percent} of {@link HadoopFsReadWriteValidator}
 * (dfsrw) controls how the operations of a run split between reads and writes.
 * The workload only needs a Hadoop {@code FileSystem}, so these run against the
 * local one instead of a cluster.
 */
public class TestHadoopFsReadPercent {

  /**
   * Operations per run of the tests that assert a split. Each one draws on its
   * own, so a split is only exact at 0 and 100 percent; a run of this many
   * leaves the share of reads far closer to the percentage than the tolerance
   * of {@link #assertSplit} allows for.
   */
  private static final int OPS = 1000;

  /** Operations of the runs that assert an exact count. */
  private static final int FEW_OPS = 8;

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

  /** Half of the operations read, unless the run asks for another split. */
  @Test
  void splitsOperationsEvenlyByDefault() {
    CommandLine cmd = runValidator(1, OPS);

    assertSplit(cmd, 50);
  }

  /**
   * The requested share of the operations reads, whether that leaves the run
   * write-heavy or read-heavy.
   */
  @ParameterizedTest
  @ValueSource(ints = {10, 75, 90})
  void splitsOperationsByPercent(int percent) {
    CommandLine cmd =
        runValidator(1, OPS, "--read-percent", String.valueOf(percent));

    assertSplit(cmd, percent);
  }

  /** Nothing is read back at 0, which leaves a pure write load. */
  @Test
  void writesEveryOperationAtZeroPercent() {
    CommandLine cmd = runValidator(1, FEW_OPS, "--read-percent", "0");

    assertEquals(FEW_OPS, writeCount(cmd));
    assertEquals(0, readCount(cmd));
  }

  /**
   * Even at 100 a thread writes once: a read validates a file the thread wrote,
   * and its first operation has nothing to read back yet.
   */
  @Test
  void readsEveryOperationButTheFirstAtFullPercent() {
    CommandLine cmd = runValidator(1, FEW_OPS, "--read-percent", "100");

    assertEquals(1, writeCount(cmd));
    assertEquals(FEW_OPS - 1, readCount(cmd));
  }

  /**
   * Every read validates, not only the first or the last one: a run whose
   * single write is read back three times fails wherever among the three the
   * content is corrupted.
   */
  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  void everyReadValidatesContent(int corruptedRead) {
    CorruptingLocalFileSystem.corruptFromRead(corruptedRead);

    int exitCode = new Freon().getCmd().execute(
        "-D", "fs.file.impl=" + CorruptingLocalFileSystem.class.getName(),
        "dfsrw",
        "-r", rootPath,
        "-p", "dfsrw-corrupt",
        // the first operation writes, so the other three all read that file
        "-n", "4",
        "-t", "1",
        "-s", "1KB",
        "--buffer", "1024",
        "--copy-buffer", "1024",
        "--read-percent", "100");

    assertNotEquals(0, exitCode,
        "Corrupted content of read " + corruptedRead + " was not detected");
  }

  @Test
  void rejectsPercentOutsideRange() {
    assertThat(execute(new Freon().getCmd(), 1, FEW_OPS, "--read-percent", "-1"))
        .isNotZero();
    assertThat(execute(new Freon().getCmd(), 1, FEW_OPS, "--read-percent", "101"))
        .isNotZero();
  }

  /**
   * Every operation was a read or a write, and the reads are close enough to
   * the requested share of them. The tolerance is six standard deviations of
   * the draw: wide enough that a passing run is not chance, narrow enough to
   * catch a split that ignores the percentage.
   */
  private static void assertSplit(CommandLine cmd, int percent) {
    long reads = readCount(cmd);
    assertEquals(OPS, reads + writeCount(cmd),
        "every operation is either a read or a write");

    double fraction = percent / 100.0;
    long tolerance =
        (long) Math.ceil(6 * Math.sqrt(OPS * fraction * (1 - fraction)));
    assertThat(reads).isCloseTo(Math.round(OPS * fraction), within(tolerance));
  }

  private CommandLine runValidator(int threads, int ops, String... args) {
    CommandLine cmd = new Freon().getCmd();
    assertEquals(0, execute(cmd, threads, ops, args),
        "Freon dfsrw command failed");
    return cmd;
  }

  private int execute(CommandLine cmd, int threads, int ops, String... args) {
    String[] fixed = {
        "dfsrw",
        "-r", rootPath,
        "-p", "dfsrw",
        "-n", String.valueOf(ops),
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
