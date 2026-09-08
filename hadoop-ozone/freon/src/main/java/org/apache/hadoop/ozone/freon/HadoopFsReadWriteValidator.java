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

import com.codahale.metrics.Timer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.StorageSize;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Freon generator that writes files and reads them back with data validation.
 * <p>
 * Each worker thread writes files and gives every write a distinct content
 * marker, so re-reading a path validates against its most recent write. The
 * thread keeps the latest CRC32 of every path it wrote, then reads back a
 * random one and verifies the checksum still matches. This detects both data
 * corruption and stale reads (an overwritten path returning older bytes) under
 * concurrent load, including in time-based (--duration) runs where paths are
 * reused.
 * <p>
 * --read-write-ratio tunes the mix: a task still writes a single file, but
 * reads back as many as the ratio asks for, so a run can be made read-heavy or
 * write-heavy without changing what any one read validates.
 * <p>
 * CRC32 keeps the validation off the critical path of the measured throughput.
 * Successive writes of a path differ in the marker only, and markers less than
 * 2^32 apart never share a CRC32, so a stale read is always detected.
 */
@Command(name = "dfsrw",
    aliases = "dfs-read-write-validator",
    description = "Write files and read them back with data validation on any "
        + "dfs compatible file system.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
public class HadoopFsReadWriteValidator extends HadoopBaseFreonGenerator
    implements Callable<Void> {

  @Option(names = {"-s", "--size"},
      description = "Size of the generated files. " +
          StorageSizeConverter.STORAGE_SIZE_DESCRIPTION,
      defaultValue = "16KB",
      converter = StorageSizeConverter.class)
  private StorageSize fileSize;

  @Option(names = {"--buffer"},
      description = "Size of buffer used to generate the file content.",
      defaultValue = "16384")
  private int bufferSize;

  @Option(names = {"--copy-buffer"},
      description = "Size of bytes written to or read from the file in one operation.",
      defaultValue = "16384")
  private int copyBufferSize;

  @Option(names = {"--max-files-per-thread"},
      description = "Maximum number of distinct files a thread writes. On reaching it the thread wraps around and "
          + "overwrites the files it already wrote, which bounds the memory it needs to keep a checksum for every "
          + "file it leaves behind. The number of writes and reads is unaffected; raise this to trade memory for "
          + "more distinct files, at the cost of overwriting fewer of them.",
      defaultValue = "10000")
  private int maxFilesPerThread;

  @Option(names = {"--read-write-ratio"},
      description = "Number of reads issued per write. 1 pairs one read with every write, 4 makes the run "
          + "read-heavy with four read-backs of each write, and 0.25 makes it write-heavy with one read-back "
          + "every fourth write. A ratio that is not a whole number is spread over the writes of a thread "
          + "rather than rounded on every one of them.",
      defaultValue = "1.0")
  private double readWriteRatio;

  private ContentGenerator contentGenerator;

  private Timer writeTimer;

  private Timer readTimer;

  private final ThreadLocal<ThreadHistory> threadHistory =
      ThreadLocal.withInitial(() -> new ThreadHistory(getThreadSequenceId()));

  @Override
  public Void call() throws Exception {
    // before init(), which already starts the HTTP server and the progress bar
    if (fileSize.toBytes() < Long.BYTES) {
      throw new IllegalArgumentException(
          "--size must be at least " + Long.BYTES + " bytes");
    }
    if (bufferSize <= 0 || copyBufferSize <= 0) {
      throw new IllegalArgumentException(
          "--buffer and --copy-buffer must be positive");
    }
    if (maxFilesPerThread <= 0) {
      throw new IllegalArgumentException(
          "--max-files-per-thread must be positive");
    }
    // NaN fails the first test, and an infinite ratio would make a single task
    // read forever
    if (!(readWriteRatio > 0) || Double.isInfinite(readWriteRatio)) {
      throw new IllegalArgumentException(
          "--read-write-ratio must be a positive finite number");
    }

    super.init();

    FileSystem fileSystem = getFileSystem();
    try {
      Path file = new Path(getRootPath() + "/" + generateObjectName(0));
      fileSystem.mkdirs(file.getParent());

      // Reserve space for the per-file marker so each file is exactly --size.
      contentGenerator = new ContentGenerator(
          fileSize.toBytes() - Long.BYTES, bufferSize, copyBufferSize);

      // not "file-read": dfsv uses that name for a read without the digest
      writeTimer = getMetrics().timer("file-write");
      readTimer = getMetrics().timer("file-read-validate");

      runTests(this::writeAndValidate);
    } finally {
      org.apache.hadoop.hdds.utils.IOUtils.closeQuietly(fileSystem);
    }

    return null;
  }

  private void writeAndValidate(long counter) throws Exception {
    ThreadHistory history = threadHistory.get();
    long fileId = counter % maxFilesPerThread;
    Path file = objectPath(fileId);
    long marker = history.nextMarker();

    long checksum;
    try {
      checksum = writeTimer.time(() -> writeFile(file, marker));
    } catch (Exception e) {
      // create() has already truncated the file, so whatever checksum the path
      // had no longer describes it. Keeping it would report the next read of
      // this path as corruption, which --fail-at-end would surface.
      history.forget(fileId);
      throw e;
    }
    history.record(fileId, checksum);

    for (int reads = history.readsDue(readWriteRatio); reads > 0; reads--) {
      validateRandomFile(history);
    }
  }

  /**
   * Read back one of the files this thread wrote, picked at random, and verify
   * that its content still matches the checksum of the latest write of that
   * path.
   */
  private void validateRandomFile(ThreadHistory history) throws Exception {
    long readId = history.randomFileId();
    Path target = objectPath(readId);
    long expected = history.checksumOf(readId);
    long actual = readTimer.time(() -> readChecksum(target));

    if (expected != actual) {
      throw new IllegalStateException(
          "Checksum of read data doesn't match the written data for " + target
              + ", expected " + expected + ", actual " + actual);
    }
  }

  /**
   * Path of the file for the given counter. The thread sequence id is part of
   * the path so each worker owns a private namespace; paths are reused once a
   * thread has written --max-files-per-thread of them, and this keeps one
   * thread from overwriting a file another thread is reading back.
   */
  private Path objectPath(long fileId) {
    return new Path(getRootPath() + "/" + generateObjectName(fileId)
        + "-t" + getThreadSequenceId());
  }

  /**
   * Write a file streaming to the filesystem and return the checksum of its
   * content, computed on the fly so large files are never held in memory. A
   * per-write marker makes each write's content, and therefore its checksum,
   * distinct, so a re-read of an overwritten path can be validated.
   */
  private long writeFile(Path file, long marker) throws IOException {
    Checksum checksum = new CRC32();
    try (CheckedOutputStream output =
        new CheckedOutputStream(getFileSystem().create(file), checksum)) {
      output.write(ByteBuffer.allocate(Long.BYTES).putLong(marker).array());
      contentGenerator.write(output);
    }
    return checksum.getValue();
  }

  /**
   * Read the file back in --copy-buffer sized chunks and return the checksum of
   * its content.
   */
  private long readChecksum(Path file) throws IOException {
    Checksum checksum = new CRC32();
    byte[] buffer = new byte[copyBufferSize];
    try (FSDataInputStream input = getFileSystem().open(file)) {
      int read;
      while ((read = input.read(buffer)) != -1) {
        checksum.update(buffer, 0, read);
      }
    }
    return checksum.getValue();
  }

  /**
   * Per-thread record of the files written and the latest checksum of each. It
   * is keyed by file id (an overwrite updates the checksum), so it holds one
   * entry per file the thread wrote and never more than
   * --max-files-per-thread. The id is kept rather than the {@link Path} it maps
   * to, which {@link #objectPath} rebuilds on demand. It also carries the
   * thread's share of the read/write ratio, see {@link #readsDue(double)}.
   */
  private static final class ThreadHistory {
    private final long markerBase;
    private int markerSeq;
    private final Map<Long, Long> checksums = new HashMap<>();
    private final List<Long> fileIds = new ArrayList<>();
    private double readCredit;

    private ThreadHistory(long threadSequenceId) {
      this.markerBase = threadSequenceId << Integer.SIZE;
    }

    /**
     * Marker for the next write of this thread. The thread sequence id occupies
     * the high half so that no two threads ever write the same content, while
     * successive writes of one thread differ in the low half only, which is
     * what keeps their CRC32 distinct.
     */
    private long nextMarker() {
      return markerBase | Integer.toUnsignedLong(markerSeq++);
    }

    private void record(long fileId, long checksum) {
      Long key = fileId;
      if (checksums.put(key, checksum) == null) {
        fileIds.add(key);
      }
    }

    /** Drop a file whose content is no longer known. */
    private void forget(long fileId) {
      Long key = fileId;
      if (checksums.remove(key) != null) {
        fileIds.remove(key);           // error path, so the scan is affordable
      }
    }

    private long randomFileId() {
      return fileIds.get(ThreadLocalRandom.current().nextInt(fileIds.size()));
    }

    private long checksumOf(long fileId) {
      return checksums.get(fileId);
    }

    /**
     * How many files to read back after the write that just completed. The
     * fraction a ratio like 2.5 leaves over is carried to the next write
     * instead of being rounded away, so the thread issues the requested number
     * of reads per write on average, and a ratio below 1 reads back every
     * n-th write rather than never reading at all.
     */
    private int readsDue(double readsPerWrite) {
      readCredit += readsPerWrite;
      int reads = (int) readCredit;
      readCredit -= reads;
      return reads;
    }
  }
}
