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
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
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

  @Option(names = {"--max-tracked-files"},
      description = "Maximum number of files a thread keeps the checksum of, and can therefore read back. Once a "
          + "thread writes more distinct files than this, validation covers a moving sample of its writes; the "
          + "files dropped from the sample stay on the file system.",
      defaultValue = "10000")
  private int maxTrackedFiles;

  private ContentGenerator contentGenerator;

  private Timer writeTimer;

  private Timer readTimer;

  private final ThreadLocal<ThreadHistory> threadHistory =
      ThreadLocal.withInitial(() -> new ThreadHistory(maxTrackedFiles, getThreadSequenceId()));

  @Override
  public Void call() throws Exception {
    super.init();

    if (fileSize.toBytes() < Long.BYTES) {
      throw new IllegalArgumentException(
          "--size must be at least " + Long.BYTES + " bytes");
    }
    if (bufferSize <= 0 || copyBufferSize <= 0) {
      throw new IllegalArgumentException(
          "--buffer and --copy-buffer must be positive");
    }
    if (maxTrackedFiles <= 0) {
      throw new IllegalArgumentException("--max-tracked-files must be positive");
    }

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
    Path file = objectPath(counter);
    long marker = history.nextMarker();

    long checksum = writeTimer.time(() -> writeFile(file, marker));
    history.record(file, checksum);

    Path target = history.randomPath();
    long expected = history.checksumOf(target);
    long actual = readTimer.time(() -> readChecksum(target));

    if (expected != actual) {
      throw new IllegalStateException(
          "Checksum of read data doesn't match the written data for " + target);
    }
  }

  /**
   * Path of the file for the given counter. The thread sequence id is part of
   * the path so each worker owns a private namespace; in --duration runs paths
   * are reused, and this keeps one thread from overwriting a file another
   * thread is reading back.
   */
  private Path objectPath(long counter) {
    return new Path(getRootPath() + "/" + generateObjectName(counter)
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

  private long readChecksum(Path file) throws IOException {
    Checksum checksum = new CRC32();
    try (FSDataInputStream input = getFileSystem().open(file)) {
      IOUtils.copyLarge(new CheckedInputStream(input, checksum),
          NullOutputStream.INSTANCE, new byte[copyBufferSize]);
    }
    return checksum.getValue();
  }

  /**
   * Per-thread record of the files written and the latest checksum of each.
   * Paths are reused across a run, so it is keyed by path (an overwrite updates
   * the checksum) and stays naturally bounded, with --max-tracked-files as the
   * hard cap for very large runs.
   */
  private static final class ThreadHistory {
    private final int maxTracked;
    private final long markerBase;
    private int markerSeq;
    private final Map<Path, Long> checksums = new HashMap<>();
    private final List<Path> paths = new ArrayList<>();

    private ThreadHistory(int maxTracked, long threadSequenceId) {
      this.maxTracked = maxTracked;
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

    private void record(Path path, long checksum) {
      if (checksums.put(path, checksum) != null) {
        return;                        // existing path, checksum just updated
      }
      if (paths.size() < maxTracked) {
        paths.add(path);
      } else {
        // Bound memory by dropping a random tracked path (file stays on FS).
        int idx = ThreadLocalRandom.current().nextInt(paths.size());
        checksums.remove(paths.set(idx, path));
      }
    }

    private Path randomPath() {
      return paths.get(ThreadLocalRandom.current().nextInt(paths.size()));
    }

    private long checksumOf(Path path) {
      return checksums.get(path);
    }
  }
}
