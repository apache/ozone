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
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.StorageSize;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Freon generator that writes files and reads them back with data validation.
 * <p>
 * Each worker thread writes files and gives every write a distinct content
 * marker, so re-reading a path validates against its most recent write. The
 * thread keeps the latest hash of every path it wrote, then reads back a random
 * one and verifies the hash still matches. This detects both data corruption
 * and stale reads (an overwritten path returning older bytes) under concurrent
 * load, including in time-based (--duration) runs where paths are reused.
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

  /**
   * Upper bound on the number of paths a thread tracks for read-back. It caps
   * memory use for large runs; the untracked files still exist on the FS.
   */
  private static final int MAX_HISTORY_PER_THREAD = 1000;

  /**
   * Digest algorithm used for the write-side hash. Must match the algorithm
   * used by {@link #getDigest} so the read-back validation compares equivalent
   * digests.
   */
  private static final String DIGEST_ALGORITHM = "MD5";

  @Option(names = {"-s", "--size"},
      description = "Size of the generated files. " +
          StorageSizeConverter.STORAGE_SIZE_DESCRIPTION,
      defaultValue = "10KB",
      converter = StorageSizeConverter.class)
  private StorageSize fileSize;

  @Option(names = {"--buffer"},
      description = "Size of buffer used to generate the file content.",
      defaultValue = "10240")
  private int bufferSize;

  @Option(names = {"--copy-buffer"},
      description = "Size of bytes written to the output in one operation.",
      defaultValue = "4096")
  private int copyBufferSize;

  private ContentGenerator contentGenerator;

  private Timer writeTimer;

  private Timer readTimer;

  private final ThreadLocal<ThreadHistory> threadHistory =
      ThreadLocal.withInitial(ThreadHistory::new);

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

    FileSystem fileSystem = getFileSystem();
    try {
      Path file = new Path(getRootPath() + "/" + generateObjectName(0));
      fileSystem.mkdirs(file.getParent());

      // Reserve space for the per-file marker so each file is exactly --size.
      contentGenerator = new ContentGenerator(
          fileSize.toBytes() - Long.BYTES, bufferSize, copyBufferSize);

      writeTimer = getMetrics().timer("file-write");
      readTimer = getMetrics().timer("file-read");

      runTests(this::writeAndValidate);
    } finally {
      IOUtils.closeQuietly(fileSystem);
    }

    return null;
  }

  private void writeAndValidate(long counter) throws Exception {
    ThreadHistory history = threadHistory.get();
    Path file = objectPath(counter);
    long marker = history.nextMarker();

    byte[] digest = writeTimer.time(() -> writeFile(file, marker));
    history.record(file, digest);

    Path target = history.randomPath();
    byte[] expected = history.digestOf(target);
    byte[] actualDigest = readTimer.time(() -> {
      try (FSDataInputStream input = getFileSystem().open(target)) {
        return getDigest(input);
      }
    });

    if (!MessageDigest.isEqual(expected, actualDigest)) {
      throw new IllegalStateException(
          "Message digest of read data doesn't match the written data for "
              + target);
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
   * Write a file streaming to the filesystem and return the digest of its
   * content, computed on the fly so large files are never held in memory. A
   * per-write marker makes each write's content, and therefore its hash,
   * distinct, so a re-read of an overwritten path can be validated.
   */
  private byte[] writeFile(Path file, long marker) throws IOException {
    MessageDigest digest = newDigest();
    try (DigestOutputStream output =
        new DigestOutputStream(getFileSystem().create(file), digest)) {
      output.write(ByteBuffer.allocate(Long.BYTES).putLong(marker).array());
      contentGenerator.write(output);
    }
    return digest.digest();
  }

  private static MessageDigest newDigest() {
    try {
      return MessageDigest.getInstance(DIGEST_ALGORITHM);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(
          "Unsupported digest algorithm: " + DIGEST_ALGORITHM, e);
    }
  }

  /**
   * Per-thread record of the files written and the latest digest of each. Paths
   * are reused across a run, so it is keyed by path (an overwrite updates the
   * digest) and stays naturally bounded, with a hard cap for very large runs.
   */
  private static final class ThreadHistory {
    private long markerSeq;
    private final Map<Path, byte[]> digests = new HashMap<>();
    private final List<Path> paths = new ArrayList<>();

    private long nextMarker() {
      return markerSeq++;
    }

    private void record(Path path, byte[] digest) {
      if (digests.put(path, digest) != null) {
        return;                          // existing path, digest just updated
      }
      if (paths.size() < MAX_HISTORY_PER_THREAD) {
        paths.add(path);
      } else {
        // Bound memory by dropping a random tracked path (file stays on FS).
        int idx = ThreadLocalRandom.current().nextInt(paths.size());
        digests.remove(paths.set(idx, path));
      }
    }

    private Path randomPath() {
      return paths.get(ThreadLocalRandom.current().nextInt(paths.size()));
    }

    private byte[] digestOf(Path path) {
      return digests.get(path);
    }
  }
}
