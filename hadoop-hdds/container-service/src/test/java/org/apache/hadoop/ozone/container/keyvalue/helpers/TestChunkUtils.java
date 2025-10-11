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

package org.apache.hadoop.ozone.container.keyvalue.helpers;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNABLE_TO_FIND_CHUNK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.util.concurrent.Striped;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.keyvalue.impl.MappedBufferManager;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link ChunkUtils}.
 */
class TestChunkUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestChunkUtils.class);

  private static final int BUFFER_CAPACITY = 1 << 20;
  private static final int MAPPED_BUFFER_THRESHOLD = 32 << 10;
  private static final Random RANDOM = new Random();
  private static final MappedBufferManager MAPPED_BUFFER_MANAGER = new MappedBufferManager(100);

  @TempDir
  private File tempDir;

  static ChunkBuffer readData(File file, long off, long len)
      throws StorageContainerException {
    LOG.info("off={}, len={}", off, len);
    return ChunkUtils.readData(len, BUFFER_CAPACITY, file, off, null,
        MAPPED_BUFFER_THRESHOLD, true, MAPPED_BUFFER_MANAGER);
  }

  @Test
  void concurrentReadOfSameFile() throws Exception {
    int threads = 10;
    String s = "Hello World";
    byte[] array = s.getBytes(UTF_8);
    ChunkBuffer data = ChunkBuffer.wrap(ByteBuffer.wrap(array));
    Path tempFile = tempDir.toPath().resolve("concurrent");
    int len = data.limit();
    int offset = 0;
    File file = tempFile.toFile();
    ChunkUtils.writeData(file, data, offset, len, null, true);
    ExecutorService executor = new ThreadPoolExecutor(threads, threads,
        0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    AtomicInteger processed = new AtomicInteger();
    AtomicBoolean failed = new AtomicBoolean();
    for (int i = 0; i < threads; i++) {
      final int threadNumber = i;
      executor.execute(() -> {
        try {
          final ChunkBuffer chunk = readData(file, offset, len);
          // There should be only one element in readBuffers
          final List<ByteBuffer> buffers = chunk.asByteBufferList();
          assertEquals(1, buffers.size());
          final ByteBuffer readBuffer = buffers.get(0);

          int remaining = readBuffer.remaining();
          byte[] readArray = new byte[remaining];
          readBuffer.get(readArray);
          LOG.info("Read data ({}): {}", threadNumber,
              new String(readArray, UTF_8));
          if (!Arrays.equals(array, readArray)) {
            failed.set(true);
          }
          assertEquals(len, remaining);
        } catch (Exception e) {
          LOG.error("Failed to read data ({})", threadNumber, e);
          failed.set(true);
        }
        processed.incrementAndGet();
      });
    }
    try {
      GenericTestUtils.waitFor(() -> processed.get() == threads,
          100, (int) TimeUnit.SECONDS.toMillis(5));
    } finally {
      executor.shutdownNow();
    }
    assertFalse(failed.get());
  }

  @Test
  void concurrentReadWriteOfSameFile() {
    final int threads = 10;
    ChunkUtils.setStripedLock(Striped.readWriteLock(threads));
    final byte[] array = "Hello World".getBytes(UTF_8);

    Path tempFile = tempDir.toPath().resolve("concurrent_read_write");
    File file = tempFile.toFile();
    AtomicInteger success = new AtomicInteger(0);
    AtomicInteger fail = new AtomicInteger(0);

    ExecutorService executor = new ThreadPoolExecutor(10, 10,
        0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    List<CompletableFuture<Void>> futures = new ArrayList<>();

    for (int i = 0; i < threads; i++) {
      final int threadNumber = i;
      final ChunkBuffer data = ChunkBuffer.wrap(ByteBuffer.wrap(array));
      final int len = data.limit();
      final int offset = i * len;

      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        try {
          ChunkUtils.writeData(file, data, offset, len, null, true);
          success.getAndIncrement();
        } catch (StorageContainerException e) {
          throw new RuntimeException(e);
        }
      }, executor).whenCompleteAsync((v, e) -> {
        if (e == null) {
          try {
            final ChunkBuffer chunk = readData(file, offset, len);
            // There should be only one element in readBuffers
            final List<ByteBuffer> buffers = chunk.asByteBufferList();
            assertEquals(1, buffers.size());
            final ByteBuffer readBuffer = buffers.get(0);

            LOG.info("Read data ({}): {}", threadNumber,
                new String(readBuffer.array(), UTF_8));
            if (!Arrays.equals(array, readBuffer.array())) {
              fail.getAndIncrement();
            }
            assertEquals(len, readBuffer.remaining());
          } catch (Exception ee) {
            LOG.error("Failed to read data ({})", threadNumber, ee);
            fail.getAndIncrement();
          }
        } else {
          fail.getAndIncrement();
        }
      }, executor);
      futures.add(future);
    }
    try {
      for (CompletableFuture<Void> future : futures) {
        future.join();
      }
    } finally {
      executor.shutdownNow();
    }
    assertEquals(success.get(), threads);
    assertEquals(fail.get(), 0);
  }

  @Test
  void serialRead() throws IOException {
    String s = "Hello World";
    byte[] array = s.getBytes(UTF_8);
    ChunkBuffer data = ChunkBuffer.wrap(ByteBuffer.wrap(array));
    Path tempFile = tempDir.toPath().resolve("serial");
    File file = tempFile.toFile();
    int len = data.limit();
    int offset = 0;
    ChunkUtils.writeData(file, data, offset, len, null, true);

    final ChunkBuffer chunk = readData(file, offset, len);
    // There should be only one element in readBuffers
    final List<ByteBuffer> buffers = chunk.asByteBufferList();
    assertEquals(1, buffers.size());
    final ByteBuffer readBuffer = buffers.get(0);
    int remain = readBuffer.remaining();
    byte[] readArray = new byte[remain];
    readBuffer.get(readArray);
    assertArrayEquals(array, readArray);
    assertEquals(len, remain);
  }

  @Test
  void validateChunkForOverwrite() throws IOException {

    Path tempFile = tempDir.toPath().resolve("overwrite");
    FileUtils.write(tempFile.toFile(), "test", UTF_8);

    assertTrue(
        ChunkUtils.validateChunkForOverwrite(tempFile.toFile(),
            new ChunkInfo("chunk", 3, 5)));

    assertFalse(
        ChunkUtils.validateChunkForOverwrite(tempFile.toFile(),
            new ChunkInfo("chunk", 5, 5)));

    try (FileChannel fileChannel =
             FileChannel.open(tempFile, StandardOpenOption.READ)) {
      assertTrue(
          ChunkUtils.validateChunkForOverwrite(fileChannel,
              new ChunkInfo("chunk", 3, 5)));

      assertFalse(
          ChunkUtils.validateChunkForOverwrite(fileChannel,
              new ChunkInfo("chunk", 5, 5)));
    }
  }

  @Test
  void readMissingFile() {
    // given
    int len = 123;
    int offset = 0;
    File nonExistentFile = new File("nosuchfile");

    // when
    StorageContainerException e = assertThrows(
        StorageContainerException.class,
        () -> readData(nonExistentFile, offset, len));

    // then
    assertEquals(UNABLE_TO_FIND_CHUNK, e.getResult());
  }

  @Test
  void testReadData() throws Exception {
    final File dir = new File(tempDir, "testReadData");
    try {
      assertTrue(dir.mkdirs());

      // large file
      final int large = 10 << 20; // 10MB
      assertThat(large).isGreaterThan(MAPPED_BUFFER_THRESHOLD);
      runTestReadFile(large, dir);

      // small file
      final int small = 30 << 10; // 30KB
      assertThat(small).isLessThanOrEqualTo(MAPPED_BUFFER_THRESHOLD);
      runTestReadFile(small, dir);

      // boundary case
      runTestReadFile(MAPPED_BUFFER_THRESHOLD, dir);

      // empty file
      runTestReadFile(0, dir);

      for (int i = 0; i < 10; i++) {
        final int length = RANDOM.nextInt(2 * MAPPED_BUFFER_THRESHOLD) + 1;
        runTestReadFile(length, dir);
      }
    } finally {
      FileUtils.deleteDirectory(dir);
    }
  }

  void runTestReadFile(int length, File dir)
      throws Exception {
    final File file;
    for (int i = length; ; i++) {
      final File f = new File(dir, "file_" + i);
      if (!f.exists()) {
        file = f;
        break;
      }
    }
    LOG.info("file: {}", file);

    // write a file
    final byte[] array = new byte[BUFFER_CAPACITY];
    final long seed = System.nanoTime();
    LOG.info("seed: {}", seed);
    RANDOM.setSeed(seed);
    try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(
        file.toPath(), StandardOpenOption.CREATE_NEW))) {
      for (int written = 0; written < length;) {
        RANDOM.nextBytes(array);
        final int remaining = length - written;
        final int toWrite = Math.min(remaining, array.length);
        out.write(array, 0, toWrite);
        written += toWrite;
      }
    }
    assertEquals(length, file.length());

    // read the file back
    final ChunkBuffer chunk = readData(file, 0, length);
    assertEquals(length, chunk.remaining());

    final List<ByteBuffer> buffers = chunk.asByteBufferList();
    LOG.info("buffers.size(): {}", buffers.size());
    assertEquals((length - 1) / BUFFER_CAPACITY + 1, buffers.size());
    LOG.info("buffer class: {}", buffers.get(0).getClass());

    RANDOM.setSeed(seed);
    for (ByteBuffer b : buffers) {
      RANDOM.nextBytes(array);
      assertEquals(ByteBuffer.wrap(array, 0, b.remaining()), b);
    }
  }
}
