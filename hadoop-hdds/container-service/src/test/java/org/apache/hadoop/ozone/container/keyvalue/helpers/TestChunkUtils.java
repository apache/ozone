/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.keyvalue.helpers;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.ozone.test.GenericTestUtils;

import org.apache.commons.io.FileUtils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNABLE_TO_FIND_CHUNK;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link ChunkUtils}.
 */
public class TestChunkUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestChunkUtils.class);

  private static final String PREFIX = TestChunkUtils.class.getSimpleName();
  private static final int BUFFER_CAPACITY = 1 << 20;
  private static final int MAPPED_BUFFER_THRESHOLD = 32 << 10;
  private static final Random RANDOM = new Random();

  static ChunkBuffer readData(File file, long off, long len)
      throws StorageContainerException {
    LOG.info("off={}, len={}", off, len);
    return ChunkUtils.readData(len, BUFFER_CAPACITY, file, off, null,
        MAPPED_BUFFER_THRESHOLD);
  }

  @Test
  public void concurrentReadOfSameFile() throws Exception {
    String s = "Hello World";
    byte[] array = s.getBytes(UTF_8);
    ChunkBuffer data = ChunkBuffer.wrap(ByteBuffer.wrap(array));
    Path tempFile = Files.createTempFile(PREFIX, "concurrent");
    try {
      int len = data.limit();
      int offset = 0;
      File file = tempFile.toFile();
      ChunkUtils.writeData(file, data, offset, len, null, true);
      int threads = 10;
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
            Assertions.assertEquals(1, buffers.size());
            final ByteBuffer readBuffer = buffers.get(0);

            LOG.info("Read data ({}): {}", threadNumber,
                new String(readBuffer.array(), UTF_8));
            if (!Arrays.equals(array, readBuffer.array())) {
              failed.set(true);
            }
            assertEquals(len, readBuffer.remaining());
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
    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void concurrentProcessing() throws Exception {
    final int perThreadWait = 1000;
    final int maxTotalWait = 5000;
    int threads = 20;
    List<Path> paths = new LinkedList<>();

    try {
      ExecutorService executor = new ThreadPoolExecutor(threads, threads,
          0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
      AtomicInteger processed = new AtomicInteger();
      for (int i = 0; i < threads; i++) {
        Path path = Files.createTempFile(PREFIX, String.valueOf(i));
        paths.add(path);
        executor.execute(() -> {
          try {
            ChunkUtils.processFileExclusively(path, () -> {
              try {
                Thread.sleep(perThreadWait);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              processed.incrementAndGet();
              return null;
            });
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        });
      }
      try {
        GenericTestUtils.waitFor(() -> processed.get() == threads,
            100, maxTotalWait);
      } finally {
        executor.shutdownNow();
      }
    } finally {
      for (Path path : paths) {
        FileUtils.deleteQuietly(path.toFile());
      }
    }
  }

  @Test
  public void serialRead() throws Exception {
    String s = "Hello World";
    byte[] array = s.getBytes(UTF_8);
    ChunkBuffer data = ChunkBuffer.wrap(ByteBuffer.wrap(array));
    Path tempFile = Files.createTempFile(PREFIX, "serial");
    try {
      File file = tempFile.toFile();
      int len = data.limit();
      int offset = 0;
      ChunkUtils.writeData(file, data, offset, len, null, true);

      final ChunkBuffer chunk = readData(file, offset, len);
      // There should be only one element in readBuffers
      final List<ByteBuffer> buffers = chunk.asByteBufferList();
      Assertions.assertEquals(1, buffers.size());
      final ByteBuffer readBuffer = buffers.get(0);

      assertArrayEquals(array, readBuffer.array());
      assertEquals(len, readBuffer.remaining());
    } catch (Exception e) {
      LOG.error("Failed to read data", e);
    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void validateChunkForOverwrite() throws IOException {

    Path tempFile = Files.createTempFile(PREFIX, "overwrite");
    FileUtils.write(tempFile.toFile(), "test", UTF_8);

    Assertions.assertTrue(
        ChunkUtils.validateChunkForOverwrite(tempFile.toFile(),
            new ChunkInfo("chunk", 3, 5)));

    Assertions.assertFalse(
        ChunkUtils.validateChunkForOverwrite(tempFile.toFile(),
            new ChunkInfo("chunk", 5, 5)));

    try (FileChannel fileChannel =
             FileChannel.open(tempFile, StandardOpenOption.READ)) {
      Assertions.assertTrue(
          ChunkUtils.validateChunkForOverwrite(fileChannel,
              new ChunkInfo("chunk", 3, 5)));

      Assertions.assertFalse(
          ChunkUtils.validateChunkForOverwrite(fileChannel,
              new ChunkInfo("chunk", 5, 5)));
    }
  }

  @Test
  public void readMissingFile() {
    // given
    int len = 123;
    int offset = 0;
    File nonExistentFile = new File("nosuchfile");

    // when
    StorageContainerException e = assertThrows(
        StorageContainerException.class,
        () -> readData(nonExistentFile, offset, len));

    // then
    Assertions.assertEquals(UNABLE_TO_FIND_CHUNK, e.getResult());
  }

  @Test
  public void testReadData() throws Exception {
    final File dir = GenericTestUtils.getTestDir("testReadData");
    try {
      Assertions.assertTrue(dir.mkdirs());

      // large file
      final int large = 10 << 20; // 10MB
      Assertions.assertTrue(large > MAPPED_BUFFER_THRESHOLD);
      runTestReadFile(large, dir, true);

      // small file
      final int small = 30 << 10; // 30KB
      Assertions.assertTrue(small <= MAPPED_BUFFER_THRESHOLD);
      runTestReadFile(small, dir, false);

      // boundary case
      runTestReadFile(MAPPED_BUFFER_THRESHOLD, dir, false);

      // empty file
      runTestReadFile(0, dir, false);

      for (int i = 0; i < 10; i++) {
        final int length = RANDOM.nextInt(2 * MAPPED_BUFFER_THRESHOLD) + 1;
        runTestReadFile(length, dir, length > MAPPED_BUFFER_THRESHOLD);
      }
    } finally {
      FileUtils.deleteDirectory(dir);
    }
  }

  void runTestReadFile(int length, File dir, boolean isMapped)
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
    Assertions.assertEquals(length, file.length());

    // read the file back
    final ChunkBuffer chunk = readData(file, 0, length);
    Assertions.assertEquals(length, chunk.remaining());

    final List<ByteBuffer> buffers = chunk.asByteBufferList();
    LOG.info("buffers.size(): {}", buffers.size());
    Assertions.assertEquals((length - 1) / BUFFER_CAPACITY + 1, buffers.size());
    LOG.info("buffer class: {}", buffers.get(0).getClass());

    RANDOM.setSeed(seed);
    for (ByteBuffer b : buffers) {
      Assertions.assertEquals(isMapped, b instanceof MappedByteBuffer);
      RANDOM.nextBytes(array);
      Assertions.assertEquals(ByteBuffer.wrap(array, 0, b.remaining()), b);
    }
  }
}
