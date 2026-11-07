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

package org.apache.hadoop.ozone.client.io;

import static org.apache.hadoop.ozone.client.io.ECStreamTestUtil.generateParity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.IntConsumer;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test for the ECBlockReconstructedInputStream class.
 */
public class TestECBlockReconstructedInputStream {

  private ECReplicationConfig repConfig;
  private ECStreamTestUtil.TestBlockInputStreamFactory streamFactory;
  private long randomSeed;
  private ThreadLocalRandom random = ThreadLocalRandom.current();
  private SplittableRandom dataGenerator;
  private ByteBufferPool bufferPool = new ElasticByteBufferPool();
  private ExecutorService ecReconstructExecutor =
      Executors.newFixedThreadPool(3);
  private OzoneConfiguration conf = new OzoneConfiguration();

  @BeforeEach
  public void setup() throws IOException {
    repConfig = new ECReplicationConfig(3, 2);
    streamFactory = new ECStreamTestUtil.TestBlockInputStreamFactory();

    randomSeed = random.nextLong();
    dataGenerator = new SplittableRandom(randomSeed);
  }

  @AfterEach
  public void teardown() {
    ecReconstructExecutor.shutdownNow();
  }

  private ECBlockReconstructedStripeInputStream createStripeInputStream(
      Map<DatanodeDetails, Integer> dnMap, long blockLength) {
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);
    streamFactory.setCurrentPipeline(keyInfo.getPipeline());
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    return new ECBlockReconstructedStripeInputStream(repConfig, keyInfo,
        null, null, streamFactory, bufferPool, ecReconstructExecutor,
        clientConfig);
  }

  @Test
  public void testBlockLengthReturned() throws IOException {
    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(4, 5);
    try (ECBlockReconstructedStripeInputStream stripeStream
        = createStripeInputStream(dnMap, 12345L)) {
      try (ECBlockReconstructedInputStream stream =
          new ECBlockReconstructedInputStream(repConfig, bufferPool,
              stripeStream)) {
        assertEquals(12345L, stream.getLength());
      }
    }
  }

  @Test
  public void testBlockIDReturned() throws IOException {
    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 4, 5);
    try (ECBlockReconstructedStripeInputStream stripeStream
        = createStripeInputStream(dnMap, 12345L)) {
      try (ECBlockReconstructedInputStream stream =
          new ECBlockReconstructedInputStream(repConfig, bufferPool,
              stripeStream)) {
        assertEquals(new BlockID(1, 1), stream.getBlockID());
      }
    }
  }

  @Test
  public void testReadDataByteBufferMultipleStripes() throws IOException {
    int readBufferSize = 4096;
    // 3 stripes and a partial chunk
    int blockLength = repConfig.getEcChunkSize() * repConfig.getData() * 3
        + repConfig.getEcChunkSize() - 1;
    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(),
        repConfig.getEcChunkSize() * 4);
    ECStreamTestUtil.randomFill(dataBufs, repConfig.getEcChunkSize(),
        dataGenerator, blockLength);
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);
    addDataStreamsToFactory(dataBufs, parity);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 4, 5);
    try (ECBlockReconstructedStripeInputStream stripeStream
        = createStripeInputStream(dnMap, blockLength)) {
      try (ECBlockReconstructedInputStream stream =
          new ECBlockReconstructedInputStream(repConfig, bufferPool,
              stripeStream)) {
        ByteBuffer b = ByteBuffer.allocate(readBufferSize);
        int totalRead = 0;
        dataGenerator = new SplittableRandom(randomSeed);
        while (totalRead < blockLength) {
          int expectedRead = Math.min(blockLength - totalRead, readBufferSize);
          long read = stream.read(b);
          totalRead += read;
          assertEquals(expectedRead, read);
          ECStreamTestUtil.assertBufferMatches(b, dataGenerator);
          b.clear();
        }
        // Next read should be EOF
        b.clear();
        long read = stream.read(b);
        assertEquals(-1, read);
        // Seek back to zero and read again to ensure the buffers are
        // re-allocated after being freed at the end of block.
        stream.seek(0);
        read = stream.read(b);
        assertEquals(readBufferSize, read);
        dataGenerator = new SplittableRandom(randomSeed);
        ECStreamTestUtil.assertBufferMatches(b, dataGenerator);
      }
    }
  }

  @Test
  public void testReadDataWithUnbuffer() throws IOException {
    // Read buffer is 16kb + 5 bytes so it does not align with stripes exactly
    int readBufferSize = 1024 * 16 + 5;
    // 3 stripes and a partial chunk
    int blockLength = repConfig.getEcChunkSize() * repConfig.getData() * 3
        + repConfig.getEcChunkSize() - 1;
    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(),
        repConfig.getEcChunkSize() * 4);
    ECStreamTestUtil.randomFill(dataBufs, repConfig.getEcChunkSize(),
        dataGenerator, blockLength);
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);
    addDataStreamsToFactory(dataBufs, parity);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 4, 5);
    try (ECBlockReconstructedStripeInputStream stripeStream
             = createStripeInputStream(dnMap, blockLength)) {
      try (ECBlockReconstructedInputStream stream =
               new ECBlockReconstructedInputStream(repConfig, bufferPool,
                   stripeStream)) {
        ByteBuffer b = ByteBuffer.allocate(readBufferSize);
        int totalRead = 0;
        dataGenerator = new SplittableRandom(randomSeed);
        while (totalRead < blockLength) {
          int expectedRead = Math.min(blockLength - totalRead, readBufferSize);
          long read = stream.read(b);
          totalRead += read;
          assertEquals(expectedRead, read);
          ECStreamTestUtil.assertBufferMatches(b, dataGenerator);
          b.clear();
          stream.unbuffer();
        }
        // Next read should be EOF
        b.clear();
        long read = stream.read(b);
        assertEquals(-1, read);
      }
    }
  }

  @Test
  public void testReadDataByteBufferUnderBufferSize() throws IOException {
    int readBufferSize = 4096;
    // Small block with less data that the read size
    int blockLength = 1024;

    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(), 1024);
    ECStreamTestUtil.randomFill(dataBufs, repConfig.getEcChunkSize(),
        dataGenerator, blockLength);
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);
    addDataStreamsToFactory(dataBufs, parity);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 4, 5);
    try (ECBlockReconstructedStripeInputStream stripeStream
        = createStripeInputStream(dnMap, blockLength)) {
      try (ECBlockReconstructedInputStream stream =
          new ECBlockReconstructedInputStream(repConfig, bufferPool,
              stripeStream)) {
        ByteBuffer b = ByteBuffer.allocate(readBufferSize);
        dataGenerator = new SplittableRandom(randomSeed);
        long read = stream.read(b);
        assertEquals(blockLength, read);
        ECStreamTestUtil.assertBufferMatches(b, dataGenerator);
        b.clear();
        // Next read should be EOF
        read = stream.read(b);
        assertEquals(-1, read);
      }
    }
  }

  @Test
  public void testReadByteAtATime() throws IOException {
    // 3 stripes and a partial chunk
    int blockLength = repConfig.getEcChunkSize() * repConfig.getData() * 3
        + repConfig.getEcChunkSize() - 1;
    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(),
        repConfig.getEcChunkSize() * 4);
    ECStreamTestUtil.randomFill(dataBufs, repConfig.getEcChunkSize(),
        dataGenerator, blockLength);
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);
    addDataStreamsToFactory(dataBufs, parity);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 4, 5);
    try (ECBlockReconstructedStripeInputStream stripeStream
        = createStripeInputStream(dnMap, blockLength)) {
      try (ECBlockReconstructedInputStream stream =
          new ECBlockReconstructedInputStream(repConfig, bufferPool,
              stripeStream)) {

        dataGenerator = new SplittableRandom(randomSeed);
        int totalRead = 0;
        while (true) {
          int val = stream.read();
          if (val == -1) {
            break;
          }
          assertEquals(dataGenerator.nextInt(255), val);
          totalRead += 1;
        }
        assertEquals(blockLength, totalRead);
      }
    }
  }

  @Test
  public void testReadByteBuffer() throws IOException {
    // 3 stripes and a partial chunk
    int blockLength = repConfig.getEcChunkSize() * repConfig.getData() * 3
        + repConfig.getEcChunkSize() - 1;
    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(),
        repConfig.getEcChunkSize() * 4);
    ECStreamTestUtil.randomFill(dataBufs, repConfig.getEcChunkSize(),
        dataGenerator, blockLength);
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);
    addDataStreamsToFactory(dataBufs, parity);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 4, 5);
    byte[] buf = new byte[1024];
    try (ECBlockReconstructedStripeInputStream stripeStream
        = createStripeInputStream(dnMap, blockLength)) {
      try (ECBlockReconstructedInputStream stream =
          new ECBlockReconstructedInputStream(repConfig, bufferPool,
              stripeStream)) {
        int totalRead = 0;
        dataGenerator = new SplittableRandom(randomSeed);
        while (totalRead < blockLength) {
          int expectedRead = Math.min(blockLength - totalRead, 1024);
          long read = stream.read(buf, 0, buf.length);
          totalRead += read;
          assertEquals(expectedRead, read);
          ECStreamTestUtil.assertBufferMatches(
              ByteBuffer.wrap(buf, 0, (int)read), dataGenerator);
        }
        // Next read should be EOF
        long read = stream.read(buf, 0, buf.length);
        assertEquals(-1, read);
      }
    }
  }

  @Test
  public void testSeek() throws IOException {
    int readBufferSize = repConfig.getEcChunkSize() + 1024;
    // 3 stripes and a partial chunk
    int blockLength = repConfig.getEcChunkSize() * repConfig.getData() * 3
        + repConfig.getEcChunkSize() - 1;
    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(),
        repConfig.getEcChunkSize() * 4);
    ECStreamTestUtil.randomFill(dataBufs, repConfig.getEcChunkSize(),
        dataGenerator, blockLength);
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);
    addDataStreamsToFactory(dataBufs, parity);

    Map<DatanodeDetails, Integer> dnMap
        = ECStreamTestUtil.createIndexMap(1, 2, 4, 5);
    try (ECBlockReconstructedStripeInputStream stripeStream
        = createStripeInputStream(dnMap, blockLength)) {
      try (ECBlockReconstructedInputStream stream =
          new ECBlockReconstructedInputStream(repConfig, bufferPool,
              stripeStream)) {
        ByteBuffer b = ByteBuffer.allocate(readBufferSize);

        int seekPosition = 0;
        for (int i = 0; i < 100; i++) {
          resetAndAdvanceDataGenerator(seekPosition);
          long expectedRead = Math.min(stream.getRemaining(), readBufferSize);
          long read = stream.read(b);
          assertEquals(expectedRead, read);
          ECStreamTestUtil.assertBufferMatches(b, dataGenerator);
          seekPosition = random.nextInt(blockLength);
          stream.seek(seekPosition);
          b.clear();
        }
        // Seeking beyond EOF should give an error
        assertThrows(IOException.class, () -> stream.seek(blockLength + 1));
      }
    }
  }

  private void resetAndAdvanceDataGenerator(long position) {
    dataGenerator = new SplittableRandom(randomSeed);
    for (long i = 0; i < position; i++) {
      dataGenerator.nextInt(255);
    }
  }

  /**
   * Return a list of num ByteBuffers of the given size.
   * @param num Number of buffers to create
   * @param size The size of each buffer
   * @return
   */
  private ByteBuffer[] allocateBuffers(int num, int size) {
    ByteBuffer[] bufs = new ByteBuffer[num];
    for (int i = 0; i < num; i++) {
      bufs[i] = ByteBuffer.allocate(size);
    }
    return bufs;
  }

  @Test
  void concurrentPositionedReadsReconstructIndependentStripes() throws Exception {
    int chunk = repConfig.getEcChunkSize();
    int blockLength = chunk * 7 - 101;
    ByteBuffer[] data = allocateBuffers(repConfig.getData(), chunk * 3);
    ECStreamTestUtil.randomFill(data, chunk, dataGenerator, blockLength);
    addDataStreamsToFactory(data, generateParity(data, repConfig));
    byte[] expected = new byte[blockLength];
    ECStreamTestUtil.randomFill(ByteBuffer.wrap(expected), new SplittableRandom(randomSeed));
    BlockLocationInfo info = ECStreamTestUtil.createKeyInfo(repConfig, blockLength,
        ECStreamTestUtil.createIndexMap(2, 3, 4, 5));
    streamFactory.setCurrentPipeline(info.getPipeline());
    ECBlockInputStreamFactory factory = ECBlockInputStreamFactoryImpl.getInstance(streamFactory, bufferPool,
        () -> ecReconstructExecutor);
    ExecutorService readers = Executors.newFixedThreadPool(2);
    try (ECBlockInputStreamProxy stream = new ECBlockInputStreamProxy(repConfig, info, null, null, factory,
        conf.getObject(OzoneClientConfig.class))) {
      List<Future<?>> futures = new ArrayList<>();
      for (int offset : new int[] {chunk - 37, chunk * 3 - 29}) {
        futures.add(readers.submit((Callable<Void>) () -> {
          byte[] actual = new byte[chunk * 3 + 53];
          stream.readFully(offset, actual);
          assertArrayEquals(Arrays.copyOfRange(expected, offset, offset + actual.length), actual);
          return null;
        }));
      }
      for (Future<?> future : futures) {
        future.get(20, TimeUnit.SECONDS);
      }
      assertEquals(0, stream.getPos());
    } finally {
      readers.shutdownNow();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testInterruptedReadRunsCellReadsNotStartedByExecutor(boolean unbuffer) throws Exception {
    int chunkSize = repConfig.getEcChunkSize();
    int blockLength = chunkSize * repConfig.getData();
    List<ByteBuffer> replicas = createReplicas(blockLength);
    // The executor keeps the cell reads without running them, as if no thread had reached them yet.
    List<FutureTask<?>> cellReads = new ArrayList<>();
    ecReconstructExecutor.shutdownNow();
    ecReconstructExecutor = mock(AbstractExecutorService.class, CALLS_REAL_METHODS);
    doAnswer(invocation -> cellReads.add(invocation.getArgument(0))).when(ecReconstructExecutor).execute(any());

    // Index 3 is missing, so the stripe is read into two data buffers and one parity buffer from the pool.
    Map<DatanodeDetails, Integer> dnMap = ECStreamTestUtil.createIndexMap(1, 2, 4, 5);
    try (ECBlockReconstructedStripeInputStream stripeStream = createStripeInputStream(dnMap, blockLength, replicas,
        index -> { });
        ECBlockReconstructedInputStream stream = new ECBlockReconstructedInputStream(repConfig, bufferPool,
            stripeStream)) {
      Thread.currentThread().interrupt();
      try {
        assertThatThrownBy(() -> stream.read(ByteBuffer.allocate(chunkSize)))
            .isInstanceOf(IOException.class)
            .hasCauseInstanceOf(InterruptedException.class);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        // Two data reads and one parity read, all done before the stream is closed, and none failed: the only
        // failed index is index 3, which has no location.
        assertThat(cellReads).hasSize(3).allMatch(FutureTask::isDone);
        assertThat(stripeStream.getFailedIndexes()).containsExactly(2);
      } finally {
        Thread.interrupted();
      }
      if (unbuffer) {
        stream.unbuffer();
        assertReusedBuffersUntouched(cellReads, chunkSize);
      }
    }

    if (!unbuffer) {
      assertReusedBuffersUntouched(cellReads, chunkSize);
    }
  }

  private void assertReusedBuffersUntouched(List<FutureTask<?>> cellReads, int chunkSize) {
    List<ByteBuffer> reused = new ArrayList<>();
    for (int i = 0; i <= repConfig.getData(); i++) {
      ByteBuffer b = bufferPool.getBuffer(false, chunkSize);
      Arrays.fill(b.array(), (byte) 0);
      reused.add(b);
    }
    cellReads.forEach(FutureTask::run);
    assertThat(reused).allSatisfy(b -> assertThat(b).isEqualTo(ByteBuffer.allocate(b.capacity())));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testInterruptedReadWaitsForCellReadRunningOnPoolThread(boolean failCell) throws Exception {
    int chunkSize = repConfig.getEcChunkSize();
    int blockLength = chunkSize * repConfig.getData();
    List<ByteBuffer> replicas = createReplicas(blockLength);
    CountDownLatch heldReadStarted = new CountDownLatch(1);
    CountDownLatch releaseHeldRead = new CountDownLatch(1);
    CountDownLatch executed = new CountDownLatch(3);
    List<FutureTask<?>> cellReads = new ArrayList<>();
    // The first cell read (index 1) is running on a pool thread before execute returns, and is held there.
    ExecutorService pool = ecReconstructExecutor;
    ecReconstructExecutor = mock(AbstractExecutorService.class, CALLS_REAL_METHODS);
    doAnswer(invocation -> {
      FutureTask<?> task = invocation.getArgument(0);
      cellReads.add(task);
      pool.execute(task);
      if (executed.getCount() == 3) {
        assertThat(heldReadStarted.await(10, TimeUnit.SECONDS)).isTrue();
      }
      executed.countDown();
      return null;
    }).when(ecReconstructExecutor).execute(any());

    Map<DatanodeDetails, Integer> dnMap = ECStreamTestUtil.createIndexMap(1, 2, 4, 5);
    try (ECBlockReconstructedInputStream stream = new ECBlockReconstructedInputStream(repConfig, bufferPool,
        createStripeInputStream(dnMap, blockLength, replicas, index -> {
          if (failCell && index == 2) {
            throw new IllegalStateException("Injected cell read failure");
          }
          if (index == 1) {
            heldReadStarted.countDown();
            Uninterruptibles.awaitUninterruptibly(releaseHeldRead);
          }
        }))) {
      FutureTask<Void> readResult = new FutureTask<>(() -> {
        try {
          assertThatThrownBy(() -> stream.read(ByteBuffer.allocate(chunkSize)))
              .isInstanceOf(IOException.class)
              .hasCauseInstanceOf(InterruptedException.class);
          assertThat(Thread.currentThread().isInterrupted()).isTrue();
          // Including the held read, which ran on a pool thread.
          assertThat(cellReads).hasSize(3).allMatch(FutureTask::isDone);
        } finally {
          Thread.interrupted();
        }
        return null;
      });
      Thread reader = new Thread(readResult);
      try {
        reader.start();
        assertThat(executed.await(10, TimeUnit.SECONDS)).isTrue();
        BooleanSupplier waiting = () -> reader.getState() == Thread.State.WAITING || readResult.isDone();
        GenericTestUtils.waitFor(waiting, 1, 10_000);
        assertThat(readResult.isDone()).isFalse();
        assertThat(releaseHeldRead.getCount()).isEqualTo(1);
        reader.interrupt();
        // Keep the cell read blocked until we have checked that interrupting the reader did not let it return.
        assertThatThrownBy(() -> readResult.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
        releaseHeldRead.countDown();
        readResult.get(10, TimeUnit.SECONDS);
      } finally {
        releaseHeldRead.countDown();
        try {
          reader.join(10_000);
        } finally {
          pool.shutdownNow();
        }
        assertThat(reader.isAlive()).isFalse();
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"true, true", "true, false", "false, true", "false, false"})
  void testReadReturnsWhenExecutorIsShutDown(boolean callerRuns, boolean interrupted) throws Exception {
    int chunkSize = repConfig.getEcChunkSize();
    int blockLength = chunkSize * repConfig.getData();
    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(), chunkSize);
    ECStreamTestUtil.randomFill(dataBufs, chunkSize, dataGenerator, blockLength);
    addDataStreamsToFactory(dataBufs, generateParity(dataBufs, repConfig));
    // Like the client's executor: after shutdown a cell read is dropped, and its future never completes.
    ecReconstructExecutor.shutdownNow();
    ecReconstructExecutor = new ThreadPoolExecutor(0, 3, 60, TimeUnit.SECONDS, new SynchronousQueue<>(),
        callerRuns ? new ThreadPoolExecutor.CallerRunsPolicy() : new ThreadPoolExecutor.AbortPolicy());
    ecReconstructExecutor.shutdownNow();

    Map<DatanodeDetails, Integer> dnMap = ECStreamTestUtil.createIndexMap(1, 2, 4, 5);
    assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
      try (ECBlockReconstructedInputStream stream = new ECBlockReconstructedInputStream(repConfig, bufferPool,
          createStripeInputStream(dnMap, blockLength))) {
        try {
          ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
          if (interrupted) {
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> stream.read(buffer))
                .isInstanceOf(IOException.class).hasCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
          } else {
            assertEquals(chunkSize, stream.read(buffer));
            ECStreamTestUtil.assertBufferMatches(buffer, new SplittableRandom(randomSeed));
          }
        } finally {
          Thread.interrupted();
        }
      }
    });
  }

  @Test
  void testInterruptedReadWithSaturatedExecutor() throws Exception {
    int chunkSize = repConfig.getEcChunkSize();
    int blockLength = chunkSize * repConfig.getData();
    List<ByteBuffer> replicas = createReplicas(blockLength);
    ecReconstructExecutor.shutdownNow();
    ecReconstructExecutor = new ThreadPoolExecutor(0, 1, 60, TimeUnit.SECONDS, new SynchronousQueue<>(),
        new ThreadPoolExecutor.CallerRunsPolicy());
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ecReconstructExecutor.execute(() -> {
      started.countDown();
      Uninterruptibles.awaitUninterruptibly(release);
    });
    assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
    try (ECBlockReconstructedStripeInputStream stripeStream = createStripeInputStream(
        ECStreamTestUtil.createIndexMap(1, 2, 4, 5), blockLength, replicas, index -> { });
        ECBlockReconstructedInputStream stream = new ECBlockReconstructedInputStream(repConfig, bufferPool,
            stripeStream)) {
      Thread.currentThread().interrupt();
      try {
        assertThatThrownBy(() -> stream.read(ByteBuffer.allocate(chunkSize)))
            .isInstanceOf(IOException.class).hasCauseInstanceOf(InterruptedException.class);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        assertThat(stripeStream.getFailedIndexes()).containsExactly(2);
      } finally {
        Thread.interrupted();
      }
    } finally {
      release.countDown();
    }
  }

  @Test
  void testInterruptedPositionedReadClosesAfterCellReadsFinish() throws Exception {
    int chunkSize = repConfig.getEcChunkSize();
    int blockLength = chunkSize * repConfig.getData();
    List<ByteBuffer> replicas = createReplicas(blockLength);
    List<FutureTask<?>> cellReads = new ArrayList<>();
    ecReconstructExecutor.shutdownNow();
    ecReconstructExecutor = mock(AbstractExecutorService.class, CALLS_REAL_METHODS);
    doAnswer(invocation -> cellReads.add(invocation.getArgument(0))).when(ecReconstructExecutor).execute(any());
    Map<DatanodeDetails, Integer> dnMap = ECStreamTestUtil.createIndexMap(1, 2, 4, 5);
    BlockLocationInfo info = ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);
    ECBlockInputStreamFactory factory = (missing, failed, rc, blockInfo, xceiver, refresh, config) ->
        new ECBlockReconstructedInputStream(repConfig, bufferPool,
            createStripeInputStream(dnMap, blockLength, replicas, index -> { }));
    try (ECBlockInputStreamProxy stream = new ECBlockInputStreamProxy(repConfig, info, null, null, factory,
        conf.getObject(OzoneClientConfig.class))) {
      Thread.currentThread().interrupt();
      try {
        assertThatThrownBy(() -> stream.readPositioned(0, ByteBuffer.allocate(chunkSize)))
            .isInstanceOf(IOException.class).hasCauseInstanceOf(InterruptedException.class);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        assertThat(cellReads).hasSize(3).allMatch(FutureTask::isDone);
      } finally {
        Thread.interrupted();
      }
      assertReusedBuffersUntouched(cellReads, chunkSize);
    }
  }

  private List<ByteBuffer> createReplicas(int blockLength) throws IOException {
    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(), repConfig.getEcChunkSize());
    ECStreamTestUtil.randomFill(dataBufs, repConfig.getEcChunkSize(), dataGenerator, blockLength);
    List<ByteBuffer> replicas = new ArrayList<>(Arrays.asList(dataBufs));
    replicas.addAll(Arrays.asList(generateParity(dataBufs, repConfig)));
    return replicas;
  }

  /**
   * A stripe reader over the given replicas, where {@code beforeRead} runs before each read of a replica. As a
   * datanode read does, a replica read fails if its thread is interrupted.
   */
  private ECBlockReconstructedStripeInputStream createStripeInputStream(Map<DatanodeDetails, Integer> dnMap,
      long blockLength, List<ByteBuffer> replicas, IntConsumer beforeRead) {
    BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);
    Pipeline ecPipeline = keyInfo.getPipeline();
    BlockInputStreamFactory factory = (rc, blockInfo, pipeline, token, xceiverFactory, refresh, config) -> {
      int index = ecPipeline.getReplicaIndex(pipeline.getNodes().get(0));
      return new ECStreamTestUtil.TestBlockInputStream(blockInfo.getBlockID(), blockInfo.getLength(),
          replicas.get(index - 1).duplicate(), index) {
        @Override
        public int read(ByteBuffer buf) throws IOException {
          beforeRead.accept(index);
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedIOException("Read of replica " + index + " interrupted");
          }
          return super.read(buf);
        }
      };
    };
    return new ECBlockReconstructedStripeInputStream(repConfig, keyInfo, null, null, factory, bufferPool,
        ecReconstructExecutor, conf.getObject(OzoneClientConfig.class));
  }

  private void addDataStreamsToFactory(ByteBuffer[] data, ByteBuffer[] parity) {
    List<ByteBuffer> dataStreams = new ArrayList<>(Arrays.asList(data));
    dataStreams.addAll(Arrays.asList(parity));
    streamFactory.setBlockStreamData(dataStreams);
  }
}
