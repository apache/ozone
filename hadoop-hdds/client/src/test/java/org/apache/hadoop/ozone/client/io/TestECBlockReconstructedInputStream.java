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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

  private void addDataStreamsToFactory(ByteBuffer[] data, ByteBuffer[] parity) {
    List<ByteBuffer> dataStreams = new ArrayList<>();
    for (ByteBuffer b : data) {
      dataStreams.add(b);
    }
    for (ByteBuffer b : parity) {
      dataStreams.add(b);
    }
    streamFactory.setBlockStreamData(dataStreams);
  }
}
