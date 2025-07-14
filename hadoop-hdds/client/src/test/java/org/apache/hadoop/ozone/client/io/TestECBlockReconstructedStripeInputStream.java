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

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.ozone.client.io.ECStreamTestUtil.generateParity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.ozone.client.io.ECStreamTestUtil.TestBlockInputStream;
import org.apache.hadoop.ozone.client.io.ECStreamTestUtil.TestBlockInputStreamFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test for the ECBlockReconstructedStripeInputStream.
 */
public class TestECBlockReconstructedStripeInputStream {

  private static final int ONEMB = 1024 * 1024;

  private ECReplicationConfig repConfig;
  private ECStreamTestUtil.TestBlockInputStreamFactory streamFactory;
  private long randomSeed;
  private ThreadLocalRandom random = ThreadLocalRandom.current();
  private SplittableRandom dataGen;
  private ByteBufferPool bufferPool = new ElasticByteBufferPool();
  private ExecutorService ecReconstructExecutor =
      Executors.newFixedThreadPool(3);
  private OzoneConfiguration conf = new OzoneConfiguration();
  
  static List<Set<Integer>> recoveryCases() { // TODO better name
    List<Set<Integer>> params = new ArrayList<>();
    params.add(emptySet()); // non-recovery
    for (int i = 0; i < 5; i++) {
      params.add(singleton(i));
    }
    params.add(ImmutableSet.of(0, 1)); // only data
    params.add(ImmutableSet.of(1, 4)); // data and parity
    params.add(ImmutableSet.of(3, 4)); // only parity
    params.add(ImmutableSet.of(2, 3)); // data and parity
    params.add(ImmutableSet.of(2, 4)); // data and parity
    return params;
  }

  @BeforeEach
  public void setup() {
    polluteByteBufferPool();
    repConfig = new ECReplicationConfig(3, 2,
        ECReplicationConfig.EcCodec.RS, ONEMB);
    streamFactory = new ECStreamTestUtil.TestBlockInputStreamFactory();

    randomSeed = random.nextLong();
    dataGen = new SplittableRandom(randomSeed);
  }

  /**
   * All the tests here use a chunk size of 1MB, but in a mixed workload
   * cluster, it is possible to have multiple EC chunk sizes. This will result
   * in the byte buffer pool having buffers of varying size and when a buffer is
   * requested it can receive a buffer larger than the request size. That caused
   * problems in HDDS-7304, so this method ensures the buffer pool has some
   * larger buffers to return.
   */
  private void polluteByteBufferPool() {
    List<ByteBuffer> bufs = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      ByteBuffer b = bufferPool.getBuffer(false, ONEMB * 3);
      bufs.add(b);
    }
    for (ByteBuffer b : bufs) {
      bufferPool.putBuffer(b);
    }
  }

  @AfterEach
  public void teardown() {
    ecReconstructExecutor.shutdownNow();
  }

  @Test
  public void testSufficientLocations() throws IOException {
    // One chunk, only 1 location.
    BlockLocationInfo keyInfo = ECStreamTestUtil
        .createKeyInfo(repConfig, 1, ONEMB);
    try (ECBlockInputStream ecb = createInputStream(keyInfo)) {
      assertTrue(ecb.hasSufficientLocations());
    }
    // Two Chunks, but missing data block 2.
    Map<DatanodeDetails, Integer> dnMap
        = ECStreamTestUtil.createIndexMap(1, 4, 5);
    keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, ONEMB * 2, dnMap);
    try (ECBlockReconstructedStripeInputStream ecb =
             createInputStream(keyInfo)) {
      assertTrue(ecb.hasSufficientLocations());
      Collection<Integer> idxs = dnMap.values();
      for (int i : idxs) {
        ecb.setRecoveryIndexes(singleton(i - 1));
        assertTrue(ecb.hasSufficientLocations());
      }

      // trying to recover all
      ecb.setRecoveryIndexes(toBufferIndexes(idxs));
      assertFalse(ecb.hasSufficientLocations());
    }

    // Three Chunks, but missing data block 2 and 3.
    dnMap = ECStreamTestUtil.createIndexMap(1, 4, 5);
    keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, ONEMB * 3, dnMap);
    try (ECBlockReconstructedStripeInputStream ecb =
             createInputStream(keyInfo)) {
      assertTrue(ecb.hasSufficientLocations());
      // Set a failed location
      List<DatanodeDetails> failed = new ArrayList<>();
      failed.add(keyInfo.getPipeline().getFirstNode());
      ecb.addFailedDatanodes(failed);
      assertFalse(ecb.hasSufficientLocations());
    }

    // Three Chunks, but missing data block 2 and 3 and parity 1.
    dnMap = ECStreamTestUtil.createIndexMap(1, 4);
    keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, ONEMB * 3, dnMap);
    try (ECBlockInputStream ecb = createInputStream(keyInfo)) {
      assertFalse(ecb.hasSufficientLocations());
    }

    // Three Chunks, all available but fail 3
    dnMap = ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);
    keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, ONEMB * 3, dnMap);
    try (ECBlockReconstructedStripeInputStream ecb =
             createInputStream(keyInfo)) {
      assertTrue(ecb.hasSufficientLocations());
      // Set a failed location
      List<DatanodeDetails> failed = new ArrayList<>();
      for (Map.Entry<DatanodeDetails, Integer> entry : dnMap.entrySet()) {
        failed.add(entry.getKey());
        boolean expected = failed.size() < 3;

        ecb.addFailedDatanodes(singleton(entry.getKey()));
        assertEquals(expected, ecb.hasSufficientLocations());
      }
    }

    try (ECBlockReconstructedStripeInputStream ecb =
             createInputStream(keyInfo)) {
      List<Integer> recover = new ArrayList<>();
      for (int i : Arrays.asList(4, 3, 2, 1, 0)) {
        recover.add(i);
        ecb.setRecoveryIndexes(recover);
        boolean expected = recover.size() < 3;
        assertEquals(expected, ecb.hasSufficientLocations());
      }
    }

    // One chunk, indexes 2 and 3 are padding, but still reported in the
    // container list. The other locations are missing so we should have
    // insufficient locations.
    dnMap = ECStreamTestUtil.createIndexMap(2, 3);
    keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, ONEMB, dnMap);
    try (ECBlockInputStream ecb = createInputStream(keyInfo)) {
      assertFalse(ecb.hasSufficientLocations());
    }
  }

  @ParameterizedTest
  @MethodSource("recoveryCases")
  void testReadFullStripesWithPartial(Set<Integer> recoveryIndexes)
      throws IOException {
    // Generate the input data for 3 full stripes and generate the parity.
    int chunkSize = repConfig.getEcChunkSize();
    int partialStripeSize = chunkSize * 2 - 1;
    int blockLength = chunkSize * repConfig.getData() * 3 + partialStripeSize;
    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(), 4 * chunkSize);
    ECStreamTestUtil.randomFill(dataBufs, chunkSize, dataGen, blockLength);

    ByteBuffer[] parity = generateParity(dataBufs, repConfig);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);

    streamFactory = new TestBlockInputStreamFactory();
    addDataStreamsToFactory(dataBufs, parity);

    BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(repConfig,
        stripeSize() * 3 + partialStripeSize, dnMap);
    streamFactory.setCurrentPipeline(keyInfo.getPipeline());

    List<Integer> outputIndexes = getOutputIndexes(recoveryIndexes);

    ByteBuffer[] bufs = allocateByteBuffers(
        outputIndexes.size(), repConfig.getEcChunkSize());

    dataGen = new SplittableRandom(randomSeed);
    try (ECBlockReconstructedStripeInputStream ecb =
             createInputStream(keyInfo)) {

      ecb.setRecoveryIndexes(recoveryIndexes);

      // Read 3 full stripes
      for (int i = 0; i < 3; i++) {
        int read = ecb.read(bufs);
        assertEquals(stripeSize(), read);

        int output = 0;
        for (int j = 0; j < repConfig.getRequiredNodes(); j++) {
          if (outputIndexes.contains(j)) {
            ECStreamTestUtil.assertBufferMatches(bufs[output++], dataGen);
          }
        }

        // Check the underlying streams have read 1 chunk per read:
        for (TestBlockInputStream bis : streamFactory.getBlockStreams()) {
          assertEquals(chunkSize * (i + 1),
              bis.getPos());
        }
        assertEquals(stripeSize() * (i + 1), ecb.getPos());
        clearBuffers(bufs);
      }
      // The next read is a partial stripe
      int read = ecb.read(bufs);
      assertEquals(partialStripeSize, read);
      int output = 0;
      for (int j = 0; j < 2; j++) {
        if (outputIndexes.contains(j)) {
          ECStreamTestUtil.assertBufferMatches(bufs[output++], dataGen);
        }
      }
      if (outputIndexes.contains(2)) {
        assertEquals(0, bufs[output].remaining());
        assertEquals(0, bufs[output].position());
      }

      // A further read should give EOF
      clearBuffers(bufs);
      read = ecb.read(bufs);
      assertEquals(-1, read);
    }
  }

  @Test
  public void testReadPartialStripe() throws IOException {
    int blockLength = repConfig.getEcChunkSize() - 1;
    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(), 3 * ONEMB);
    ECStreamTestUtil
        .randomFill(dataBufs, repConfig.getEcChunkSize(), dataGen, blockLength);
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);
    addDataStreamsToFactory(dataBufs, parity);

    ByteBuffer[] bufs = allocateByteBuffers(repConfig);
    // We have a length that is less than a single chunk, so blocks 2 and 3
    // are padding and will not be present. Block 1 is lost and needs recovered
    // from the parity and padded blocks 2 and 3.
    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(4, 5);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);
    streamFactory.setCurrentPipeline(keyInfo.getPipeline());
    dataGen = new SplittableRandom(randomSeed);
    try (ECBlockReconstructedStripeInputStream ecb =
        createInputStream(keyInfo)) {
      int read = ecb.read(bufs);
      assertEquals(blockLength, read);
      ECStreamTestUtil.assertBufferMatches(bufs[0], dataGen);
      assertEquals(0, bufs[1].remaining());
      assertEquals(0, bufs[1].position());
      assertEquals(0, bufs[2].remaining());
      assertEquals(0, bufs[2].position());
      // Check the underlying streams have been advanced by 1 blockLength:
      for (TestBlockInputStream bis : streamFactory.getBlockStreams()) {
        assertEquals(blockLength, bis.getPos());
      }
      assertEquals(ecb.getPos(), blockLength);
      clearBuffers(bufs);
      // A further read should give EOF
      read = ecb.read(bufs);
      assertEquals(-1, read);
    }
  }

  @Test
  void recoverPartialStripe() throws IOException {
    int ecChunkSize = repConfig.getEcChunkSize();
    int blockLength = ecChunkSize - 1;
    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(), 3 * ONEMB);
    ECStreamTestUtil
        .randomFill(dataBufs, ecChunkSize, dataGen, blockLength);
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);
    addDataStreamsToFactory(dataBufs, parity);

    // We have a length that is less than a single chunk, so blocks 2 and 3 are
    // padding and will not be present. Parity blocks are lost and need to be
    // recovered from block 1 and padded blocks 2 and 3.
    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 4, 5);
    ByteBuffer[] bufs = allocateByteBuffers(2, ecChunkSize);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);
    streamFactory.setCurrentPipeline(keyInfo.getPipeline());
    dataGen = new SplittableRandom(randomSeed);
    try (ECBlockReconstructedStripeInputStream ecb =
             createInputStream(keyInfo)) {
      ecb.setRecoveryIndexes(Arrays.asList(3, 4));

      int read = ecb.read(bufs);
      assertEquals(blockLength, read);
      ECStreamTestUtil.assertBufferMatches(bufs[0], dataGen);
      ECStreamTestUtil.assertBufferMatches(bufs[1], dataGen);
      // Check the underlying streams have been advanced by 1 blockLength:
      for (TestBlockInputStream bis : streamFactory.getBlockStreams()) {
        assertEquals(blockLength, bis.getPos());
      }
      assertEquals(ecb.getPos(), blockLength);
      clearBuffers(bufs);
      // A further read should give EOF
      read = ecb.read(bufs);
      assertEquals(-1, read);
    }
  }

  @Test
  public void testReadPartialStripeTwoChunks() throws IOException {
    int chunkSize = repConfig.getEcChunkSize();
    int blockLength = chunkSize * 2 - 1;

    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(), 3 * ONEMB);
    ECStreamTestUtil
        .randomFill(dataBufs, repConfig.getEcChunkSize(), dataGen, blockLength);
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);
    addDataStreamsToFactory(dataBufs, parity);

    ByteBuffer[] bufs = allocateByteBuffers(repConfig);
    // We have a length that is less than a single chunk, so blocks 2 and 3
    // are padding and will not be present. Block 1 is lost and needs recovered
    // from the parity and padded blocks 2 and 3.
    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(4, 5);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);
    streamFactory.setCurrentPipeline(keyInfo.getPipeline());
    dataGen = new SplittableRandom(randomSeed);
    try (ECBlockReconstructedStripeInputStream ecb =
        createInputStream(keyInfo)) {
      int read = ecb.read(bufs);
      assertEquals(blockLength, read);
      ECStreamTestUtil.assertBufferMatches(bufs[0], dataGen);
      ECStreamTestUtil.assertBufferMatches(bufs[1], dataGen);
      assertEquals(0, bufs[2].remaining());
      assertEquals(0, bufs[2].position());
      // Check the underlying streams have been advanced by 1 chunk:
      for (TestBlockInputStream bis : streamFactory.getBlockStreams()) {
        assertEquals(chunkSize, bis.getPos());
      }
      assertEquals(ecb.getPos(), blockLength);
      clearBuffers(bufs);
      // A further read should give EOF
      read = ecb.read(bufs);
      assertEquals(-1, read);
    }
  }

  @Test
  public void testReadPartialStripeThreeChunks() throws IOException {
    int chunkSize = repConfig.getEcChunkSize();
    int blockLength = chunkSize * 3 - 1;

    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(), 3 * ONEMB);
    ECStreamTestUtil
        .randomFill(dataBufs, repConfig.getEcChunkSize(), dataGen, blockLength);
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);

    // We have a length that is less than a stripe, so chunks 1 and 2 are full.
    // Block 1 is lost and needs recovered
    // from the parity and padded blocks 2 and 3.

    List<Map<DatanodeDetails, Integer>> locations = new ArrayList<>();
    // Two data missing
    locations.add(ECStreamTestUtil.createIndexMap(3, 4, 5));
    // Two data missing
    locations.add(ECStreamTestUtil.createIndexMap(1, 4, 5));
    // One data missing - the last one
    locations.add(ECStreamTestUtil.createIndexMap(1, 2, 5));
    // One data and one parity missing
    locations.add(ECStreamTestUtil.createIndexMap(2, 3, 4));
    // One data and one parity missing
    locations.add(ECStreamTestUtil.createIndexMap(1, 2, 4));
    // No indexes missing
    locations.add(ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5));

    for (Map<DatanodeDetails, Integer> dnMap : locations) {
      streamFactory = new TestBlockInputStreamFactory();
      addDataStreamsToFactory(dataBufs, parity);
      ByteBuffer[] bufs = allocateByteBuffers(repConfig);

      BlockLocationInfo keyInfo =
          ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);
      streamFactory.setCurrentPipeline(keyInfo.getPipeline());
      dataGen = new SplittableRandom(randomSeed);
      try (ECBlockReconstructedStripeInputStream ecb =
          createInputStream(keyInfo)) {
        int read = ecb.read(bufs);
        assertEquals(blockLength, read);
        ECStreamTestUtil.assertBufferMatches(bufs[0], dataGen);
        ECStreamTestUtil.assertBufferMatches(bufs[1], dataGen);
        ECStreamTestUtil.assertBufferMatches(bufs[2], dataGen);
        // Check the underlying streams have been advanced by 1 chunk:
        for (TestBlockInputStream bis : streamFactory.getBlockStreams()) {
          assertEquals(0, bis.getRemaining());
        }
        assertEquals(ecb.getPos(), blockLength);
        clearBuffers(bufs);
        // A further read should give EOF
        read = ecb.read(bufs);
        assertEquals(-1, read);
      }
    }
  }

  @Test
  public void testErrorThrownIfBlockNotLongEnough() throws IOException {
    int blockLength = repConfig.getEcChunkSize() - 1;
    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(), 3 * ONEMB);
    ECStreamTestUtil
        .randomFill(dataBufs, repConfig.getEcChunkSize(), dataGen, blockLength);
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);
    addDataStreamsToFactory(dataBufs, parity);

    // Set the parity buffer limit to be less than the block length
    parity[0].limit(blockLength - 1);
    parity[1].limit(blockLength - 1);

    ByteBuffer[] bufs = allocateByteBuffers(repConfig);
    // We have a length that is less than a single chunk, so blocks 2 and 3
    // are padding and will not be present. Block 1 is lost and needs recovered
    // from the parity and padded blocks 2 and 3.
    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(4, 5);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);
    streamFactory.setCurrentPipeline(keyInfo.getPipeline());
    try (ECBlockReconstructedStripeInputStream ecb = createInputStream(keyInfo)) {
      assertThrows(InsufficientLocationsException.class, () -> ecb.read(bufs));
    }
  }

  @Test
  void testNoErrorIfSpareLocationToRead() throws IOException {
    int chunkSize = repConfig.getEcChunkSize();
    int blockLength = chunkSize * 3 - 1;

    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(), 3 * ONEMB);
    ECStreamTestUtil
        .randomFill(dataBufs, repConfig.getEcChunkSize(), dataGen, blockLength);
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);

    // We have a length that is less than a stripe, so chunks 1 and 2 are full.
    // Block 1 is lost and needs recovered
    // from the parity and padded blocks 2 and 3.

    List<Map<DatanodeDetails, Integer>> locations = new ArrayList<>();
    // Two data missing
    locations.add(ECStreamTestUtil.createIndexMap(3, 4, 5));
    // Two data missing
    locations.add(ECStreamTestUtil.createIndexMap(1, 4, 5));
    // One data missing - the last one
    locations.add(ECStreamTestUtil.createIndexMap(1, 2, 5));
    // One data and one parity missing
    locations.add(ECStreamTestUtil.createIndexMap(2, 3, 4));
    // One data and one parity missing
    locations.add(ECStreamTestUtil.createIndexMap(1, 2, 4));
    // No indexes missing
    locations.add(ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5));

    DatanodeDetails spare = MockDatanodeDetails.randomDatanodeDetails();

    for (Map<DatanodeDetails, Integer> dnMap : locations) {
      streamFactory = new TestBlockInputStreamFactory();
      addDataStreamsToFactory(dataBufs, parity);
      ByteBuffer[] bufs = allocateByteBuffers(repConfig);

      // this index fails, but has spare replica
      int failing = dnMap.values().iterator().next();
      streamFactory.setFailIndexes(failing);
      dnMap.put(spare, failing);

      BlockLocationInfo keyInfo =
          ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);
      streamFactory.setCurrentPipeline(keyInfo.getPipeline());

      dataGen = new SplittableRandom(randomSeed);
      try (ECBlockReconstructedStripeInputStream ecb =
               createInputStream(keyInfo)) {
        int read = ecb.read(bufs);
        assertEquals(blockLength, read);
        ECStreamTestUtil.assertBufferMatches(bufs[0], dataGen);
        ECStreamTestUtil.assertBufferMatches(bufs[1], dataGen);
        ECStreamTestUtil.assertBufferMatches(bufs[2], dataGen);
        // Check the underlying streams have been advanced by 1 chunk:
        for (TestBlockInputStream bis : streamFactory.getBlockStreams()) {
          assertEquals(0, bis.getRemaining());
        }
        assertEquals(ecb.getPos(), blockLength);
        clearBuffers(bufs);
        // A further read should give EOF
        read = ecb.read(bufs);
        assertEquals(-1, read);
      }
    }
  }

  @Test
  public void testSeek() throws IOException {
    // Generate the input data for 3 full stripes and generate the parity
    // and a partial stripe
    int chunkSize = repConfig.getEcChunkSize();
    int partialStripeSize = chunkSize * 2 - 1;
    int dataLength = stripeSize() * 3 + partialStripeSize;
    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(), 4 * chunkSize);
    ECStreamTestUtil
        .randomFill(dataBufs, repConfig.getEcChunkSize(), dataGen, dataLength);
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);

    List<Map<DatanodeDetails, Integer>> locations = new ArrayList<>();
    // Two data missing
    locations.add(ECStreamTestUtil.createIndexMap(1, 4, 5));
    // One data missing
    locations.add(ECStreamTestUtil.createIndexMap(1, 2, 4, 5));
    // Two data missing including first
    locations.add(ECStreamTestUtil.createIndexMap(2, 4, 5));
    // One data and one parity missing
    locations.add(ECStreamTestUtil.createIndexMap(2, 3, 4));
    // No locations missing
    locations.add(ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5));

    for (Map<DatanodeDetails, Integer> dnMap : locations) {
      streamFactory = new TestBlockInputStreamFactory();
      addDataStreamsToFactory(dataBufs, parity);

      BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(repConfig,
          stripeSize() * 3 + partialStripeSize, dnMap);
      streamFactory.setCurrentPipeline(keyInfo.getPipeline());

      ByteBuffer[] bufs = allocateByteBuffers(repConfig);
      try (ECBlockReconstructedStripeInputStream ecb =
          createInputStream(keyInfo)) {

        // Read Stripe 1
        int read = ecb.read(bufs);
        for (int j = 0; j < bufs.length; j++) {
          validateContents(dataBufs[j], bufs[j], 0, chunkSize);
        }
        assertEquals(stripeSize(), read);
        assertEquals(dataLength - stripeSize(), ecb.getRemaining());

        // Seek to 0 and read again
        clearBuffers(bufs);
        ecb.seek(0);
        ecb.read(bufs);
        for (int j = 0; j < bufs.length; j++) {
          validateContents(dataBufs[j], bufs[j], 0, chunkSize);
        }
        assertEquals(stripeSize(), read);
        assertEquals(dataLength - stripeSize(), ecb.getRemaining());

        // Seek to the last stripe
        // Seek to the last stripe
        clearBuffers(bufs);
        ecb.seek(stripeSize() * 3);
        read = ecb.read(bufs);
        validateContents(dataBufs[0], bufs[0], 3 * chunkSize, chunkSize);
        validateContents(dataBufs[1], bufs[1], 3 * chunkSize, chunkSize - 1);
        assertEquals(0, bufs[2].remaining());
        assertEquals(partialStripeSize, read);
        assertEquals(0, ecb.getRemaining());

        // seek to the start of stripe 3
        clearBuffers(bufs);
        ecb.seek(stripeSize() * (long)2);
        read = ecb.read(bufs);
        for (int j = 0; j < bufs.length; j++) {
          validateContents(dataBufs[j], bufs[j], 2 * chunkSize, chunkSize);
        }
        assertEquals(stripeSize(), read);
        assertEquals(partialStripeSize, ecb.getRemaining());
      }
    }
  }

  @Test
  public void testSeekToPartialOffsetFails() {
    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 4, 5);
    BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(repConfig,
        stripeSize() * 3, dnMap);
    streamFactory.setCurrentPipeline(keyInfo.getPipeline());

    try (ECBlockReconstructedStripeInputStream ecb = createInputStream(keyInfo)) {
      IOException e = assertThrows(IOException.class, () -> ecb.seek(10));
      assertEquals("Requested position 10 does not align " +
          "with a stripe offset", e.getMessage());
    }
  }

  private Integer getRandomStreamIndex(Set<Integer> set) {
    return set.stream().skip(RandomUtils.secure().randomInt(0, set.size()))
        .findFirst().orElse(null);
  }

  @Test
  public void testErrorReadingBlockContinuesReading() throws IOException {
    // Generate the input data for 3 full stripes and generate the parity.
    int chunkSize = repConfig.getEcChunkSize();
    int partialStripeSize = chunkSize * 2 - 1;
    int blockLength = repConfig.getEcChunkSize() * repConfig.getData() * 3
        + partialStripeSize;
    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(),
        4 * chunkSize);
    ECStreamTestUtil
        .randomFill(dataBufs, repConfig.getEcChunkSize(), dataGen, blockLength);
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);

    for (int k = 0; k < 5; k++) {
      Set<Integer> failed = new HashSet<>();
      streamFactory = new TestBlockInputStreamFactory();
      addDataStreamsToFactory(dataBufs, parity);

      // Data block index 3 is missing and needs recovered initially.
      Map<DatanodeDetails, Integer> dnMap =
          ECStreamTestUtil.createIndexMap(1, 2, 4, 5);
      BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(repConfig,
          stripeSize() * 3 + partialStripeSize, dnMap);
      streamFactory.setCurrentPipeline(keyInfo.getPipeline());

      ByteBuffer[] bufs = allocateByteBuffers(repConfig);
      try (ECBlockReconstructedStripeInputStream ecb =
          createInputStream(keyInfo)) {
        // After reading the first stripe, make one of the streams error
        for (int i = 0; i < 3; i++) {
          int read = ecb.read(bufs);
          for (int j = 0; j < bufs.length; j++) {
            validateContents(dataBufs[j], bufs[j], i * chunkSize, chunkSize);
          }
          assertEquals(stripeSize() * (i + 1), ecb.getPos());
          assertEquals(stripeSize(), read);
          clearBuffers(bufs);
          if (i == 0) {
            Integer failStream =
                getRandomStreamIndex(streamFactory.getStreamIndexes());
            streamFactory.getBlockStream(failStream)
                .setShouldError(true);
            failed.add(failStream);
          }
        }
        // The next read is a partial stripe
        int read = ecb.read(bufs);
        assertEquals(partialStripeSize, read);
        validateContents(dataBufs[0], bufs[0], 3 * chunkSize, chunkSize);
        validateContents(dataBufs[1], bufs[1], 3 * chunkSize, chunkSize - 1);
        assertEquals(0, bufs[2].remaining());
        assertEquals(0, bufs[2].position());

        // seek back to zero and read a stripe to re-open the streams
        ecb.seek(0);
        clearBuffers(bufs);
        ecb.read(bufs);
        // Now fail another random stream and the read should fail with
        // insufficient locations
        Set<Integer> currentStreams =
            new HashSet<>(streamFactory.getStreamIndexes());
        currentStreams.removeAll(failed);
        Integer failStream = getRandomStreamIndex(currentStreams);
        streamFactory.getBlockStream(failStream)
            .setShouldError(true);
        clearBuffers(bufs);
        assertThrows(InsufficientLocationsException.class, () -> ecb.read(bufs));
      }
    }
  }

  @Test
  public void testAllLocationsFailOnFirstRead() throws IOException {
    // This test simulates stale nodes. When the nodes are stale, but not yet
    // dead, the locations will still be given to the client and it will try to
    // read them, but the read will always fail.
    // Additionally, if the key is small (less than 2 EC chunks), the locations
    // for the indexes which are all padding will be returned to the client and
    // this can confuse the "sufficient locations" check, resulting in a strange
    // error when selecting parity indexes (HDDS-6258)
    int chunkSize = repConfig.getEcChunkSize();
    int partialStripeSize = chunkSize;
    int blockLength = partialStripeSize;
    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(), chunkSize);
    ECStreamTestUtil
        .randomFill(dataBufs, repConfig.getEcChunkSize(), dataGen, blockLength);
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);

    streamFactory = new TestBlockInputStreamFactory();
    addDataStreamsToFactory(dataBufs, parity);
    // Fail all the indexes containing data on their first read.
    streamFactory.setFailIndexes(1, 4, 5);
    // The locations contain the padded indexes, as will often be the case
    // when containers are reported by SCM.
    Map<DatanodeDetails, Integer> dnMap =
          ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);
    BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(repConfig,
        blockLength, dnMap);
    streamFactory.setCurrentPipeline(keyInfo.getPipeline());

    ByteBuffer[] bufs = allocateByteBuffers(repConfig);
    try (ECBlockReconstructedStripeInputStream ecb =
             createInputStream(keyInfo)) {
      assertThrows(InsufficientLocationsException.class,
          () -> ecb.read(bufs));
    }
  }

  @Test
  public void testFailedLocationsAreNotRead() throws IOException {
    // Generate the input data for 3 full stripes and generate the parity.
    int chunkSize = repConfig.getEcChunkSize();
    int partialStripeSize = chunkSize * 2 - 1;
    int blockLength = chunkSize * repConfig.getData() * 3 + partialStripeSize;
    ByteBuffer[] dataBufs = allocateBuffers(repConfig.getData(), 4 * chunkSize);
    ECStreamTestUtil.randomFill(dataBufs, chunkSize, dataGen, blockLength);
    ByteBuffer[] parity = generateParity(dataBufs, repConfig);

    streamFactory = new TestBlockInputStreamFactory();
    addDataStreamsToFactory(dataBufs, parity);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);
    BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(repConfig,
        stripeSize() * 3 + partialStripeSize, dnMap);
    streamFactory.setCurrentPipeline(keyInfo.getPipeline());

    ByteBuffer[] bufs = allocateByteBuffers(repConfig);
    dataGen = new SplittableRandom(randomSeed);
    try (ECBlockReconstructedStripeInputStream ecb =
        createInputStream(keyInfo)) {
      List<DatanodeDetails> failed = new ArrayList<>();
      // Set the first 3 DNs as failed
      for (Map.Entry<DatanodeDetails, Integer> e : dnMap.entrySet()) {
        if (e.getValue() <= 2) {
          failed.add(e.getKey());
        }
      }
      ecb.addFailedDatanodes(failed);

      // Read full stripe
      int read = ecb.read(bufs);
      for (ByteBuffer buffer : bufs) {
        ECStreamTestUtil.assertBufferMatches(buffer, dataGen);
      }
      assertEquals(stripeSize(), read);

      // Now ensure that streams with repIndexes 1, 2 and 3 have not been
      // created in the stream factory, indicating we did not read them.
      List<TestBlockInputStream> streams = streamFactory.getBlockStreams();
      for (TestBlockInputStream stream : streams) {
        assertThat(stream.getEcReplicaIndex()).isGreaterThan(2);
      }
    }
  }

  private ECBlockReconstructedStripeInputStream createInputStream(
      BlockLocationInfo keyInfo) {
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    return new ECBlockReconstructedStripeInputStream(repConfig, keyInfo,
        null, null, streamFactory, bufferPool, ecReconstructExecutor,
        clientConfig);
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

  /**
   * Validates that the data buffer has the same contents as the source buffer,
   * starting the checks in the src at offset and for count bytes.
   * @param src The source of the data
   * @param data The data which should be checked against the source
   * @param offset The starting point in the src buffer
   * @param count How many bytes to check.
   */
  private void validateContents(ByteBuffer src, ByteBuffer data, int offset,
      int count) {
    byte[] srcArray = src.array();
    assertEquals(count, data.remaining());
    for (int i = offset; i < offset + count; i++) {
      assertEquals(srcArray[i], data.get(), "Element " + i);
    }
    data.flip();
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

  private int stripeSize() {
    return stripeSize(repConfig);
  }

  private int stripeSize(ECReplicationConfig rconfig) {
    return rconfig.getEcChunkSize() * rconfig.getData();
  }

  private void clearBuffers(ByteBuffer[] bufs) {
    for (ByteBuffer b : bufs) {
      b.clear();
    }
  }

  private ByteBuffer[] allocateByteBuffers(ECReplicationConfig rConfig) {
    return allocateByteBuffers(rConfig.getData(), rConfig.getEcChunkSize());
  }

  private ByteBuffer[] allocateByteBuffers(int count, int capacity) {
    ByteBuffer[] bufs = new ByteBuffer[count];
    for (int i = 0; i < bufs.length; i++) {
      bufs[i] = ByteBuffer.allocate(capacity);
    }
    return bufs;
  }

  private List<Integer> getOutputIndexes(Set<Integer> recoveryIndexes) {
    return recoveryIndexes.isEmpty()
        ? Arrays.asList(0, 1, 2)
        : new ArrayList<>(recoveryIndexes);
  }

  private static Set<Integer> toBufferIndexes(Collection<Integer> dnIdxs) {
    return dnIdxs.stream()
        .mapToInt(Integer::intValue)
        .map(i -> i - 1)
        .boxed()
        .collect(toSet());
  }

}
