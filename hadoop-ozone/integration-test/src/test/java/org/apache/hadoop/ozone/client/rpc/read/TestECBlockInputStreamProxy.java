/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.client.rpc.read;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.ozone.client.io.BadDataLocationException;
import org.apache.hadoop.ozone.client.io.ECBlockInputStreamFactory;
import org.apache.hadoop.ozone.client.io.ECBlockInputStreamProxy;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

/**
 * Unit tests for the  ECBlockInputStreamProxy class.
 */
public class TestECBlockInputStreamProxy {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestECBlockInputStreamProxy.class);

  private static final int ONEMB = 1024 * 1024;
  private ECReplicationConfig repConfig;
  private TestECBlockInputStreamFactory streamFactory;

  private long randomSeed;
  private ThreadLocalRandom random = ThreadLocalRandom.current();
  private SplittableRandom dataGenerator;

  @Before
  public void setup() {
    repConfig = new ECReplicationConfig(3, 2);
    streamFactory = new TestECBlockInputStreamFactory();
    randomSeed = random.nextLong();
    dataGenerator = new SplittableRandom(randomSeed);
  }

  @Test
  public void testExpectedDataLocations() {
    Assert.assertEquals(1,
        ECBlockInputStreamProxy.expectedDataLocations(repConfig, 1));
    Assert.assertEquals(2,
        ECBlockInputStreamProxy.expectedDataLocations(repConfig, ONEMB + 1));
    Assert.assertEquals(3,
        ECBlockInputStreamProxy.expectedDataLocations(repConfig, 3 * ONEMB));
    Assert.assertEquals(3,
        ECBlockInputStreamProxy.expectedDataLocations(repConfig, 10 * ONEMB));

    repConfig = new ECReplicationConfig(6, 3);
    Assert.assertEquals(1,
        ECBlockInputStreamProxy.expectedDataLocations(repConfig, 1));
    Assert.assertEquals(2,
        ECBlockInputStreamProxy.expectedDataLocations(repConfig, ONEMB + 1));
    Assert.assertEquals(3,
        ECBlockInputStreamProxy.expectedDataLocations(repConfig, 3 * ONEMB));
    Assert.assertEquals(6,
        ECBlockInputStreamProxy.expectedDataLocations(repConfig, 10 * ONEMB));
  }

  @Test
  public void testAvailableDataLocations() {
    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 4, 5);
    OmKeyLocationInfo blockInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 1024, dnMap);
    Assert.assertEquals(2, ECBlockInputStreamProxy.availableDataLocations(
        repConfig, blockInfo.getPipeline()));

    dnMap = ECStreamTestUtil.createIndexMap(1, 4, 5);
    blockInfo = ECStreamTestUtil.createKeyInfo(repConfig, 1024, dnMap);
    Assert.assertEquals(1, ECBlockInputStreamProxy.availableDataLocations(
        repConfig, blockInfo.getPipeline()));

    dnMap = ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);
    blockInfo = ECStreamTestUtil.createKeyInfo(repConfig, 1024, dnMap);
    Assert.assertEquals(3, ECBlockInputStreamProxy.availableDataLocations(
        repConfig, blockInfo.getPipeline()));
  }

  @Test
  public void testCorrectStreamCreatedDependingOnDataLocations()
      throws IOException {
    int blockLength = 5 * ONEMB;
    ByteBuffer data = ByteBuffer.allocate(blockLength);
    ECStreamTestUtil.randomFill(data, dataGenerator);
    streamFactory.setData(data);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);
    OmKeyLocationInfo blockInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);

    try (ECBlockInputStreamProxy bis = createBISProxy(repConfig, blockInfo)) {
      // Not all locations present, so we expect on;y the "missing=true" stream
      // to be present.
      Assert.assertTrue(streamFactory.getStreams().containsKey(false));
      Assert.assertFalse(streamFactory.getStreams().containsKey(true));
    }

    streamFactory = new TestECBlockInputStreamFactory();
    streamFactory.setData(data);
    dnMap = ECStreamTestUtil.createIndexMap(2, 3, 4, 5);
    blockInfo = ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);

    try (ECBlockInputStreamProxy bis = createBISProxy(repConfig, blockInfo)) {
      // Not all locations present, so we expect on;y the "missing=true" stream
      // to be present.
      Assert.assertFalse(streamFactory.getStreams().containsKey(false));
      Assert.assertTrue(streamFactory.getStreams().containsKey(true));
    }
  }

  @Test
  public void testCanReadNonReconstructionToEOF()
      throws IOException {
    int blockLength = 5 * ONEMB;
    ByteBuffer data = ByteBuffer.allocate(blockLength);
    ECStreamTestUtil.randomFill(data, dataGenerator);
    streamFactory.setData(data);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);
    OmKeyLocationInfo blockInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);

    ByteBuffer readBuffer = ByteBuffer.allocate(100);
    dataGenerator = new SplittableRandom(randomSeed);
    try (ECBlockInputStreamProxy bis = createBISProxy(repConfig, blockInfo)) {
      while(true) {
        int read = bis.read(readBuffer);
        ECStreamTestUtil.assertBufferMatches(readBuffer, dataGenerator);
        readBuffer.clear();
        if (read < 100) {
          break;
        }
      }
      readBuffer.clear();
      int read = bis.read(readBuffer);
      Assert.assertEquals(-1, read);
    }
  }

  @Test
  public void testCanReadReconstructionToEOF()
      throws IOException {
    int blockLength = 5 * ONEMB;
    ByteBuffer data = ByteBuffer.allocate(blockLength);
    ECStreamTestUtil.randomFill(data, dataGenerator);
    streamFactory.setData(data);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(2, 3, 4, 5);
    OmKeyLocationInfo blockInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);

    ByteBuffer readBuffer = ByteBuffer.allocate(100);
    dataGenerator = new SplittableRandom(randomSeed);
    try (ECBlockInputStreamProxy bis = createBISProxy(repConfig, blockInfo)) {
      while(true) {
        int read = bis.read(readBuffer);
        ECStreamTestUtil.assertBufferMatches(readBuffer, dataGenerator);
        readBuffer.clear();
        if (read < 100) {
          break;
        }
      }
      readBuffer.clear();
      int read = bis.read(readBuffer);
      Assert.assertEquals(-1, read);
    }
  }

  @Test
  public void testCanHandleErrorAndFailOverToReconstruction()
      throws IOException {
    int blockLength = 5 * ONEMB;
    ByteBuffer data = ByteBuffer.allocate(blockLength);
    ECStreamTestUtil.randomFill(data, dataGenerator);
    streamFactory.setData(data);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);
    OmKeyLocationInfo blockInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);

    ByteBuffer readBuffer = ByteBuffer.allocate(100);
    DatanodeDetails badDN = blockInfo.getPipeline().getFirstNode();

    dataGenerator = new SplittableRandom(randomSeed);
    try (ECBlockInputStreamProxy bis = createBISProxy(repConfig, blockInfo)) {
      // Perform one read to get the stream created
      int read = bis.read(readBuffer);
      Assert.assertEquals(100, read);
      ECStreamTestUtil.assertBufferMatches(readBuffer, dataGenerator);
      // Setup an error to be thrown part through a read, so the dataBuffer
      // will have been advanced by 50 bytes before the error. This tests it
      // correctly rewinds and the same data is loaded again from the other
      // stream.
      streamFactory.getStreams().get(false).setShouldError(true, 151,
          new BadDataLocationException(badDN, "Simulated Error"));
      while(true) {
        readBuffer.clear();
        read = bis.read(readBuffer);
        ECStreamTestUtil.assertBufferMatches(readBuffer, dataGenerator);
        if (read < 100) {
          break;
        }
      }
      readBuffer.clear();
      read = bis.read(readBuffer);
      Assert.assertEquals(-1, read);
    }
  }

  private ECBlockInputStreamProxy createBISProxy(ECReplicationConfig rConfig,
      OmKeyLocationInfo blockInfo) {
    return new ECBlockInputStreamProxy(
        rConfig, blockInfo, true, null, null, streamFactory);
  }

  private static class TestECBlockInputStreamFactory
      implements ECBlockInputStreamFactory {

    private ByteBuffer data;

    private Map<Boolean, ECStreamTestUtil.TestBlockInputStream> streams
        = new HashMap<>();

    public void setData(ByteBuffer data) {
      this.data = data;
    }

    public Map<Boolean, ECStreamTestUtil.TestBlockInputStream> getStreams() {
      return streams;
    }

    @Override
    public BlockExtendedInputStream create(boolean missingLocations,
        ReplicationConfig repConfig, OmKeyLocationInfo blockInfo,
        boolean verifyChecksum, XceiverClientFactory xceiverFactory,
        Function<BlockID, Pipeline> refreshFunction) {
      ByteBuffer wrappedBuffer =
          ByteBuffer.wrap(data.array(), 0, data.capacity());
      ECStreamTestUtil.TestBlockInputStream is =
          new ECStreamTestUtil.TestBlockInputStream(blockInfo.getBlockID(),
              blockInfo.getLength(), wrappedBuffer);
      streams.put(missingLocations, is);
      return is;
    }
  }

}
