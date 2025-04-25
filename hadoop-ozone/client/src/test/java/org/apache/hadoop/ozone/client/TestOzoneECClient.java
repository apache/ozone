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

package org.apache.hadoop.ozone.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfigValidator;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.io.BlockOutputStreamEntry;
import org.apache.hadoop.ozone.client.io.BlockStreamAccessor;
import org.apache.hadoop.ozone.client.io.ECKeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ozone.erasurecode.rawcoder.RSRawErasureCoderFactory;
import org.apache.ozone.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Real unit test for OzoneECClient.
 * <p>
 * Used for testing Ozone client without external network calls.
 */
public class TestOzoneECClient {
  private int chunkSize = 1024;
  private int dataBlocks = 3;
  private int parityBlocks = 2;
  private int inputSize = chunkSize * dataBlocks;
  private OzoneClient client;
  private ObjectStore store;
  private String keyName = UUID.randomUUID().toString();
  private String volumeName = UUID.randomUUID().toString();
  private String bucketName = UUID.randomUUID().toString();
  private byte[][] inputChunks = new byte[dataBlocks][chunkSize];
  private final MockXceiverClientFactory factoryStub =
      new MockXceiverClientFactory();
  private OzoneConfiguration conf = createConfiguration();
  private MultiNodePipelineBlockAllocator allocator =
      new MultiNodePipelineBlockAllocator(conf, dataBlocks + parityBlocks, 15);
  private final MockOmTransport transportStub = new MockOmTransport(allocator);
  private final RawErasureEncoder encoder =
      new RSRawErasureCoderFactory().createEncoder(
          new ECReplicationConfig(dataBlocks, parityBlocks));

  @BeforeEach
  public void init() throws IOException {
    createNewClient(conf, transportStub);
  }

  private void createNewClient(ConfigurationSource config,
      MockBlockAllocator blkAllocator) throws IOException {
    createNewClient(config, new MockOmTransport(blkAllocator));
  }

  private void createNewClient(ConfigurationSource config,
      final MockOmTransport transport) throws IOException {
    client = new OzoneClient(config, new RpcClient(config, null) {

      @Override
      protected OmTransport createOmTransport(String omServiceId) {
        return transport;
      }

      @Override
      protected XceiverClientFactory createXceiverClientFactory(
          ServiceInfoEx serviceInfo) {
        return factoryStub;
      }
    });

    store = client.getObjectStore();
    initInputChunks();
  }

  private void initInputChunks() {
    for (int i = 0; i < dataBlocks; i++) {
      inputChunks[i] = getBytesWith(i + 1, chunkSize);
    }
  }

  private byte[] getBytesWith(int singleDigitNumber, int total) {
    StringBuilder builder = new StringBuilder(singleDigitNumber);
    for (int i = 1; i <= total; i++) {
      builder.append(singleDigitNumber);
    }
    return builder.toString().getBytes(UTF_8);
  }

  @AfterEach
  public void close() throws IOException {
    client.close();
  }

  @Test
  public void testPutECKeyAndCheckDNStoredData() throws IOException {
    OzoneBucket bucket = writeIntoECKey(inputChunks, keyName, null);
    OzoneKey key = bucket.getKey(keyName);
    assertEquals(keyName, key.getName());
    Map<DatanodeDetails, MockDatanodeStorage> storages =
        factoryStub.getStorages();
    DatanodeDetails[] dnDetails =
        storages.keySet().toArray(new DatanodeDetails[0]);
    Arrays.sort(dnDetails);
    for (int i = 0; i < inputChunks.length; i++) {
      MockDatanodeStorage datanodeStorage = storages.get(dnDetails[i]);
      assertEquals(1, datanodeStorage.getAllBlockData().size());
      ByteString content =
          datanodeStorage.getAllBlockData().values().iterator().next();
      assertEquals(new String(inputChunks[i], UTF_8),
          content.toStringUtf8());
    }
  }

  @Test
  public void testPutECKeyAndCheckParityData() throws IOException {
    OzoneBucket bucket = writeIntoECKey(inputChunks, keyName, null);
    final ByteBuffer[] dataBuffers = new ByteBuffer[dataBlocks];
    for (int i = 0; i < inputChunks.length; i++) {
      dataBuffers[i] = ByteBuffer.wrap(inputChunks[i]);
    }
    final ByteBuffer[] parityBuffers = new ByteBuffer[parityBlocks];
    for (int i = 0; i < parityBlocks; i++) {
      parityBuffers[i] = ByteBuffer.allocate(chunkSize);
    }
    encoder.encode(dataBuffers, parityBuffers);
    OzoneKey key = bucket.getKey(keyName);
    assertEquals(keyName, key.getName());
    Map<DatanodeDetails, MockDatanodeStorage> storages =
        factoryStub.getStorages();
    DatanodeDetails[] dnDetails =
        storages.keySet().toArray(new DatanodeDetails[0]);
    Arrays.sort(dnDetails);

    for (int i = dataBlocks; i < parityBlocks + dataBlocks; i++) {
      MockDatanodeStorage datanodeStorage = storages.get(dnDetails[i]);
      assertEquals(1, datanodeStorage.getAllBlockData().size());
      ByteString content =
          datanodeStorage.getAllBlockData().values().iterator().next();
      assertEquals(
          new String(parityBuffers[i - dataBlocks].array(), UTF_8),
          content.toStringUtf8());
    }

  }

  @Test
  public void testPutECKeyAndReadContent() throws IOException {
    OzoneBucket bucket = writeIntoECKey(inputChunks, keyName, null);
    OzoneKey key = bucket.getKey(keyName);
    assertEquals(keyName, key.getName());
    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] fileContent = new byte[chunkSize];
      for (int i = 0; i < dataBlocks; i++) {
        assertEquals(inputChunks[i].length, is.read(fileContent));
        assertArrayEquals(fileContent, inputChunks[i]);
      }
      // A further read should give EOF
      assertEquals(-1, is.read(fileContent));
    }
  }

  @Test
  public void testCreateBucketWithDefaultReplicationConfig()
      throws IOException {
    final OzoneBucket bucket = writeIntoECKey(inputChunks, keyName,
        new DefaultReplicationConfig(
            new ECReplicationConfig(dataBlocks, parityBlocks,
                ECReplicationConfig.EcCodec.RS, chunkSize)));

    // create key without mentioning replication config. Since we set EC
    // replication in bucket, key should be EC key.
    try (OzoneOutputStream out = bucket.createKey("mykey", inputSize)) {
      assertInstanceOf(ECKeyOutputStream.class, out.getOutputStream());
      for (byte[] inputChunk : inputChunks) {
        out.write(inputChunk);
      }
    }
  }

  @Test
  public void test4ChunksInSingleWriteOp() throws IOException {
    testMultipleChunksInSingleWriteOp(4);
  }

  // Test random number of chunks in single write op.
  @Test
  public void test5ChunksInSingleWriteOp() throws IOException {
    testMultipleChunksInSingleWriteOp(5);
  }

  @Test
  public void test6ChunksInSingleWriteOp() throws IOException {
    testMultipleChunksInSingleWriteOp(6);
  }

  @Test
  public void test7ChunksInSingleWriteOp() throws IOException {
    testMultipleChunksInSingleWriteOp(7);
  }

  @Test
  public void test9ChunksInSingleWriteOp() throws IOException {
    testMultipleChunksInSingleWriteOp(9);
  }

  @Test
  public void test10ChunksInSingleWriteOp() throws IOException {
    testMultipleChunksInSingleWriteOp(10);
  }

  @Test
  public void testChunksInSingleWriteOpWithOffset() throws IOException {
    testMultipleChunksInSingleWriteOp(100, 12, 11);
  }

  @Test
  public void test12ChunksInSingleWriteOp() throws IOException {
    testMultipleChunksInSingleWriteOp(12);
  }

  private void testMultipleChunksInSingleWriteOp(int numChunks)
          throws IOException {
    testMultipleChunksInSingleWriteOp(0, numChunks, numChunks);
  }

  private void testMultipleChunksInSingleWriteOp(int offset, int bufferChunks,
                                                 int numChunks)
      throws IOException {
    byte[] inputData = new byte[offset + bufferChunks * chunkSize];
    for (int i = 0; i < numChunks; i++) {
      int start = offset + (i * chunkSize);
      Arrays.fill(inputData, start, start + chunkSize - 1,
          String.valueOf(i % 9).getBytes(UTF_8)[0]);
    }
    final OzoneBucket bucket = writeIntoECKey(offset, numChunks * chunkSize,
            inputData, keyName, new DefaultReplicationConfig(
                    new ECReplicationConfig(dataBlocks, parityBlocks,
                            ECReplicationConfig.EcCodec.RS, chunkSize)));
    OzoneKey key = bucket.getKey(keyName);
    validateContent(offset, numChunks * chunkSize, inputData, bucket, key);
  }

  private void validateContent(byte[] inputData,
                               OzoneBucket bucket,
                               OzoneKey key) throws IOException {
    validateContent(0, inputData.length, inputData, bucket, key);
  }

  private void validateContent(int offset, int length, byte[] inputData,
                               OzoneBucket bucket,
      OzoneKey key) throws IOException {
    assertEquals(keyName, key.getName());
    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] fileContent = new byte[length];
      assertEquals(length, is.read(fileContent));
      assertEquals(new String(Arrays.copyOfRange(inputData, offset,
                      offset + length),
                      UTF_8),
          new String(fileContent, UTF_8));
    }
  }

  @Test
  public void testSmallerThanChunkSize() throws IOException {
    byte[] firstSmallChunk = new byte[chunkSize - 1];
    Arrays.fill(firstSmallChunk, 0, firstSmallChunk.length - 1,
        Byte.parseByte("1"));

    writeIntoECKey(firstSmallChunk, keyName,
        new DefaultReplicationConfig(
            new ECReplicationConfig(dataBlocks, parityBlocks,
                ECReplicationConfig.EcCodec.RS, chunkSize)));
    OzoneManagerProtocolProtos.KeyLocationList blockList =
        transportStub.getKeys().get(volumeName).get(bucketName).get(keyName)
            .getKeyLocationListList().get(0);

    Map<DatanodeDetails, MockDatanodeStorage> storages =
        factoryStub.getStorages();
    OzoneManagerProtocolProtos.KeyLocation keyLocations =
        blockList.getKeyLocations(0);

    List<MockDatanodeStorage> dns = new ArrayList<>();
    for (int i = 0; i < dataBlocks + parityBlocks; i++) {
      HddsProtos.DatanodeDetailsProto member =
          blockList.getKeyLocations(0).getPipeline().getMembers(i);
      MockDatanodeStorage mockDatanodeStorage =
          storages.get(getMatchingStorage(storages, member.getUuid()));
      dns.add(mockDatanodeStorage);
    }
    String firstBlockData = dns.get(0).getFullBlockData(new BlockID(
        keyLocations.getBlockID().getContainerBlockID().getContainerID(),
        keyLocations.getBlockID().getContainerBlockID().getLocalID()));

    assertArrayEquals(
        firstSmallChunk, firstBlockData.getBytes(UTF_8));

    final ByteBuffer[] dataBuffers = new ByteBuffer[dataBlocks];
    dataBuffers[0] = ByteBuffer.wrap(firstSmallChunk);
    //Let's pad the remaining length equal to firstSmall chunk len
    for (int i = 1; i < dataBlocks; i++) {
      dataBuffers[i] = ByteBuffer.allocate(firstSmallChunk.length);
      Arrays.fill(dataBuffers[i].array(), 0, firstSmallChunk.length, (byte) 0);
    }

    final ByteBuffer[] parityBuffers = new ByteBuffer[parityBlocks];
    for (int i = 0; i < parityBlocks; i++) {
      parityBuffers[i] = ByteBuffer.allocate(firstSmallChunk.length);
    }
    encoder.encode(dataBuffers, parityBuffers);

    //Lets assert the parity data.
    for (int i = dataBlocks; i < dataBlocks + parityBlocks; i++) {
      String parityBlockData = dns.get(i).getFullBlockData(new BlockID(
          keyLocations.getBlockID().getContainerBlockID().getContainerID(),
          keyLocations.getBlockID().getContainerBlockID().getLocalID()));
      String expected =
          new String(parityBuffers[i - dataBlocks].array(), UTF_8);
      assertEquals(expected, parityBlockData);
      assertEquals(expected.length(), parityBlockData.length());

    }
  }

  @Test
  public void testPutBlockHasBlockGroupLen() throws IOException {
    OzoneBucket bucket = writeIntoECKey(inputChunks, keyName, null);
    OzoneKey key = bucket.getKey(keyName);
    assertEquals(keyName, key.getName());
    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] fileContent = new byte[chunkSize];
      for (int i = 0; i < dataBlocks; i++) {
        assertEquals(inputChunks[i].length, is.read(fileContent));
        assertArrayEquals(fileContent, inputChunks[i]);
      }

      Map<DatanodeDetails, MockDatanodeStorage> storages =
          factoryStub.getStorages();
      OzoneManagerProtocolProtos.KeyLocationList blockList =
          transportStub.getKeys().get(volumeName).get(bucketName).get(keyName).
              getKeyLocationListList().get(0);

      // Check all node putBlock requests has block group length included.
      for (int i = 0; i < dataBlocks + parityBlocks; i++) {
        MockDatanodeStorage mockDatanodeStorage = storages.get(
            getMatchingStorage(storages,
                blockList.getKeyLocations(0).getPipeline().getMembers(i)
                    .getUuid()));
        final OzoneKeyDetails keyDetails = bucket.getKey(keyName);

        ContainerProtos.BlockData block = mockDatanodeStorage.getBlock(
            ContainerProtos.DatanodeBlockID.newBuilder().setContainerID(
                keyDetails.getOzoneKeyLocations().get(0).getContainerID())
                .setLocalID(
                    keyDetails.getOzoneKeyLocations().get(0).getLocalID())
                .setBlockCommitSequenceId(1).setReplicaIndex(
                blockList.getKeyLocations(0).getPipeline()
                    .getMemberReplicaIndexes(i)).build());

        List<ContainerProtos.KeyValue> metadataList =
            block.getMetadataList().stream().filter(kv -> kv.getKey()
                .equals(OzoneConsts.BLOCK_GROUP_LEN_KEY_IN_PUT_BLOCK))
                .collect(Collectors.toList());

        assertEquals(3L * chunkSize,
            Long.parseLong(metadataList.get(0).getValue()));
      }
    }
  }

  private static DatanodeDetails getMatchingStorage(
      Map<DatanodeDetails, MockDatanodeStorage> storages, String uuid) {
    Iterator<DatanodeDetails> iterator = storages.keySet().iterator();
    while (iterator.hasNext()) {
      DatanodeDetails dn = iterator.next();
      if (dn.getUuid().toString().equals(uuid)) {
        return dn;
      }
    }
    return null;
  }

  @Test
  public void testMultipleChunksWithPartialChunkInSingleWriteOp()
      throws IOException {
    final int partialChunkLen = 10;
    final int numFullChunks = 9;
    final int inputBuffLen = (numFullChunks * chunkSize) + partialChunkLen;
    byte[] inputData = new byte[inputBuffLen];
    for (int i = 0; i < numFullChunks; i++) {
      int start = (i * chunkSize);
      Arrays.fill(inputData, start, start + chunkSize - 1,
          String.valueOf(i).getBytes(UTF_8)[0]);
    }
    //fill the last partial chunk as well.
    Arrays.fill(inputData, (numFullChunks * chunkSize),
        ((numFullChunks * chunkSize)) + partialChunkLen - 1, (byte) 1);
    final OzoneBucket bucket = writeIntoECKey(inputData, keyName,
        new DefaultReplicationConfig(
            new ECReplicationConfig(dataBlocks, parityBlocks,
                ECReplicationConfig.EcCodec.RS, chunkSize)));
    OzoneKey key = bucket.getKey(keyName);
    validateContent(inputData, bucket, key);
  }

  @Test
  public void testCommitKeyInfo()
      throws IOException {
    final OzoneBucket bucket = writeIntoECKey(inputChunks, keyName,
        new DefaultReplicationConfig(
            new ECReplicationConfig(dataBlocks, parityBlocks,
                ECReplicationConfig.EcCodec.RS, chunkSize)));

    // create key without mentioning replication config. Since we set EC
    // replication in bucket, key should be EC key.
    try (OzoneOutputStream out = bucket.createKey("mykey", 6 * inputSize)) {
      assertInstanceOf(ECKeyOutputStream.class, out.getOutputStream());
      // Block Size is 2kb, so to create 3 blocks we need 6 iterations here
      for (int j = 0; j < 6; j++) {
        for (byte[] inputChunk : inputChunks) {
          out.write(inputChunk);
        }
      }
    }
    OzoneManagerProtocolProtos.KeyLocationList blockList =
        transportStub.getKeys().get(volumeName).get(bucketName).get("mykey")
            .getKeyLocationListList().get(0);

    assertEquals(3, blockList.getKeyLocationsCount());
    // As the mock allocator allocates block with id's increasing sequentially
    // from 1. Therefore the block should be in the order with id starting 1, 2
    // and then 3.
    for (int i = 0; i < 3; i++) {
      long localId = blockList.getKeyLocationsList().get(i).getBlockID()
          .getContainerBlockID().getLocalID();
      assertEquals(i + 1, localId);
    }

    assertEquals(1,
        transportStub.getKeys().get(volumeName).get(bucketName).get("mykey")
            .getKeyLocationListCount());
    assertEquals(inputChunks[0].length * 3 * 6,
        transportStub.getKeys().get(volumeName).get(bucketName).get("mykey")
            .getDataSize());
  }

  @Test
  public void testPartialStripeWithSingleChunkAndPadding() throws IOException {
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    try (OzoneOutputStream out = bucket.createKey(keyName, inputSize,
        new ECReplicationConfig(dataBlocks, parityBlocks,
            ECReplicationConfig.EcCodec.RS, chunkSize), new HashMap<>())) {
      for (int i = 0; i < inputChunks[0].length; i++) {
        out.write(inputChunks[0][i]);
      }
    }
    OzoneKey key = bucket.getKey(keyName);
    validateContent(inputChunks[0], bucket, key);
  }

  @Test
  public void testPartialStripeLessThanSingleChunkWithPadding()
      throws IOException {
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    try (OzoneOutputStream out = bucket.createKey(keyName, inputSize,
        new ECReplicationConfig(dataBlocks, parityBlocks,
            ECReplicationConfig.EcCodec.RS, chunkSize), new HashMap<>())) {
      for (int i = 0; i < inputChunks[0].length - 1; i++) {
        out.write(inputChunks[0][i]);
      }
    }
    OzoneKey key = bucket.getKey(keyName);
    validateContent(Arrays.copyOf(inputChunks[0], inputChunks[0].length - 1),
        bucket, key);
  }

  @Test
  public void testPartialStripeWithPartialLastChunk()
      throws IOException {
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    // Last chunk is one byte short of the others.
    byte[] lastChunk =
        Arrays.copyOf(inputChunks[inputChunks.length - 1],
            inputChunks[inputChunks.length - 1].length - 1);

    int inSize = chunkSize * (inputChunks.length - 1) + lastChunk.length;
    try (OzoneOutputStream out = bucket.createKey(keyName, inSize,
        new ECReplicationConfig(dataBlocks, parityBlocks,
            ECReplicationConfig.EcCodec.RS, chunkSize), new HashMap<>())) {
      for (int i = 0; i < inputChunks.length - 1; i++) {
        out.write(inputChunks[i]);
      }

      for (byte chunkData : lastChunk) {
        out.write(chunkData);
      }
    }

    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] fileContent = new byte[chunkSize];
      for (int i = 0; i < 2; i++) {
        assertEquals(inputChunks[i].length, is.read(fileContent));
        assertArrayEquals(inputChunks[i], fileContent);
      }
      assertEquals(lastChunk.length, is.read(fileContent));
      assertArrayEquals(lastChunk, Arrays.copyOf(fileContent, lastChunk.length));
      // A further read should give EOF
      assertEquals(-1, is.read(fileContent));
    }
  }

  @Test
  public void test10D4PConfigWithPartialStripe()
      throws IOException {
    // A large block size try to trigger potential overflow
    // refer to: HDDS-6295
    conf.set(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE,
        OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT);
    int dataBlks = 10;
    int parityBlks = 4;
    MultiNodePipelineBlockAllocator blkAllocator =
        new MultiNodePipelineBlockAllocator(conf, dataBlks + parityBlks, 14);
    createNewClient(conf, blkAllocator);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    // A partial chunk to trigger partialStripe check
    // in ECKeyOutputStream.close()
    int inSize = chunkSize - 1;
    byte[] partialChunk = new byte[inSize];

    try (OzoneOutputStream out = bucket.createKey(keyName, inSize,
        new ECReplicationConfig(dataBlks, parityBlks,
            ECReplicationConfig.EcCodec.RS, chunkSize), new HashMap<>())) {
      out.write(partialChunk);
    }

    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] fileContent = new byte[chunkSize];
      assertEquals(inSize, is.read(fileContent));
      assertArrayEquals(partialChunk, Arrays.copyOf(fileContent, inSize));
    }
  }

  @Test
  public void testWriteShouldFailIfMoreThanParityNodesFail()
      throws Exception {
    testNodeFailuresWhileWriting(new int[] {0, 1, 2}, 3, 2);
  }

  @Test
  public void testWriteShouldSuccessIfLessThanParityNodesFail()
      throws Exception {
    testNodeFailuresWhileWriting(new int[] {0}, 2, 2);
  }

  @Test
  public void testWriteShouldSuccessIf4NodesFailed() throws Exception {
    testNodeFailuresWhileWriting(new int[] {0, 1, 2, 3}, 1, 2);
  }

  @Test
  public void testWriteShouldSuccessWithAdditional1BlockGroupAfterFailure()
      throws Exception {
    testNodeFailuresWhileWriting(new int[] {0, 1, 2, 3}, 10, 3);
  }

  @Test
  public void testStripeWriteRetriesOn2Failures() throws Exception {
    OzoneConfiguration con = createConfiguration();
    // Cluster has 15 nodes. So, first we will create 3 block groups with
    // distinct nodes in each. Block Group 1:  0-4, Block Group 2: 5-9, Block
    // Group 3: 10-14
    // To mark the node failed in the second block group.
    int[] nodesIndexesToMarkFailure = new int[2];
    nodesIndexesToMarkFailure[0] = 0;
    // To mark the node failed in the second block group also.
    nodesIndexesToMarkFailure[1] = 5;
    // Mocked MultiNodePipelineBlockAllocator#allocateBlock implementation
    // should pick next good block group as we have 15 nodes.
    int clusterSize = 15;
    testStripeWriteRetriesOnFailures(con, clusterSize,
        nodesIndexesToMarkFailure);
    // It should have used 3rd block group also. So, total initialized nodes
    // count should be clusterSize.
    assertEquals(clusterSize, factoryStub.getStorages().size());
  }

  @Test
  public void testStripeWriteRetriesOn3Failures() throws Exception {
    OzoneConfiguration con = createConfiguration();

    int[] nodesIndexesToMarkFailure = new int[3];
    nodesIndexesToMarkFailure[0] = 0;
    // To mark the node failed in the second block group.
    nodesIndexesToMarkFailure[1] = 5;
    // To mark the node failed in the third block group.
    nodesIndexesToMarkFailure[2] = 10;
    // Mocked MultiNodePipelineBlockAllocator#allocateBlock implementation will
    // pick the remaining goods for the next block group.
    int clusterSize = 15;
    testStripeWriteRetriesOnFailures(con, clusterSize,
        nodesIndexesToMarkFailure);
    // It should have used 3rd block group also. So, total initialized nodes
    // count should be clusterSize.
    assertEquals(clusterSize, factoryStub.getStorages().size());
  }

  // The mocked impl throws IllegalStateException when there are not enough
  // nodes in allocateBlock request.
  @Test
  public void testStripeWriteRetriesOnAllNodeFailures() throws Exception {
    OzoneConfiguration con = createConfiguration();

    // After writing first stripe, we will mark all nodes as bad in the cluster.
    int clusterSize = 5;
    int[] nodesIndexesToMarkFailure = new int[clusterSize];
    for (int i = 0; i < nodesIndexesToMarkFailure.length; i++) {
      nodesIndexesToMarkFailure[i] = i;
    }
    // Mocked MultiNodePipelineBlockAllocator#allocateBlock implementation can
    // not pick new block group as all nodes in cluster marked as bad.
    assertThrows(IllegalStateException.class, () ->
        testStripeWriteRetriesOnFailures(con, clusterSize,
            nodesIndexesToMarkFailure));
  }

  @Test
  public void testStripeWriteRetriesOn4FailuresWith3RetriesAllowed()
      throws Exception {
    OzoneConfiguration con = createConfiguration();
    con.setInt(OzoneConfigKeys.OZONE_CLIENT_MAX_EC_STRIPE_WRITE_RETRIES, 3);

    int[] nodesIndexesToMarkFailure = new int[4];
    nodesIndexesToMarkFailure[0] = 0;
    //To mark node failed in second block group.
    nodesIndexesToMarkFailure[1] = 5;
    //To mark node failed in third block group.
    nodesIndexesToMarkFailure[2] = 10;
    //To mark node failed in fourth block group.
    nodesIndexesToMarkFailure[3] = 15;
    // Mocked MultiNodePipelineBlockAllocator#allocateBlock implementation can
    // pick good block group, but client retries should be limited
    // OZONE_CLIENT_MAX_EC_STRIPE_WRITE_RETRIES_ON_FAILURE(here it was
    // configured as 3). So, it should fail as we have marked 3 nodes as bad.
    IOException e = assertThrows(IOException.class,
            () -> testStripeWriteRetriesOnFailures(con, 20, nodesIndexesToMarkFailure));
    assertEquals(
        "Completed max allowed retries 3 on stripe failures.",
        e.getMessage());
  }

  public void testStripeWriteRetriesOnFailures(OzoneConfiguration con,
      int clusterSize, int[] nodesIndexesToMarkFailure) throws Exception {
    close();
    MultiNodePipelineBlockAllocator blkAllocator =
        new MultiNodePipelineBlockAllocator(con, dataBlocks + parityBlocks,
            clusterSize);
    createNewClient(con, blkAllocator);
    int numChunksToWriteAfterFailure = 3;
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    try (OzoneOutputStream out = bucket.createKey(keyName, 1024 * 3,
        new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
            chunkSize), new HashMap<>())) {
      for (int i = 0; i < dataBlocks; i++) {
        out.write(inputChunks[i]);
      }
      waitForFlushingThreadToFinish((ECKeyOutputStream) out.getOutputStream());
      assertEquals(5, factoryStub.getStorages().size());
      List<DatanodeDetails> failedDNs = new ArrayList<>();
      List<HddsProtos.DatanodeDetailsProto> dns = blkAllocator.getClusterDns();

      for (int nodeIndex : nodesIndexesToMarkFailure) {
        failedDNs.add(DatanodeDetails.getFromProtoBuf(dns.get(nodeIndex)));
      }

      // First let's set storage as bad
      factoryStub.setFailedStorages(failedDNs);

      // Writer should be able to write by using 3rd block group.
      for (int i = 0; i < numChunksToWriteAfterFailure; i++) {
        out.write(inputChunks[i]);
      }
    }
    final OzoneKeyDetails key = bucket.getKey(keyName);
    // Data supposed to store in single block group. Since we introduced the
    // failures after first stripe, the second stripe data should have been
    // written into new blockgroup. So, we should have 2 block groups. That
    // means two keyLocations.
    assertEquals(2, key.getOzoneKeyLocations().size());
    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] fileContent = new byte[chunkSize];
      for (int i = 0; i < dataBlocks; i++) {
        assertEquals(inputChunks[i].length, is.read(fileContent));
        assertArrayEquals(inputChunks[i], fileContent);
      }
      for (int i = 0; i < numChunksToWriteAfterFailure; i++) {
        assertEquals(inputChunks[i].length, is.read(fileContent));
        assertArrayEquals(inputChunks[i], fileContent);
      }
    }
  }

  public void testNodeFailuresWhileWriting(int[] nodesIndexesToMarkFailure,
      int numChunksToWriteAfterFailure, int numExpectedBlockGrps)
      throws Exception {
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    try (OzoneOutputStream out = bucket.createKey(keyName, 1024 * 3,
        new ECReplicationConfig(3, 2,
            ECReplicationConfig.EcCodec.RS,
            chunkSize), new HashMap<>())) {
      for (int i = 0; i < dataBlocks; i++) {
        out.write(inputChunks[i]);
      }
      waitForFlushingThreadToFinish((ECKeyOutputStream) out.getOutputStream());

      List<DatanodeDetails> failedDNs = new ArrayList<>();
      List<HddsProtos.DatanodeDetailsProto> dns = allocator.getClusterDns();
      for (int nodeIndex : nodesIndexesToMarkFailure) {
        failedDNs.add(DatanodeDetails.getFromProtoBuf(dns.get(nodeIndex)));
      }

      // First let's set storage as bad
      factoryStub.setFailedStorages(failedDNs);

      for (int i = 0; i < numChunksToWriteAfterFailure; i++) {
        out.write(inputChunks[i % dataBlocks]);
      }
    }
    final OzoneKeyDetails key = bucket.getKey(keyName);
    // Data supposed to store in single block group. Since we introduced the
    // failures after first stripe, the second stripe data should have been
    // written into new block group. So, we should have numExpectedBlockGrps.
    // That means two keyLocations.
    assertEquals(numExpectedBlockGrps, key.getOzoneKeyLocations().size());
    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] fileContent = new byte[chunkSize];
      for (int i = 0; i < dataBlocks; i++) {
        assertEquals(inputChunks[i].length, is.read(fileContent));
        assertArrayEquals(inputChunks[i], fileContent);
      }
      for (int i = 0; i < numChunksToWriteAfterFailure; i++) {
        assertEquals(inputChunks[i % dataBlocks].length,
            is.read(fileContent));
        assertArrayEquals(inputChunks[i % dataBlocks], fileContent);
      }
    }
  }

  @Test
  public void testExcludeOnDNFailure() throws Exception {
    testExcludeFailedDN(IntStream.range(0, 5), IntStream.empty());
  }

  @Test
  public void testExcludeOnDNClosed() throws Exception {
    testExcludeFailedDN(IntStream.empty(), IntStream.range(0, 5));
  }

  @Test
  public void testExcludeOnDNMixed() throws Exception {
    testExcludeFailedDN(IntStream.range(0, 3), IntStream.range(3, 5));
  }

  private void testExcludeFailedDN(IntStream failedDNIndex,
      IntStream closedDNIndex) throws Exception {
    close();
    OzoneConfiguration con = createConfiguration();
    MultiNodePipelineBlockAllocator blkAllocator =
        new MultiNodePipelineBlockAllocator(con, dataBlocks + parityBlocks, 10);
    createNewClient(con, blkAllocator);

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    ECReplicationConfig repConfig = new ECReplicationConfig(
        dataBlocks, parityBlocks, ECReplicationConfig.EcCodec.RS, chunkSize);

    try (OzoneOutputStream out = bucket.createKey(keyName,
        2L * dataBlocks * chunkSize, repConfig, new HashMap<>())) {

      ECKeyOutputStream ecKeyOut = assertInstanceOf(ECKeyOutputStream.class, out.getOutputStream());

      List<HddsProtos.DatanodeDetailsProto> dns = blkAllocator.getClusterDns();

      for (int i = 0; i < dataBlocks; i++) {
        out.write(inputChunks[i % dataBlocks]);
      }
      waitForFlushingThreadToFinish((ECKeyOutputStream) out.getOutputStream());

      // Then let's mark datanodes with closed container
      List<DatanodeDetails> closedDNs = closedDNIndex
          .mapToObj(i -> DatanodeDetails.getFromProtoBuf(dns.get(i)))
          .collect(Collectors.toList());
      factoryStub.mockStorageFailure(closedDNs,
          new ContainerNotOpenException("Mocked"));

      // Then let's mark failed datanodes
      List<DatanodeDetails> failedDNs = failedDNIndex
          .mapToObj(i -> DatanodeDetails.getFromProtoBuf(dns.get(i)))
          .collect(Collectors.toList());
      factoryStub.setFailedStorages(failedDNs);

      for (int i = 0; i < dataBlocks; i++) {
        out.write(inputChunks[i % dataBlocks]);
      }
      waitForFlushingThreadToFinish((ECKeyOutputStream) out.getOutputStream());

      // Assert excludeList only includes failedDNs
      assertArrayEquals(failedDNs.toArray(new DatanodeDetails[0]),
          ecKeyOut.getExcludeList().getDatanodes()
              .toArray(new DatanodeDetails[0]));
    }
  }

  @Test
  public void testLargeWriteOfMultipleStripesWithStripeFailure()
      throws Exception {
    close();
    OzoneConfiguration con = createConfiguration();
    // block size of 3KB could hold 3 full stripes
    con.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 3, StorageUnit.KB);
    con.setInt(OzoneConfigKeys.OZONE_CLIENT_MAX_EC_STRIPE_WRITE_RETRIES, 3);
    MultiNodePipelineBlockAllocator blkAllocator =
        new MultiNodePipelineBlockAllocator(con, dataBlocks + parityBlocks,
            15);
    createNewClient(con, blkAllocator);

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    // should write > 1 full stripe to trigger potential issue
    int numFullStripesBeforeFailure = 2;
    int numChunksToWriteAfterFailure = dataBlocks;
    int numExpectedBlockGrps = 2;
    // fail the DNs for parity blocks
    int[] nodesIndexesToMarkFailure = {3, 4};

    try (OzoneOutputStream out = bucket.createKey(keyName,
        1024 * dataBlocks * numFullStripesBeforeFailure
            + numChunksToWriteAfterFailure,
        new ECReplicationConfig(dataBlocks, parityBlocks,
            ECReplicationConfig.EcCodec.RS,
            chunkSize), new HashMap<>())) {
      for (int j = 0; j < numFullStripesBeforeFailure; j++) {
        for (int i = 0; i < dataBlocks; i++) {
          out.write(inputChunks[i]);
        }
      }
      waitForFlushingThreadToFinish((ECKeyOutputStream) out.getOutputStream());

      List<DatanodeDetails> failedDNs = new ArrayList<>();
      List<HddsProtos.DatanodeDetailsProto> dns = allocator.getClusterDns();
      for (int nodeIndex : nodesIndexesToMarkFailure) {
        failedDNs.add(DatanodeDetails.getFromProtoBuf(dns.get(nodeIndex)));
      }

      // First let's set storage as bad
      factoryStub.setFailedStorages(failedDNs);

      for (int i = 0; i < numChunksToWriteAfterFailure; i++) {
        out.write(inputChunks[i % dataBlocks]);
      }
    }

    final OzoneKeyDetails key = bucket.getKey(keyName);
    // Data supposed to store in single block group. Since we introduced the
    // failures after first stripe, the second stripe data should have been
    // written into new block group. So, we should have numExpectedBlockGrps.
    // That means two keyLocations.
    assertEquals(numExpectedBlockGrps, key.getOzoneKeyLocations().size());
    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] fileContent = new byte[chunkSize];
      for (int i = 0; i < dataBlocks * numFullStripesBeforeFailure; i++) {
        assertEquals(inputChunks[i % dataBlocks].length,
            is.read(fileContent));
        assertArrayEquals(inputChunks[i % dataBlocks], fileContent);
      }
      for (int i = 0; i < numChunksToWriteAfterFailure; i++) {
        assertEquals(inputChunks[i % dataBlocks].length,
            is.read(fileContent));
        assertArrayEquals(inputChunks[i % dataBlocks], fileContent);
      }
    }
  }

  @Test
  public void testPartialStripeWithPartialChunkRetry()
      throws IOException {
    close();
    OzoneConfiguration con = createConfiguration();
    // block size of 3KB could hold 3 full stripes
    con.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 3, StorageUnit.KB);
    con.setInt(OzoneConfigKeys.OZONE_CLIENT_MAX_EC_STRIPE_WRITE_RETRIES, 3);
    MultiNodePipelineBlockAllocator blkAllocator =
        new MultiNodePipelineBlockAllocator(con, dataBlocks + parityBlocks, 15);
    createNewClient(con, blkAllocator);

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    int numFullChunks = 7;
    //Prepare additional partial chunk.
    int partialChunkSize = 1020;
    byte[] partialChunk = new byte[partialChunkSize];
    Arrays.fill(partialChunk, 0, partialChunk.length, "1".getBytes(UTF_8)[0]);

    // A partial chunk to trigger partialStripe check
    // in ECKeyOutputStream.close()
    int inSize = chunkSize;
    try (OzoneOutputStream out = bucket.createKey(keyName, inSize,
        new ECReplicationConfig(dataBlocks, parityBlocks,
            ECReplicationConfig.EcCodec.RS, chunkSize), new HashMap<>())) {
      for (int i = 0; i < numFullChunks; i++) {
        out.write(inputChunks[i % dataBlocks]);
      }

      out.write(partialChunk);

      int[] nodesIndexesToMarkFailure = new int[] {0, 4};
      List<DatanodeDetails> failedDNs = new ArrayList<>();
      List<HddsProtos.DatanodeDetailsProto> dns = blkAllocator.getClusterDns();
      for (int nodeIndex : nodesIndexesToMarkFailure) {
        failedDNs.add(DatanodeDetails.getFromProtoBuf(dns.get(nodeIndex)));
      }

      // First let's set storage as bad
      factoryStub.setFailedStorages(failedDNs);

    }

    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] fileContent = new byte[chunkSize];
      for (int i = 0; i < numFullChunks; i++) {
        assertEquals(inputChunks[i % dataBlocks].length,
            is.read(fileContent));
        assertArrayEquals(inputChunks[i % dataBlocks], fileContent);
      }

      byte[] partialChunkToRead = new byte[partialChunkSize];
      assertEquals(partialChunkToRead.length, is.read(partialChunkToRead));
      assertArrayEquals(partialChunk, partialChunkToRead);

      assertEquals(-1, is.read(partialChunkToRead));
    }
  }

  @Test
  void testDiscardPreAllocatedBlocksPreventRetryExceeds()
      throws Exception {
    close();
    OzoneConfiguration con = createConfiguration();
    int maxRetries = 3;
    con.setInt(OzoneConfigKeys.OZONE_CLIENT_MAX_EC_STRIPE_WRITE_RETRIES,
        maxRetries);
    MultiNodePipelineBlockAllocator blkAllocator =
        new MultiNodePipelineBlockAllocator(con, dataBlocks + parityBlocks,
            15);
    createNewClient(con, blkAllocator);

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    int numStripesBeforeFailure = 1;
    int numStripesAfterFailure = 1;
    int numStripesTotal = numStripesBeforeFailure + numStripesAfterFailure;
    int numExpectedBlockGrps = 2;
    // fail any DNs to trigger retry
    int[] nodesIndexesToMarkFailure = {0, 1};
    long keySize = (long) chunkSize * dataBlocks * numStripesTotal;

    try (OzoneOutputStream out = bucket.createKey(keyName, keySize,
        new ECReplicationConfig(dataBlocks, parityBlocks,
            ECReplicationConfig.EcCodec.RS,
            chunkSize), new HashMap<>())) {
      ECKeyOutputStream kos = assertInstanceOf(ECKeyOutputStream.class, out.getOutputStream());
      List<OmKeyLocationInfo> blockInfos = getAllLocationInfoList(kos);
      assertEquals(1, blockInfos.size());

      // Mock some pre-allocated blocks to the key,
      // should be > maxRetries
      int numPreAllocatedBlocks = maxRetries + 1;
      BlockID blockID = blockInfos.get(0).getBlockID();
      Pipeline pipeline = blockInfos.get(0).getPipeline();
      List<OmKeyLocationInfo> omKeyLocationInfos = new ArrayList<>();
      for (int i = 0; i < numPreAllocatedBlocks; i++) {
        BlockID nextBlockID = new BlockID(blockID.getContainerID(),
            blockID.getLocalID() + i + 1);
        omKeyLocationInfos.add(new OmKeyLocationInfo.Builder()
            .setBlockID(nextBlockID)
            .setPipeline(pipeline)
            .build());
      }
      OmKeyLocationInfoGroup omKeyLocationInfoGroup =
          new OmKeyLocationInfoGroup(0, omKeyLocationInfos);
      kos.addPreallocateBlocks(omKeyLocationInfoGroup, 0);

      // Good writes
      for (int j = 0; j < numStripesBeforeFailure; j++) {
        for (int i = 0; i < dataBlocks; i++) {
          out.write(inputChunks[i]);
        }
      }
      waitForFlushingThreadToFinish((ECKeyOutputStream) out.getOutputStream());

      // Make the writes fail to trigger retry
      List<DatanodeDetails> failedDNs = new ArrayList<>();
      List<HddsProtos.DatanodeDetailsProto> dns = allocator.getClusterDns();
      for (int nodeIndex : nodesIndexesToMarkFailure) {
        failedDNs.add(DatanodeDetails.getFromProtoBuf(dns.get(nodeIndex)));
      }
      // First let's set storage as bad
      factoryStub.setFailedStorages(failedDNs);

      // Writes that will retry due to failed DNs
      for (int j = 0; j < numStripesAfterFailure; j++) {
        for (int i = 0; i < dataBlocks; i++) {
          out.write(inputChunks[i]);
        }
      }
    }

    final OzoneKeyDetails key = bucket.getKey(keyName);
    assertEquals(numExpectedBlockGrps,
        key.getOzoneKeyLocations().size());

    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] fileContent = new byte[chunkSize];
      for (int i = 0; i < dataBlocks * numStripesTotal; i++) {
        assertEquals(inputChunks[i % dataBlocks].length,
            is.read(fileContent));
        assertArrayEquals(inputChunks[i % dataBlocks], fileContent);
      }
    }
  }

  private OzoneBucket writeIntoECKey(byte[] data, String key,
      DefaultReplicationConfig defaultReplicationConfig) throws IOException {
    return writeIntoECKey(0, data.length, data, key, defaultReplicationConfig);
  }

  private OzoneBucket writeIntoECKey(int offset, int length, byte[] data,
      String key, DefaultReplicationConfig defaultReplicationConfig)
      throws IOException {
    return writeIntoECKey(new int[]{offset}, new int[]{length},
            new byte[][] {data}, key, defaultReplicationConfig);
  }

  private OzoneBucket writeIntoECKey(byte[][] chunks, String key,
      DefaultReplicationConfig defaultReplicationConfig) throws IOException {
    int[] offsets = new int[chunks.length];
    Arrays.fill(offsets, 0);
    int[] lengths = Arrays.stream(chunks)
            .mapToInt(chunk -> chunk.length).toArray();
    return writeIntoECKey(offsets, lengths, chunks,
            key, defaultReplicationConfig);
  }

  private OzoneBucket writeIntoECKey(int[] offsets, int[] lengths,
                                     byte[][] chunks,
                                     String key,
      DefaultReplicationConfig defaultReplicationConfig) throws IOException {
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    if (defaultReplicationConfig != null) {
      final BucketArgs.Builder builder = BucketArgs.newBuilder();
      builder.setDefaultReplicationConfig(defaultReplicationConfig);
      volume.createBucket(bucketName, builder.build());
    } else {
      volume.createBucket(bucketName);
    }
    OzoneBucket bucket = volume.getBucket(bucketName);

    int size = (int) Arrays.stream(chunks).mapToLong(a -> a.length).sum();
    try (OzoneOutputStream out = bucket.createKey(key, size,
        new ECReplicationConfig(dataBlocks, parityBlocks,
            ECReplicationConfig.EcCodec.RS, chunkSize), new HashMap<>())) {
      for (int i = 0; i < chunks.length; i++) {
        out.write(chunks[i], offsets[i], lengths[i]);
      }
    }
    return bucket;
  }

  private List<OmKeyLocationInfo> getAllLocationInfoList(
      ECKeyOutputStream kos) {
    List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
    for (BlockOutputStreamEntry streamEntry : kos.getStreamEntries()) {
      BlockStreamAccessor streamAccessor =
          new BlockStreamAccessor(streamEntry);
      OmKeyLocationInfo info =
          new OmKeyLocationInfo.Builder()
              .setBlockID(streamAccessor.getStreamBlockID())
              .setLength(streamAccessor.getStreamCurrentPosition())
              .setOffset(0)
              .setToken(streamAccessor.getStreamToken())
              .setPipeline(streamAccessor.getStreamPipeline())
              .build();
      locationInfoList.add(info);
    }
    return locationInfoList;
  }

  private static void waitForFlushingThreadToFinish(
      ECKeyOutputStream ecOut) throws Exception {
    final long checkpoint = System.currentTimeMillis();
    ecOut.insertFlushCheckpoint(checkpoint);
    GenericTestUtils.waitFor(() -> ecOut.getFlushCheckpoint() == checkpoint,
        100, 10000);
  }

  private static OzoneConfiguration createConfiguration() {
    OzoneConfiguration conf = new OzoneConfiguration();

    ReplicationConfigValidator validator =
        conf.getObject(ReplicationConfigValidator.class);
    validator.disableValidation();
    conf.setFromObject(validator);

    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 2,
        StorageUnit.KB);

    return conf;
  }
}
