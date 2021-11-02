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

package org.apache.hadoop.ozone.client;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.io.ECKeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ozone.erasurecode.rawcoder.RSRawErasureCoderFactory;
import org.apache.ozone.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;

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
  private final XceiverClientFactory factoryStub =
      new MockXceiverClientFactory();
  private OzoneConfiguration conf = new OzoneConfiguration();
  private MockOmTransport transportStub = new MockOmTransport(
      new MultiNodePipelineBlockAllocator(conf, dataBlocks + parityBlocks));
  private final RawErasureEncoder encoder =
      new RSRawErasureCoderFactory().createEncoder(
          new ECReplicationConfig(dataBlocks, parityBlocks));

  @Before
  public void init() throws IOException {
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 2,
        StorageUnit.KB);

    client = new OzoneClient(conf, new RpcClient(conf, null) {

      @Override
      protected OmTransport createOmTransport(String omServiceId)
          throws IOException {
        return transportStub;
      }

      @Override
      protected XceiverClientFactory createXceiverClientFactory(
          List<X509Certificate> x509Certificates) throws IOException {
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

  @After
  public void close() throws IOException {
    client.close();
  }

  @Test
  public void testPutECKeyAndCheckDNStoredData() throws IOException {
    OzoneBucket bucket = writeIntoECKey(inputChunks, keyName, null);
    OzoneKey key = bucket.getKey(keyName);
    Assert.assertEquals(keyName, key.getName());
    Map<DatanodeDetails, MockDatanodeStorage> storages =
        ((MockXceiverClientFactory) factoryStub).getStorages();
    DatanodeDetails[] dnDetails =
        storages.keySet().toArray(new DatanodeDetails[storages.size()]);
    Arrays.sort(dnDetails);
    for (int i = 0; i < inputChunks.length; i++) {
      MockDatanodeStorage datanodeStorage = storages.get(dnDetails[i]);
      Assert.assertEquals(1, datanodeStorage.getAllBlockData().size());
      ByteString content =
          datanodeStorage.getAllBlockData().values().iterator().next();
      Assert.assertEquals(new String(inputChunks[i], UTF_8),
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
    Assert.assertEquals(keyName, key.getName());
    Map<DatanodeDetails, MockDatanodeStorage> storages =
        ((MockXceiverClientFactory) factoryStub).getStorages();
    DatanodeDetails[] dnDetails =
        storages.keySet().toArray(new DatanodeDetails[storages.size()]);
    Arrays.sort(dnDetails);

    for (int i = dataBlocks; i < parityBlocks + dataBlocks; i++) {
      MockDatanodeStorage datanodeStorage = storages.get(dnDetails[i]);
      Assert.assertEquals(1, datanodeStorage.getAllBlockData().size());
      ByteString content =
          datanodeStorage.getAllBlockData().values().iterator().next();
      Assert.assertEquals(
          new String(parityBuffers[i - dataBlocks].array(), UTF_8),
          content.toStringUtf8());
    }

  }

  @Test
  public void testPutECKeyAndReadContent() throws IOException {
    OzoneBucket bucket = writeIntoECKey(inputChunks, keyName, null);
    OzoneKey key = bucket.getKey(keyName);
    Assert.assertEquals(keyName, key.getName());
    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] fileContent = new byte[chunkSize];
      for (int i=0; i<dataBlocks; i++) {
        Assert.assertEquals(inputChunks[i].length, is.read(fileContent));
        Assert.assertTrue(Arrays.equals(inputChunks[i], fileContent));
      }
      // A further read should give EOF
      Assert.assertEquals(-1, is.read(fileContent));
    }
  }

  @Test
  public void testCreateBucketWithDefaultReplicationConfig()
      throws IOException {
    final OzoneBucket bucket = writeIntoECKey(inputChunks, keyName,
        new DefaultReplicationConfig(ReplicationType.EC,
            new ECReplicationConfig(dataBlocks, parityBlocks,
                ECReplicationConfig.EcCodec.RS, chunkSize)));

    // create key without mentioning replication config. Since we set EC
    // replication in bucket, key should be EC key.
    try (OzoneOutputStream out = bucket.createKey("mykey", inputSize)) {
      Assert.assertTrue(out.getOutputStream() instanceof ECKeyOutputStream);
      for (int i = 0; i < inputChunks.length; i++) {
        out.write(inputChunks[i]);
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
  public void test12ChunksInSingleWriteOp() throws IOException {
    testMultipleChunksInSingleWriteOp(12);
  }

  public void testMultipleChunksInSingleWriteOp(int numChunks)
      throws IOException {
    byte[] inputData = new byte[numChunks * chunkSize];
    for (int i = 0; i < numChunks; i++) {
      int start = (i * chunkSize);
      Arrays.fill(inputData, start, start + chunkSize - 1,
          String.valueOf(i % 9).getBytes(UTF_8)[0]);
    }
    final OzoneBucket bucket = writeIntoECKey(inputData, keyName,
        new DefaultReplicationConfig(ReplicationType.EC,
            new ECReplicationConfig(dataBlocks, parityBlocks,
                ECReplicationConfig.EcCodec.RS, chunkSize)));
    OzoneKey key = bucket.getKey(keyName);
    validateContent(inputData, bucket, key);
  }

  private void validateContent(byte[] inputData, OzoneBucket bucket,
      OzoneKey key) throws IOException {
    Assert.assertEquals(keyName, key.getName());
    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] fileContent = new byte[inputData.length];
      Assert.assertEquals(inputData.length, is.read(fileContent));
      Assert.assertEquals(new String(inputData, UTF_8),
          new String(fileContent, UTF_8));
    }
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
        new DefaultReplicationConfig(ReplicationType.EC,
            new ECReplicationConfig(dataBlocks, parityBlocks,
                ECReplicationConfig.EcCodec.RS, chunkSize)));
    OzoneKey key = bucket.getKey(keyName);
    validateContent(inputData, bucket, key);
  }

  @Test
  public void testCommitKeyInfo()
      throws IOException {
    final OzoneBucket bucket = writeIntoECKey(inputChunks, keyName,
        new DefaultReplicationConfig(ReplicationType.EC,
            new ECReplicationConfig(dataBlocks, parityBlocks,
                ECReplicationConfig.EcCodec.RS, chunkSize)));

    // create key without mentioning replication config. Since we set EC
    // replication in bucket, key should be EC key.
    try (OzoneOutputStream out = bucket.createKey("mykey", 6*inputSize)) {
      Assert.assertTrue(out.getOutputStream() instanceof ECKeyOutputStream);
      // Block Size is 2kb, so to create 3 blocks we need 6 iterations here
      for (int j = 0; j < 6; j++) {
        for (int i = 0; i < inputChunks.length; i++) {
          out.write(inputChunks[i]);
        }
      }
    }
    OzoneManagerProtocolProtos.KeyLocationList blockList =
        transportStub.getKeys().get(volumeName).get(bucketName).get("mykey")
            .getKeyLocationListList().get(0);

    Assert.assertEquals(3, blockList.getKeyLocationsCount());
    // As the mock allocator allocates block with id's increasing sequentially
    // from 1. Therefore the block should be in the order with id starting 1, 2
    // and then 3.
    for (int i = 0; i < 3; i++) {
      long localId = blockList.getKeyLocationsList().get(i).getBlockID()
          .getContainerBlockID().getLocalID();
      Assert.assertEquals(i + 1, localId);
    }

    Assert.assertEquals(1,
        transportStub.getKeys().get(volumeName).get(bucketName).get("mykey")
            .getKeyLocationListCount());
    Assert.assertEquals(inputChunks[0].length * 3 * 6,
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
      for (int i = 0; i < inputChunks[0].length-1; i++) {
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

      for (int i = 0; i < lastChunk.length; i++) {
        out.write(lastChunk[i]);
      }
    }

    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] fileContent = new byte[chunkSize];
      for (int i=0; i<2; i++) {
        Assert.assertEquals(inputChunks[i].length, is.read(fileContent));
        Assert.assertTrue(Arrays.equals(inputChunks[i], fileContent));
      }
      Assert.assertEquals(lastChunk.length, is.read(fileContent));
      Assert.assertTrue(Arrays.equals(lastChunk,
          Arrays.copyOf(fileContent, lastChunk.length)));
      // A further read should give EOF
      Assert.assertEquals(-1, is.read(fileContent));
    }
  }

  private OzoneBucket writeIntoECKey(byte[] data, String key,
      DefaultReplicationConfig defaultReplicationConfig) throws IOException {
    return writeIntoECKey(new byte[][] {data}, key, defaultReplicationConfig);
  }

  private OzoneBucket writeIntoECKey(byte[][] chunks, String key,
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
        out.write(chunks[i]);
      }
    }
    return bucket;
  }
}