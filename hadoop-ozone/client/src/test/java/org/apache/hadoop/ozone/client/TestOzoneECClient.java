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

import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.InMemoryConfiguration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.ErasureCodecOptions;
import org.apache.hadoop.io.erasurecode.codec.RSErasureCodec;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.hadoop.ozone.client.io.ECKeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
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
  private OzoneClient client;
  private ObjectStore store;
  private String keyName = UUID.randomUUID().toString();
  private String volumeName = UUID.randomUUID().toString();
  private String bucketName = UUID.randomUUID().toString();
  private byte[][] inputChunks = new byte[dataBlocks][chunkSize];
  private final XceiverClientFactory factoryStub =
      new MockXceiverClientFactory();
  private final MockOmTransport transportStub = new MockOmTransport(
      new MultiNodePipelineBlockAllocator(dataBlocks + parityBlocks));
  private ECSchema schema = new ECSchema("rs", dataBlocks, parityBlocks);
  private ErasureCodecOptions options = new ErasureCodecOptions(schema);
  private OzoneConfiguration conf = new OzoneConfiguration();
  private RSErasureCodec codec = new RSErasureCodec(conf, options);
  private final RawErasureEncoder encoder = CodecUtil.createRawEncoder(conf,
      SystemErasureCodingPolicies.getPolicies().get(1).getCodecName(),
      codec.getCoderOptions());

  @Before
  public void init() throws IOException {
    ConfigurationSource config = new InMemoryConfiguration();
    client = new OzoneClient(config, new RpcClient(config, null) {

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
    final ByteBuffer[] dataBuffers = new ByteBuffer[3];
    for (int i = 0; i < inputChunks.length; i++) {
      dataBuffers[i] = ByteBuffer.wrap(inputChunks[i]);
    }
    final ByteBuffer[] parityBuffers = new ByteBuffer[parityBlocks];
    for (int i = 0; i < parityBlocks; i++) {
      parityBuffers[i] = ByteBuffer.allocate(1024);
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
      byte[] fileContent = new byte[1024];
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
    try (OzoneOutputStream out = bucket.createKey("mykey", 2000)) {
      Assert.assertTrue(out.getOutputStream() instanceof ECKeyOutputStream);
      for (int i = 0; i < inputChunks.length; i++) {
        out.write(inputChunks[i]);
      }
    }
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
    try (OzoneOutputStream out = bucket.createKey("mykey", 2000)) {
      Assert.assertTrue(out.getOutputStream() instanceof ECKeyOutputStream);
      for (int i = 0; i < inputChunks.length; i++) {
        out.write(inputChunks[i]);
      }
    }
    Assert.assertEquals(1,
        transportStub.getKeys().get(volumeName).get(bucketName).get(keyName)
            .getKeyLocationListCount());
    Assert.assertEquals(inputChunks[0].length * 3,
        transportStub.getKeys().get(volumeName).get(bucketName).get(keyName)
            .getDataSize());
  }

  @Test
  public void testPartialStripeWithSingleChunkAndPadding() throws IOException {
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    try (OzoneOutputStream out = bucket.createKey(keyName, 2000,
        new ECReplicationConfig(dataBlocks, parityBlocks,
            ECReplicationConfig.EcCodec.RS, chunkSize), new HashMap<>())) {
      for (int i = 0; i < inputChunks[0].length; i++) {
        out.write(inputChunks[0][i]);
      }
    }

    OzoneKey key = bucket.getKey(keyName);
    Assert.assertEquals(keyName, key.getName());
    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] fileContent = new byte[1024];
      Assert.assertEquals(inputChunks[0].length, is.read(fileContent));
      Assert.assertEquals(new String(inputChunks[0], UTF_8),
          new String(fileContent, UTF_8));
    }
  }

  @Test
  public void testPartialStripeLessThanSingleChunkWithPadding()
      throws IOException {
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    try (OzoneOutputStream out = bucket.createKey(keyName, 2000,
        new ECReplicationConfig(dataBlocks, parityBlocks,
            ECReplicationConfig.EcCodec.RS, chunkSize), new HashMap<>())) {
      for (int i = 0; i < inputChunks[0].length-1; i++) {
        out.write(inputChunks[0][i]);
      }
    }

    OzoneKey key = bucket.getKey(keyName);
    Assert.assertEquals(keyName, key.getName());
    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] fileContent = new byte[1023];
      Assert.assertEquals(inputChunks[0].length - 1, is.read(fileContent));
      Assert.assertEquals(
          new String(Arrays.copyOf(inputChunks[0], inputChunks[0].length - 1),
              UTF_8), new String(fileContent, UTF_8));
    }
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

    try (OzoneOutputStream out = bucket.createKey(keyName, 2000,
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
      byte[] fileContent = new byte[1024];
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

    try (OzoneOutputStream out = bucket.createKey(key, 2000,
        new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
            chunkSize), new HashMap<>())) {
      for (int i = 0; i < chunks.length; i++) {
        out.write(chunks[i]);
      }
    }
    return bucket;
  }

  private void updatePipelineToKeepSingleNode(int keepingNodeIndex) {
    Map<String, Map<String, Map<String, OzoneManagerProtocolProtos.KeyInfo>>>
        keys = ((MockOmTransport) transportStub).getKeys();
    Map<String, Map<String, OzoneManagerProtocolProtos.KeyInfo>> vol =
        keys.get(keys.keySet().iterator().next());

    Map<String, OzoneManagerProtocolProtos.KeyInfo> buck =
        vol.get(vol.keySet().iterator().next());
    OzoneManagerProtocolProtos.KeyInfo keyInfo =
        buck.get(buck.keySet().iterator().next());
    HddsProtos.Pipeline.Builder builder =
        HddsProtos.Pipeline.newBuilder().setFactor(keyInfo.getFactor())
            .setType(keyInfo.getType()).setId(HddsProtos.PipelineID.newBuilder()
            .setUuid128(HddsProtos.UUID.newBuilder().setLeastSigBits(1L)
                .setMostSigBits(1L).build()).build());

    // Keeping only the given position node in pipeline.
    builder.addMembers(HddsProtos.DatanodeDetailsProto.newBuilder().setUuid128(
        HddsProtos.UUID.newBuilder().setLeastSigBits(keepingNodeIndex)
            .setMostSigBits(keepingNodeIndex).build()).setHostName("localhost")
        .setIpAddress("1.2.3.4").addPorts(
            HddsProtos.Port.newBuilder().setName("EC")
                .setValue(1234 + keepingNodeIndex).build()).build());

    HddsProtos.Pipeline pipeline = builder.build();
    List<OzoneManagerProtocolProtos.KeyLocation> results = new ArrayList<>();
    results.add(OzoneManagerProtocolProtos.KeyLocation.newBuilder()
        .setPipeline(pipeline).setBlockID(
            HddsProtos.BlockID.newBuilder().setBlockCommitSequenceId(1L)
                .setContainerBlockID(
                    HddsProtos.ContainerBlockID.newBuilder().setContainerID(1L)
                        .setLocalID(0L).build()).build()).setOffset(0L)
        .setLength(keyInfo.getDataSize()).build());

    final OzoneManagerProtocolProtos.KeyInfo keyInfo1 =
        OzoneManagerProtocolProtos.KeyInfo.newBuilder()
            .setVolumeName(keyInfo.getVolumeName())
            .setBucketName(keyInfo.getBucketName())
            .setKeyName(keyInfo.getKeyName())
            .setCreationTime(keyInfo.getCreationTime())
            .setModificationTime(keyInfo.getModificationTime())
            .setType(keyInfo.getType()).setFactor(keyInfo.getFactor())
            .setDataSize(keyInfo.getDataSize()).setLatestVersion(0L)
            .addKeyLocationList(
                OzoneManagerProtocolProtos.KeyLocationList.newBuilder()
                    .addAllKeyLocations(results)).build();
    buck.put(keyInfo.getKeyName(), keyInfo1);
  }
}