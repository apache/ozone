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

package org.apache.hadoop.ozone.client.checksum;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType.CRC32;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.InMemoryConfiguration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.client.MockOmTransport;
import org.apache.hadoop.ozone.client.MockXceiverClientFactory;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Time;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Unit tests for Replicated and EC FileChecksumHelper class.
 */
public class TestFileChecksumHelper {
  private final FileChecksum noCachedChecksum = null;
  private OzoneClient client;
  private ObjectStore store;
  private OzoneVolume volume;
  private RpcClient rpcClient;

  @BeforeEach
  public void init() throws IOException {
    ConfigurationSource config = new InMemoryConfiguration();
    OzoneClientConfig clientConfig = config.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumType(ContainerProtos.ChecksumType.CRC32C);

    ((InMemoryConfiguration)config).setFromObject(clientConfig);

    rpcClient = new RpcClient(config, null) {

      @Override
      protected OmTransport createOmTransport(
          String omServiceId) {
        return new MockOmTransport();
      }

      @Nonnull
      @Override
      protected XceiverClientFactory createXceiverClientFactory(
          ServiceInfoEx serviceInfo) {
        return new MockXceiverClientFactory();
      }
    };

    client = new OzoneClient(config, rpcClient);

    store = client.getObjectStore();
  }

  @AfterEach
  public void close() throws IOException {
    client.close();
  }

  private OmKeyInfo omKeyInfo(ReplicationType type, FileChecksum cachedChecksum, List<OmKeyLocationInfo> locationInfo) {
    ReplicationConfig config = type == ReplicationType.EC ? new ECReplicationConfig(6, 3)
        : RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE);

    return new OmKeyInfo.Builder()
        .setVolumeName("vol1")
        .setBucketName("bucket")
        .setKeyName("key")
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, locationInfo)))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(0)
        .setReplicationConfig(config)
        .setFileEncryptionInfo(null)
        .setFileChecksum(cachedChecksum)
        .setAcls(null)
        .build();
  }

  private BaseFileChecksumHelper checksumHelper(ReplicationType type, OzoneVolume mockVolume, OzoneBucket mockBucket,
      int length, OzoneClientConfig.ChecksumCombineMode combineMode, RpcClient mockRpcClient, OmKeyInfo keyInfo)
      throws IOException {
    return type == ReplicationType.RATIS ? new ReplicatedFileChecksumHelper(
        mockVolume, mockBucket, "dummy", length, combineMode, mockRpcClient)
        : new ECFileChecksumHelper(
        mockVolume, mockBucket, "dummy", length, combineMode, mockRpcClient, keyInfo);
  }

  private Pipeline pipeline(ReplicationType type, List<DatanodeDetails> datanodeDetails) {
    ReplicationConfig config = type == ReplicationType.RATIS ? RatisReplicationConfig
        .getInstance(HddsProtos.ReplicationFactor.THREE)
        : new ECReplicationConfig(6, 3);

    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(config)
        .setState(Pipeline.PipelineState.CLOSED)
        .setNodes(datanodeDetails)
        .build();
  }

  @ParameterizedTest
  @EnumSource(names = {"EC", "RATIS"})
  public void testEmptyBlock(ReplicationType helperType) throws IOException {
    // test the file checksum of a file with an empty block.
    RpcClient mockRpcClient = mock(RpcClient.class);
    OmKeyInfo omKeyInfo = omKeyInfo(helperType, noCachedChecksum, new ArrayList<>());
    OzoneVolume mockVolume = mock(OzoneVolume.class);
    OzoneBucket mockBucket = mock(OzoneBucket.class);
    OzoneClientConfig.ChecksumCombineMode combineMode =
        OzoneClientConfig.ChecksumCombineMode.MD5MD5CRC;

    OzoneManagerProtocol om = mock(OzoneManagerProtocol.class);
    when(mockRpcClient.getOzoneManagerClient()).thenReturn(om);
    when(om.lookupKey(any())).thenReturn(omKeyInfo);
    when(mockVolume.getName()).thenReturn("vol1");
    when(mockBucket.getName()).thenReturn("bucket1");


    BaseFileChecksumHelper helper =
        checksumHelper(helperType, mockVolume, mockBucket, 10, combineMode, mockRpcClient, omKeyInfo);
    helper.compute();
    FileChecksum fileChecksum = helper.getFileChecksum();
    assertInstanceOf(MD5MD5CRC32GzipFileChecksum.class, fileChecksum);
    assertEquals(DataChecksum.Type.CRC32,
        ((MD5MD5CRC32GzipFileChecksum) fileChecksum).getCrcType());

    // test negative length
    helper =
        checksumHelper(helperType, mockVolume, mockBucket, -1, combineMode, mockRpcClient, omKeyInfo);
    helper.compute();
    assertNull(helper.getKeyLocationInfoList());
  }

  @ParameterizedTest
  @EnumSource(names = {"EC", "RATIS"})
  public void testOneBlock(ReplicationType helperType) throws IOException {
    // test the file checksum of a file with one block.
    OzoneConfiguration conf = new OzoneConfiguration();
    RpcClient mockRpcClient = mock(RpcClient.class);
    List<DatanodeDetails> dns = Collections.singletonList(
        DatanodeDetails.newBuilder().setUuid(UUID.randomUUID()).build());
    Pipeline pipeline = pipeline(helperType, dns);
    BlockID blockID = new BlockID(1, 1);
    OmKeyLocationInfo omKeyLocationInfo =
        new OmKeyLocationInfo.Builder()
            .setPipeline(pipeline)
            .setBlockID(blockID)
            .build();
    List<OmKeyLocationInfo> omKeyLocationInfoList =
        Collections.singletonList(omKeyLocationInfo);
    OmKeyInfo omKeyInfo = omKeyInfo(helperType, noCachedChecksum, omKeyLocationInfoList);
    XceiverClientGrpc xceiverClientGrpc =
        new XceiverClientGrpc(pipeline, conf) {
          @Override
          public XceiverClientReply sendCommandAsync(
              ContainerProtos.ContainerCommandRequestProto request,
              DatanodeDetails dn) {
            return buildValidResponse(helperType);
          }
        };
    XceiverClientFactory factory = mock(XceiverClientFactory.class);
    OzoneManagerProtocol om = mock(OzoneManagerProtocol.class);
    when(factory.acquireClientForReadData(any())).
        thenReturn(xceiverClientGrpc);
    when(mockRpcClient.getXceiverClientManager()).thenReturn(factory);
    when(mockRpcClient.getOzoneManagerClient()).thenReturn(om);
    when(om.lookupKey(any())).thenReturn(omKeyInfo);

    OzoneVolume mockVolume = mock(OzoneVolume.class);
    when(mockVolume.getName()).thenReturn("vol1");
    OzoneBucket mockBucket = mock(OzoneBucket.class);
    when(mockBucket.getName()).thenReturn("bucket1");

    OzoneClientConfig.ChecksumCombineMode combineMode =
        OzoneClientConfig.ChecksumCombineMode.MD5MD5CRC;

    BaseFileChecksumHelper helper = checksumHelper(
        helperType, mockVolume, mockBucket, 10, combineMode, mockRpcClient, omKeyInfo);

    helper.compute();
    FileChecksum fileChecksum = helper.getFileChecksum();
    assertInstanceOf(MD5MD5CRC32GzipFileChecksum.class, fileChecksum);
    assertEquals(1, helper.getKeyLocationInfoList().size());

    FileChecksum cachedChecksum = new MD5MD5CRC32GzipFileChecksum();
    /// test cached checksum
    OmKeyInfo omKeyInfoWithChecksum = omKeyInfo(helperType, cachedChecksum, omKeyLocationInfoList);
    when(om.lookupKey(any())).
        thenReturn(omKeyInfoWithChecksum);

    helper = checksumHelper(
        helperType, mockVolume, mockBucket, 10, combineMode, mockRpcClient, omKeyInfo);

    helper.compute();
    fileChecksum = helper.getFileChecksum();
    assertInstanceOf(MD5MD5CRC32GzipFileChecksum.class, fileChecksum);
    assertEquals(1, helper.getKeyLocationInfoList().size());
  }

  private XceiverClientReply buildValidResponse(ReplicationType type) {
    // return a GetBlockResponse message of a block and its chunk checksums.
    ContainerProtos.DatanodeBlockID blockID =
        ContainerProtos.DatanodeBlockID.newBuilder()
        .setContainerID(1)
        .setLocalID(1)
        .setBlockCommitSequenceId(1).build();

    byte[] byteArray = new byte[12];
    ByteString byteString = ByteString.copyFrom(byteArray);

    ContainerProtos.ChecksumData checksumData =
        ContainerProtos.ChecksumData.newBuilder()
        .setType(CRC32)
        .setBytesPerChecksum(1024)
        .addChecksums(byteString)
        .build();

    ContainerProtos.ChunkInfo.Builder chunkInfoBuilder = ContainerProtos.ChunkInfo.newBuilder()
        .setChunkName("dummy_chunk")
        .setOffset(1)
        .setLen(10)
        .setChecksumData(checksumData);

    if (type == ReplicationType.EC) {
      chunkInfoBuilder.setStripeChecksum(byteString);
    }

    ContainerProtos.ChunkInfo chunkInfo = chunkInfoBuilder.build();

    ContainerProtos.BlockData blockData =
        ContainerProtos.BlockData.newBuilder()
            .setBlockID(blockID)
            .addChunks(chunkInfo)
            .build();
    ContainerProtos.GetBlockResponseProto getBlockResponseProto
        = ContainerProtos.GetBlockResponseProto.newBuilder()
        .setBlockData(blockData)
        .build();

    ContainerProtos.ContainerCommandResponseProto resp =
        ContainerProtos.ContainerCommandResponseProto.newBuilder()
            .setCmdType(ContainerProtos.Type.GetBlock)
            .setResult(ContainerProtos.Result.SUCCESS)
            .setGetBlock(getBlockResponseProto)
            .build();
    final CompletableFuture<ContainerProtos.ContainerCommandResponseProto>
        replyFuture = new CompletableFuture<>();
    replyFuture.complete(resp);
    return new XceiverClientReply(replyFuture);
  }

  private OzoneBucket getOzoneBucket() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    return volume.getBucket(bucketName);
  }

  /**
   * Write a real key and compute file checksum of it.
   *
   * @throws IOException
   */
  @Test
  public void testPutKeyChecksum() throws IOException {
    String value = new String(new byte[1024], UTF_8);
    OzoneBucket bucket = getOzoneBucket();

    for (int i = 0; i < 1; i++) {
      String keyName = UUID.randomUUID().toString();

      try (OzoneOutputStream out = bucket
          .createKey(keyName, value.getBytes(UTF_8).length,
              ReplicationType.RATIS, ONE, new HashMap<>())) {
        out.write(value.getBytes(UTF_8));
        out.write(value.getBytes(UTF_8));
      }

      OzoneClientConfig.ChecksumCombineMode combineMode =
          OzoneClientConfig.ChecksumCombineMode.MD5MD5CRC;

      ReplicatedFileChecksumHelper helper = new ReplicatedFileChecksumHelper(
          volume, bucket, keyName, 10, combineMode, rpcClient);

      helper.compute();
      FileChecksum fileChecksum = helper.getFileChecksum();
      assertEquals(DataChecksum.Type.CRC32C,
          ((MD5MD5CRC32FileChecksum)fileChecksum).getCrcType());
      assertEquals(1, helper.getKeyLocationInfoList().size());
    }
  }
}
