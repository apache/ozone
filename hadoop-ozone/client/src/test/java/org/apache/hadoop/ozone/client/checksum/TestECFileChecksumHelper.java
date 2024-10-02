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
package org.apache.hadoop.ozone.client.checksum;

import jakarta.annotation.Nonnull;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.InMemoryConfiguration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.client.MockOmTransport;
import org.apache.hadoop.ozone.client.MockXceiverClientFactory;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType.CRC32;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for ECFileChecksumHelper class.
 */
public class TestECFileChecksumHelper {
  private OzoneClient client;
  private RpcClient rpcClient;

  @BeforeEach
  public void init() throws IOException {
    ConfigurationSource config = new InMemoryConfiguration();
    OzoneClientConfig clientConfig = config.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumType(ContainerProtos.ChecksumType.CRC32C);

    ((InMemoryConfiguration) config).setFromObject(clientConfig);

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
  }

  @AfterEach
  public void close() throws IOException {
    client.close();
  }


  @Test
  public void testEmptyBlock() throws IOException {
    // test the file checksum of a file with an empty block.
    RpcClient mockRpcClient = mock(RpcClient.class);
    OzoneManagerProtocol om = mock(OzoneManagerProtocol.class);
    OzoneVolume mockVolume = mock(OzoneVolume.class);
    OzoneBucket mockBucket = mock(OzoneBucket.class);
    when(mockRpcClient.getOzoneManagerClient()).thenReturn(om);
    when(mockVolume.getName()).thenReturn("vol1");
    when(mockBucket.getName()).thenReturn("bucket1");
    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
        .setVolumeName(mockVolume.getName())
        .setBucketName(mockBucket.getName())
        .setKeyName("dummy key")
        .addOmKeyLocationInfoGroup(new OmKeyLocationInfoGroup(0, new ArrayList<>()))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(0)
        .setReplicationConfig(new ECReplicationConfig(6, 3))
        .setFileEncryptionInfo(null)
        .setAcls(null)
        .build();
    when(om.lookupKey(any())).thenReturn(omKeyInfo);
    OzoneClientConfig.ChecksumCombineMode combineMode =
        OzoneClientConfig.ChecksumCombineMode.MD5MD5CRC;
    ECFileChecksumHelper helper = new ECFileChecksumHelper(
        mockVolume, mockBucket, "dummy", 10, combineMode, mockRpcClient, omKeyInfo);

    helper.compute();
    FileChecksum fileChecksum = helper.getFileChecksum();
    assertInstanceOf(MD5MD5CRC32GzipFileChecksum.class, fileChecksum);
    assertEquals(DataChecksum.Type.CRC32,
        ((MD5MD5CRC32GzipFileChecksum) fileChecksum).getCrcType());

    // test negative length
    helper = new ECFileChecksumHelper(
        mockVolume, mockBucket, "dummy", -1, combineMode, mockRpcClient, omKeyInfo);
    helper.compute();
    assertNull(helper.getKeyLocationInfoList());
  }

  @Test
  public void testOneBlock() throws IOException {
    // test the file checksum of a file with one block.
    OzoneConfiguration conf = new OzoneConfiguration();
    List<DatanodeDetails> dns = Collections.singletonList(
        DatanodeDetails.newBuilder().setUuid(UUID.randomUUID()).build());
    Pipeline pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(new ECReplicationConfig(6, 3))
        .setState(Pipeline.PipelineState.CLOSED)
        .setNodes(dns)
        .build();
    RpcClient mockRpcClient = mock(RpcClient.class);
    XceiverClientGrpc xceiverClientGrpc =
        new XceiverClientGrpc(pipeline, conf) {
          @Override
          public XceiverClientReply sendCommandAsync(
              ContainerProtos.ContainerCommandRequestProto request,
              DatanodeDetails dn) {
            return buildValidResponse();
          }
        };
    XceiverClientFactory factory = mock(XceiverClientFactory.class);
    OzoneManagerProtocol om = mock(OzoneManagerProtocol.class);
    OzoneVolume mockVolume = mock(OzoneVolume.class);
    OzoneBucket mockBucket = mock(OzoneBucket.class);
    when(mockVolume.getName()).thenReturn("vol1");
    when(mockBucket.getName()).thenReturn("bucket1");
    when(factory.acquireClientForReadData(any())).
        thenReturn(xceiverClientGrpc);
    when(mockRpcClient.getXceiverClientManager()).thenReturn(factory);
    when(mockRpcClient.getOzoneManagerClient()).thenReturn(om);

    BlockID blockID = new BlockID(1, 1);
    OmKeyLocationInfo omKeyLocationInfo =
        new OmKeyLocationInfo.Builder().setPipeline(pipeline)
            .setBlockID(blockID)
            .build();

    List<OmKeyLocationInfo> omKeyLocationInfoList =
        Collections.singletonList(omKeyLocationInfo);

    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
        .setVolumeName(mockVolume.getName())
        .setBucketName(mockBucket.getName())
        .setKeyName("dummy")
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(10)
        .setReplicationConfig(new ECReplicationConfig(6, 3))
        .setFileEncryptionInfo(null)
        .setAcls(null)
        .addOmKeyLocationInfoGroup(new OmKeyLocationInfoGroup(0, omKeyLocationInfoList))
        .build();

    when(om.lookupKey(any())).thenReturn(omKeyInfo);

    OzoneClientConfig.ChecksumCombineMode combineMode =
        OzoneClientConfig.ChecksumCombineMode.MD5MD5CRC;

    ECFileChecksumHelper helper = new ECFileChecksumHelper(
        mockVolume, mockBucket, "dummy", 10, combineMode, mockRpcClient, omKeyInfo);

    helper.compute();
    FileChecksum fileChecksum = helper.getFileChecksum();
    assertInstanceOf(MD5MD5CRC32GzipFileChecksum.class, fileChecksum);
    assertEquals(1, helper.getKeyLocationInfoList().size());
  }

  private XceiverClientReply buildValidResponse() {
    // return a GetBlockResponse message of a block and its chunk checksums.
    ContainerProtos.DatanodeBlockID blockID =
        ContainerProtos.DatanodeBlockID.newBuilder()
            .setContainerID(1)
            .setLocalID(1)
            .setBlockCommitSequenceId(1).build();

    byte[] stripeByteArray = new byte[12];
    ByteString stripeString = ByteString.copyFrom(stripeByteArray);

    ContainerProtos.ChecksumData checksumData =
        ContainerProtos.ChecksumData.newBuilder()
            .setType(CRC32)
            .setBytesPerChecksum(1024)
            .addChecksums(stripeString)
            .build();

    ContainerProtos.ChunkInfo chunkInfo =
        ContainerProtos.ChunkInfo.newBuilder()
            .setChunkName("dummy_chunk")
            .setOffset(1)
            .setLen(10)
            .setChecksumData(checksumData)
            .setStripeChecksum(stripeString)
            .build();

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

}
