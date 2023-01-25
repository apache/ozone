/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetCommittedBlockLengthResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusException;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.util.ExitUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.rules.Timeout;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.google.common.collect.Sets.newHashSet;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This class includes the integration test-cases to verify the integration
 * between client and OM to keep container location cache eventually
 * consistent. For example, when clients facing particular errors reading data
 * from datanodes, they should inform OM to refresh location cache and OM
 * should in turn contact SCM to get the updated container location.
 *
 * This integration verifies clients and OM using mocked Datanode and SCM
 * protocols.
 */
public class TestOmContainerLocationCache {

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = Timeout.seconds(300);
  private static ScmBlockLocationProtocol mockScmBlockLocationProtocol;
  private static StorageContainerLocationProtocol mockScmContainerClient;
  private static OzoneConfiguration conf;
  private static OMMetadataManager metadataManager;
  private static File dir;
  private static final String BUCKET_NAME = "bucket1";
  private static final String VERSIONED_BUCKET_NAME = "versionedBucket1";
  private static final String VOLUME_NAME = "vol1";
  private static OzoneManager om;
  private static RpcClient rpcClient;
  private static ObjectStore objectStore;
  private static XceiverClientGrpc mockDn1Protocol;
  private static XceiverClientGrpc mockDn2Protocol;
  private static final DatanodeDetails DN1 =
      MockDatanodeDetails.createDatanodeDetails(UUID.randomUUID());
  private static final DatanodeDetails DN2 =
      MockDatanodeDetails.createDatanodeDetails(UUID.randomUUID());
  private static long testContainerId = 1L;


  @BeforeAll
  public static void setUp() throws Exception {
    ExitUtils.disableSystemExit();

    conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, "127.0.0.1:0");
    dir = GenericTestUtils.getRandomizedTestDir();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.toString());
    conf.set(OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY, "true");
    conf.setLong(OZONE_KEY_PREALLOCATION_BLOCKS_MAX, 10);

    mockScmBlockLocationProtocol = mock(ScmBlockLocationProtocol.class);
    mockScmContainerClient =
        Mockito.mock(StorageContainerLocationProtocol.class);

    OmTestManagers omTestManagers = new OmTestManagers(conf,
        mockScmBlockLocationProtocol, mockScmContainerClient);
    om = omTestManagers.getOzoneManager();
    metadataManager = omTestManagers.getMetadataManager();

    rpcClient = new RpcClient(conf, null) {
      @NotNull
      @Override
      protected XceiverClientFactory createXceiverClientFactory(
          List<X509Certificate> x509Certificates) throws IOException {
        return mockDataNodeClientFactory();
      }
    };

    objectStore = new ObjectStore(conf, rpcClient);

    createVolume(VOLUME_NAME);
    createBucket(VOLUME_NAME, BUCKET_NAME, false);
    createBucket(VOLUME_NAME, VERSIONED_BUCKET_NAME, true);
  }

  @AfterAll
  public static void cleanup() throws Exception {
    om.stop();
    FileUtils.deleteDirectory(dir);
  }

  private static XceiverClientManager mockDataNodeClientFactory()
      throws IOException {
    mockDn1Protocol = spy(new XceiverClientGrpc(createPipeline(DN1), conf));
    mockDn2Protocol = spy(new XceiverClientGrpc(createPipeline(DN2), conf));
    XceiverClientManager manager = mock(XceiverClientManager.class);
    when(manager.acquireClient(argThat(matchPipeline(DN1))))
        .thenReturn(mockDn1Protocol);
    when(manager.acquireClientForReadData(argThat(matchPipeline(DN1))))
        .thenReturn(mockDn1Protocol);

    when(manager.acquireClient(argThat(matchPipeline(DN2))))
        .thenReturn(mockDn2Protocol);
    when(manager.acquireClientForReadData(argThat(matchPipeline(DN2))))
        .thenReturn(mockDn2Protocol);
    return manager;
  }

  private static ArgumentMatcher<Pipeline> matchPipeline(DatanodeDetails dn) {
    return argument -> argument != null
        && argument.getNodes().get(0).getUuid().equals(dn.getUuid());
  }

  private static void createBucket(String volumeName, String bucketName,
                                   boolean isVersionEnabled)
      throws IOException {
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setIsVersionEnabled(isVersionEnabled)
        .build();

    OMRequestTestUtils.addBucketToOM(metadataManager, bucketInfo);
  }

  private static void createVolume(String volumeName) throws IOException {
    OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setAdminName("bilbo")
        .setOwnerName("bilbo")
        .build();
    OMRequestTestUtils.addVolumeToOM(metadataManager, volumeArgs);
  }

  @BeforeEach
  @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
  public void beforeEach() {
    testContainerId++;
    Mockito.reset(mockScmBlockLocationProtocol, mockScmContainerClient,
        mockDn1Protocol, mockDn2Protocol);
    when(mockDn1Protocol.getPipeline()).thenReturn(createPipeline(DN1));
    when(mockDn2Protocol.getPipeline()).thenReturn(createPipeline(DN2));
  }

  /**
   * Verify that in a happy case, container location is cached and reused
   * in OM.
   */
  @Test
  public void containerCachedInHappyCase() throws Exception {
    byte[] data = "Test content".getBytes(UTF_8);

    mockScmAllocationOnDn1(testContainerId, 1L);
    mockWriteChunkResponse(mockDn1Protocol);
    mockPutBlockResponse(mockDn1Protocol, testContainerId, 1L, data);

    OzoneBucket bucket = objectStore.getVolume(VOLUME_NAME)
        .getBucket(BUCKET_NAME);

    // Create keyName1.
    String keyName1 = "key1";
    try (OzoneOutputStream os = bucket.createKey(keyName1, data.length)) {
      IOUtils.write(data, os);
    }

    mockScmGetContainerPipeline(testContainerId, DN1);

    // Read keyName1.
    OzoneKeyDetails key1 = bucket.getKey(keyName1);
    verify(mockScmContainerClient, times(1))
        .getContainerWithPipelineBatch(newHashSet(testContainerId));

    mockGetBlock(mockDn1Protocol, testContainerId, 1L, data, null, null);
    mockReadChunk(mockDn1Protocol, testContainerId, 1L, data, null, null);
    try (InputStream is = key1.getContent()) {
      byte[] read = new byte[(int) key1.getDataSize()];
      IOUtils.read(is, read);
      Assertions.assertArrayEquals(data, read);
    }

    // Create keyName2 in the same container to reuse the cache
    String keyName2 = "key2";
    try (OzoneOutputStream os = bucket.createKey(keyName2, data.length)) {
      IOUtils.write(data, os);
    }
    // Read keyName2.
    OzoneKeyDetails key2 = bucket.getKey(keyName2);
    try (InputStream is = key2.getContent()) {
      byte[] read = new byte[(int) key2.getDataSize()];
      IOUtils.read(is, read);
      Assertions.assertArrayEquals(data, read);
    }
    // Ensure SCM is not called once again.
    verify(mockScmContainerClient, times(1))
        .getContainerWithPipelineBatch(newHashSet(testContainerId));
  }

  private static Stream<Arguments> errorsTriggerRefresh() {
    return Stream.of(
        Arguments.of(null, Result.CLOSED_CONTAINER_IO),
        Arguments.of(null, Result.CONTAINER_NOT_FOUND),
        Arguments.of(new StatusException(Status.UNAVAILABLE), null),
        Arguments.of(new StatusRuntimeException(Status.UNAVAILABLE), null)
    );
  }

  private static Stream<Arguments> errorsNotTriggerRefresh() {
    return Stream.of(
        Arguments.of(new StatusException(Status.UNAUTHENTICATED), null,
            SCMSecurityException.class),
        Arguments.of(new IOException("Any random IO exception."), null,
            IOException.class)
    );
  }

  /**
   * Verify that in case a client got errors calling datanodes GetBlock,
   * the client correctly requests OM to refresh relevant container location
   * from SCM.
   */
  @ParameterizedTest
  @MethodSource("errorsTriggerRefresh")
  public void containerRefreshedAfterDatanodeGetBlockError(
      Exception dnException, Result dnResponseCode) throws Exception {
    byte[] data = "Test content".getBytes(UTF_8);

    mockScmAllocationOnDn1(testContainerId, 1L);
    mockWriteChunkResponse(mockDn1Protocol);
    mockPutBlockResponse(mockDn1Protocol, testContainerId, 1L, data);

    OzoneBucket bucket = objectStore.getVolume(VOLUME_NAME)
        .getBucket(BUCKET_NAME);

    String keyName = "key";
    try (OzoneOutputStream os = bucket.createKey(keyName, data.length)) {
      IOUtils.write(data, os);
    }

    mockScmGetContainerPipeline(testContainerId, DN1);

    OzoneKeyDetails key1 = bucket.getKey(keyName);

    verify(mockScmContainerClient, times(1))
        .getContainerWithPipelineBatch(newHashSet(testContainerId));

    try (InputStream is = key1.getContent()) {
      // Simulate dn1 got errors, and the container's moved to dn2.
      mockGetBlock(mockDn1Protocol, testContainerId, 1L, null,
          dnException, dnResponseCode);
      mockScmGetContainerPipeline(testContainerId, DN2);
      mockGetBlock(mockDn2Protocol, testContainerId, 1L, data, null, null);
      mockReadChunk(mockDn2Protocol, testContainerId, 1L, data, null, null);

      byte[] read = new byte[(int) key1.getDataSize()];
      IOUtils.read(is, read);
      Assertions.assertArrayEquals(data, read);
    }

    // verify SCM is called one more time to refresh.
    verify(mockScmContainerClient, times(2))
        .getContainerWithPipelineBatch(newHashSet(testContainerId));
  }

  /**
   * Verify that in case a client got errors datanodes ReadChunk,the client
   * correctly requests OM to refresh relevant container location from
   * SCM.
   */
  @ParameterizedTest
  @MethodSource("errorsTriggerRefresh")
  public void containerRefreshedAfterDatanodeReadChunkError(
      Exception dnException, Result dnResponseCode) throws Exception {
    byte[] data = "Test content".getBytes(UTF_8);

    mockScmAllocationOnDn1(testContainerId, 1L);
    mockWriteChunkResponse(mockDn1Protocol);
    mockPutBlockResponse(mockDn1Protocol, testContainerId, 1L, data);

    OzoneBucket bucket = objectStore.getVolume(VOLUME_NAME)
        .getBucket(BUCKET_NAME);

    String keyName = "key";
    try (OzoneOutputStream os = bucket.createKey(keyName, data.length)) {
      IOUtils.write(data, os);
    }

    mockScmGetContainerPipeline(testContainerId, DN1);

    OzoneKeyDetails key1 = bucket.getKey(keyName);

    verify(mockScmContainerClient, times(1))
        .getContainerWithPipelineBatch(newHashSet(testContainerId));

    try (InputStream is = key1.getContent()) {
      // simulate dn1 goes down, the container's to dn2.
      mockGetBlock(mockDn1Protocol, testContainerId, 1L, data, null, null);
      mockReadChunk(mockDn1Protocol, testContainerId, 1L, null,
          dnException, dnResponseCode);
      mockScmGetContainerPipeline(testContainerId, DN2);
      mockGetBlock(mockDn2Protocol, testContainerId, 1L, data, null, null);
      mockReadChunk(mockDn2Protocol, testContainerId, 1L, data, null, null);

      byte[] read = new byte[(int) key1.getDataSize()];
      IOUtils.read(is, read);
      Assertions.assertArrayEquals(data, read);
    }

    // verify SCM is called one more time to refresh.
    verify(mockScmContainerClient, times(2))
        .getContainerWithPipelineBatch(newHashSet(testContainerId));
  }

  /**
   * Verify that in case a client got particular errors datanodes GetBlock,
   * the client fails correctly fast and don't invoke cache refresh.
   */
  @ParameterizedTest
  @MethodSource("errorsNotTriggerRefresh")
  public void containerNotRefreshedAfterDatanodeGetBlockError(
      Exception ex, Result errorCode, Class<? extends Exception> expectedEx)
      throws Exception {
    byte[] data = "Test content".getBytes(UTF_8);

    mockScmAllocationOnDn1(testContainerId, 1L);
    mockWriteChunkResponse(mockDn1Protocol);
    mockPutBlockResponse(mockDn1Protocol, testContainerId, 1L, data);

    OzoneBucket bucket = objectStore.getVolume(VOLUME_NAME)
        .getBucket(BUCKET_NAME);

    String keyName = "key";
    try (OzoneOutputStream os = bucket.createKey(keyName, data.length)) {
      IOUtils.write(data, os);
    }

    mockScmGetContainerPipeline(testContainerId, DN1);

    OzoneKeyDetails key1 = bucket.getKey(keyName);

    verify(mockScmContainerClient, times(1))
        .getContainerWithPipelineBatch(newHashSet(testContainerId));

    try (InputStream is = key1.getContent()) {
      // simulate dn1 got errors, and the container's moved to dn2.
      mockGetBlock(mockDn1Protocol, testContainerId, 1L, null, ex, errorCode);

      assertThrows(expectedEx,
          () -> IOUtils.read(is, new byte[(int) key1.getDataSize()]));
    }

    // verify SCM is called one more time to refresh.
    verify(mockScmContainerClient, times(1))
        .getContainerWithPipelineBatch(newHashSet(testContainerId));
  }

  /**
   * Verify that in case a client got particular errors datanodes ReadChunk,
   * the client fails correctly fast and don't invoke cache refresh.
   */
  @ParameterizedTest
  @MethodSource("errorsNotTriggerRefresh")
  public void containerNotRefreshedAfterDatanodeReadChunkError(
      Exception dnException, Result dnResponseCode,
      Class<? extends Exception> expectedEx) throws Exception {
    byte[] data = "Test content".getBytes(UTF_8);

    mockScmAllocationOnDn1(testContainerId, 1L);
    mockWriteChunkResponse(mockDn1Protocol);
    mockPutBlockResponse(mockDn1Protocol, testContainerId, 1L, data);

    OzoneBucket bucket = objectStore.getVolume(VOLUME_NAME)
        .getBucket(BUCKET_NAME);

    String keyName = "key";
    try (OzoneOutputStream os = bucket.createKey(keyName, data.length)) {
      IOUtils.write(data, os);
    }

    mockScmGetContainerPipeline(testContainerId, DN1);

    OzoneKeyDetails key1 = bucket.getKey(keyName);

    verify(mockScmContainerClient, times(1))
        .getContainerWithPipelineBatch(newHashSet(testContainerId));

    try (InputStream is = key1.getContent()) {
      // simulate dn1 got errors, and the container's moved to dn2.
      mockGetBlock(mockDn1Protocol, testContainerId, 1L, data, null, null);
      mockReadChunk(mockDn1Protocol, testContainerId, 1L, null,
          dnException, dnResponseCode);

      assertThrows(expectedEx,
          () -> IOUtils.read(is, new byte[(int) key1.getDataSize()]));
    }

    // verify SCM is called one more time to refresh.
    verify(mockScmContainerClient, times(1))
        .getContainerWithPipelineBatch(newHashSet(testContainerId));
  }

  private void mockPutBlockResponse(XceiverClientSpi mockDnProtocol,
                                    long containerId, long localId,
                                    byte[] data)
      throws IOException, ExecutionException, InterruptedException {
    GetCommittedBlockLengthResponseProto build =
        GetCommittedBlockLengthResponseProto.newBuilder()
            .setBlockLength(8)
            .setBlockID(createBlockId(containerId, localId))
            .build();
    ContainerCommandResponseProto putResponse =
        ContainerCommandResponseProto.newBuilder()
            .setPutBlock(PutBlockResponseProto.newBuilder()
                .setCommittedBlockLength(build).build())
            .setResult(Result.SUCCESS)
            .setCmdType(Type.PutBlock)
            .build();
    doAnswer(invocation ->
        new XceiverClientReply(completedFuture(putResponse)))
        .when(mockDnProtocol)
        .sendCommandAsync(argThat(matchCmd(Type.PutBlock)));
  }

  @NotNull
  private ContainerProtos.DatanodeBlockID createBlockId(long containerId,
                                                        long localId) {
    return ContainerProtos.DatanodeBlockID.newBuilder()
        .setContainerID(containerId)
        .setLocalID(localId).build();
  }

  private void mockWriteChunkResponse(XceiverClientSpi mockDnProtocol)
      throws IOException, ExecutionException, InterruptedException {
    ContainerCommandResponseProto writeResponse =
        ContainerCommandResponseProto.newBuilder()
            .setWriteChunk(WriteChunkResponseProto.newBuilder().build())
            .setResult(Result.SUCCESS)
            .setCmdType(Type.WriteChunk)
            .build();
    doAnswer(invocation ->
        new XceiverClientReply(completedFuture(writeResponse)))
        .when(mockDnProtocol)
        .sendCommandAsync(argThat(matchCmd(Type.WriteChunk)));
  }

  private ArgumentMatcher<ContainerCommandRequestProto> matchCmd(Type type) {
    return argument -> argument != null && argument.getCmdType() == type;
  }

  private void mockScmAllocationOnDn1(long containerID,
                                      long localId) throws IOException {
    ContainerBlockID blockId = new ContainerBlockID(containerID, localId);
    AllocatedBlock block = new AllocatedBlock.Builder()
        .setPipeline(createPipeline(DN1))
        .setContainerBlockID(blockId)
        .build();
    when(mockScmBlockLocationProtocol
        .allocateBlock(Mockito.anyLong(), Mockito.anyInt(),
            any(ReplicationConfig.class),
            Mockito.anyString(),
            any(ExcludeList.class)))
        .thenReturn(Collections.singletonList(block));
  }

  private void mockScmGetContainerPipeline(long containerId,
                                           DatanodeDetails dn)
      throws IOException {
    Pipeline pipeline = createPipeline(dn);
    ContainerInfo containerInfo = new ContainerInfo.Builder()
        .setContainerID(containerId)
        .setPipelineID(pipeline.getId()).build();
    List<ContainerWithPipeline> containerWithPipelines =
        Collections.singletonList(
            new ContainerWithPipeline(containerInfo, pipeline));

    when(mockScmContainerClient.getContainerWithPipelineBatch(
        newHashSet(containerId))).thenReturn(containerWithPipelines);
  }

  private void mockGetBlock(XceiverClientGrpc mockDnProtocol,
                            long containerId, long localId,
                            byte[] data,
                            Exception exception,
                            Result errorCode) throws Exception {

    final CompletableFuture<ContainerCommandResponseProto> response;
    if (exception != null) {
      response = new CompletableFuture<>();
      response.completeExceptionally(exception);
    } else if (errorCode != null) {
      ContainerCommandResponseProto getBlockResp =
          ContainerCommandResponseProto.newBuilder()
              .setResult(errorCode)
              .setCmdType(Type.GetBlock)
              .build();
      response = completedFuture(getBlockResp);
    } else {
      ContainerCommandResponseProto getBlockResp =
          ContainerCommandResponseProto.newBuilder()
              .setGetBlock(GetBlockResponseProto.newBuilder()
                  .setBlockData(BlockData.newBuilder()
                      .addChunks(createChunkInfo(data))
                      .setBlockID(createBlockId(containerId, localId))
                      .build())
                  .build()
              )
              .setResult(Result.SUCCESS)
              .setCmdType(Type.GetBlock)
              .build();
      response = completedFuture(getBlockResp);
    }
    doAnswer(invocation -> new XceiverClientReply(response))
        .when(mockDnProtocol)
        .sendCommandAsync(argThat(matchCmd(Type.GetBlock)), any());
  }

  @NotNull
  private ChunkInfo createChunkInfo(byte[] data) throws Exception {
    Checksum checksum = new Checksum(ChecksumType.CRC32, 4);
    return ChunkInfo.newBuilder()
        .setOffset(0)
        .setLen(data.length)
        .setChunkName("chunk1")
        .setChecksumData(checksum.computeChecksum(data).getProtoBufMessage())
        .build();
  }

  private void mockReadChunk(XceiverClientGrpc mockDnProtocol,
                             long containerId, long localId,
                             byte[] data,
                             Exception exception,
                             Result errorCode) throws Exception {
    final CompletableFuture<ContainerCommandResponseProto> response;
    if (exception != null) {
      response = new CompletableFuture<>();
      response.completeExceptionally(exception);
    } else if (errorCode != null) {
      ContainerCommandResponseProto readChunkResp =
          ContainerCommandResponseProto.newBuilder()
              .setResult(errorCode)
              .setCmdType(Type.ReadChunk)
              .build();
      response = completedFuture(readChunkResp);
    } else {
      ContainerCommandResponseProto readChunkResp =
          ContainerCommandResponseProto.newBuilder()
              .setReadChunk(ReadChunkResponseProto.newBuilder()
                  .setBlockID(createBlockId(containerId, localId))
                  .setChunkData(createChunkInfo(data))
                  .setData(ByteString.copyFrom(data))
                  .build()
              )
              .setResult(Result.SUCCESS)
              .setCmdType(Type.ReadChunk)
              .build();
      response = completedFuture(readChunkResp);
    }

    doAnswer(invocation -> new XceiverClientReply(response))
        .when(mockDnProtocol)
        .sendCommandAsync(argThat(matchCmd(Type.ReadChunk)), any());

  }

  private static Pipeline createPipeline(DatanodeDetails dn) {
    return Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setNodes(Collections.singletonList(dn))
        .build();
  }
}