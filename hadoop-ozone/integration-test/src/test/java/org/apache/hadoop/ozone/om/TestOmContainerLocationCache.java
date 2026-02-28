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

package org.apache.hadoop.ozone.om;

import static com.google.common.collect.Sets.newHashSet;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.NO_REPLICA_FOUND;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
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
import org.apache.hadoop.hdds.scm.net.InnerNode;
import org.apache.hadoop.hdds.scm.net.InnerNodeImpl;
import org.apache.hadoop.hdds.scm.net.NetConstants;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusException;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatcher;

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
  @TempDir
  private static File dir;
  private static ScmBlockLocationProtocol mockScmBlockLocationProtocol;
  private static StorageContainerLocationProtocol mockScmContainerClient;
  private static OzoneConfiguration conf;
  private static OMMetadataManager metadataManager;
  private static final String BUCKET_NAME = "bucket1";
  private static final String VERSIONED_BUCKET_NAME = "versionedBucket1";
  private static final String VOLUME_NAME = "vol1";
  private static OzoneManager om;
  private static ObjectStore objectStore;
  private static XceiverClientGrpc mockDn1Protocol;
  private static XceiverClientGrpc mockDn2Protocol;
  private static XceiverClientGrpc mockDnEcProtocol;
  private static final DatanodeDetails DN1 =
      MockDatanodeDetails.createDatanodeDetails(DatanodeID.randomID());
  private static final DatanodeDetails DN2 =
      MockDatanodeDetails.createDatanodeDetails(DatanodeID.randomID());
  private static final DatanodeDetails DN3 =
      MockDatanodeDetails.createDatanodeDetails(DatanodeID.randomID());
  private static final DatanodeDetails DN4 =
      MockDatanodeDetails.createDatanodeDetails(DatanodeID.randomID());
  private static final DatanodeDetails DN5 =
      MockDatanodeDetails.createDatanodeDetails(DatanodeID.randomID());
  private static final AtomicLong CONTAINER_ID = new AtomicLong(1);
  private static OzoneClient ozoneClient;

  @BeforeAll
  public static void setUp() throws Exception {
    ExitUtils.disableSystemExit();

    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.toString());
    conf.set(OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY, "true");
    conf.setLong(OZONE_KEY_PREALLOCATION_BLOCKS_MAX, 10);

    mockScmBlockLocationProtocol = mock(ScmBlockLocationProtocol.class);
    mockScmContainerClient =
        mock(StorageContainerLocationProtocol.class);
    InnerNode.Factory factory = InnerNodeImpl.FACTORY;
    when(mockScmBlockLocationProtocol.getNetworkTopology()).thenReturn(
        factory.newInnerNode("", "", null, NetConstants.ROOT_LEVEL, 1));

    OmTestManagers omTestManagers = new OmTestManagers(conf,
        mockScmBlockLocationProtocol, mockScmContainerClient);
    om = omTestManagers.getOzoneManager();
    ozoneClient = omTestManagers.getRpcClient();
    metadataManager = omTestManagers.getMetadataManager();

    RpcClient rpcClient = new RpcClient(conf, null) {
      @Nonnull
      @Override
      protected XceiverClientFactory createXceiverClientFactory(
          ServiceInfoEx serviceInfo) throws IOException {
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
    ozoneClient.close();
    om.stop();
  }

  private static XceiverClientManager mockDataNodeClientFactory()
      throws IOException {
    mockDn1Protocol = spy(new XceiverClientGrpc(createPipeline(DN1), conf));
    mockDn2Protocol = spy(new XceiverClientGrpc(createPipeline(DN2), conf));
    mockDnEcProtocol = spy(new XceiverClientGrpc(createEcPipeline(
        ImmutableMap.of(DN1, 1, DN2, 2, DN3, 3, DN4, 4, DN5, 5)), conf));
    XceiverClientManager manager = mock(XceiverClientManager.class);
    when(manager.acquireClient(argThat(matchEmptyPipeline())))
        .thenCallRealMethod();
    when(manager.acquireClient(argThat(matchEmptyPipeline()),
        anyBoolean())).thenCallRealMethod();
    when(manager.acquireClientForReadData(argThat(matchEmptyPipeline())))
        .thenCallRealMethod();

    when(manager.acquireClient(argThat(matchPipeline(DN1))))
        .thenReturn(mockDn1Protocol);
    when(manager.acquireClientForReadData(argThat(matchPipeline(DN1))))
        .thenReturn(mockDn1Protocol);

    when(manager.acquireClient(argThat(matchPipeline(DN2))))
        .thenReturn(mockDn2Protocol);
    when(manager.acquireClientForReadData(argThat(matchPipeline(DN2))))
        .thenReturn(mockDn2Protocol);

    when(manager.acquireClient(argThat(matchEcPipeline())))
        .thenReturn(mockDnEcProtocol);
    when(manager.acquireClientForReadData(argThat(matchEcPipeline())))
        .thenReturn(mockDnEcProtocol);
    return manager;
  }

  private static ArgumentMatcher<Pipeline> matchEmptyPipeline() {
    return argument -> argument != null
        && argument.getNodes().isEmpty();
  }

  private static ArgumentMatcher<Pipeline> matchPipeline(DatanodeDetails dn) {
    return argument -> argument != null && !argument.getNodes().isEmpty()
        && argument.getNodes().get(0).getUuid().equals(dn.getUuid());
  }

  private static ArgumentMatcher<Pipeline> matchEcPipeline() {
    return argument -> argument != null && !argument.getNodes().isEmpty()
        && argument.getReplicationConfig() instanceof ECReplicationConfig;
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
  public void beforeEach() throws IOException {
    CONTAINER_ID.getAndIncrement();
    reset(mockScmBlockLocationProtocol, mockScmContainerClient,
        mockDn1Protocol, mockDn2Protocol, mockDnEcProtocol);
    InnerNode.Factory factory = InnerNodeImpl.FACTORY;
    when(mockScmBlockLocationProtocol.getNetworkTopology()).thenReturn(
        factory.newInnerNode("", "", null, NetConstants.ROOT_LEVEL, 1));
    when(mockDn1Protocol.getPipeline()).thenReturn(createPipeline(DN1));
    when(mockDn2Protocol.getPipeline()).thenReturn(createPipeline(DN2));
    when(mockDnEcProtocol.getPipeline()).thenReturn(createEcPipeline(
        ImmutableMap.of(DN1, 1, DN2, 2, DN3, 3, DN4, 4, DN5, 5)));
  }

  /**
   * Verify that in a happy case, container location is cached and reused
   * in OM.
   */
  @Test
  public void containerCachedInHappyCase() throws Exception {
    byte[] data = "Test content".getBytes(UTF_8);

    mockScmAllocationOnDn1(CONTAINER_ID.get(), 1L);
    mockWriteChunkResponse(mockDn1Protocol);
    mockPutBlockResponse(mockDn1Protocol, CONTAINER_ID.get(), 1L, data);

    OzoneBucket bucket = objectStore.getVolume(VOLUME_NAME)
        .getBucket(BUCKET_NAME);

    // Create keyName1.
    String keyName1 = "key1";
    try (OzoneOutputStream os = bucket.createKey(keyName1, data.length)) {
      IOUtils.write(data, os);
    }

    mockScmGetContainerPipeline(CONTAINER_ID.get(), DN1);

    // Read keyName1.
    OzoneKeyDetails key1 = bucket.getKey(keyName1);
    verify(mockScmContainerClient, times(1))
        .getContainerWithPipelineBatch(newHashSet(CONTAINER_ID.get()));

    mockGetBlock(mockDn1Protocol, CONTAINER_ID.get(), 1L, data, null, null);
    mockReadChunk(mockDn1Protocol, CONTAINER_ID.get(), 1L, data, null, null);
    try (InputStream is = key1.getContent()) {
      byte[] read = new byte[(int) key1.getDataSize()];
      IOUtils.read(is, read);
      assertArrayEquals(data, read);
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
      assertArrayEquals(data, read);
    }
    // Ensure SCM is not called once again.
    verify(mockScmContainerClient, times(1))
        .getContainerWithPipelineBatch(newHashSet(CONTAINER_ID.get()));
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

    mockScmAllocationOnDn1(CONTAINER_ID.get(), 1L);
    mockWriteChunkResponse(mockDn1Protocol);
    mockPutBlockResponse(mockDn1Protocol, CONTAINER_ID.get(), 1L, data);

    OzoneBucket bucket = objectStore.getVolume(VOLUME_NAME)
        .getBucket(BUCKET_NAME);

    String keyName = "key";
    try (OzoneOutputStream os = bucket.createKey(keyName, data.length)) {
      IOUtils.write(data, os);
    }

    mockScmGetContainerPipeline(CONTAINER_ID.get(), DN1);

    OzoneKeyDetails key1 = bucket.getKey(keyName);

    verify(mockScmContainerClient, times(1))
        .getContainerWithPipelineBatch(newHashSet(CONTAINER_ID.get()));

    try (InputStream is = key1.getContent()) {
      // Simulate dn1 got errors, and the container's moved to dn2.
      mockGetBlock(mockDn1Protocol, CONTAINER_ID.get(), 1L, null,
          dnException, dnResponseCode);
      mockScmGetContainerPipeline(CONTAINER_ID.get(), DN2);
      mockGetBlock(mockDn2Protocol, CONTAINER_ID.get(), 1L, data, null, null);
      mockReadChunk(mockDn2Protocol, CONTAINER_ID.get(), 1L, data, null, null);

      byte[] read = new byte[(int) key1.getDataSize()];
      IOUtils.read(is, read);
      assertArrayEquals(data, read);
    }

    // verify SCM is called one more time to refresh.
    verify(mockScmContainerClient, times(2))
        .getContainerWithPipelineBatch(newHashSet(CONTAINER_ID.get()));
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

    mockScmAllocationOnDn1(CONTAINER_ID.get(), 1L);
    mockWriteChunkResponse(mockDn1Protocol);
    mockPutBlockResponse(mockDn1Protocol, CONTAINER_ID.get(), 1L, data);

    OzoneBucket bucket = objectStore.getVolume(VOLUME_NAME)
        .getBucket(BUCKET_NAME);

    String keyName = "key";
    try (OzoneOutputStream os = bucket.createKey(keyName, data.length)) {
      IOUtils.write(data, os);
    }

    mockScmGetContainerPipeline(CONTAINER_ID.get(), DN1);

    OzoneKeyDetails key1 = bucket.getKey(keyName);

    verify(mockScmContainerClient, times(1))
        .getContainerWithPipelineBatch(newHashSet(CONTAINER_ID.get()));

    try (InputStream is = key1.getContent()) {
      // simulate dn1 goes down, the container's to dn2.
      mockGetBlock(mockDn1Protocol, CONTAINER_ID.get(), 1L, data, null, null);
      mockReadChunk(mockDn1Protocol, CONTAINER_ID.get(), 1L, null,
          dnException, dnResponseCode);
      mockScmGetContainerPipeline(CONTAINER_ID.get(), DN2);
      mockGetBlock(mockDn2Protocol, CONTAINER_ID.get(), 1L, data, null, null);
      mockReadChunk(mockDn2Protocol, CONTAINER_ID.get(), 1L, data, null, null);

      byte[] read = new byte[(int) key1.getDataSize()];
      IOUtils.read(is, read);
      assertArrayEquals(data, read);
    }

    // verify SCM is called one more time to refresh.
    verify(mockScmContainerClient, times(2))
        .getContainerWithPipelineBatch(newHashSet(CONTAINER_ID.get()));
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

    mockScmAllocationOnDn1(CONTAINER_ID.get(), 1L);
    mockWriteChunkResponse(mockDn1Protocol);
    mockPutBlockResponse(mockDn1Protocol, CONTAINER_ID.get(), 1L, data);

    OzoneBucket bucket = objectStore.getVolume(VOLUME_NAME)
        .getBucket(BUCKET_NAME);

    String keyName = "key";
    try (OzoneOutputStream os = bucket.createKey(keyName, data.length)) {
      IOUtils.write(data, os);
    }

    mockScmGetContainerPipeline(CONTAINER_ID.get(), DN1);

    OzoneKeyDetails key1 = bucket.getKey(keyName);

    verify(mockScmContainerClient, times(1))
        .getContainerWithPipelineBatch(newHashSet(CONTAINER_ID.get()));

    try (InputStream is = key1.getContent()) {
      // simulate dn1 got errors, and the container's moved to dn2.
      mockGetBlock(mockDn1Protocol, CONTAINER_ID.get(), 1L, null, ex,
          errorCode);

      assertThrows(expectedEx,
          () -> IOUtils.read(is, new byte[(int) key1.getDataSize()]));
    }

    // verify SCM is called one more time to refresh.
    verify(mockScmContainerClient, times(1))
        .getContainerWithPipelineBatch(newHashSet(CONTAINER_ID.get()));
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

    mockScmAllocationOnDn1(CONTAINER_ID.get(), 1L);
    mockWriteChunkResponse(mockDn1Protocol);
    mockPutBlockResponse(mockDn1Protocol, CONTAINER_ID.get(), 1L, data);

    OzoneBucket bucket = objectStore.getVolume(VOLUME_NAME)
        .getBucket(BUCKET_NAME);

    String keyName = "key";
    try (OzoneOutputStream os = bucket.createKey(keyName, data.length)) {
      IOUtils.write(data, os);
    }

    mockScmGetContainerPipeline(CONTAINER_ID.get(), DN1);

    OzoneKeyDetails key1 = bucket.getKey(keyName);

    verify(mockScmContainerClient, times(1))
        .getContainerWithPipelineBatch(newHashSet(CONTAINER_ID.get()));

    try (InputStream is = key1.getContent()) {
      // simulate dn1 got errors, and the container's moved to dn2.
      mockGetBlock(mockDn1Protocol, CONTAINER_ID.get(), 1L, data, null, null);
      mockReadChunk(mockDn1Protocol, CONTAINER_ID.get(), 1L, null,
          dnException, dnResponseCode);

      assertThrows(expectedEx,
          () -> IOUtils.read(is, new byte[(int) key1.getDataSize()]));
    }

    // verify SCM is called one more time to refresh.
    verify(mockScmContainerClient, times(1))
        .getContainerWithPipelineBatch(newHashSet(CONTAINER_ID.get()));
  }

  /**
   * Verify that in situation that SCM returns empty pipelines (that prevents
   * clients from reading data), the empty pipelines are not cached and
   * subsequent key reads re-fetch container data from SCM.
   */
  @Test
  public void containerRefreshedOnEmptyPipelines() throws Exception {
    byte[] data = "Test content".getBytes(UTF_8);

    mockScmAllocationOnDn1(CONTAINER_ID.get(), 1L);
    mockWriteChunkResponse(mockDn1Protocol);
    mockPutBlockResponse(mockDn1Protocol, CONTAINER_ID.get(), 1L, data);

    OzoneBucket bucket = objectStore.getVolume(VOLUME_NAME)
        .getBucket(BUCKET_NAME);

    String keyName = "key";
    try (OzoneOutputStream os = bucket.createKey(keyName, data.length)) {
      IOUtils.write(data, os);
    }

    // All datanodes go down and scm returns empty pipeline for the container.
    mockScmGetContainerPipelineEmpty(CONTAINER_ID.get());

    OzoneKeyDetails key1 = bucket.getKey(keyName);

    verify(mockScmContainerClient, times(1))
        .getContainerWithPipelineBatch(newHashSet(CONTAINER_ID.get()));

    // verify that the effort to read will result in a NO_REPLICA_FOUND error.
    Exception ex =
        assertThrows(IllegalArgumentException.class, () -> {
          try (InputStream is = key1.getContent()) {
            IOUtils.read(is, new byte[(int) key1.getDataSize()]);
          }
        });
    assertEquals(NO_REPLICA_FOUND.toString(), ex.getMessage());

    // but the empty pipeline is not cached, and when some data node is back.
    mockScmGetContainerPipeline(CONTAINER_ID.get(), DN1);
    mockGetBlock(mockDn1Protocol, CONTAINER_ID.get(), 1L, data, null, null);
    mockReadChunk(mockDn1Protocol, CONTAINER_ID.get(), 1L, data, null, null);
    // the subsequent effort to read the key is success.
    OzoneKeyDetails updatedKey1 = bucket.getKey(keyName);
    try (InputStream is = updatedKey1.getContent()) {
      byte[] read = new byte[(int) key1.getDataSize()];
      IOUtils.read(is, read);
      assertArrayEquals(data, read);
    }
    // verify SCM is called one more time to refetch the container pipeline..
    verify(mockScmContainerClient, times(2))
        .getContainerWithPipelineBatch(newHashSet(CONTAINER_ID.get()));
  }

  @Test
  public void containerRefreshedOnInsufficientEcPipelines() throws Exception {
    int chunkSize = 1024 * 1024;
    int dataBlocks = 3;
    int parityBlocks = 2;
    int inputSize = chunkSize * dataBlocks;
    byte[][] inputChunks = new byte[dataBlocks][chunkSize];

    mockScmAllocationEcPipeline(CONTAINER_ID.get(), 1L);
    mockWriteChunkResponse(mockDnEcProtocol);
    mockPutBlockResponse(mockDnEcProtocol, CONTAINER_ID.get(), 1L, null);

    OzoneBucket bucket = objectStore.getVolume(VOLUME_NAME).getBucket(BUCKET_NAME);

    String keyName = "ecKey";
    try (OzoneOutputStream os = bucket.createKey(keyName, inputSize,
        new ECReplicationConfig(dataBlocks, parityBlocks, ECReplicationConfig.EcCodec.RS,
            chunkSize), new HashMap<>())) {
      for (int i = 0; i < dataBlocks; i++) {
        os.write(inputChunks[i]);
      }
    }

    // case1: pipeline replicaIndexes missing some data indexes, should not cache
    mockScmGetContainerEcPipeline(CONTAINER_ID.get(), ImmutableMap.of(DN1, 1, DN2, 2, DN4, 4));
    bucket.getKey(keyName);
    verify(mockScmContainerClient, times(1))
        .getContainerWithPipelineBatch(newHashSet(CONTAINER_ID.get()));
    bucket.getKey(keyName);
    verify(mockScmContainerClient, times(2))
        .getContainerWithPipelineBatch(newHashSet(CONTAINER_ID.get()));

    // case2: pipeline replicaIndexes contain all data indexes, should cache
    mockScmGetContainerEcPipeline(CONTAINER_ID.get(), ImmutableMap.of(DN1, 1, DN2, 2, DN3, 3, DN4, 4));
    bucket.getKey(keyName);
    verify(mockScmContainerClient, times(3))
        .getContainerWithPipelineBatch(newHashSet(CONTAINER_ID.get()));
    bucket.getKey(keyName);
    verify(mockScmContainerClient, times(3))
        .getContainerWithPipelineBatch(newHashSet(CONTAINER_ID.get()));
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

  @Nonnull
  private ContainerProtos.DatanodeBlockID createBlockId(long containerId,
                                                        long localId) {
    return ContainerProtos.DatanodeBlockID.newBuilder()
        .setContainerID(containerId)
        .setLocalID(localId).build();
  }

  private void mockWriteChunkResponse(XceiverClientSpi mockDnProtocol)
      throws IOException, ExecutionException, InterruptedException {
    doAnswer(invocation ->
        new XceiverClientReply(
            completedFuture(
                createWriteChunkResponse(
                    (ContainerCommandRequestProto)invocation.getArgument(0)))))
        .when(mockDnProtocol)
        .sendCommandAsync(argThat(matchCmd(Type.WriteChunk)));
  }

  ContainerCommandResponseProto createWriteChunkResponse(
      ContainerCommandRequestProto request) {
    ContainerProtos.WriteChunkRequestProto writeChunk = request.getWriteChunk();

    WriteChunkResponseProto.Builder builder =
        WriteChunkResponseProto.newBuilder();
    if (writeChunk.hasBlock()) {
      ContainerProtos.BlockData
          blockData = writeChunk.getBlock().getBlockData();

      GetCommittedBlockLengthResponseProto response =
          GetCommittedBlockLengthResponseProto.newBuilder()
          .setBlockID(blockData.getBlockID())
          .setBlockLength(blockData.getSize())
          .build();

      builder.setCommittedBlockLength(response);
    }
    return ContainerCommandResponseProto.newBuilder()
        .setWriteChunk(builder.build())
        .setResult(Result.SUCCESS)
        .setCmdType(Type.WriteChunk)
        .build();
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
        .allocateBlock(anyLong(), anyInt(),
            any(ReplicationConfig.class),
            anyString(),
            any(ExcludeList.class),
            anyString(),
            any(StorageType.class)))
        .thenReturn(Collections.singletonList(block));
  }

  private void mockScmAllocationEcPipeline(long containerID, long localId)
      throws IOException {
    ContainerBlockID blockId = new ContainerBlockID(containerID, localId);
    AllocatedBlock block = new AllocatedBlock.Builder()
        .setPipeline(createEcPipeline(ImmutableMap.of(DN1, 1, DN2, 2, DN3, 3, DN4, 4, DN5, 5)))
        .setContainerBlockID(blockId)
        .build();
    when(mockScmBlockLocationProtocol
        .allocateBlock(anyLong(), anyInt(),
            any(ECReplicationConfig.class),
            anyString(),
            any(ExcludeList.class),
            anyString(),
            any(StorageType.class)))
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

  private void mockScmGetContainerPipelineEmpty(long containerId)
      throws IOException {
    Pipeline pipeline = createPipeline(Collections.emptyList());
    ContainerInfo containerInfo = new ContainerInfo.Builder()
        .setContainerID(containerId)
        .setPipelineID(pipeline.getId()).build();
    List<ContainerWithPipeline> containerWithPipelines =
        Collections.singletonList(
            new ContainerWithPipeline(containerInfo, pipeline));

    when(mockScmContainerClient.getContainerWithPipelineBatch(
        newHashSet(containerId))).thenReturn(containerWithPipelines);
  }

  private void mockScmGetContainerEcPipeline(long containerId, Map<DatanodeDetails, Integer> indexes)
      throws IOException {
    Pipeline pipeline = createEcPipeline(indexes);
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

  @Nonnull
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
    return createPipeline(Collections.singletonList(dn));
  }

  private static Pipeline createPipeline(List<DatanodeDetails> nodes) {
    return Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setNodes(nodes)
        .build();
  }

  private static Pipeline createEcPipeline(Map<DatanodeDetails, Integer> indexes) {
    return Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(new ECReplicationConfig(3, 2))
        .setReplicaIndexes(indexes)
        .setNodes(new ArrayList<>(indexes.keySet()))
        .build();
  }
}
