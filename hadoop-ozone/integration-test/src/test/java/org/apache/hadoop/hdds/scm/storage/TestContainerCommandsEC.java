/*
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

package org.apache.hadoop.hdds.scm.storage;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig.EcCodec;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ListBlockResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.WritableECContainerProvider.WritableECContainerProviderConfig;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.token.ContainerTokenIdentifier;
import org.apache.hadoop.hdds.security.token.ContainerTokenSecretManager;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSecretManager;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClientTestImpl;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.InsufficientLocationsException;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECContainerOperationClient;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCoordinator;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.READ;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.WRITE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.newWriteChunkRequestBuilder;

/**
 * This class tests container commands on EC containers.
 */
public class TestContainerCommandsEC {

  private static final String ANY_USER = "any";
  private static MiniOzoneCluster cluster;
  private static StorageContainerManager scm;
  private static OzoneClient rpcClient;
  private static ObjectStore store;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static final String SCM_ID = UUID.randomUUID().toString();
  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static final int EC_DATA = 3;
  private static final int EC_PARITY = 2;
  private static final EcCodec EC_CODEC = EcCodec.RS;
  private static final int EC_CHUNK_SIZE = 1024 * 1024;
  private static final int STRIPE_DATA_SIZE = EC_DATA * EC_CHUNK_SIZE;
  private static final int NUM_DN = EC_DATA + EC_PARITY + 3;
  private static byte[][] inputChunks = new byte[EC_DATA][EC_CHUNK_SIZE];

  // Each key size will be in range [min, max), min inclusive, max exclusive
  private static final int[][] KEY_SIZE_RANGES =
      new int[][] {{1, EC_CHUNK_SIZE}, {EC_CHUNK_SIZE, EC_CHUNK_SIZE + 1},
          {EC_CHUNK_SIZE + 1, STRIPE_DATA_SIZE},
          {STRIPE_DATA_SIZE, STRIPE_DATA_SIZE + 1},
          {STRIPE_DATA_SIZE + 1, STRIPE_DATA_SIZE + EC_CHUNK_SIZE},
          {STRIPE_DATA_SIZE + EC_CHUNK_SIZE, STRIPE_DATA_SIZE * 2}};
  private static byte[][] values;
  private static long containerID;
  private static Pipeline pipeline;
  private static List<DatanodeDetails> datanodeDetails;
  private static Token<ContainerTokenIdentifier> containerToken;
  private static ContainerTokenSecretManager containerTokenGenerator;
  private static OzoneBlockTokenSecretManager blockTokenGenerator;
  private List<XceiverClientSpi> clients = null;
  private static OzoneConfiguration config;
  private static CertificateClient certClient;

  private static OzoneBucket classBucket;
  private static OzoneVolume classVolume;
  private static ReplicationConfig repConfig;

  @BeforeAll
  public static void init() throws Exception {
    config = new OzoneConfiguration();
    config.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    config.setBoolean(OzoneConfigKeys.OZONE_ACL_ENABLED, true);
    config.set(OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS,
        OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    config.setBoolean(HDDS_BLOCK_TOKEN_ENABLED, true);
    config.setBoolean(HDDS_CONTAINER_TOKEN_ENABLED, true);
    DatanodeConfiguration dnConf = config.getObject(
        DatanodeConfiguration.class);
    dnConf.setBlockDeletionInterval(Duration.ofSeconds(1));
    config.setFromObject(dnConf);
    startCluster(config);
    prepareData(KEY_SIZE_RANGES);
  }

  @AfterAll
  public static void stop() throws IOException {
    stopCluster();
  }

  private Pipeline createSingleNodePipeline(Pipeline ecPipeline,
      DatanodeDetails node, int replicaIndex) {

    Map<DatanodeDetails, Integer> indicesForSinglePipeline = new HashMap<>();
    indicesForSinglePipeline.put(node, replicaIndex);

    return Pipeline.newBuilder().setId(ecPipeline.getId())
        .setReplicationConfig(ecPipeline.getReplicationConfig())
        .setState(ecPipeline.getPipelineState())
        .setNodes(ImmutableList.of(node))
        .setReplicaIndexes(indicesForSinglePipeline).build();
  }

  @BeforeEach
  public void connectToDatanodes() {
    clients = new ArrayList<>(datanodeDetails.size());
    for (int i = 0; i < datanodeDetails.size(); i++) {
      clients.add(new XceiverClientGrpc(
          createSingleNodePipeline(pipeline, datanodeDetails.get(i), i + 1),
          cluster.getConf()));
    }
  }

  @AfterEach
  public void closeClients() {
    if (clients == null) {
      return;
    }
    for (XceiverClientSpi c : clients) {
      c.close();
    }
    clients = null;
  }

  private Function<Integer, Integer> chunksInReplicaFunc(int i) {
    if (i < EC_DATA) {
      return (keySize) -> {
        int dataBlocks = (keySize + EC_CHUNK_SIZE - 1) / EC_CHUNK_SIZE;
        return (dataBlocks + EC_DATA - 1 - i) / EC_DATA;
      };
    } else {
      return (keySize) -> (keySize + STRIPE_DATA_SIZE - 1) / STRIPE_DATA_SIZE;
    }
  }

  private void closeAllPipelines(ReplicationConfig replicationConfig) {
    scm.getPipelineManager().getPipelines(replicationConfig,
            Pipeline.PipelineState.OPEN)
        .forEach(p -> {
          try {
            scm.getPipelineManager().closePipeline(p, false);
          } catch (IOException e) {
            throw new RuntimeException(e);
          } catch (TimeoutException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  public void testOrphanBlock() throws Exception {
    // Close all pipelines so we are guaranteed to get a new one
    closeAllPipelines(repConfig);
    // First write a full stripe, which is chunksize * dataNum
    int keyLen = EC_CHUNK_SIZE * EC_DATA;
    String keyName = UUID.randomUUID().toString();
    try (OutputStream out = classBucket
        .createKey(keyName, keyLen, repConfig, new HashMap<>())) {
      out.write(RandomUtils.nextBytes(keyLen));
    }
    long orphanContainerID = classBucket.getKey(keyName)
        .getOzoneKeyLocations().get(0).getContainerID();

    PipelineID orphanPipelineID = scm.getContainerManager()
        .getContainer(ContainerID.valueOf(orphanContainerID)).getPipelineID();

    Pipeline orphanPipeline = scm.getPipelineManager()
        .getPipeline(orphanPipelineID);

    Token<ContainerTokenIdentifier> orphanContainerToken =
        containerTokenGenerator.generateToken(
            ANY_USER, new ContainerID(orphanContainerID));

    // Close the container by closing the pipeline
    scm.getPipelineManager().closePipeline(orphanPipeline, false);

    // Find the datanode hosting Replica index = 2
    DatanodeDetails dn2 = null;
    HddsDatanodeService dn2Service = null;
    List<DatanodeDetails> pipelineNodes = orphanPipeline.getNodes();
    for (DatanodeDetails node : pipelineNodes) {
      if (orphanPipeline.getReplicaIndex(node) == 2) {
        dn2 = node;
        break;
      }
    }
    // Find the Cluster node corresponding to the datanode hosting index = 2
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      if (dn.getDatanodeDetails().equals(dn2)) {
        dn2Service = dn;
        break;
      }
    }

    if (dn2 == null || dn2Service == null) {
      throw new RuntimeException("Could not find datanode hosting index 2");
    }

    // Wait for all replicas in the pipeline to report as closed.
    GenericTestUtils.waitFor(() -> {
      try {
        return scm.getContainerManager().getContainerReplicas(
            ContainerID.valueOf(orphanContainerID)).stream()
            .allMatch(cr -> cr.getState() ==
                StorageContainerDatanodeProtocolProtos.
                    ContainerReplicaProto.State.CLOSED);
      } catch (ContainerNotFoundException e) {
        throw new RuntimeException(e);
      }
    }, 500, 10000);

    // Get the block ID of the key we have just written. This will be used to
    // delete the block from one of the datanode to make the stripe look like
    // a orphan block.
    long localID = classBucket.getKey(keyName)
        .getOzoneKeyLocations().get(0).getLocalID();

    // Create a delete command for the block and sent it.
    DeleteBlocksCommand deleteBlocksCommand =
        new DeleteBlocksCommand(ImmutableList.of(
            StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction
                .newBuilder()
                .setContainerID(orphanContainerID)
                .addLocalID(localID)
                .setTxID(1L)
                .setCount(10)
                .build()));
    dn2Service.getDatanodeStateMachine().getContext()
        .addCommand(deleteBlocksCommand);

    try (XceiverClientGrpc client = new XceiverClientGrpc(
        createSingleNodePipeline(orphanPipeline, dn2, 1), cluster.getConf())) {
      // Wait for the block to be actually deleted
      GenericTestUtils.waitFor(() -> {
        try {
          ListBlockResponseProto response = ContainerProtocolCalls
              .listBlock(client, orphanContainerID, null, Integer.MAX_VALUE,
                  orphanContainerToken);
          for (BlockData bd : response.getBlockDataList()) {
            if (bd.getBlockID().getLocalID() == localID) {
              return false;
            }
          }
          return true;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }, 500, 30000);
    }

    // Create a reconstruction command to create a new copy of indexes 4 and 5
    // which means 1 to 3 must be available. However we know the block
    // information is missing for index 2. As all containers in the stripe must
    // have the block information, this makes the stripe look like a orphan
    // block, where the write went to some nodes but not all.
    SortedMap<Integer, DatanodeDetails> sourceNodeMap = new TreeMap<>();
    for (DatanodeDetails node : orphanPipeline.getNodes()) {
      if (orphanPipeline.getReplicaIndex(node) <= EC_DATA) {
        sourceNodeMap.put(orphanPipeline.getReplicaIndex(node), node);
      }
    }
    // Here we find some spare nodes - ie nodes in the cluster that are not in
    // the original pipeline.
    List<DatanodeDetails> targets = cluster.getHddsDatanodes().stream()
        .map(HddsDatanodeService::getDatanodeDetails)
        .filter(d -> !orphanPipeline.getNodes().contains(d))
        .limit(2)
        .collect(Collectors.toList());
    SortedMap<Integer, DatanodeDetails> targetNodeMap = new TreeMap<>();
    for (int j = 0; j < targets.size(); j++) {
      targetNodeMap.put(EC_DATA + j + 1, targets.get(j));
    }

    try (ECReconstructionCoordinator coordinator =
        new ECReconstructionCoordinator(config, certClient, null,
            ECReconstructionMetrics.create())) {

      // Attempt to reconstruct the container.
      coordinator.reconstructECContainerGroup(orphanContainerID,
          (ECReplicationConfig) repConfig,
          sourceNodeMap, targetNodeMap);
    }

    // Check the block listing for the recovered containers 4 or 5 and they
    // should be present but with no blocks as the only block in the container
    // was an orphan block.
    try (XceiverClientGrpc reconClient = new XceiverClientGrpc(
        createSingleNodePipeline(orphanPipeline, targetNodeMap.get(4), 4),
        cluster.getConf())) {
      ListBlockResponseProto response = ContainerProtocolCalls
          .listBlock(reconClient, orphanContainerID, null, Integer.MAX_VALUE,
              orphanContainerToken);
      long count = response.getBlockDataList().stream()
          .filter(bd -> bd.getBlockID().getLocalID() == localID)
          .count();

      Assert.assertEquals(0L, count);
      Assert.assertEquals(0, response.getBlockDataList().size());
    }
  }

  @Test
  public void testListBlock() throws Exception {
    for (int i = 0; i < datanodeDetails.size(); i++) {
      final int minKeySize = i < EC_DATA ? i * EC_CHUNK_SIZE : 0;
      final int minNumExpectedBlocks =
          (int) Arrays.stream(values).mapToInt(v -> v.length)
              .filter(s -> s > minKeySize).count();
      Function<Integer, Integer> expectedChunksFunc = chunksInReplicaFunc(i);
      final int minNumExpectedChunks =
          Arrays.stream(values).mapToInt(v -> v.length)
              .map(expectedChunksFunc::apply).sum();
      if (minNumExpectedBlocks == 0) {
        final int j = i;
        Throwable t = Assertions.assertThrows(StorageContainerException.class,
            () -> ContainerProtocolCalls
                .listBlock(clients.get(j), containerID, null,
                    minNumExpectedBlocks + 1, containerToken));
        Assertions
            .assertEquals("ContainerID " + containerID + " does not exist",
                t.getMessage());
        continue;
      }
      ListBlockResponseProto response = ContainerProtocolCalls
          .listBlock(clients.get(i), containerID, null, Integer.MAX_VALUE,
              containerToken);
      Assertions.assertTrue(
          minNumExpectedBlocks <= response.getBlockDataList().stream().filter(
              k -> k.getChunksCount() > 0 && k.getChunks(0).getLen() > 0)
              .collect(Collectors.toList()).size(),
          "blocks count should be same or more than min expected" +
              " blocks count on DN " + i);
      Assertions.assertTrue(
          minNumExpectedChunks <= response.getBlockDataList().stream()
              .mapToInt(BlockData::getChunksCount).sum(),
          "chunks count should be same or more than min expected" +
              " chunks count on DN " + i);
    }
  }

  @Test
  public void testCreateRecoveryContainer() throws Exception {
    try (XceiverClientManager xceiverClientManager =
        new XceiverClientManager(config)) {
      ECReplicationConfig replicationConfig = new ECReplicationConfig(3, 2);
      Pipeline newPipeline =
          scm.getPipelineManager().createPipeline(replicationConfig);
      scm.getPipelineManager().activatePipeline(newPipeline.getId());
      final ContainerInfo container = scm.getContainerManager()
          .allocateContainer(replicationConfig, "test");
      Token<ContainerTokenIdentifier> cToken = containerTokenGenerator
          .generateToken(ANY_USER, container.containerID());
      scm.getContainerManager().getContainerStateManager()
          .addContainer(container.getProtobuf());

      XceiverClientSpi dnClient = xceiverClientManager.acquireClient(
          createSingleNodePipeline(newPipeline, newPipeline.getNodes().get(0),
              2));
      try {
        // To create the actual situation, container would have been in closed
        // state at SCM.
        scm.getContainerManager().getContainerStateManager()
            .updateContainerState(container.containerID().getProtobuf(),
                HddsProtos.LifeCycleEvent.FINALIZE);
        scm.getContainerManager().getContainerStateManager()
            .updateContainerState(container.containerID().getProtobuf(),
                HddsProtos.LifeCycleEvent.CLOSE);

        //Create the recovering container in DN.
        String encodedToken = cToken.encodeToUrlString();
        ContainerProtocolCalls.createRecoveringContainer(dnClient,
            container.containerID().getProtobuf().getId(),
            encodedToken, 4);

        BlockID blockID = ContainerTestHelper
            .getTestBlockID(container.containerID().getProtobuf().getId());
        Token<? extends TokenIdentifier> blockToken =
            blockTokenGenerator.generateToken(ANY_USER, blockID,
                EnumSet.of(READ, WRITE), Long.MAX_VALUE);
        byte[] data = "TestData".getBytes(UTF_8);
        ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
            newWriteChunkRequestBuilder(newPipeline, blockID,
                    ChunkBuffer.wrap(ByteBuffer.wrap(data)), 0)
                .setEncodedToken(blockToken.encodeToUrlString())
                .build();
        dnClient.sendCommand(writeChunkRequest);

        // Now, explicitly make a putKey request for the block.
        ContainerProtos.ContainerCommandRequestProto putKeyRequest =
            ContainerTestHelper.getPutBlockRequest(newPipeline,
                writeChunkRequest.getWriteChunk());
        dnClient.sendCommand(putKeyRequest);

        ContainerProtos.ReadContainerResponseProto readContainerResponseProto =
            ContainerProtocolCalls.readContainer(dnClient,
                container.containerID().getProtobuf().getId(), encodedToken);
        Assert.assertEquals(ContainerProtos.ContainerDataProto.State.RECOVERING,
            readContainerResponseProto.getContainerData().getState());
        // Container at SCM should be still in closed state.
        Assert.assertEquals(HddsProtos.LifeCycleState.CLOSED,
            scm.getContainerManager().getContainerStateManager()
                .getContainer(container.containerID()).getState());
        // close container call
        ContainerProtocolCalls.closeContainer(dnClient,
            container.containerID().getProtobuf().getId(), encodedToken);
        // Make sure we have the container and readable.
        readContainerResponseProto = ContainerProtocolCalls
            .readContainer(dnClient,
                container.containerID().getProtobuf().getId(), encodedToken);
        Assert.assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
            readContainerResponseProto.getContainerData().getState());
        ContainerProtos.ReadChunkResponseProto readChunkResponseProto =
            ContainerProtocolCalls.readChunk(dnClient,
                writeChunkRequest.getWriteChunk().getChunkData(), blockID, null,
                blockToken);
        ByteBuffer[] readOnlyByteBuffersArray = BufferUtils
            .getReadOnlyByteBuffersArray(
                readChunkResponseProto.getDataBuffers().getBuffersList());
        Assert.assertEquals(readOnlyByteBuffersArray[0].limit(), data.length);
        byte[] readBuff = new byte[readOnlyByteBuffersArray[0].limit()];
        readOnlyByteBuffersArray[0].get(readBuff, 0, readBuff.length);
        Assert.assertArrayEquals(data, readBuff);
      } finally {
        xceiverClientManager.releaseClient(dnClient, false);
      }
    }
  }

  private static byte[] getBytesWith(int singleDigitNumber, int total) {
    StringBuilder builder = new StringBuilder(singleDigitNumber);
    for (int i = 1; i <= total; i++) {
      builder.append(singleDigitNumber);
    }
    return builder.toString().getBytes(UTF_8);
  }

  @ParameterizedTest
  @MethodSource("recoverableMissingIndexes")
  void testECReconstructionCoordinatorWith(List<Integer> missingIndexes)
      throws Exception {
    testECReconstructionCoordinator(missingIndexes, 3);
  }

  @Test
  void testECReconstructionWithPartialStripe()
          throws Exception {
    testECReconstructionCoordinator(ImmutableList.of(4, 5), 1);
  }


  static Stream<List<Integer>> recoverableMissingIndexes() {
    return Stream
        .concat(IntStream.rangeClosed(1, 5).mapToObj(ImmutableList::of), Stream
            .of(ImmutableList.of(2, 3), ImmutableList.of(2, 4),
                ImmutableList.of(3, 5), ImmutableList.of(4, 5)));
  }

  /**
   * Tests the reconstruction of data when more than parity blocks missed.
   * Test should throw InsufficientLocationsException.
   */
  @Test
  public void testECReconstructionCoordinatorWithMissingIndexes135() {
    InsufficientLocationsException exception =
        Assert.assertThrows(InsufficientLocationsException.class, () -> {
          testECReconstructionCoordinator(ImmutableList.of(1, 3, 5), 3);
        });

    String expectedMessage =
        "There are insufficient datanodes to read the EC block";
    String actualMessage = exception.getMessage();

    Assert.assertEquals(expectedMessage, actualMessage);
  }

  private void testECReconstructionCoordinator(List<Integer> missingIndexes,
      int numInputChunks) throws Exception {
    ObjectStore objectStore = rpcClient.getObjectStore();
    String keyString = UUID.randomUUID().toString();
    String volumeName = UUID.randomUUID().toString();
    String bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
    OzoneVolume volume = objectStore.getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    createKeyAndWriteData(keyString, bucket, numInputChunks);

    try (
        XceiverClientManager xceiverClientManager =
            new XceiverClientManager(config);
        ECReconstructionCoordinator coordinator =
            new ECReconstructionCoordinator(config, certClient,
                 null, ECReconstructionMetrics.create())) {

      ECReconstructionMetrics metrics =
          coordinator.getECReconstructionMetrics();
      OzoneKeyDetails key = bucket.getKey(keyString);
      long conID = key.getOzoneKeyLocations().get(0).getContainerID();
      Token<ContainerTokenIdentifier> cToken = containerTokenGenerator
          .generateToken(ANY_USER, new ContainerID(conID));

      //Close the container first.
      closeContainer(conID);

      Pipeline containerPipeline = scm.getPipelineManager().getPipeline(
          scm.getContainerManager().getContainer(ContainerID.valueOf(conID))
              .getPipelineID());

      SortedMap<Integer, DatanodeDetails> sourceNodeMap = new TreeMap<>();

      List<DatanodeDetails> nodeSet = containerPipeline.getNodes();
      List<Pipeline> containerToDeletePipeline = new ArrayList<>();
      for (DatanodeDetails srcDn : nodeSet) {
        int replIndex = containerPipeline.getReplicaIndex(srcDn);
        if (missingIndexes.contains(replIndex)) {
          containerToDeletePipeline.add(
              createSingleNodePipeline(containerPipeline, srcDn, replIndex));
          continue;
        }
        sourceNodeMap.put(replIndex, srcDn);
      }

      //Find nodes outside of pipeline
      List<DatanodeDetails> clusterDnsList =
          cluster.getHddsDatanodes().stream().map(k -> k.getDatanodeDetails())
              .collect(Collectors.toList());
      List<DatanodeDetails> targetNodes = new ArrayList<>();
      for (DatanodeDetails clusterDN : clusterDnsList) {
        if (!nodeSet.contains(clusterDN)) {
          targetNodes.add(clusterDN);
          if (targetNodes.size() == missingIndexes.size()) {
            break;
          }
        }
      }

      Assert.assertEquals(missingIndexes.size(), targetNodes.size());

      List<org.apache.hadoop.ozone.container.common.helpers.BlockData[]>
          blockDataArrList = new ArrayList<>();
      try (ECContainerOperationClient ecContainerOperationClient =
               new ECContainerOperationClient(config, certClient)) {
        for (int j = 0; j < containerToDeletePipeline.size(); j++) {
          Pipeline p = containerToDeletePipeline.get(j);
          org.apache.hadoop.ozone.container.common.helpers.BlockData[]
              blockData = ecContainerOperationClient.listBlock(
                  conID, p.getFirstNode(),
                  (ECReplicationConfig) p.getReplicationConfig(),
                  cToken);
          blockDataArrList.add(blockData);
          // Delete the first index container
          XceiverClientSpi client = xceiverClientManager.acquireClient(
              p);
          try {
            ContainerProtocolCalls.deleteContainer(
                client,
                conID, true, cToken.encodeToUrlString());
          } finally {
            xceiverClientManager.releaseClient(client, false);
          }
        }

        //Give the new target to reconstruct the container
        SortedMap<Integer, DatanodeDetails> targetNodeMap = new TreeMap<>();
        for (int k = 0; k < missingIndexes.size(); k++) {
          targetNodeMap.put(missingIndexes.get(k), targetNodes.get(k));
        }

        coordinator.reconstructECContainerGroup(conID,
            (ECReplicationConfig) containerPipeline.getReplicationConfig(),
            sourceNodeMap, targetNodeMap);

        // Assert the original container metadata with the new recovered one
        Iterator<Map.Entry<Integer, DatanodeDetails>> iterator =
            targetNodeMap.entrySet().iterator();
        int i = 0;
        while (iterator.hasNext()) {
          Map.Entry<Integer, DatanodeDetails> next = iterator.next();
          DatanodeDetails targetDN = next.getValue();
          Map<DatanodeDetails, Integer> indexes = new HashMap<>();
          indexes.put(targetNodeMap.entrySet().iterator().next().getValue(),
              targetNodeMap.entrySet().iterator().next().getKey());
          Pipeline newTargetPipeline = Pipeline.newBuilder()
              .setId(PipelineID.randomId())
              .setReplicationConfig(containerPipeline.getReplicationConfig())
              .setReplicaIndexes(indexes)
              .setState(Pipeline.PipelineState.CLOSED)
              .setNodes(ImmutableList.of(targetDN)).build();

          org.apache.hadoop.ozone.container.common.helpers.BlockData[]
              reconstructedBlockData =
              ecContainerOperationClient
                  .listBlock(conID, newTargetPipeline.getFirstNode(),
                      (ECReplicationConfig) newTargetPipeline
                          .getReplicationConfig(), cToken);
          Assert.assertEquals(blockDataArrList.get(i).length,
              reconstructedBlockData.length);
          checkBlockData(blockDataArrList.get(i), reconstructedBlockData);
          XceiverClientSpi client = xceiverClientManager.acquireClient(
              newTargetPipeline);
          try {
            ContainerProtos.ReadContainerResponseProto readContainerResponse =
                ContainerProtocolCalls.readContainer(
                    client, conID,
                    cToken.encodeToUrlString());
            Assert.assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
                readContainerResponse.getContainerData().getState());
          } finally {
            xceiverClientManager.releaseClient(client, false);
          }
          i++;
        }
        Assertions.assertEquals(metrics.getReconstructionTotal(), 1L);
      }
    }
  }

  private void createKeyAndWriteData(String keyString, OzoneBucket bucket,
      int numChunks) throws IOException {
    for (int i = 0; i < numChunks; i++) {
      inputChunks[i] = getBytesWith(i + 1, EC_CHUNK_SIZE);
    }
    try (OzoneOutputStream out = bucket.createKey(keyString, 4096,
        new ECReplicationConfig(3, 2, EcCodec.RS, EC_CHUNK_SIZE),
        new HashMap<>())) {
      Assert.assertTrue(out.getOutputStream() instanceof KeyOutputStream);
      for (int i = 0; i < numChunks; i++) {
        out.write(inputChunks[i]);
      }
    }
  }

  @Test
  public void testECReconstructionCoordinatorShouldCleanupContainersOnFailure()
      throws Exception {
    List<Integer> missingIndexes = ImmutableList.of(1, 3);
    ObjectStore objectStore = rpcClient.getObjectStore();
    String keyString = UUID.randomUUID().toString();
    String volumeName = UUID.randomUUID().toString();
    String bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
    OzoneVolume volume = objectStore.getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    createKeyAndWriteData(keyString, bucket, 3);

    OzoneKeyDetails key = bucket.getKey(keyString);
    long conID = key.getOzoneKeyLocations().get(0).getContainerID();
    Token<ContainerTokenIdentifier> cToken =
        containerTokenGenerator.generateToken(ANY_USER, new ContainerID(conID));
    closeContainer(conID);

    Pipeline containerPipeline = scm.getPipelineManager().getPipeline(
        scm.getContainerManager().getContainer(ContainerID.valueOf(conID))
            .getPipelineID());

    List<DatanodeDetails> nodeSet = containerPipeline.getNodes();
    SortedMap<Integer, DatanodeDetails> sourceNodeMap = new TreeMap<>();
    nodeSet.stream().filter(k -> {
      int replIndex = containerPipeline.getReplicaIndex(k);
      return !missingIndexes.contains(replIndex);
    }).forEach(dn -> {
      sourceNodeMap.put(containerPipeline.getReplicaIndex(dn), dn);
    });

    //Find a good node outside of pipeline
    List<DatanodeDetails> clusterDnsList =
        cluster.getHddsDatanodes().stream().map(k -> k.getDatanodeDetails())
            .collect(Collectors.toList());
    DatanodeDetails goodTargetNode = null;
    for (DatanodeDetails clusterDN : clusterDnsList) {
      if (!nodeSet.contains(clusterDN)) {
        goodTargetNode = clusterDN;
        break;
      }
    }

    //Give the new target to reconstruct the container
    SortedMap<Integer, DatanodeDetails> targetNodeMap = new TreeMap<>();
    targetNodeMap.put(1, goodTargetNode);
    // Replace one of the target node with wrong to simulate failure at target.
    DatanodeDetails invalidTargetNode =
        MockDatanodeDetails.randomDatanodeDetails();
    targetNodeMap.put(3, invalidTargetNode);

    Assert.assertThrows(IOException.class, () -> {
      try (ECReconstructionCoordinator coordinator =
          new ECReconstructionCoordinator(config, certClient,
              null, ECReconstructionMetrics.create())) {
        coordinator.reconstructECContainerGroup(conID,
            (ECReplicationConfig) containerPipeline.getReplicationConfig(),
            sourceNodeMap, targetNodeMap);
      }
    });
    final DatanodeDetails targetDNToCheckContainerCLeaned = goodTargetNode;
    StorageContainerException ex =
        Assert.assertThrows(StorageContainerException.class, () -> {
          try (ECContainerOperationClient client =
              new ECContainerOperationClient(config, certClient)) {
            client.listBlock(conID, targetDNToCheckContainerCLeaned,
                new ECReplicationConfig(3, 2), cToken);
          }
        });
    Assert.assertEquals("ContainerID 1 does not exist", ex.getMessage());
  }

  private void closeContainer(long conID)
      throws IOException, InvalidStateTransitionException, TimeoutException {
    //Close the container first.
    scm.getContainerManager().getContainerStateManager().updateContainerState(
        HddsProtos.ContainerID.newBuilder().setId(conID).build(),
        HddsProtos.LifeCycleEvent.FINALIZE);
    scm.getContainerManager().getContainerStateManager().updateContainerState(
        HddsProtos.ContainerID.newBuilder().setId(conID).build(),
        HddsProtos.LifeCycleEvent.CLOSE);
  }

  private void checkBlockData(
      org.apache.hadoop.ozone.container.common.helpers.BlockData[] blockData,
      org.apache.hadoop.ozone.container.common.helpers.BlockData[]
          reconstructedBlockData) {

    for (int i = 0; i < blockData.length; i++) {
      List<ContainerProtos.ChunkInfo> oldBlockDataChunks =
          blockData[i].getChunks();
      List<ContainerProtos.ChunkInfo> newBlockDataChunks =
          reconstructedBlockData[i].getChunks();
      for (int j = 0; j < oldBlockDataChunks.size(); j++) {
        ContainerProtos.ChunkInfo chunkInfo = oldBlockDataChunks.get(j);
        if (chunkInfo.getLen() == 0) {
          // let's ignore the empty chunks
          continue;
        }
        Assert.assertEquals(chunkInfo, newBlockDataChunks.get(j));
      }
    }
  }

  public static void startCluster(OzoneConfiguration conf) throws Exception {

    // Set minimum pipeline to 1 to ensure all data is written to
    // the same container group
    WritableECContainerProviderConfig writableECContainerProviderConfig =
        conf.getObject(WritableECContainerProviderConfig.class);
    writableECContainerProviderConfig.setMinimumPipelines(1);
    conf.setFromObject(writableECContainerProviderConfig);

    OzoneManager.setTestSecureOmFlag(true);
    certClient = new CertificateClientTestImpl(conf);

    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(NUM_DN)
        .setScmId(SCM_ID).setClusterId(CLUSTER_ID)
        .setCertificateClient(certClient)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.getOzoneManager().startSecretManager();
    scm = cluster.getStorageContainerManager();
    rpcClient = OzoneClientFactory.getRpcClient(conf);
    store = rpcClient.getObjectStore();
    storageContainerLocationClient =
        cluster.getStorageContainerLocationClient();
  }

  public static void prepareData(int[][] ranges) throws Exception {
    final String volumeName = UUID.randomUUID().toString();
    final String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    classVolume = store.getVolume(volumeName);
    classVolume.createBucket(bucketName);
    classBucket = classVolume.getBucket(bucketName);
    repConfig =
        new ECReplicationConfig(EC_DATA, EC_PARITY, EC_CODEC, EC_CHUNK_SIZE);
    values = new byte[ranges.length][];
    for (int i = 0; i < ranges.length; i++) {
      int keySize = RandomUtils.nextInt(ranges[i][0], ranges[i][1]);
      values[i] = RandomUtils.nextBytes(keySize);
      final String keyName = UUID.randomUUID().toString();
      try (OutputStream out = classBucket
          .createKey(keyName, values[i].length, repConfig, new HashMap<>())) {
        out.write(values[i]);
      }
    }
//    List<ContainerID> containerIDs =
//        new ArrayList<>(scm.getContainerManager().getContainerIDs());
    List<ContainerID> containerIDs =
            scm.getContainerManager().getContainers()
                    .stream()
                    .map(ContainerInfo::containerID)
                    .collect(Collectors.toList());
    Assertions.assertEquals(1, containerIDs.size());
    containerID = containerIDs.get(0).getId();
    List<Pipeline> pipelines = scm.getPipelineManager().getPipelines(repConfig);
    Assertions.assertEquals(1, pipelines.size());
    pipeline = pipelines.get(0);
    datanodeDetails = pipeline.getNodes();

    OzoneConfiguration tweakedConfig = new OzoneConfiguration(config);
    tweakedConfig.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    SecurityConfig conf = new SecurityConfig(tweakedConfig);
    long tokenLifetime = TimeUnit.DAYS.toMillis(1);
    containerTokenGenerator = new ContainerTokenSecretManager(
        conf, tokenLifetime);
    containerTokenGenerator.start(certClient);
    blockTokenGenerator = new OzoneBlockTokenSecretManager(
        conf, tokenLifetime);
    blockTokenGenerator.start(certClient);
    containerToken = containerTokenGenerator
        .generateToken(ANY_USER, new ContainerID(containerID));
  }

  public static void stopCluster() throws IOException {
    if (rpcClient != null) {
      rpcClient.close();
    }

    if (storageContainerLocationClient != null) {
      storageContainerLocationClient.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }

    if (blockTokenGenerator != null) {
      blockTokenGenerator.stop();
    }

    if (containerTokenGenerator != null) {
      containerTokenGenerator.stop();
    }
  }

}
