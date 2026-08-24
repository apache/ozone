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

package org.apache.hadoop.hdds.scm.storage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_UNHEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.READ;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.WRITE;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.newWriteChunkRequestBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableList;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
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
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.hdds.security.token.ContainerTokenIdentifier;
import org.apache.hadoop.hdds.security.token.ContainerTokenSecretManager;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSecretManager;
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
import org.apache.hadoop.ozone.client.SecretKeyTestClient;
import org.apache.hadoop.ozone.client.io.BlockOutputStreamEntry;
import org.apache.hadoop.ozone.client.io.InsufficientLocationsException;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECContainerOperationClient;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCoordinator;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
  private static final int EC_DATA = 3;
  private static final int EC_PARITY = 2;
  private static final EcCodec EC_CODEC = EcCodec.RS;
  private static final int EC_CHUNK_SIZE = 1024 * 1024;
  private static final int STRIPE_DATA_SIZE = EC_DATA * EC_CHUNK_SIZE;
  private static final int NUM_DN = EC_DATA + EC_PARITY + 3;
  // Data slots are EC_DATA + 1 so we can generate enough data to have a full stripe
  // plus one extra chunk.
  private static byte[][] inputChunks = new byte[EC_DATA + 1][EC_CHUNK_SIZE];

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
  private static SecretKeyClient secretKeyClient;
  private List<XceiverClientSpi> clients = null;
  private static OzoneConfiguration config;
  private static CertificateClient certClient;

  private static OzoneBucket classBucket;
  private static ReplicationConfig repConfig;

  @BeforeAll
  public static void init() throws Exception {
    config = new OzoneConfiguration();
    config.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    config.setTimeDuration(ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
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
            scm.getPipelineManager().closePipeline(p.getId());
          } catch (IOException e) {
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
      out.write(RandomUtils.secure().randomBytes(keyLen));
    }
    long orphanContainerID = classBucket.getKey(keyName)
        .getOzoneKeyLocations().get(0).getContainerID();

    PipelineID orphanPipelineID = scm.getContainerManager()
        .getContainer(ContainerID.valueOf(orphanContainerID)).getPipelineID();

    Pipeline orphanPipeline = scm.getPipelineManager()
        .getPipeline(orphanPipelineID);

    Token<ContainerTokenIdentifier> orphanContainerToken =
        containerTokenGenerator.generateToken(
            ANY_USER, ContainerID.valueOf(orphanContainerID));

    // Close the container by closing the pipeline
    scm.getPipelineManager().closePipeline(orphanPipeline.getId());

    // Find the datanode hosting Replica index = 2
    HddsDatanodeService dn2Service = null;
    List<DatanodeDetails> pipelineNodes = orphanPipeline.getNodes();
    DatanodeDetails dn2 = findDatanodeWithIndex(
        orphanPipeline, pipelineNodes, 2);
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
    StateContext context = dn2Service.getDatanodeStateMachine().getContext();
    deleteBlocksCommand.setTerm(context.getTermOfLeaderSCM().isPresent() ?
            context.getTermOfLeaderSCM().getAsLong() : 0);
    context.addCommand(deleteBlocksCommand);

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
             new ECReconstructionCoordinator(config, certClient,
                 secretKeyClient, null,
                 ECReconstructionMetrics.create(), "")) {

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

      assertEquals(0L, count);
      assertEquals(0, response.getBlockDataList().size());
    }
  }

  private DatanodeDetails findDatanodeWithIndex(Pipeline orphanPipeline,
      List<DatanodeDetails> pipelineNodes, int index) {
    for (DatanodeDetails node : pipelineNodes) {
      if (orphanPipeline.getReplicaIndex(node) == index) {
        return node;
      }
    }
    return null;
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
        Throwable t = assertThrows(StorageContainerException.class,
            () -> ContainerProtocolCalls
                .listBlock(clients.get(j), containerID, null,
                    minNumExpectedBlocks + 1, containerToken));
        assertEquals("ContainerID " + containerID + " does not exist",
                t.getMessage());
        continue;
      }
      ListBlockResponseProto response = ContainerProtocolCalls
          .listBlock(clients.get(i), containerID, null, Integer.MAX_VALUE,
              containerToken);
      assertThat(minNumExpectedBlocks)
          .withFailMessage("blocks count should be same or more than min expected" +
               " blocks count on DN " + i)
          .isLessThanOrEqualTo(response.getBlockDataList().stream().filter(
              k -> k.getChunksCount() > 0 && k.getChunks(0).getLen() > 0)
              .collect(Collectors.toList()).size());
      assertThat(minNumExpectedChunks)
          .withFailMessage("chunks count should be same or more than min expected" +
              " chunks count on DN " + i)
          .isLessThanOrEqualTo(response.getBlockDataList().stream()
              .mapToInt(BlockData::getChunksCount).sum());
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
      int replicaIndex = 4;
      XceiverClientSpi dnClient = xceiverClientManager.acquireClient(
          createSingleNodePipeline(newPipeline, newPipeline.getNodes().get(0),
              2));
      try {
        // To create the actual situation, container would have been in closed
        // state at SCM.
        scm.getContainerManager().getContainerStateManager()
            .updateContainerStateWithSequenceId(container.containerID().getProtobuf(),
                HddsProtos.LifeCycleEvent.FINALIZE, 0L);
        scm.getContainerManager().getContainerStateManager()
            .updateContainerStateWithSequenceId(container.containerID().getProtobuf(),
                HddsProtos.LifeCycleEvent.CLOSE, 0L);

        //Create the recovering container in DN.
        String encodedToken = cToken.encodeToUrlString();
        ContainerProtocolCalls.createRecoveringContainer(dnClient,
            container.containerID().getProtobuf().getId(),
            encodedToken, replicaIndex);

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
        assertEquals(ContainerProtos.ContainerDataProto.State.RECOVERING,
            readContainerResponseProto.getContainerData().getState());
        // Container at SCM should be still in closed state.
        assertEquals(HddsProtos.LifeCycleState.CLOSED,
            scm.getContainerManager().getContainerStateManager()
                .getContainer(container.containerID()).getState());
        // close container call
        ContainerProtocolCalls.closeContainer(dnClient,
            container.containerID().getProtobuf().getId(), encodedToken);
        // Make sure we have the container and readable.
        readContainerResponseProto = ContainerProtocolCalls
            .readContainer(dnClient,
                container.containerID().getProtobuf().getId(), encodedToken);
        assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
            readContainerResponseProto.getContainerData().getState());
        ContainerProtos.ReadChunkResponseProto readChunkResponseProto =
            ContainerProtocolCalls.readChunk(dnClient,
                writeChunkRequest.getWriteChunk().getChunkData(),
                blockID.getDatanodeBlockIDProtobufBuilder().setReplicaIndex(replicaIndex).build(), null,
                blockToken);
        ByteBuffer[] readOnlyByteBuffersArray = BufferUtils
            .getReadOnlyByteBuffersArray(
                readChunkResponseProto.getDataBuffers().getBuffersList());
        assertEquals(readOnlyByteBuffersArray[0].limit(), data.length);
        byte[] readBuff = new byte[readOnlyByteBuffersArray[0].limit()];
        readOnlyByteBuffersArray[0].get(readBuff, 0, readBuff.length);
        assertArrayEquals(data, readBuff);
      } finally {
        xceiverClientManager.releaseClient(dnClient, false);
      }
    }
  }

  @Test
  public void testCreateRecoveryContainerAfterDNRestart() throws Exception {
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

      DatanodeDetails targetDN = newPipeline.getNodes().get(0);
      XceiverClientSpi dnClient = xceiverClientManager.acquireClient(
          createSingleNodePipeline(newPipeline, targetDN,
              2));
      try {
        // To create the actual situation, container would have been in closed
        // state at SCM.
        scm.getContainerManager().getContainerStateManager()
            .updateContainerStateWithSequenceId(container.containerID().getProtobuf(),
                HddsProtos.LifeCycleEvent.FINALIZE, 0L);
        scm.getContainerManager().getContainerStateManager()
            .updateContainerStateWithSequenceId(container.containerID().getProtobuf(),
                HddsProtos.LifeCycleEvent.CLOSE, 0L);

        //Create the recovering container in target DN.
        String encodedToken = cToken.encodeToUrlString();
        ContainerProtocolCalls.createRecoveringContainer(dnClient,
            container.containerID().getProtobuf().getId(),
            encodedToken, 4);

        // Restart the DN.
        cluster.restartHddsDatanode(targetDN, true);

        // Recovering container state after DN restart should be UNHEALTHY.
        assertEquals(ContainerProtos.ContainerDataProto.State.UNHEALTHY,
            cluster.getHddsDatanode(targetDN)
                .getDatanodeStateMachine()
                .getContainer()
                .getContainerSet()
                .getContainer(container.getContainerID())
                .getContainerState());

        // Writes to recovering container after DN restart should fail
        // because the container is marked UNHEALTHY
        // and hence does not accept any writes.
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
        scm.getPipelineManager().activatePipeline(newPipeline.getId());

        try {
          dnClient.sendCommand(writeChunkRequest);
        } catch (StorageContainerException e) {
          assertEquals(CONTAINER_UNHEALTHY, e.getResult());
        }

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
  void testECReconstructionCoordinatorWith(List<Integer> missingIndexes, boolean triggerRetry)
      throws Exception {
    testECReconstructionCoordinator(missingIndexes, 3, triggerRetry);
  }

  @ParameterizedTest
  @MethodSource("recoverableMissingIndexes")
  void testECReconstructionCoordinatorWithPartialStripe(List<Integer> missingIndexes,
      boolean triggerRetry) throws Exception {
    testECReconstructionCoordinator(missingIndexes, 1, triggerRetry);
  }

  @ParameterizedTest
  @MethodSource("recoverableMissingIndexes")
  void testECReconstructionCoordinatorWithFullAndPartialStripe(List<Integer> missingIndexes,
      boolean triggerRetry) throws Exception {
    testECReconstructionCoordinator(missingIndexes, 4, triggerRetry);
  }

  static Stream<Arguments> recoverableMissingIndexes() {
    Stream<Arguments> args = IntStream.rangeClosed(1, 5).mapToObj(i -> arguments(ImmutableList.of(i), true));
    Stream<Arguments> args1 = IntStream.rangeClosed(1, 5).mapToObj(i -> arguments(ImmutableList.of(i), false));
    Stream<Arguments> args2 =  Stream.of(arguments(ImmutableList.of(2, 3), true),
        arguments(ImmutableList.of(2, 4), true), arguments(ImmutableList.of(3, 5), true));
    Stream<Arguments> args3 =  Stream.of(arguments(ImmutableList.of(2, 3), false),
        arguments(ImmutableList.of(2, 4), false), arguments(ImmutableList.of(3, 5), false));
    return Stream.concat(Stream.concat(args, args1), Stream.concat(args2, args3));
  }

  /**
   * Tests the reconstruction of data when more than parity blocks missed.
   * Test should throw InsufficientLocationsException.
   */
  @Test
  public void testECReconstructionCoordinatorWithMissingIndexes135() {
    InsufficientLocationsException exception =
        assertThrows(InsufficientLocationsException.class, () -> {
          testECReconstructionCoordinator(ImmutableList.of(1, 3, 5), 3, false);
        });

    String expectedMessage =
        "There are insufficient datanodes to read the EC block";
    String actualMessage = exception.getMessage();

    assertEquals(expectedMessage, actualMessage);
  }

  private void testECReconstructionCoordinator(List<Integer> missingIndexes,
      int numInputChunks, boolean triggerRetry) throws Exception {
    ObjectStore objectStore = rpcClient.getObjectStore();
    String keyString = UUID.randomUUID().toString();
    String volumeName = UUID.randomUUID().toString();
    String bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
    OzoneVolume volume = objectStore.getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    createKeyAndWriteData(keyString, bucket, numInputChunks, triggerRetry);

    try (
        XceiverClientManager xceiverClientManager =
            new XceiverClientManager(config);
        ECReconstructionCoordinator coordinator =
            new ECReconstructionCoordinator(config, certClient, secretKeyClient,
                null, ECReconstructionMetrics.create(), "2")) {

      ECReconstructionMetrics metrics =
          coordinator.getECReconstructionMetrics();
      OzoneKeyDetails key = bucket.getKey(keyString);
      long conID = key.getOzoneKeyLocations().get(0).getContainerID();
      Token<ContainerTokenIdentifier> cToken = containerTokenGenerator
          .generateToken(ANY_USER, ContainerID.valueOf(conID));

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

      assertEquals(missingIndexes.size(), targetNodes.size());

      List<org.apache.hadoop.ozone.container.common.helpers.BlockData[]>
          blockDataArrList = new ArrayList<>();
      try (ECContainerOperationClient ecContainerOperationClient = new ECContainerOperationClient(config, certClient)) {
        for (Pipeline deletePipeline : containerToDeletePipeline) {
          org.apache.hadoop.ozone.container.common.helpers.BlockData[] blockData =
              ecContainerOperationClient.listBlock(
                  conID,
                  deletePipeline.getFirstNode(),
                  (ECReplicationConfig) deletePipeline.getReplicationConfig(),
                  cToken
              );

          blockDataArrList.add(blockData);
          // Delete the first index container
          XceiverClientSpi client = xceiverClientManager.acquireClient(deletePipeline);
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
          assertEquals(blockDataArrList.get(i).length,
              reconstructedBlockData.length);
          checkBlockDataWithRetry(blockDataArrList.get(i), reconstructedBlockData, triggerRetry);
          XceiverClientSpi client = xceiverClientManager.acquireClient(
              newTargetPipeline);
          try {
            ContainerProtos.ReadContainerResponseProto readContainerResponse =
                ContainerProtocolCalls.readContainer(
                    client, conID,
                    cToken.encodeToUrlString());
            assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
                readContainerResponse.getContainerData().getState());
          } finally {
            xceiverClientManager.releaseClient(client, false);
          }
          i++;
        }
        assertEquals(1L, metrics.getReconstructionTotal());
      }
    }
  }

  private void createKeyAndWriteData(String keyString, OzoneBucket bucket,
      int numChunks, boolean triggerRetry) throws IOException {
    for (int i = 0; i < numChunks; i++) {
      inputChunks[i] = getBytesWith(i + 1, EC_CHUNK_SIZE);
    }
    try (OzoneOutputStream out = bucket.createKey(keyString, 4096,
        new ECReplicationConfig(3, 2, EcCodec.RS, EC_CHUNK_SIZE),
        new HashMap<>())) {
      assertInstanceOf(KeyOutputStream.class, out.getOutputStream());
      for (int i = 0; i < numChunks; i++) {
        // We generally wait until the data is written to the last chunk
        // before attempting to trigger CloseContainer.
        // We use an asynchronous approach for this trigger,
        // aiming to ensure that closing the container does not interfere with the write operation.
        // However, this process often needs to be executed multiple times before it takes effect.
        if (i == numChunks - 1 && triggerRetry) {
          triggerRetryByCloseContainer(out);
        }
        out.write(inputChunks[i]);
      }
    }
  }

  private void triggerRetryByCloseContainer(OzoneOutputStream out) {
    CompletableFuture.runAsync(() -> {
      BlockOutputStreamEntry blockOutputStreamEntry = out.getKeyOutputStream().getStreamEntries().get(0);
      BlockID entryBlockID = blockOutputStreamEntry.getBlockID();
      long entryContainerID = entryBlockID.getContainerID();
      Pipeline entryPipeline = blockOutputStreamEntry.getPipeline();
      Map<DatanodeDetails, Integer> replicaIndexes = entryPipeline.getReplicaIndexes();
      try {
        for (Map.Entry<DatanodeDetails, Integer> entry : replicaIndexes.entrySet()) {
          DatanodeDetails key = entry.getKey();
          Integer value = entry.getValue();
          XceiverClientManager xceiverClientManager = new XceiverClientManager(config);
          Token<ContainerTokenIdentifier> cToken = containerTokenGenerator
              .generateToken(ANY_USER, ContainerID.valueOf(entryContainerID));
          XceiverClientSpi client = xceiverClientManager.acquireClient(
              createSingleNodePipeline(entryPipeline, key, value));
          try {
            ContainerProtocolCalls.closeContainer(client, entryContainerID, cToken.encodeToUrlString());
          } finally {
            xceiverClientManager.releaseClient(client, false);
          }
          break;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
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
    createKeyAndWriteData(keyString, bucket, 3, false);

    OzoneKeyDetails key = bucket.getKey(keyString);
    long conID = key.getOzoneKeyLocations().get(0).getContainerID();
    Token<ContainerTokenIdentifier> cToken =
        containerTokenGenerator.generateToken(ANY_USER, ContainerID.valueOf(conID));
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

    assertThrows(IOException.class, () -> {
      try (ECReconstructionCoordinator coordinator =
               new ECReconstructionCoordinator(config, certClient,
                   secretKeyClient,
                   null, ECReconstructionMetrics.create(), "")) {
        coordinator.reconstructECContainerGroup(conID,
            (ECReplicationConfig) containerPipeline.getReplicationConfig(),
            sourceNodeMap, targetNodeMap);
      }
    });
    final DatanodeDetails targetDNToCheckContainerCLeaned = goodTargetNode;
    StorageContainerException ex =
        assertThrows(StorageContainerException.class, () -> {
          try (ECContainerOperationClient client =
              new ECContainerOperationClient(config, certClient)) {
            client.listBlock(conID, targetDNToCheckContainerCLeaned,
                new ECReplicationConfig(3, 2), cToken);
          }
        });
    assertEquals("ContainerID 1 does not exist", ex.getMessage());
  }

  private void closeContainer(long conID)
      throws IOException, InvalidStateTransitionException {
    //Close the container first.
    scm.getContainerManager().getContainerStateManager().updateContainerStateWithSequenceId(
        HddsProtos.ContainerID.newBuilder().setId(conID).build(),
        HddsProtos.LifeCycleEvent.FINALIZE, 0L);
    scm.getContainerManager().getContainerStateManager().updateContainerStateWithSequenceId(
        HddsProtos.ContainerID.newBuilder().setId(conID).build(),
        HddsProtos.LifeCycleEvent.CLOSE, 0L);
  }

  private void checkBlockDataWithRetry(
      org.apache.hadoop.ozone.container.common.helpers.BlockData[] blockData,
      org.apache.hadoop.ozone.container.common.helpers.BlockData[]
      reconstructedBlockData, boolean triggerRetry) {
    if (triggerRetry) {
      for (int i = 0; i < reconstructedBlockData.length; i++) {
        assertEquals(blockData[i].getBlockID(), reconstructedBlockData[i].getBlockID());
        List<ContainerProtos.ChunkInfo> oldBlockDataChunks = blockData[i].getChunks();
        List<ContainerProtos.ChunkInfo> newBlockDataChunks = reconstructedBlockData[i].getChunks();
        for (int j = 0; j < newBlockDataChunks.size(); j++) {
          ContainerProtos.ChunkInfo chunkInfo = oldBlockDataChunks.get(j);
          assertEquals(chunkInfo, newBlockDataChunks.get(j));
        }
      }
      return;
    }
    checkBlockData(blockData, reconstructedBlockData);
  }

  private void checkBlockData(
      org.apache.hadoop.ozone.container.common.helpers.BlockData[] blockData,
      org.apache.hadoop.ozone.container.common.helpers.BlockData[]
          reconstructedBlockData) {

    for (int i = 0; i < blockData.length; i++) {
      assertEquals(blockData[i].getBlockID(), reconstructedBlockData[i].getBlockID());
      assertEquals(blockData[i].getSize(), reconstructedBlockData[i].getSize());
      assertEquals(blockData[i].getMetadata(), reconstructedBlockData[i].getMetadata());
      List<ContainerProtos.ChunkInfo> oldBlockDataChunks =
          blockData[i].getChunks();
      List<ContainerProtos.ChunkInfo> newBlockDataChunks =
          reconstructedBlockData[i].getChunks();
      for (int j = 0; j < oldBlockDataChunks.size(); j++) {
        ContainerProtos.ChunkInfo chunkInfo = oldBlockDataChunks.get(j);
        assertEquals(chunkInfo, newBlockDataChunks.get(j));
      }
      // Ensure there are no extra chunks in the reconstructed block
      assertEquals(oldBlockDataChunks.size(), newBlockDataChunks.size());
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
    secretKeyClient = new SecretKeyTestClient();

    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(NUM_DN)
        .setCertificateClient(certClient)
        .setSecretKeyClient(secretKeyClient)
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
    OzoneVolume classVolume = store.getVolume(volumeName);
    classVolume.createBucket(bucketName);
    classBucket = classVolume.getBucket(bucketName);
    repConfig =
        new ECReplicationConfig(EC_DATA, EC_PARITY, EC_CODEC, EC_CHUNK_SIZE);
    values = new byte[ranges.length][];
    for (int i = 0; i < ranges.length; i++) {
      int keySize = RandomUtils.secure().randomInt(ranges[i][0], ranges[i][1]);
      values[i] = RandomUtils.secure().randomBytes(keySize);
      final String keyName = UUID.randomUUID().toString();
      try (OutputStream out = classBucket
          .createKey(keyName, values[i].length, repConfig, new HashMap<>())) {
        out.write(values[i]);
      }
    }

    List<ContainerID> containerIDs =
            scm.getContainerManager().getContainers()
                    .stream()
                    .map(ContainerInfo::containerID)
                    .collect(Collectors.toList());
    assertEquals(1, containerIDs.size());
    containerID = containerIDs.get(0).getId();
    List<Pipeline> pipelines = scm.getPipelineManager().getPipelines(repConfig);
    assertEquals(1, pipelines.size());
    pipeline = pipelines.get(0);
    datanodeDetails = pipeline.getNodes();

    long tokenLifetime = TimeUnit.DAYS.toMillis(1);
    containerTokenGenerator = new ContainerTokenSecretManager(
        tokenLifetime, secretKeyClient);
    blockTokenGenerator = new OzoneBlockTokenSecretManager(
        tokenLifetime, secretKeyClient);
    containerToken = containerTokenGenerator
        .generateToken(ANY_USER, ContainerID.valueOf(containerID));
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
  }

}
