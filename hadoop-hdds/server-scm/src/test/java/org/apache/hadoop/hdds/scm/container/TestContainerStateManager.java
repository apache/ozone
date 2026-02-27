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

package org.apache.hadoop.hdds.scm.container;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getContainer;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getECContainer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.common.helpers.InvalidContainerStateException;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

/**
 * Testing ContainerStatemanager.
 */
public class TestContainerStateManager {

  private ContainerStateManager containerStateManager;
  @TempDir
  private File testDir;
  private DBStore dbStore;
  private Pipeline pipeline;
  private MockNodeManager nodeManager;
  private ContainerManager containerManager;
  private SCMContext scmContext;
  private EventPublisher publisher;

  @BeforeEach
  public void init() throws IOException, TimeoutException, InvalidStateTransitionException {
    OzoneConfiguration conf = new OzoneConfiguration();
    SCMHAManager scmhaManager = SCMHAManagerStub.getInstance(true);
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(conf, SCMDBDefinition.get());
    PipelineManager pipelineManager = mock(PipelineManager.class);
    pipeline = Pipeline.newBuilder().setState(Pipeline.PipelineState.CLOSED)
            .setId(PipelineID.randomId())
            .setReplicationConfig(StandaloneReplicationConfig.getInstance(
                ReplicationFactor.THREE))
            .setNodes(new ArrayList<>()).build();
    when(pipelineManager.createPipeline(StandaloneReplicationConfig.getInstance(
        ReplicationFactor.THREE))).thenReturn(pipeline);
    when(pipelineManager.containsPipeline(any(PipelineID.class))).thenReturn(true);


    containerStateManager = ContainerStateManagerImpl.newBuilder()
        .setConfiguration(conf)
        .setPipelineManager(pipelineManager)
        .setRatisServer(scmhaManager.getRatisServer())
        .setContainerStore(SCMDBDefinition.CONTAINERS.getTable(dbStore))
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .setContainerReplicaPendingOps(new ContainerReplicaPendingOps(
            Clock.system(ZoneId.systemDefault()), null))
        .build();

    nodeManager = new MockNodeManager(true, 10);
    containerManager = mock(ContainerManager.class);
    scmContext = SCMContext.emptyContext();
    scmContext.updateLeaderAndTerm(true, 1L);
    publisher = mock(EventPublisher.class);

    when(containerManager.getContainer(any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainer((ContainerID) invocation.getArguments()[0]));

    when(containerManager.getContainerReplicas(any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainerReplicas((ContainerID) invocation.getArguments()[0]));

    doAnswer(invocation -> {
      containerStateManager.updateContainerStateWithSequenceId(
          ((ContainerID) invocation.getArguments()[0]).getProtobuf(),
          (HddsProtos.LifeCycleEvent) invocation.getArguments()[1], 0L);
      return null;
    }).when(containerManager).updateContainerState(
        any(ContainerID.class), any(HddsProtos.LifeCycleEvent.class));

    doAnswer(invocation -> {
      containerStateManager.updateContainerReplica(
          (ContainerReplica) invocation.getArguments()[1]);
      return null;
    }).when(containerManager).updateContainerReplica(
        any(ContainerID.class), any(ContainerReplica.class));

    doAnswer(invocation -> {
      containerStateManager.removeContainerReplica(
          (ContainerReplica) invocation.getArguments()[1]);
      return null;
    }).when(containerManager).removeContainerReplica(
        any(ContainerID.class), any(ContainerReplica.class));

    doAnswer(invocation -> {
      containerStateManager.transitionDeletingOrDeletedToClosedState(
          ((ContainerID) invocation.getArgument(0)).getProtobuf());
      return null;
    }).when(containerManager).transitionDeletingOrDeletedToClosedState(any(ContainerID.class));

  }

  @AfterEach
  public void tearDown() throws Exception {
    if (dbStore != null) {
      dbStore.close();
    }
  }

  @Test
  public void checkReplicationStateOK()
      throws IOException, TimeoutException {
    //GIVEN
    ContainerInfo c1 = allocateContainer();

    DatanodeDetails d1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails d2 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails d3 = MockDatanodeDetails.randomDatanodeDetails();

    addReplica(c1, d1);
    addReplica(c1, d2);
    addReplica(c1, d3);

    //WHEN
    Set<ContainerReplica> replicas = containerStateManager
        .getContainerReplicas(c1.containerID());

    //THEN
    assertEquals(3, replicas.size());
  }

  @Test
  public void checkReplicationStateMissingReplica()
      throws IOException, TimeoutException {
    //GIVEN

    ContainerInfo c1 = allocateContainer();

    DatanodeDetails d1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails d2 = MockDatanodeDetails.randomDatanodeDetails();

    addReplica(c1, d1);
    addReplica(c1, d2);

    //WHEN
    Set<ContainerReplica> replicas = containerStateManager
        .getContainerReplicas(c1.containerID());

    assertEquals(2, replicas.size());
    assertEquals(3, c1.getReplicationConfig().getRequiredNodes());
  }

  @ParameterizedTest
  @EnumSource(value = HddsProtos.LifeCycleState.class,
      names = {"DELETING", "DELETED"})
  public void testTransitionDeletingOrDeletedToClosedState(HddsProtos.LifeCycleState lifeCycleState)
      throws IOException {
    HddsProtos.ContainerInfoProto.Builder builder = HddsProtos.ContainerInfoProto.newBuilder();
    builder.setContainerID(1)
        .setState(lifeCycleState)
        .setUsedBytes(0)
        .setNumberOfKeys(0)
        .setOwner("root")
        .setReplicationType(HddsProtos.ReplicationType.RATIS)
        .setReplicationFactor(ReplicationFactor.THREE);

    HddsProtos.ContainerInfoProto container = builder.build();
    HddsProtos.ContainerID cid = HddsProtos.ContainerID.newBuilder().setId(container.getContainerID()).build();
    containerStateManager.addContainer(container);
    containerStateManager.transitionDeletingOrDeletedToClosedState(cid);
    assertEquals(HddsProtos.LifeCycleState.CLOSED, containerStateManager.getContainer(ContainerID.getFromProtobuf(cid))
        .getState());
  }

  @ParameterizedTest
  @EnumSource(value = HddsProtos.LifeCycleState.class,
      names = {"CLOSING", "QUASI_CLOSED", "CLOSED", "RECOVERING"})
  public void testTransitionContainerToClosedStateAllowOnlyDeletingOrDeletedContainer(
      HddsProtos.LifeCycleState initialState) throws IOException {
    // Negative test for non-OPEN Ratis container -> CLOSED transitions. OPEN -> CLOSED is tested in:
    // TestContainerManagerImpl#testTransitionContainerToClosedStateAllowOnlyDeletingOrDeletedContainers

    HddsProtos.ContainerInfoProto.Builder builder = HddsProtos.ContainerInfoProto.newBuilder();
    builder.setContainerID(1)
        .setState(initialState)
        .setUsedBytes(0)
        .setNumberOfKeys(0)
        .setOwner("root")
        .setReplicationType(HddsProtos.ReplicationType.RATIS)
        .setReplicationFactor(ReplicationFactor.THREE);

    HddsProtos.ContainerInfoProto container = builder.build();
    HddsProtos.ContainerID cid = HddsProtos.ContainerID.newBuilder().setId(container.getContainerID()).build();
    containerStateManager.addContainer(container);
    try {
      containerStateManager.transitionDeletingOrDeletedToClosedState(cid);
      fail("Was expecting an Exception, but did not catch any.");
    } catch (IOException e) {
      assertInstanceOf(InvalidContainerStateException.class, e.getCause().getCause());
    }
  }

  /**
   * DELETED container + CLOSED replica with BCSID <= container seqId + RATIS replication.
   * Expected: Force delete command should be sent (force=true)
   */
  @Test
  public void testDeletedContainerWithStaleClosedReplicaRatis()
      throws IOException {
    final ContainerInfo container = getContainer(HddsProtos.LifeCycleState.DELETED);
    containerStateManager.addContainer(container.getProtobuf());
    assertEquals(HddsProtos.ReplicationType.RATIS, container.getReplicationType());
    final DatanodeDetails datanode = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator().next();
    // Report CLOSED replica with BCSID = 10000L (equal to container's seqId)
    DeleteContainerCommand deleteCmd = sendReportAndCaptureDeleteCommand(
        container, datanode, 10000L, false, 0, true);
    // Verify force delete
    verifyForceDeleteCommand(deleteCmd, container.containerID(), true,
        "Delete command should have force=true for stale RATIS replica");
    verifyContainerState(container.containerID(), HddsProtos.LifeCycleState.DELETED);
  }

  /**
   * Test: DELETED container + CLOSED replica with BCSID < container seqId + RATIS.
   * Expected: Force delete command should be sent (BCSID is lower)
   */
  @Test
  public void testDeletedContainerWithLowerBcsidStaleReplicaRatis()
      throws IOException {
    final ContainerInfo container = getContainer(HddsProtos.LifeCycleState.DELETED);
    containerStateManager.addContainer(container.getProtobuf());
    final DatanodeDetails datanode = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator().next();
    // Report CLOSED replica with BCSID = 9000L (lower than container's 10000L)
    DeleteContainerCommand deleteCmd = sendReportAndCaptureDeleteCommand(
        container, datanode, 9000L, false, 0, true);
    verifyForceDeleteCommand(deleteCmd, container.containerID(), true,
        "Delete command should have force=true for stale RATIS replica with lower BCSID");
  }

  /**
   * DELETED EC container + CLOSED replica with BCSID <= container seqId.
   * Expected: Should NOT send force delete
   * Should transition to CLOSED instead
   */
  @Test
  public void testDeletedECContainerWithStaleClosedReplicaShouldNotForceDelete()
      throws IOException {
    final DatanodeDetails datanode = randomDatanodeDetails();
    nodeManager.register(datanode, null, null);
    // Create a DELETED EC container
    ECReplicationConfig repConfig = new ECReplicationConfig(3, 2);
    final ContainerInfo ecContainer = getECContainer(
        HddsProtos.LifeCycleState.DELETED,
        PipelineID.randomId(),
        repConfig);
    containerStateManager.addContainer(ecContainer.getProtobuf());
    assertEquals(HddsProtos.ReplicationType.EC, ecContainer.getReplicationType());
    // Report CLOSED replica with BCSID = container's seqId
    sendReportAndCaptureDeleteCommand(ecContainer, datanode,
        ecContainer.getSequenceId(), false, 1, false);
    // Container should transition to CLOSED
    verifyContainerState(ecContainer.containerID(), HddsProtos.LifeCycleState.CLOSED);
  }

  private DeleteContainerCommand sendReportAndCaptureDeleteCommand(
      ContainerInfo container, DatanodeDetails datanode,
      long bcsId, boolean isEmpty, int replicaIndex, boolean reqCommandSend) {
    final ContainerReportHandler reportHandler =
        new ContainerReportHandler(nodeManager, containerManager, scmContext, null);
    final StorageContainerDatanodeProtocolProtos.ContainerReportsProto containerReport =
        getContainerReportsProto(container.containerID(),
            ContainerReplicaProto.State.CLOSED,
            datanode.getUuidString(),
            bcsId,
            isEmpty,
            replicaIndex);
    final SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode reportFromDatanode =
        new SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode(datanode, containerReport);
    reportHandler.onMessage(reportFromDatanode, publisher);
    // Capture the delete command
    ArgumentCaptor<CommandForDatanode<?>> commandCaptor =
        ArgumentCaptor.forClass(CommandForDatanode.class);
    if (reqCommandSend) {
      verify(publisher, times(1))
          .fireEvent(eq(SCMEvents.DATANODE_COMMAND), commandCaptor.capture());
      CommandForDatanode<?> capturedCommand = commandCaptor.getValue();
      assertEquals(DeleteContainerCommand.class, capturedCommand.getCommand().getClass());
      return (DeleteContainerCommand) capturedCommand.getCommand();
    } else {
      verify(publisher, times(0))
          .fireEvent(eq(SCMEvents.DATANODE_COMMAND), commandCaptor.capture());
      return null;
    }
  }

  private void verifyForceDeleteCommand(DeleteContainerCommand deleteCmd,
      ContainerID expectedContainerId, boolean expectedForce, String message) {
    assertEquals(expectedForce, deleteCmd.isForce(), message);
    assertEquals(expectedContainerId.getId(), deleteCmd.getContainerID());
  }

  /**
   * Verifies the container is in the expected state.
   */
  private void verifyContainerState(ContainerID containerId,
      HddsProtos.LifeCycleState expectedState) throws IOException {
    assertEquals(expectedState, containerManager.getContainer(containerId).getState());
  }

  @Test
  public void testGetContainerIDs() throws IOException {
    ContainerInfo openContainerInfo = new ContainerInfo.Builder()
        .setContainerID(1)
        .setState(HddsProtos.LifeCycleState.OPEN)
        .setSequenceId(100L)
        .setOwner("scm")
        .setPipelineID(PipelineID.randomId())
        .setReplicationConfig(
            RatisReplicationConfig
                .getInstance(ReplicationFactor.THREE))
        .build();

    ContainerInfo closedContainerInfo = new ContainerInfo.Builder()
        .setContainerID(2)
        .setState(HddsProtos.LifeCycleState.CLOSED)
        .setSequenceId(200L)
        .setOwner("scm")
        .setPipelineID(PipelineID.randomId())
        .setReplicationConfig(
            RatisReplicationConfig
                .getInstance(ReplicationFactor.THREE))
        .build();

    containerStateManager.addContainer(openContainerInfo.getProtobuf());
    containerStateManager.addContainer(closedContainerInfo.getProtobuf());

    assertEquals(2, containerStateManager.getContainerIDs(ContainerID.MIN, 10).size());
    assertEquals(1, containerStateManager.getContainerIDs(
        HddsProtos.LifeCycleState.CLOSED, ContainerID.MIN, 10).size());
  }

  @Test
  public void testSequenceIdOnStateUpdate() throws Exception {
    ContainerID containerID = ContainerID.valueOf(3L);
    long sequenceId = 100L;

    ContainerInfo containerInfo = new ContainerInfo.Builder()
        .setContainerID(containerID.getId())
        .setState(HddsProtos.LifeCycleState.OPEN)
        .setSequenceId(sequenceId)
        .setOwner("scm")
        .setPipelineID(PipelineID.randomId())
        .setReplicationConfig(
            RatisReplicationConfig
                .getInstance(ReplicationFactor.THREE))
        .build();

    containerStateManager.addContainer(containerInfo.getProtobuf());

    // Try to update with a higher sequenceId
    containerStateManager.updateContainerStateWithSequenceId(
        containerID.getProtobuf(),
        HddsProtos.LifeCycleEvent.FINALIZE,
        sequenceId + 1);

    ContainerInfo afterFirst = containerStateManager.getContainer(containerID);
    long currentSequenceId = afterFirst.getSequenceId();
    // Sequence id should be updated with latest sequence id
    assertEquals(sequenceId + 1, currentSequenceId);

    // Try updating with older sequenceId
    containerStateManager.updateContainerStateWithSequenceId(
        containerID.getProtobuf(),
        HddsProtos.LifeCycleEvent.CLOSE,
        sequenceId - 10); // Older sequenceId

    // Assert - SequenceId should not change
    ContainerInfo finalInfo = containerStateManager.getContainer(containerID);
    assertEquals(finalInfo.getSequenceId(), currentSequenceId);
  }

  private void addReplica(ContainerInfo cont, DatanodeDetails node) {
    ContainerReplica replica = ContainerReplica.newBuilder()
        .setContainerID(cont.containerID())
        .setContainerState(ContainerReplicaProto.State.CLOSED)
        .setDatanodeDetails(node)
        .build();
    containerStateManager.updateContainerReplica(replica);
  }

  private ContainerInfo allocateContainer()
      throws IOException, TimeoutException {

    final ContainerInfo containerInfo = new ContainerInfo.Builder()
        .setState(HddsProtos.LifeCycleState.OPEN)
        .setPipelineID(pipeline.getId())
        .setUsedBytes(0)
        .setNumberOfKeys(0)
        .setOwner("root")
        .setContainerID(1)
        .setDeleteTransactionId(0)
        .setReplicationConfig(pipeline.getReplicationConfig())
        .build();

    containerStateManager.addContainer(containerInfo.getProtobuf());
    return containerInfo;
  }

  private static StorageContainerDatanodeProtocolProtos.ContainerReportsProto getContainerReportsProto(
      final ContainerID containerId,
      final ContainerReplicaProto.State state,
      final String originNodeId,
      final long bcsId,
      final boolean isEmpty,
      final int replicaIndex) {
    final StorageContainerDatanodeProtocolProtos.ContainerReportsProto.Builder crBuilder =
        StorageContainerDatanodeProtocolProtos.ContainerReportsProto.newBuilder();
    final ContainerReplicaProto replicaProto =
        ContainerReplicaProto.newBuilder()
            .setContainerID(containerId.getProtobuf().getId())
            .setState(state)
            .setOriginNodeId(originNodeId)
            .setSize(5368709120L)
            .setUsed(isEmpty ? 0L : 2000000000L)
            .setKeyCount(isEmpty ? 0L : 100000000L)
            .setReadCount(100000000L)
            .setWriteCount(100000000L)
            .setReadBytes(2000000000L)
            .setWriteBytes(2000000000L)
            .setBlockCommitSequenceId(bcsId)
            .setDeleteTransactionId(0)
            .setReplicaIndex(replicaIndex)
            .setIsEmpty(isEmpty)
            .build();
    return crBuilder.addReports(replicaProto).build();
  }

}
