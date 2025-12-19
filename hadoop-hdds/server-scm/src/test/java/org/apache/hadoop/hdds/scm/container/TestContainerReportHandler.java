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
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getContainerReports;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getECContainer;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getReplicas;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.MockPipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test the behaviour of the ContainerReportHandler.
 */
public class TestContainerReportHandler {

  private MockNodeManager nodeManager;
  private ContainerManager containerManager;
  private ContainerStateManager containerStateManager;
  private EventPublisher publisher;
  @TempDir
  private File testDir;
  private DBStore dbStore;
  private PipelineManager pipelineManager;

  @BeforeEach
  void setup() throws IOException, InvalidStateTransitionException {
    final OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
    nodeManager = new MockNodeManager(true, 10);
    containerManager = mock(ContainerManager.class);
    dbStore = DBStoreBuilder.createDBStore(conf, SCMDBDefinition.get());
    SCMHAManager scmhaManager = SCMHAManagerStub.getInstance(true);
    pipelineManager =
        new MockPipelineManager(dbStore, scmhaManager, nodeManager);
    containerStateManager = ContainerStateManagerImpl.newBuilder()
        .setConfiguration(conf)
        .setPipelineManager(pipelineManager)
        .setRatisServer(scmhaManager.getRatisServer())
        .setContainerStore(SCMDBDefinition.CONTAINERS.getTable(dbStore))
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .setContainerReplicaPendingOps(new ContainerReplicaPendingOps(
            Clock.system(ZoneId.systemDefault()), null))
        .build();
    publisher = mock(EventPublisher.class);

    when(containerManager.getContainer(any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainer(((ContainerID)invocation
                .getArguments()[0])));

    when(containerManager.getContainerReplicas(
        any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainerReplicas(((ContainerID)invocation
                .getArguments()[0])));

    doAnswer(invocation -> {
      containerStateManager
          .updateContainerState(((ContainerID)invocation
                  .getArguments()[0]).getProtobuf(),
              (HddsProtos.LifeCycleEvent)invocation.getArguments()[1]);
      return null;
    }).when(containerManager).updateContainerState(
        any(ContainerID.class),
        any(HddsProtos.LifeCycleEvent.class));

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

  static Stream<Arguments> containerAndReplicaStates() {
    // Replication types to test
    List<HddsProtos.ReplicationType> replicationTypes = Arrays.asList(
        HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationType.EC
    );

    // Container states to test
    List<HddsProtos.LifeCycleState> containerStates = Arrays.asList(
        HddsProtos.LifeCycleState.DELETING,
        HddsProtos.LifeCycleState.DELETED
    );

    // Replica states to test
    List<ContainerReplicaProto.State> replicaStates = Arrays.asList(
        ContainerReplicaProto.State.QUASI_CLOSED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSING,
        ContainerReplicaProto.State.OPEN,
        ContainerReplicaProto.State.UNHEALTHY
    );

    List<ContainerReplicaProto.State> invalidReplicaStates = Arrays.asList(
        ContainerReplicaProto.State.INVALID,
        ContainerReplicaProto.State.DELETED
    );

    // Generate all combinations, container state * replica state
    List<Arguments> combinations = new ArrayList<>();
    for (HddsProtos.ReplicationType replicationType : replicationTypes) {
      for (HddsProtos.LifeCycleState containerState : containerStates) {
        for (ContainerReplicaProto.State replicaState : replicaStates) {
          if (replicationType == HddsProtos.ReplicationType.EC &&
              replicaState.equals(ContainerReplicaProto.State.QUASI_CLOSED)) {
            continue;
          }
          for (ContainerReplicaProto.State invalidState : invalidReplicaStates) {
            combinations.add(Arguments.of(replicationType, containerState, replicaState, invalidState));
          }
        }
      }
    }

    return combinations.stream();
  }

  private void testReplicaIndexUpdate(ContainerInfo container,
               DatanodeDetails dn, int replicaIndex,
               Map<DatanodeDetails, Integer> expectedReplicaMap) {
    final ContainerReportsProto containerReport = getContainerReportsProto(
            container.containerID(), ContainerReplicaProto.State.CLOSED,
            dn.getUuidString(), 2000000000L, 100000000L, 10000L, replicaIndex);
    final ContainerReportFromDatanode containerReportFromDatanode =
            new ContainerReportFromDatanode(dn, containerReport);
    final ContainerReportHandler reportHandler = new ContainerReportHandler(
            nodeManager, containerManager);
    reportHandler.onMessage(containerReportFromDatanode, publisher);
    assertEquals(containerStateManager
            .getContainerReplicas(container.containerID()).stream()
            .collect(Collectors.toMap(ContainerReplica::getDatanodeDetails,
                    ContainerReplica::getReplicaIndex)), expectedReplicaMap);

  }

  @Test
  public void testECReplicaIndexValidation() throws NodeNotFoundException,
          IOException, TimeoutException {
    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
            NodeStatus.inServiceHealthy()).iterator();
    final DatanodeDetails datanodeOne = nodeIterator.next();
    final DatanodeDetails datanodeTwo = nodeIterator.next();
    final DatanodeDetails datanodeThree = nodeIterator.next();
    final DatanodeDetails datanodeFour = nodeIterator.next();
    final DatanodeDetails datanodeFive = nodeIterator.next();
    ECReplicationConfig replicationConfig = new ECReplicationConfig(3, 2);
    final ContainerInfo container = getECContainer(LifeCycleState.CLOSED,
            PipelineID.randomId(), replicationConfig);
    nodeManager.addContainer(datanodeOne, container.containerID());
    nodeManager.addContainer(datanodeTwo, container.containerID());
    nodeManager.addContainer(datanodeThree, container.containerID());
    nodeManager.addContainer(datanodeFour, container.containerID());
    nodeManager.addContainer(datanodeFive, container.containerID());
    containerStateManager.addContainer(container.getProtobuf());
    Set<ContainerReplica> replicas =
            HddsTestUtils.getReplicasWithReplicaIndex(container.containerID(),
            ContainerReplicaProto.State.CLOSED,
            HddsTestUtils.CONTAINER_USED_BYTES_DEFAULT,
            HddsTestUtils.CONTAINER_NUM_KEYS_DEFAULT,
            1000000L,
            datanodeOne, datanodeTwo, datanodeThree,
            datanodeFour, datanodeFive);
    Map<DatanodeDetails, Integer> replicaMap = replicas.stream()
            .collect(Collectors.toMap(ContainerReplica::getDatanodeDetails,
                    ContainerReplica::getReplicaIndex));
    replicas.forEach(containerStateManager::updateContainerReplica);
    testReplicaIndexUpdate(container, datanodeOne, 0, replicaMap);
    testReplicaIndexUpdate(container, datanodeOne, 6, replicaMap);
    replicaMap.put(datanodeOne, 2);
    testReplicaIndexUpdate(container, datanodeOne, 2, replicaMap);
  }

  @Test
  public void testUnderReplicatedContainer()
      throws NodeNotFoundException, IOException, TimeoutException {
    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);
    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();
    final DatanodeDetails datanodeOne = nodeIterator.next();
    final DatanodeDetails datanodeTwo = nodeIterator.next();
    final DatanodeDetails datanodeThree = nodeIterator.next();

    final ContainerInfo containerOne = getContainer(LifeCycleState.CLOSED);
    final ContainerInfo containerTwo = getContainer(LifeCycleState.CLOSED);
    final Set<ContainerID> containerIDSet = Stream.of(
        containerOne.containerID(), containerTwo.containerID())
        .collect(Collectors.toSet());

    nodeManager.setContainers(datanodeOne, containerIDSet);
    nodeManager.setContainers(datanodeTwo, containerIDSet);
    nodeManager.setContainers(datanodeThree, containerIDSet);

    containerStateManager.addContainer(containerOne.getProtobuf());
    containerStateManager.addContainer(containerTwo.getProtobuf());

    getReplicas(containerOne.containerID(),
        ContainerReplicaProto.State.CLOSED,
        datanodeOne, datanodeTwo, datanodeThree)
        .forEach(containerStateManager::updateContainerReplica);

    getReplicas(containerTwo.containerID(),
        ContainerReplicaProto.State.CLOSED,
        datanodeOne, datanodeTwo, datanodeThree)
        .forEach(containerStateManager::updateContainerReplica);

    // SCM expects both containerOne and containerTwo to be in all the three
    // datanodes datanodeOne, datanodeTwo and datanodeThree

    // Now datanodeOne is sending container report in which containerOne is
    // missing.

    // containerOne becomes under replicated.
    final ContainerReportsProto containerReport = getContainerReportsProto(
        containerTwo.containerID(), ContainerReplicaProto.State.CLOSED,
        datanodeOne.getUuidString());
    final ContainerReportFromDatanode containerReportFromDatanode =
        new ContainerReportFromDatanode(datanodeOne, containerReport);
    reportHandler.onMessage(containerReportFromDatanode, publisher);
    assertEquals(2, containerManager.getContainerReplicas(
        containerOne.containerID()).size());

  }

  @Test
  public void testOverReplicatedContainer() throws NodeNotFoundException,
      IOException, TimeoutException {

    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);

    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();
    final DatanodeDetails datanodeOne = nodeIterator.next();
    final DatanodeDetails datanodeTwo = nodeIterator.next();
    final DatanodeDetails datanodeThree = nodeIterator.next();
    final DatanodeDetails datanodeFour = nodeIterator.next();

    final ContainerInfo containerOne = getContainer(LifeCycleState.CLOSED);
    final ContainerInfo containerTwo = getContainer(LifeCycleState.CLOSED);

    final Set<ContainerID> containerIDSet = Stream.of(
        containerOne.containerID(), containerTwo.containerID())
        .collect(Collectors.toSet());

    nodeManager.setContainers(datanodeOne, containerIDSet);
    nodeManager.setContainers(datanodeTwo, containerIDSet);
    nodeManager.setContainers(datanodeThree, containerIDSet);

    containerStateManager.addContainer(containerOne.getProtobuf());
    containerStateManager.addContainer(containerTwo.getProtobuf());

    getReplicas(containerOne.containerID(),
        ContainerReplicaProto.State.CLOSED,
        datanodeOne, datanodeTwo, datanodeThree)
        .forEach(containerStateManager::updateContainerReplica);

    getReplicas(containerTwo.containerID(),
        ContainerReplicaProto.State.CLOSED,
        datanodeOne, datanodeTwo, datanodeThree)
        .forEach(containerStateManager::updateContainerReplica);

    // SCM expects both containerOne and containerTwo to be in all the three
    // datanodes datanodeOne, datanodeTwo and datanodeThree

    // Now datanodeFour is sending container report which has containerOne.

    // containerOne becomes over replicated.

    final ContainerReportsProto containerReport = getContainerReportsProto(
        containerOne.containerID(), ContainerReplicaProto.State.CLOSED,
        datanodeFour.getUuidString());
    final ContainerReportFromDatanode containerReportFromDatanode =
        new ContainerReportFromDatanode(datanodeFour, containerReport);
    reportHandler.onMessage(containerReportFromDatanode, publisher);

    assertEquals(4, containerManager.getContainerReplicas(
        containerOne.containerID()).size());
  }

  @Test
  public void testClosingToClosed() throws NodeNotFoundException, IOException,
      TimeoutException {
    /*
     * The container is in CLOSING state and all the replicas are in
     * OPEN/CLOSING state.
     *
     * The datanode reports that one of the replica is now CLOSED.
     *
     * In this case SCM should mark the container as CLOSED.
     */

    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);

    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();
    final DatanodeDetails datanodeOne = nodeIterator.next();
    final DatanodeDetails datanodeTwo = nodeIterator.next();
    final DatanodeDetails datanodeThree = nodeIterator.next();

    final ContainerInfo containerOne = getContainer(LifeCycleState.CLOSING);
    final ContainerInfo containerTwo = getContainer(LifeCycleState.CLOSED);

    final Set<ContainerID> containerIDSet = Stream.of(
        containerOne.containerID(), containerTwo.containerID())
        .collect(Collectors.toSet());

    final Set<ContainerReplica> containerOneReplicas = getReplicas(
        containerOne.containerID(),
        ContainerReplicaProto.State.CLOSING,
        datanodeOne);

    containerOneReplicas.addAll(getReplicas(
        containerOne.containerID(),
        ContainerReplicaProto.State.OPEN,
        datanodeTwo, datanodeThree));

    final Set<ContainerReplica> containerTwoReplicas = getReplicas(
        containerTwo.containerID(),
        ContainerReplicaProto.State.CLOSED,
        datanodeOne, datanodeTwo, datanodeThree);

    nodeManager.setContainers(datanodeOne, containerIDSet);
    nodeManager.setContainers(datanodeTwo, containerIDSet);
    nodeManager.setContainers(datanodeThree, containerIDSet);

    containerStateManager.addContainer(containerOne.getProtobuf());
    containerStateManager.addContainer(containerTwo.getProtobuf());

    containerOneReplicas.forEach(containerStateManager::updateContainerReplica);
    containerTwoReplicas.forEach(containerStateManager::updateContainerReplica);

    final ContainerReportsProto containerReport = getContainerReportsProto(
        containerOne.containerID(), ContainerReplicaProto.State.CLOSED,
        datanodeOne.getUuidString());
    final ContainerReportFromDatanode containerReportFromDatanode =
        new ContainerReportFromDatanode(datanodeOne, containerReport);
    reportHandler.onMessage(containerReportFromDatanode, publisher);

    assertEquals(LifeCycleState.CLOSED,
        containerManager.getContainer(containerOne.containerID()).getState());
  }

  @Test
  public void testClosingToClosedForECContainer()
      throws NodeNotFoundException, IOException, TimeoutException {
    // Create an EC 3-2 container
    ECReplicationConfig replicationConfig = new ECReplicationConfig(3, 2);
    final ContainerInfo container = getECContainer(LifeCycleState.CLOSING,
        PipelineID.randomId(), replicationConfig);
    List<DatanodeDetails> dns = setupECContainerForTesting(container);
    createAndHandleContainerReport(container.containerID(),
        ContainerReplicaProto.State.CLOSED, dns.get(1), 2);
    // index is 2; container shouldn't transition to CLOSED
    assertEquals(LifeCycleState.CLOSING, containerManager.getContainer(container.containerID()).getState());

    createAndHandleContainerReport(container.containerID(),
        ContainerReplicaProto.State.CLOSED, dns.get(2), 3);
    // index is 3; container shouldn't transition to CLOSED
    assertEquals(LifeCycleState.CLOSING, containerManager.getContainer(container.containerID()).getState());

    createAndHandleContainerReport(container.containerID(),
        ContainerReplicaProto.State.CLOSED, dns.get(0), 1);
    // index is 1; container should transition to CLOSED
    assertEquals(LifeCycleState.CLOSED, containerManager.getContainer(container.containerID()).getState());

    // Test with an EC 6-3 container
    replicationConfig = new ECReplicationConfig(6, 3);
    final ContainerInfo container2 = getECContainer(LifeCycleState.CLOSING,
        PipelineID.randomId(), replicationConfig);
    dns = setupECContainerForTesting(container2);

    createAndHandleContainerReport(container2.containerID(),
        ContainerReplicaProto.State.CLOSED, dns.get(5), 6);
    // index is 6, container shouldn't transition to CLOSED
    assertEquals(LifeCycleState.CLOSING, containerManager.getContainer(container2.containerID()).getState());

    createAndHandleContainerReport(container2.containerID(),
        ContainerReplicaProto.State.CLOSED, dns.get(8), 9);
    // index is 9, container should transition to CLOSED
    assertEquals(LifeCycleState.CLOSED, containerManager.getContainer(container2.containerID()).getState());
  }

  /**
   * Helper method to get a container with specified replication type and state for testing.
   * @param replicationType HddsProtos.ReplicationType
   * @param containerState HddsProtos.LifeCycleState
   * @return ContainerInfo
   */
  private ContainerInfo getContainerHelper(
      HddsProtos.ReplicationType replicationType,
      final HddsProtos.LifeCycleState containerState) {
    switch (replicationType) {
    case RATIS:
      return getContainer(containerState);
    case EC:
      return getECContainer(containerState, PipelineID.randomId(), new ECReplicationConfig(6, 3));
    default:
      fail("Unsupported replication type: " + replicationType);
    }
    // make the compiler happy
    return null;
  }

  /**
   * Tests that a DELETING or DELETED RATIS/EC container transitions to CLOSED if a non-empty replica in OPEN, CLOSING,
   * CLOSED, QUASI_CLOSED or UNHEALTHY state is reported.
   * It should not transition if the replica is in INVALID or DELETED states.
   */
  @ParameterizedTest
  @MethodSource("containerAndReplicaStates")
  public void containerShouldTransitionFromDeletingOrDeletedToClosedWhenNonEmptyReplica(
      HddsProtos.ReplicationType replicationType,
      LifeCycleState containerState,
      ContainerReplicaProto.State replicaState,
      ContainerReplicaProto.State invalidReplicaState)
      throws IOException {

    ContainerInfo container = getContainerHelper(replicationType, containerState);
    containerStateManager.addContainer(container.getProtobuf());

    // set up a non-empty replica in replicaState
    DatanodeDetails dnWithValidReplica = nodeManager.getNodes(NodeStatus.inServiceHealthy()).get(0);
    ContainerReplicaProto.Builder builder = ContainerReplicaProto.newBuilder();
    ContainerReplicaProto validReplica = builder
        .setContainerID(container.getContainerID())
        .setIsEmpty(false)
        .setState(replicaState)
        .setKeyCount(0L)
        .setBlockCommitSequenceId(123L)
        .setReplicaIndex(1)
        .setOriginNodeId(dnWithValidReplica.getUuidString())
        .build();

    // set up a non-empty replica in invalidReplicaState
    DatanodeDetails dnWithInvalidReplica = nodeManager.getNodes(NodeStatus.inServiceHealthy()).get(1);
    ContainerReplicaProto invalidReplica = builder
        .setIsEmpty(false)
        .setState(invalidReplicaState)
        .setReplicaIndex(2)
        .setOriginNodeId(dnWithInvalidReplica.getUuidString())
        .build();

    // should not transition on processing the invalid replica's report
    ContainerReportHandler containerReportHandler = new ContainerReportHandler(nodeManager, containerManager);
    ContainerReportsProto invalidContainerReport = getContainerReports(invalidReplica);
    containerReportHandler
        .onMessage(new ContainerReportFromDatanode(dnWithInvalidReplica, invalidContainerReport), publisher);
    assertEquals(containerState, containerStateManager.getContainer(container.containerID()).getState());
    /**
     * containerState,        DELETED
     * replicaState,          QUASI_CLOSED
     * invalidReplicaState,   INVALID
     * replicationType        EC
     */

    // should transition on processing the valid replica's report
    ContainerReportsProto closedContainerReport = getContainerReports(validReplica);
    containerReportHandler
        .onMessage(new ContainerReportFromDatanode(dnWithValidReplica, closedContainerReport), publisher);
    assertEquals(LifeCycleState.CLOSED, containerStateManager.getContainer(container.containerID()).getState());

    // verify that no delete command is issued for non-empty replica, regardless of container state
    verify(publisher, times(0))
        .fireEvent(eq(SCMEvents.DATANODE_COMMAND), any(CommandForDatanode.class));
  }

  @ParameterizedTest
  @MethodSource("containerAndReplicaStates")
  public void containerShouldNotTransitionFromDeletingOrDeletedToClosedWhenEmptyReplica(
      HddsProtos.ReplicationType replicationType,
      LifeCycleState containerState,
      ContainerReplicaProto.State replicaState) throws IOException {

    ContainerInfo container = getContainerHelper(replicationType, containerState);
    containerStateManager.addContainer(container.getProtobuf());

    // set up an empty replica
    DatanodeDetails dnWithEmptyReplica = nodeManager.getNodes(NodeStatus.inServiceHealthy()).get(0);
    ContainerReplicaProto.Builder builder = ContainerReplicaProto.newBuilder();
    ContainerReplicaProto emptyReplica = builder
        .setContainerID(container.getContainerID())
        .setIsEmpty(true)
        .setState(replicaState)
        .setKeyCount(0L)
        .setBlockCommitSequenceId(123L)
        .setReplicaIndex(1)
        .setOriginNodeId(dnWithEmptyReplica.getUuidString())
        .build();

    ContainerReportHandler containerReportHandler = new ContainerReportHandler(nodeManager, containerManager);
    ContainerReportsProto emptyContainerReport = getContainerReports(emptyReplica);
    containerReportHandler
        .onMessage(new ContainerReportFromDatanode(dnWithEmptyReplica, emptyContainerReport), publisher);
    assertEquals(containerState, containerStateManager.getContainer(container.containerID()).getState());

    // verify number of datanode command fired (e.g. whether delete command is issued for a replica)
    int countDatanodeCommandFired = 0;
    if (containerState == LifeCycleState.DELETED) {
      // when the container is in DELETED state, the empty replica would be removed
      countDatanodeCommandFired++;
    }
    verify(publisher, times(countDatanodeCommandFired))
        .fireEvent(eq(SCMEvents.DATANODE_COMMAND), any(CommandForDatanode.class));
  }

  /**
   * Creates the required number of DNs that will hold a replica each for the
   * specified EC container. Registers these DNs with the NodeManager, adds this
   * container and its replicas to ContainerStateManager etc.
   *
   * @param container must be an EC container
   * @return List of datanodes that host replicas of this container
   */
  private List<DatanodeDetails> setupECContainerForTesting(
      ContainerInfo container)
      throws IOException, TimeoutException, NodeNotFoundException {
    assertEquals(HddsProtos.ReplicationType.EC,
        container.getReplicationType());
    final int numDatanodes =
        container.getReplicationConfig().getRequiredNodes();
    // Register required number of datanodes with NodeManager
    List<DatanodeDetails> dns = new ArrayList<>(numDatanodes);
    for (int i = 0; i < numDatanodes; i++) {
      dns.add(randomDatanodeDetails());
      nodeManager.register(dns.get(i), null, null);
    }

    // Add this container to ContainerStateManager
    containerStateManager.addContainer(container.getProtobuf());

    // Create its replicas and add them to ContainerStateManager
    Set<ContainerReplica> replicas =
        HddsTestUtils.getReplicasWithReplicaIndex(container.containerID(),
            ContainerReplicaProto.State.CLOSING,
            HddsTestUtils.CONTAINER_USED_BYTES_DEFAULT,
            HddsTestUtils.CONTAINER_NUM_KEYS_DEFAULT,
            container.getSequenceId(),
            dns.toArray(new DatanodeDetails[0]));
    for (ContainerReplica r : replicas) {
      containerStateManager.updateContainerReplica(r);
    }

    // Tell NodeManager that each DN hosts a replica of this container
    for (DatanodeDetails dn : dns) {
      nodeManager.addContainer(dn, container.containerID());
    }
    return dns;
  }

  private void createAndHandleContainerReport(ContainerID containerID,
                                              ContainerReplicaProto.State state,
                                              DatanodeDetails datanodeDetails,
                                              int replicaIndex) {
    final ContainerReportFromDatanode containerReportFromDatanode =
        getContainerReportFromDatanode(containerID, state,
            datanodeDetails, 2000000000L, 100000L, replicaIndex);
    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);
    reportHandler.onMessage(containerReportFromDatanode, publisher);
  }

  @Test
  public void testClosingToQuasiClosed()
      throws NodeNotFoundException, IOException {
    /*
     * The container is in CLOSING state and all the replicas are in
     * OPEN/CLOSING state.
     *
     * The datanode reports that the replica is now QUASI_CLOSED.
     *
     * In this case SCM should move the container to QUASI_CLOSED.
     */

    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);

    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();
    final DatanodeDetails datanodeOne = nodeIterator.next();
    final DatanodeDetails datanodeTwo = nodeIterator.next();
    final DatanodeDetails datanodeThree = nodeIterator.next();

    final ContainerInfo containerOne = getContainer(LifeCycleState.CLOSING);
    final ContainerInfo containerTwo = getContainer(LifeCycleState.CLOSED);

    final Set<ContainerID> containerIDSet = Stream.of(
        containerOne.containerID(), containerTwo.containerID())
        .collect(Collectors.toSet());

    final Set<ContainerReplica> containerOneReplicas = getReplicas(
        containerOne.containerID(),
        ContainerReplicaProto.State.CLOSING,
        datanodeOne, datanodeTwo);
    containerOneReplicas.addAll(getReplicas(
        containerOne.containerID(),
        ContainerReplicaProto.State.OPEN,
        datanodeThree));
    final Set<ContainerReplica> containerTwoReplicas = getReplicas(
        containerTwo.containerID(),
        ContainerReplicaProto.State.CLOSED,
        datanodeOne, datanodeTwo, datanodeThree);

    nodeManager.setContainers(datanodeOne, containerIDSet);
    nodeManager.setContainers(datanodeTwo, containerIDSet);
    nodeManager.setContainers(datanodeThree, containerIDSet);

    containerStateManager.addContainer(containerOne.getProtobuf());
    containerStateManager.addContainer(containerTwo.getProtobuf());

    containerOneReplicas.forEach(containerStateManager::updateContainerReplica);
    containerTwoReplicas.forEach(containerStateManager::updateContainerReplica);

    final ContainerReportsProto containerReport = getContainerReportsProto(
        containerOne.containerID(), ContainerReplicaProto.State.QUASI_CLOSED,
        datanodeOne.getUuidString());
    final ContainerReportFromDatanode containerReportFromDatanode =
        new ContainerReportFromDatanode(datanodeOne, containerReport);
    reportHandler.onMessage(containerReportFromDatanode, publisher);

    assertEquals(LifeCycleState.QUASI_CLOSED, containerManager.getContainer(containerOne.containerID()).getState());
  }

  @Test
  public void testQuasiClosedToClosed()
      throws NodeNotFoundException, IOException {
    /*
     * The container is in QUASI_CLOSED state.
     *  - One of the replica is in QUASI_CLOSED state
     *  - The other two replica are in OPEN/CLOSING state
     *
     * The datanode reports the second replica is now CLOSED.
     *
     * In this case SCM should CLOSE the container.
     */

    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);
    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();

    final DatanodeDetails datanodeOne = nodeIterator.next();
    final DatanodeDetails datanodeTwo = nodeIterator.next();
    final DatanodeDetails datanodeThree = nodeIterator.next();

    final ContainerInfo containerOne =
        getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerInfo containerTwo =
        getContainer(LifeCycleState.CLOSED);

    final Set<ContainerID> containerIDSet = Stream.of(
        containerOne.containerID(), containerTwo.containerID())
        .collect(Collectors.toSet());
    final Set<ContainerReplica> containerOneReplicas = getReplicas(
        containerOne.containerID(),
        ContainerReplicaProto.State.QUASI_CLOSED,
        10000L,
        datanodeOne);
    containerOneReplicas.addAll(getReplicas(
        containerOne.containerID(),
        ContainerReplicaProto.State.CLOSING,
        datanodeTwo, datanodeThree));
    final Set<ContainerReplica> containerTwoReplicas = getReplicas(
        containerTwo.containerID(),
        ContainerReplicaProto.State.CLOSED,
        datanodeOne, datanodeTwo, datanodeThree);

    nodeManager.setContainers(datanodeOne, containerIDSet);
    nodeManager.setContainers(datanodeTwo, containerIDSet);
    nodeManager.setContainers(datanodeThree, containerIDSet);

    containerStateManager.addContainer(containerOne.getProtobuf());
    containerStateManager.addContainer(containerTwo.getProtobuf());

    containerOneReplicas.forEach(containerStateManager::updateContainerReplica);
    containerTwoReplicas.forEach(containerStateManager::updateContainerReplica);

    final ContainerReportsProto containerReport = getContainerReportsProto(
        containerOne.containerID(), ContainerReplicaProto.State.CLOSED,
        datanodeOne.getUuidString());

    final ContainerReportFromDatanode containerReportFromDatanode =
        new ContainerReportFromDatanode(datanodeOne, containerReport);
    reportHandler.onMessage(containerReportFromDatanode, publisher);

    assertEquals(LifeCycleState.CLOSED, containerManager.getContainer(containerOne.containerID()).getState());
  }

  @ParameterizedTest
  @EnumSource(value = LifeCycleState.class, names = {"CLOSING", "QUASI_CLOSED"})
  public void testContainerStateTransitionToClosedWithMismatchingBCSID(LifeCycleState lcState)
      throws NodeNotFoundException, IOException {
    /*
     * Negative test. When a replica with a (lower) mismatching bcsId gets reported,
     * expect the ContainerReportHandler thread to not throw uncaught exception.
     * (That exception lead to ContainerReportHandler thread crash before HDDS-12150.)
     */
    final ContainerReportHandler reportHandler =
        new ContainerReportHandler(nodeManager, containerManager);
    final Iterator<DatanodeDetails> nodeIterator =
        nodeManager.getNodes(NodeStatus.inServiceHealthy()).iterator();

    final DatanodeDetails dn1 = nodeIterator.next();
    final DatanodeDetails dn2 = nodeIterator.next();
    final DatanodeDetails dn3 = nodeIterator.next();

    // Initial sequenceId 10000L is set here
    final ContainerInfo container1 = getContainer(lcState);

    nodeManager.addContainer(dn1, container1.containerID());
    nodeManager.addContainer(dn2, container1.containerID());
    nodeManager.addContainer(dn3, container1.containerID());

    containerStateManager.addContainer(container1.getProtobuf());

    // Generate container report with replica in CLOSED state with intentional lower bcsId
    final ContainerReportsProto containerReport = getContainerReportsProto(
        container1.containerID(), ContainerReplicaProto.State.CLOSED,
        dn1.getUuidString(),
        2000L);
    final ContainerReportFromDatanode containerReportFromDatanode =
        new ContainerReportFromDatanode(dn1, containerReport);

    // Handler should NOT throw IllegalArgumentException
    try {
      reportHandler.onMessage(containerReportFromDatanode, publisher);
    } catch (IllegalArgumentException iaEx) {
      fail("Handler should not throw IllegalArgumentException: " + iaEx.getMessage());
    }

    // Because the container report is ignored, the container remains in the same previous state in SCM
    assertEquals(lcState, containerManager.getContainer(container1.containerID()).getState());
  }

  @Test
  public void openContainerKeyAndBytesUsedUpdatedToMinimumOfAllReplicas()
      throws IOException, TimeoutException {
    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);
    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();

    Pipeline pipeline = pipelineManager.createPipeline(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));

    final DatanodeDetails datanodeOne = nodeIterator.next();
    final DatanodeDetails datanodeTwo = nodeIterator.next();
    final DatanodeDetails datanodeThree = nodeIterator.next();

    final ContainerReplicaProto.State replicaState
        = ContainerReplicaProto.State.OPEN;
    final ContainerInfo containerOne =
        getContainer(LifeCycleState.OPEN, pipeline.getId());

    containerStateManager.addContainer(containerOne.getProtobuf());
    // Container loaded, no replicas reported from DNs. Expect zeros for
    // usage values.
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
            .getUsedBytes());
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
            .getNumberOfKeys());

    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeOne, 50L, 60L), publisher);

    // Single replica reported - ensure values are updated
    assertEquals(50L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(60L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeTwo, 50L, 60L), publisher);
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeThree, 50L, 60L), publisher);

    // All 3 DNs are reporting the same values. Counts should be as expected.
    assertEquals(50L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(60L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Now each DN reports a different lesser value. Counts should be the min
    // reported.
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeOne, 1L, 10L), publisher);
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeTwo, 2L, 11L), publisher);
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeThree, 3L, 12L), publisher);

    // All 3 DNs are reporting different values. The actual value should be the
    // minimum.
    assertEquals(1L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(10L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Have the lowest value report a higher value and ensure the new value
    // is the minimum
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeOne, 3L, 12L), publisher);

    assertEquals(2L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(11L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());
  }

  @Test
  public void notOpenContainerKeyAndBytesUsedUpdatedToMaximumOfAllReplicas()
      throws IOException, TimeoutException {
    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);
    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();

    final DatanodeDetails datanodeOne = nodeIterator.next();
    final DatanodeDetails datanodeTwo = nodeIterator.next();
    final DatanodeDetails datanodeThree = nodeIterator.next();

    final ContainerReplicaProto.State replicaState
        = ContainerReplicaProto.State.CLOSED;
    final ContainerInfo containerOne = getContainer(LifeCycleState.CLOSED);

    containerStateManager.addContainer(containerOne.getProtobuf());
    // Container loaded, no replicas reported from DNs. Expect zeros for
    // usage values.
    assertEquals(0L, containerOne.getUsedBytes());
    assertEquals(0L, containerOne.getNumberOfKeys());

    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeOne, 50L, 60L), publisher);

    // Single replica reported - ensure values are updated
    assertEquals(50L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(60L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeTwo, 50L, 60L), publisher);
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeThree, 50L, 60L), publisher);

    // All 3 DNs are reporting the same values. Counts should be as expected.
    assertEquals(50L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(60L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Now each DN reports a different lesser value. Counts should be the max
    // reported.
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeOne, 1L, 10L), publisher);
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeTwo, 2L, 11L), publisher);
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeThree, 3L, 12L), publisher);

    // All 3 DNs are reporting different values. The actual value should be the
    // maximum.
    assertEquals(3L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(12L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Have the highest value report a lower value and ensure the new value
    // is the new maximumu
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeThree, 1L, 10L), publisher);

    assertEquals(2L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(11L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());
  }

  @Test
  public void openECContainerKeyAndBytesUsedUpdatedToMinimumOfAllReplicas()
      throws IOException, TimeoutException {
    final ECReplicationConfig repConfig = new ECReplicationConfig(3, 2);
    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);

    Pipeline pipeline = pipelineManager.createPipeline(repConfig);
    Map<Integer, DatanodeDetails> dns = new HashMap<>();
    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();
    for (int i = 1; i <= repConfig.getRequiredNodes(); i++) {
      dns.put(i, nodeIterator.next());
    }
    final ContainerReplicaProto.State replicaState
        = ContainerReplicaProto.State.OPEN;
    final ContainerInfo containerOne =
        getECContainer(LifeCycleState.OPEN, pipeline.getId(), repConfig);

    containerStateManager.addContainer(containerOne.getProtobuf());
    // Container loaded, no replicas reported from DNs. Expect zeros for
    // usage values.
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Report from data index 2 - should not update stats
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(2), 50L, 60L, 2), publisher);
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Report from replica 1, it should update
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(1), 50L, 60L, 1), publisher);
    assertEquals(50L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(60L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Parity 1 report a greater value, but as the container is own the stats
    // should be the min value.
    // Report from replica 1, it should update
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(4), 80L, 90L, 4), publisher);
    assertEquals(50L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(60L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Parity 2 reports a lesser value, so the stored values should update
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(5), 40, 30, 5), publisher);
    assertEquals(40L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(30L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Report from data index 3 - should not update stats even though it has
    // lesser values
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(2), 10L, 10L, 2), publisher);
    assertEquals(40L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(30L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());
  }

  @Test
  public void closedECContainerKeyAndBytesUsedUpdatedToMinimumOfAllReplicas()
      throws IOException, TimeoutException {
    final ECReplicationConfig repConfig = new ECReplicationConfig(3, 2);
    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);

    Pipeline pipeline = pipelineManager.createPipeline(repConfig);
    Map<Integer, DatanodeDetails> dns = new HashMap<>();
    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();
    for (int i = 1; i <= repConfig.getRequiredNodes(); i++) {
      dns.put(i, nodeIterator.next());
    }
    final ContainerReplicaProto.State replicaState
        = ContainerReplicaProto.State.OPEN;
    final ContainerInfo containerOne =
        getECContainer(LifeCycleState.CLOSED, pipeline.getId(), repConfig);

    containerStateManager.addContainer(containerOne.getProtobuf());
    // Container loaded, no replicas reported from DNs. Expect zeros for
    // usage values.
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Report from data index 2 - should not update stats
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(2), 50L, 60L, 2), publisher);
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Report from replica 1, it should update
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(1), 50L, 60L, 1), publisher);
    assertEquals(50L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(60L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Parity 1 report a greater value, as the container is closed the stats
    // should be the max value.
    // Report from replica 1, it should update
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(4), 80L, 90L, 4), publisher);
    assertEquals(80L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(90L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Parity 2 reports a lesser value, so the stored values should not update
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(5), 40, 30, 5), publisher);
    assertEquals(80L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(90L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Report from data index 3 - should not update stats even though it has
    // greater values
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(2), 110L, 120L, 2), publisher);
    assertEquals(80L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(90L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testStaleReplicaOfDeletedContainer(boolean isEmpty) throws NodeNotFoundException, IOException {
    final ContainerReportHandler reportHandler = new ContainerReportHandler(nodeManager, containerManager);

    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();
    final DatanodeDetails datanodeOne = nodeIterator.next();
    final ContainerInfo containerOne = getContainer(LifeCycleState.DELETED);

    final Set<ContainerID> containerIDSet = Stream.of(
        containerOne.containerID())
        .collect(Collectors.toSet());

    nodeManager.setContainers(datanodeOne, containerIDSet);
    containerStateManager.addContainer(containerOne.getProtobuf());

    final ContainerReportsProto containerReport = getContainerReportsProto(
        containerOne.containerID(), ContainerReplicaProto.State.CLOSED,
        datanodeOne.getUuidString(), 0, isEmpty);
    final ContainerReportFromDatanode containerReportFromDatanode =
        new ContainerReportFromDatanode(datanodeOne, containerReport);
    reportHandler.onMessage(containerReportFromDatanode, publisher);

    if (isEmpty) {
      // Expect the replica to be deleted when it is empty
      verify(publisher, times(1)).fireEvent(any(), any(CommandForDatanode.class));
    } else {
      // Expect the replica to stay when it is NOT empty
      verify(publisher, times(0)).fireEvent(any(), any(CommandForDatanode.class));
    }
    assertEquals(1, containerManager.getContainerReplicas(containerOne.containerID()).size());
  }

  @Test
  public void testWithNoContainerDataChecksum() throws Exception {
    final ContainerReportHandler reportHandler = new ContainerReportHandler(nodeManager, containerManager);

    final int numNodes = 3;
    List<DatanodeDetails> datanodes = nodeManager.getNodes(NodeStatus.inServiceHealthy()).stream()
        .limit(numNodes)
        .collect(Collectors.toList());

    // Create a container and put one replica on each datanode.
    final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    ContainerID contID = container.containerID();
    final Set<ContainerID> containerIDSet = Stream.of(contID).collect(Collectors.toSet());

    for (DatanodeDetails dn: datanodes) {
      nodeManager.setContainers(dn, containerIDSet);
    }

    containerStateManager.addContainer(container.getProtobuf());

    getReplicas(contID, ContainerReplicaProto.State.CLOSED, 0, datanodes)
        .forEach(containerStateManager::updateContainerReplica);

    // Container manager should now be aware of 3 replicas of each container.
    assertEquals(numNodes, containerManager.getContainerReplicas(contID).size());

    // All replicas should start with an empty data checksum in SCM.
    boolean contOneDataChecksumsEmpty = containerManager.getContainerReplicas(contID).stream()
        .allMatch(r -> r.getDataChecksum() == 0);
    assertTrue(contOneDataChecksumsEmpty, "Replicas of container one should not yet have any data checksums.");

    // Send a report to SCM from one datanode that still does not have a data checksum.
    int numReportsSent = 0;
    for (DatanodeDetails dn: datanodes) {
      final ContainerReportsProto dnReportProto = getContainerReportsProto(
          contID, ContainerReplicaProto.State.CLOSED, dn.getUuidString());
      final ContainerReportFromDatanode dnReport = new ContainerReportFromDatanode(dn, dnReportProto);
      reportHandler.onMessage(dnReport, publisher);
      numReportsSent++;
    }
    assertEquals(numNodes, numReportsSent);

    // Regardless of which datanode sent the report, none of them have checksums, so all replica's data checksums
    // should remain empty.
    boolean containerDataChecksumEmpty = containerManager.getContainerReplicas(contID).stream()
        .allMatch(r -> r.getDataChecksum() == 0);
    assertTrue(containerDataChecksumEmpty, "Replicas of the container should not have any data checksums.");
  }

  @Test
  public void testWithContainerDataChecksum() throws Exception {
    final ContainerReportHandler reportHandler = new ContainerReportHandler(nodeManager, containerManager);

    final int numNodes = 3;
    List<DatanodeDetails> datanodes = nodeManager.getNodes(NodeStatus.inServiceHealthy()).stream()
        .limit(numNodes)
        .collect(Collectors.toList());

    // Create a container and put one replica on each datanode.
    final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    ContainerID contID = container.containerID();
    final Set<ContainerID> containerIDSet = Stream.of(contID).collect(Collectors.toSet());

    for (DatanodeDetails dn: datanodes) {
      nodeManager.setContainers(dn, containerIDSet);
    }

    containerStateManager.addContainer(container.getProtobuf());

    getReplicas(contID, ContainerReplicaProto.State.CLOSED, 0, datanodes)
        .forEach(containerStateManager::updateContainerReplica);

    // Container manager should now be aware of 3 replicas of each container.
    assertEquals(numNodes, containerManager.getContainerReplicas(contID).size());

    // All replicas should start with an empty data checksum in SCM.
    boolean dataChecksumsEmpty = containerManager.getContainerReplicas(contID).stream()
        .allMatch(r -> r.getDataChecksum() == 0);
    assertTrue(dataChecksumsEmpty, "Replicas of container one should not yet have any data checksums.");

    // For each datanode, send a container report with a mismatched checksum.
    for (DatanodeDetails dn: datanodes) {
      ContainerReportsProto dnReportProto = getContainerReportsProto(
          contID, ContainerReplicaProto.State.CLOSED, dn.getUuidString());
      ContainerReplicaProto replicaWithChecksum = dnReportProto.getReports(0).toBuilder()
          .setDataChecksum(createUniqueDataChecksumForReplica(contID, dn.getUuidString()))
          .build();
      ContainerReportsProto reportWithChecksum = dnReportProto.toBuilder()
          .clearReports()
          .addReports(replicaWithChecksum)
          .build();
      final ContainerReportFromDatanode dnReport = new ContainerReportFromDatanode(dn, reportWithChecksum);
      reportHandler.onMessage(dnReport, publisher);
    }

    // All the replicas should have different checksums.
    // Since the containers don't have any data in this test, different checksums are based on container ID and
    // datanode ID.
    int numReplicasChecked = 0;
    for (ContainerReplica replica: containerManager.getContainerReplicas(contID)) {
      long expectedChecksum = createUniqueDataChecksumForReplica(contID, replica.getDatanodeDetails().getUuidString());
      assertEquals(expectedChecksum, replica.getDataChecksum());
      numReplicasChecked++;
    }
    assertEquals(numNodes, numReplicasChecked);

    // For each datanode, send a container report with a matching checksum.
    // This simulates reconciliation running.
    for (DatanodeDetails dn: datanodes) {
      ContainerReportsProto dnReportProto = getContainerReportsProto(
          contID, ContainerReplicaProto.State.CLOSED, dn.getUuidString());
      ContainerReplicaProto replicaWithChecksum = dnReportProto.getReports(0).toBuilder()
          .setDataChecksum(createMatchingDataChecksumForReplica(contID))
          .build();
      ContainerReportsProto reportWithChecksum = dnReportProto.toBuilder()
          .clearReports()
          .addReports(replicaWithChecksum)
          .build();
      final ContainerReportFromDatanode dnReport = new ContainerReportFromDatanode(dn, reportWithChecksum);
      reportHandler.onMessage(dnReport, publisher);
    }

    // All the replicas should now have matching checksums.
    // Since the containers don't have any data in this test, the matching checksums are based on container ID only.
    numReplicasChecked = 0;
    for (ContainerReplica replica: containerManager.getContainerReplicas(contID)) {
      long expectedChecksum = createMatchingDataChecksumForReplica(contID);
      assertEquals(expectedChecksum, replica.getDataChecksum());
      numReplicasChecked++;
    }
    assertEquals(numNodes, numReplicasChecked);
  }

  /**
   * Generates a placeholder data checksum for testing that is specific to a container replica.
   */
  protected static long createUniqueDataChecksumForReplica(ContainerID containerID, String datanodeID) {
    return (datanodeID + containerID).hashCode();
  }

  /**
   * Generates a placeholder data checksum for testing that is the same for all container replicas.
   */
  protected static long createMatchingDataChecksumForReplica(ContainerID containerID) {
    return Objects.hashCode(containerID);
  }

  private ContainerReportFromDatanode getContainerReportFromDatanode(
      ContainerID containerId, ContainerReplicaProto.State state,
      DatanodeDetails dn, long bytesUsed, long keyCount) {
    return getContainerReportFromDatanode(containerId, state, dn, bytesUsed,
        keyCount, 0);
  }

  private ContainerReportFromDatanode getContainerReportFromDatanode(
      ContainerID containerId, ContainerReplicaProto.State state,
      DatanodeDetails dn, long bytesUsed, long keyCount, int replicaIndex) {
    ContainerReportsProto containerReport = getContainerReportsProto(
        containerId, state, dn.getUuidString(), bytesUsed, keyCount,
        10000L, replicaIndex);

    return new ContainerReportFromDatanode(dn, containerReport);
  }

  protected static ContainerReportsProto getContainerReportsProto(
      final ContainerID containerId, final ContainerReplicaProto.State state,
      final String originNodeId) {
    return getContainerReportsProto(containerId, state, originNodeId,
        2000000000L, 100000000L, 10000L, 0);
  }

  protected static ContainerReportsProto getContainerReportsProto(
      final ContainerID containerId, final ContainerReplicaProto.State state,
      final String originNodeId, final long bcsId) {
    return getContainerReportsProto(containerId, state, originNodeId,
        2000000000L, 100000000L, bcsId, 0);
  }

  protected static ContainerReportsProto getContainerReportsProto(
      final ContainerID containerId, final ContainerReplicaProto.State state,
      final String originNodeId, int replicaIndex) {
    return getContainerReportsProto(containerId, state, originNodeId,
        2000000000L, 100000000L, 10000L, replicaIndex, false);
  }

  protected static ContainerReportsProto getContainerReportsProto(
      final ContainerID containerId, final ContainerReplicaProto.State state,
      final String originNodeId, int replicaIndex, boolean isEmpty) {
    return getContainerReportsProto(containerId, state, originNodeId,
        2000000000L, 100000000L, 10000L, replicaIndex, isEmpty);
  }

  protected static ContainerReportsProto getContainerReportsProto(
      final ContainerID containerId, final ContainerReplicaProto.State state,
      final String originNodeId, final long bcsId, int replicaIndex) {
    return getContainerReportsProto(containerId, state, originNodeId,
        2000000000L, 100000000L, bcsId, replicaIndex);
  }

  protected static ContainerReportsProto getContainerReportsProto(
      final ContainerID containerId, final ContainerReplicaProto.State state,
      final String originNodeId, final long usedBytes, final long keyCount,
      final long bcsId, final int replicaIndex) {
    return getContainerReportsProto(containerId, state, originNodeId, usedBytes,
        keyCount, bcsId, replicaIndex, false);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  protected static ContainerReportsProto getContainerReportsProto(
      final ContainerID containerId, final ContainerReplicaProto.State state,
      final String originNodeId, final long usedBytes, final long keyCount,
      final long bcsId, final int replicaIndex, final boolean isEmpty) {
    final ContainerReportsProto.Builder crBuilder =
        ContainerReportsProto.newBuilder();
    final ContainerReplicaProto replicaProto =
        ContainerReplicaProto.newBuilder()
            .setContainerID(containerId.getId())
            .setState(state)
            .setOriginNodeId(originNodeId)
            .setSize(5368709120L)
            .setUsed(usedBytes)
            .setKeyCount(keyCount)
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
