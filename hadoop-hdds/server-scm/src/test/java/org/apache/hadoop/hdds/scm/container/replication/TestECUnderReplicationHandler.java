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

package org.apache.hadoop.hdds.scm.container.replication;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.UNHEALTHY;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainer;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerReplica;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult.UnderReplicatedHealthResult;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.InsufficientDatanodesException;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests the ECUnderReplicationHandling functionality.
 */
public class TestECUnderReplicationHandler {
  private ECReplicationConfig repConfig;
  private ContainerInfo container;
  private NodeManager nodeManager;
  private ReplicationManager replicationManager;
  private ReplicationManagerMetrics metrics;
  private OzoneConfiguration conf;
  private PlacementPolicy policy;
  private static final int DATA = 3;
  private static final int PARITY = 2;
  private PlacementPolicy ecPlacementPolicy;
  private int remainingMaintenanceRedundancy = 1;
  private Set<Pair<DatanodeDetails, SCMCommand<?>>> commandsSent;
  private final AtomicBoolean throwOverloadedExceptionOnReplication
      = new AtomicBoolean(false);
  private final AtomicBoolean throwOverloadedExceptionOnReconstruction
      = new AtomicBoolean(false);

  @BeforeEach
  void setup(@TempDir File testDir) throws NodeNotFoundException,
      CommandTargetOverloadedException, NotLeaderException {
    nodeManager = new MockNodeManager(true, 10) {
      @Override
      public NodeStatus getNodeStatus(DatanodeDetails dd) {
        return NodeStatus.valueOf(dd.getPersistedOpState(), HddsProtos.NodeState.HEALTHY);
      }
    };
    replicationManager = mock(ReplicationManager.class);
    ReplicationManager.ReplicationManagerConfiguration rmConf =
        new ReplicationManager.ReplicationManagerConfiguration();
    when(replicationManager.getConfig())
        .thenReturn(rmConf);
    metrics = ReplicationManagerMetrics.create(replicationManager);
    when(replicationManager.getMetrics()).thenReturn(metrics);
    when(replicationManager.getContainerReplicaPendingOps()).thenReturn(mock(ContainerReplicaPendingOps.class));

    when(replicationManager.getNodeStatus(any(DatanodeDetails.class)))
        .thenAnswer(invocation -> {
          DatanodeDetails dd = invocation.getArgument(0);
          return NodeStatus.valueOf(dd.getPersistedOpState(), HddsProtos.NodeState.HEALTHY);
        });

    commandsSent = new HashSet<>();
    ReplicationTestUtil.mockRMSendDatanodeCommand(
        replicationManager, commandsSent);
    ReplicationTestUtil.mockRMSendThrottleReplicateCommand(
        replicationManager, commandsSent,
        throwOverloadedExceptionOnReplication);
    ReplicationTestUtil.mockSendThrottledReconstructionCommand(
        replicationManager, commandsSent,
        throwOverloadedExceptionOnReconstruction);

    conf = SCMTestUtils.getConf(testDir);
    repConfig = new ECReplicationConfig(DATA, PARITY);
    container = createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    policy = ReplicationTestUtil
            .getSimpleTestPlacementPolicy(nodeManager, conf);
    NodeSchema[] schemas =
        new NodeSchema[] {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager.getInstance().init(schemas, true);
    ecPlacementPolicy = mock(PlacementPolicy.class);
    when(ecPlacementPolicy.validateContainerPlacement(anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(2, 2, 3));
  }

  @AfterEach
  void cleanup() {
    if (metrics != null) {
      metrics.unRegister();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"rs-6-3-1024k", "rs-10-4-1024k"})
  void defersNonCriticalPartialReconstruction(String rep) throws IOException {
    final ECReplicationConfig ec = new ECReplicationConfig(rep);
    final int data = ec.getData();
    final int parity = ec.getParity();
    ECUnderReplicationHandler subject = new ECUnderReplicationHandler(
        ecPlacementPolicy, conf, replicationManager);
    UnderReplicatedHealthResult result = mockUnderReplicated(ec);

    Set<DatanodeDetails> excluded = excludeInReplicationManager(1);

    final int remainingRedundancy = 1;
    PlacementPolicySpy spy = new PlacementPolicySpy(ecPlacementPolicy,
        data + parity + excluded.size() - 1);
    Set<ContainerReplica> replicas = createReplicas(data + remainingRedundancy);

    // WHEN
    InsufficientDatanodesException e = assertThrows(
        InsufficientDatanodesException.class,
        () -> subject.processAndSendCommands(replicas, emptyList(), result, 2));

    // THEN
    assertEquals(2, spy.callCount());
    assertExcluded(excluded, spy.excludedNodes(0));
    assertUsedNodes(replicas, spy.usedNodes(0));
    assertExcluded(emptySet(), spy.excludedNodes(1));
    assertUsedNodes(replicas, spy.usedNodes(1));
    assertEquals(parity - remainingRedundancy, e.getRequiredNodes());
    assertEquals(e.getRequiredNodes() - excluded.size(), e.getAvailableNodes());
    verify(replicationManager, never())
        .sendThrottledReconstructionCommand(any(), any());
    assertEquals(1, metrics.getECPartialReconstructionSkippedTotal());
  }

  private static UnderReplicatedHealthResult mockUnderReplicated(
      ECReplicationConfig ec) {
    UnderReplicatedHealthResult result =
        mock(UnderReplicatedHealthResult.class);
    when(result.getContainerInfo())
        .thenReturn(createContainer(HddsProtos.LifeCycleState.CLOSED, ec));
    return result;
  }

  private static void assertExcluded(Set<DatanodeDetails> excluded,
      List<DatanodeDetails> excludedNodes) {
    assertEquals(excluded, new TreeSet<>(excludedNodes));
  }

  private Set<DatanodeDetails> excludeInReplicationManager(int count) {
    Set<DatanodeDetails> excluded = IntStream.range(0, count)
        .mapToObj(i -> MockDatanodeDetails.randomDatanodeDetails())
        .collect(toSet());
    when(replicationManager.getExcludedNodes())
        .thenReturn(excluded);
    return excluded;
  }

  @ParameterizedTest
  @ValueSource(strings = {"rs-3-2-1024k", "rs-6-3-1024k", "rs-10-4-1024k"})
  void performsCriticalPartialReconstruction(String rep) throws IOException {
    final ECReplicationConfig ec = new ECReplicationConfig(rep);
    final int data = ec.getData();
    final int parity = ec.getParity();
    ECUnderReplicationHandler subject = new ECUnderReplicationHandler(
        ecPlacementPolicy, conf, replicationManager);
    UnderReplicatedHealthResult result = mockUnderReplicated(ec);

    DatanodeDetails excludedByRM = MockDatanodeDetails.randomDatanodeDetails();
    Set<DatanodeDetails> excluded = singleton(excludedByRM);
    when(replicationManager.getExcludedNodes())
        .thenReturn(excluded);

    final int remainingRedundancy = 0;
    PlacementPolicySpy spy = new PlacementPolicySpy(ecPlacementPolicy,
        data + parity + excluded.size() - 1);
    Set<ContainerReplica> replicas = createReplicas(data + remainingRedundancy);

    // WHEN
    InsufficientDatanodesException e = assertThrows(
        InsufficientDatanodesException.class,
        () -> subject.processAndSendCommands(replicas, emptyList(), result, 2));

    // THEN
    assertEquals(1, spy.callCount());
    assertEquals(singletonList(excludedByRM), spy.excludedNodes(0));
    assertUsedNodes(replicas, spy.usedNodes(0));
    assertEquals(parity - remainingRedundancy, e.getRequiredNodes());
    assertEquals(e.getRequiredNodes() - excluded.size(), e.getAvailableNodes());
    verify(replicationManager, times(1))
        .sendThrottledReconstructionCommand(any(), any());
    assertEquals(1, metrics.getECPartialReconstructionCriticalTotal());
  }

  @Test
  void excludesOverloadedNodes() throws IOException {
    ECUnderReplicationHandler subject = new ECUnderReplicationHandler(
        ecPlacementPolicy, conf, replicationManager);
    UnderReplicatedHealthResult result =
        mock(UnderReplicatedHealthResult.class);
    when(result.getContainerInfo()).thenReturn(container);

    DatanodeDetails excludedByRM = MockDatanodeDetails.randomDatanodeDetails();
    when(replicationManager.getExcludedNodes())
        .thenReturn(singleton(excludedByRM));

    PlacementPolicySpy spy = new PlacementPolicySpy(ecPlacementPolicy,
        repConfig.getRequiredNodes() + 1);

    // WHEN
    Set<ContainerReplica> replicas = createReplicas(3);
    subject.processAndSendCommands(
        replicas, emptyList(), result, 2);

    // THEN
    assertEquals(1, spy.callCount());
    assertEquals(singletonList(excludedByRM), spy.excludedNodes(0));
    assertUsedNodes(replicas, spy.usedNodes(0));
    verify(replicationManager, times(1))
        .sendThrottledReconstructionCommand(any(), any());
  }

  private static void assertUsedNodes(Set<ContainerReplica> replicas,
      List<DatanodeDetails> usedNodes) {
    assertEquals(replicas.size(), usedNodes.size());
    for (ContainerReplica r : replicas) {
      assertThat(usedNodes).contains(r.getDatanodeDetails());
    }
  }

  private static Set<ContainerReplica> createReplicas(int count) {
    ContainerID id = ContainerID.valueOf(1);
    return IntStream.rangeClosed(1, count)
        .mapToObj(i -> createContainerReplica(id, i, IN_SERVICE, CLOSED))
        .collect(toSet());
  }

  @Test
  public void testUnderReplicationWithMissingParityIndex5() throws IOException {
    Set<ContainerReplica> availableReplicas = createReplicas(4);
    testUnderReplicationWithMissingIndexes(ImmutableList.of(5),
        availableReplicas, 0, 0, policy);
  }

  @Test
  public void testUnderReplicationWithMissingIndex34() throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 5));
    testUnderReplicationWithMissingIndexes(ImmutableList.of(3, 4),
        availableReplicas, 0, 0, policy);
  }

  @Test
  public void testUnderReplicationWithMissingIndex2345() throws IOException {
    Set<ContainerReplica> availableReplicas = createReplicas(1);
    testUnderReplicationWithMissingIndexes(ImmutableList.of(2, 3, 4, 5),
        availableReplicas, 0, 0, policy);
  }

  @Test
  public void testUnderReplicationWithMissingIndex12345() throws IOException {
    Set<ContainerReplica> availableReplicas = new HashSet<>();
    testUnderReplicationWithMissingIndexes(ImmutableList.of(1, 2, 3, 4, 5),
        availableReplicas, 0, 0, policy);
  }

  @Test
  public void testThrowsWhenTargetsOverloaded() throws IOException {
    Set<ContainerReplica> availableReplicas = createReplicas(4);

    doThrow(new CommandTargetOverloadedException("Overloaded"))
        .when(replicationManager).sendThrottledReconstructionCommand(
            any(), any());

    assertThrows(CommandTargetOverloadedException.class, () ->
        testUnderReplicationWithMissingIndexes(ImmutableList.of(5),
            availableReplicas, 0, 0, policy));
  }

  @Test
  public void testUnderReplicationWithDecomIndex1() throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5));
    Set<Pair<DatanodeDetails, SCMCommand<?>>> cmds =
        testUnderReplicationWithMissingIndexes(
            Lists.emptyList(), availableReplicas, 1, 0, policy);
    assertEquals(1, cmds.size());
    // Check the replicate command has index 1 set
    ReplicateContainerCommand cmd = (ReplicateContainerCommand) cmds
        .iterator().next().getValue();
    assertEquals(1, cmd.getReplicaIndex());
  }


  // Test used to reproduce the issue reported in HDDS-8171 and then adjusted
  // to ensure only a single command is sent for HDDS-8172.
  @Test
  public void testUnderReplicationWithDecomIndexAndMaintOnSameIndex()
      throws IOException {
    Set<ContainerReplica> availableReplicas = new LinkedHashSet<>();
    ContainerReplica deadMaintenance =
        createContainerReplica(container.containerID(),
            1, IN_MAINTENANCE, CLOSED);
    availableReplicas.add(deadMaintenance);

    nodeManager = new MockNodeManager(true, 10) {
      @Override
      public NodeStatus getNodeStatus(DatanodeDetails dd) {
        if (dd.equals(deadMaintenance.getDatanodeDetails())) {
          return NodeStatus.valueOf(dd.getPersistedOpState(),
              HddsProtos.NodeState.DEAD);
        }
        return NodeStatus.valueOf(dd.getPersistedOpState(), HddsProtos.NodeState.HEALTHY);
      }
    };

    availableReplicas.addAll(ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1),
            Pair.of(IN_MAINTENANCE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5)));

    // Note that maintenanceIndexes is set to zero as we do not expect any
    // maintenance commands to be created, as they are solved by the earlier
    // decommission command.
    Set<Pair<DatanodeDetails, SCMCommand<?>>> cmds =
        testUnderReplicationWithMissingIndexes(
            Lists.emptyList(), availableReplicas, 1, 0, policy);
    assertEquals(1, cmds.size());
    // Check the replicate command has index 1 set
    for (Pair<DatanodeDetails, SCMCommand<?>> c : cmds) {
      // Ensure neither of the commands are for the dead maintenance node
      assertNotEquals(deadMaintenance.getDatanodeDetails(),
          c.getKey());
    }
  }

  @Test
  public void testUnderReplicationWithDecomNodesOverloaded()
      throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5));
    doThrow(new CommandTargetOverloadedException("Overloaded"))
        .when(replicationManager).sendThrottledReplicationCommand(
            any(), anyList(), any(), anyInt());

    assertThrows(CommandTargetOverloadedException.class, () ->
        testUnderReplicationWithMissingIndexes(
            Lists.emptyList(), availableReplicas, 1, 0, policy));
  }

  @Test
  public void testUnderReplicationWithDecomIndex12() throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1),
            Pair.of(DECOMMISSIONING, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5));
    testUnderReplicationWithMissingIndexes(Lists.emptyList(), availableReplicas,
        2, 0, policy);
  }

  @Test
  public void testUnderReplicationWithMixedDecomAndMissingIndexes()
      throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1),
            Pair.of(DECOMMISSIONING, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4));
    testUnderReplicationWithMissingIndexes(ImmutableList.of(5),
        availableReplicas, 2, 0, policy);
  }

  @Test
  public void testUnderReplicationWithMaintenanceIndex12() throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_MAINTENANCE, 1),
            Pair.of(IN_MAINTENANCE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5));
    testUnderReplicationWithMissingIndexes(Lists.emptyList(), availableReplicas,
        0, 2, policy);
  }

  @Test
  public void testUnderReplicationWithMaintenanceAndMissingIndexes()
      throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_MAINTENANCE, 1),
            Pair.of(IN_MAINTENANCE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4));
    testUnderReplicationWithMissingIndexes(ImmutableList.of(5),
        availableReplicas, 0, 2, policy);
  }

  @Test
  public void testUnderReplicationWithMissingDecomAndMaintenanceIndexes()
      throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_MAINTENANCE, 1),
            Pair.of(IN_MAINTENANCE, 2), Pair.of(DECOMMISSIONING, 3),
            Pair.of(IN_SERVICE, 4));
    testUnderReplicationWithMissingIndexes(ImmutableList.of(5),
        availableReplicas, 1, 2, policy);
  }

  /**
   * The expectation is that an under replicated container should recover
   * even if datanodes hosting new replicas don't satisfy placement policy.
   */
  @Test
  public void testUnderReplicationWithInvalidPlacement()
          throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1),
            Pair.of(DECOMMISSIONING, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4));
    PlacementPolicy mockedPolicy = spy(policy);
    ContainerPlacementStatus mockedContainerPlacementStatus =
            mock(ContainerPlacementStatus.class);
    when(mockedContainerPlacementStatus.isPolicySatisfied())
        .thenReturn(false);
    when(mockedPolicy.validateContainerPlacement(anyList(), anyInt()))
        .thenReturn(mockedContainerPlacementStatus);
    when(mockedPolicy.validateContainerPlacement(anyList(), anyInt()))
        .thenAnswer(invocationOnMock -> {
          Set<DatanodeDetails> dns =
              new HashSet<>(invocationOnMock.getArgument(0));
          assertTrue(availableReplicas.stream()
              .map(ContainerReplica::getDatanodeDetails)
              .filter(dn -> dn.getPersistedOpState() == IN_SERVICE)
              .allMatch(dns::contains));
          return mockedContainerPlacementStatus;
        });
    testUnderReplicationWithMissingIndexes(ImmutableList.of(5),
            availableReplicas, 2, 0, mockedPolicy);
  }

  @Test
  public void testExceptionIfNoNodesFound() {
    PlacementPolicy noNodesPolicy = ReplicationTestUtil
        .getNoNodesTestPlacementPolicy(nodeManager, conf);
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1),
            Pair.of(DECOMMISSIONING, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4));
    assertThrows(SCMException.class, () ->
        testUnderReplicationWithMissingIndexes(ImmutableList.of(5),
            availableReplicas, 2, 0, noNodesPolicy));
  }

  @Test
  public void testUnderRepSentToOverRepHandlerIfNoNewNodes()
      throws IOException {
    PlacementPolicy noNodesPolicy = ReplicationTestUtil
        .getNoNodesTestPlacementPolicy(nodeManager, conf);

    ContainerReplica overRepReplica =
        createContainerReplica(container.containerID(),
            4, IN_SERVICE, CLOSED);
    ContainerReplica decomReplica =
        createContainerReplica(container.containerID(),
        5, DECOMMISSIONING, CLOSED);
    ContainerReplica maintReplica =
        createContainerReplica(container.containerID(),
            5, ENTERING_MAINTENANCE, CLOSED);

    List<ContainerReplica> replicasToAdd = new ArrayList<>();
    replicasToAdd.add(null);
    replicasToAdd.add(decomReplica);
    replicasToAdd.add(maintReplica);

    ECUnderReplicationHandler ecURH =
        new ECUnderReplicationHandler(
            noNodesPolicy, conf, replicationManager);
    UnderReplicatedHealthResult underRep =
        new UnderReplicatedHealthResult(container,
            1, false, false, false);

    // The underRepHandler processes in stages. First missing indexes, then
    // decommission and then maintenance. If a stage cannot find new nodes and
    // there are no commands created yet, then we should either throw, or pass
    // control to the over rep handler if the container is also over replicated.
    // In this loop we first have the container under replicated with a missing
    // index, then with an under-rep index, and finally with a maintenance
    // index. In all cases, initially have it not over replicated, so it should
    // throw an exception, and then make it also over replicated, returning the
    // commands to fix the over replication instead.
    for (ContainerReplica toAdd : replicasToAdd) {
      clearInvocations(replicationManager);
      Set<ContainerReplica> availableReplicas = createReplicas(4);
      if (toAdd != null) {
        availableReplicas.add(toAdd);
      }

      assertThrows(SCMException.class,
          () -> ecURH.processAndSendCommands(availableReplicas,
              emptyList(), underRep, 2));

      // Now adjust replicas so it is also over replicated. This time before
      // throwing it should call the OverRepHandler and return whatever it
      // returns, which in this case is a delete command for replica index 4.
      availableReplicas.add(overRepReplica);

      assertThrows(SCMException.class,
          () -> ecURH.processAndSendCommands(availableReplicas,
              emptyList(), underRep, 2));
      verify(replicationManager, times(1))
          .processOverReplicatedContainer(underRep);
    }
  }

  /**
   * Tests that under replication handling tries to delete an UNHEALTHY
   * replica if no target datanodes are found. It should delete only
   * one UNHEALTHY replica so that the replica's host DN becomes available as a
   * target for reconstruction/replication of a healthy replica.
   */
  @Test
  public void testUnhealthyNodeDeletedIfNoTargetsFound()
      throws IOException {
    PlacementPolicy noNodesPolicy = ReplicationTestUtil
        .getNoNodesTestPlacementPolicy(nodeManager, conf);

    ContainerReplica decomReplica =
        createContainerReplica(container.containerID(),
            5, DECOMMISSIONING, CLOSED);
    ContainerReplica maintReplica =
        createContainerReplica(container.containerID(),
            5, ENTERING_MAINTENANCE, CLOSED);

    List<ContainerReplica> replicasToAdd = new ArrayList<>();
    replicasToAdd.add(null);
    replicasToAdd.add(decomReplica);
    replicasToAdd.add(maintReplica);

    ECUnderReplicationHandler ecURH =
        new ECUnderReplicationHandler(
            noNodesPolicy, conf, replicationManager);
    UnderReplicatedHealthResult underRep =
        new UnderReplicatedHealthResult(container,
            1, false, false, false);
    ContainerReplica unhealthyReplica =
        createContainerReplica(container.containerID(),
            4, IN_SERVICE, UNHEALTHY);

    /*
     The underRepHandler processes in stages. First missing indexes, then
     decommission and then maintenance. If a stage cannot find new nodes and
     there are no commands created yet, then we should either throw, or pass
     control to the over rep handler if the container is also over
     replicated, or try to delete an UNHEALTHY replica if one is present.
     In this loop we first have the container under replicated with a missing
     index, then with a decommissioning index, and finally with a maintenance
     index. In all cases, initially there are no UNHEALTHY replicas, so it
     should throw an exception. Then we add 2 UNHEALTHY replicas, so
     it should return the command to delete one.
     */
    for (ContainerReplica toAdd : replicasToAdd) {
      clearInvocations(replicationManager);
      Set<ContainerReplica> existingReplicas = ReplicationTestUtil
          .createReplicas(Pair.of(IN_SERVICE, 5),
              Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
              Pair.of(IN_SERVICE, 4));
      if (toAdd != null) {
        existingReplicas.add(toAdd);
      }

      // should throw an SCMException indicating no targets were found
      assertThrows(SCMException.class,
          () -> ecURH.processAndSendCommands(existingReplicas,
              emptyList(), underRep, 2));
      verify(replicationManager, times(0))
          .sendThrottledDeleteCommand(eq(container), anyInt(),
              any(DatanodeDetails.class), anyBoolean());

      /*
      Now, for the same container, also add an UNHEALTHY replica. The handler
      should catch the SCMException that says no targets were found and try
      to handle it by deleting the UNHEALTHY replica.
      */
      existingReplicas.add(unhealthyReplica);
      existingReplicas.add(
          createContainerReplica(container.containerID(),
              1, IN_SERVICE, UNHEALTHY));

      /*
      Mock such that when replication manager is called to send a delete
      command, we add the command to commandsSet and later use it for
      assertions.
      */
      commandsSent.clear();
      doAnswer(invocation -> {
        commandsSent.add(Pair.of(invocation.getArgument(2),
            createDeleteContainerCommand(invocation.getArgument(0),
                invocation.getArgument(1))));
        return null;
      })
          .when(replicationManager)
          .sendThrottledDeleteCommand(any(ContainerInfo.class),
              anyInt(), any(DatanodeDetails.class),
              eq(true));

      assertThrows(SCMException.class,
          () -> ecURH.processAndSendCommands(existingReplicas,
              emptyList(), underRep, 2));
      verify(replicationManager, times(1))
          .sendThrottledDeleteCommand(container,
              unhealthyReplica.getReplicaIndex(),
              unhealthyReplica.getDatanodeDetails(), true);
      assertEquals(1, commandsSent.size());
      Pair<DatanodeDetails, SCMCommand<?>> command =
          commandsSent.iterator().next();
      assertEquals(SCMCommandProto.Type.deleteContainerCommand,
          command.getValue().getType());
      DeleteContainerCommand deleteCommand =
          (DeleteContainerCommand) command.getValue();
      assertEquals(unhealthyReplica.getDatanodeDetails(),
          command.getKey());
      assertEquals(container.containerID(),
          ContainerID.valueOf(deleteCommand.getContainerID()));
      assertEquals(unhealthyReplica.getReplicaIndex(),
          deleteCommand.getReplicaIndex());
    }
  }

  @Test
  public void testPartialReconstructionIfNotEnoughNodes() {
    Set<ContainerReplica> availableReplicas = createReplicas(3);
    PlacementPolicy placementPolicy = ReplicationTestUtil
        .getInsufficientNodesTestPlacementPolicy(nodeManager, conf, 2);
    ECUnderReplicationHandler ecURH = new ECUnderReplicationHandler(
        placementPolicy, conf, replicationManager);

    UnderReplicatedHealthResult underRep =
        new UnderReplicatedHealthResult(container,
            0, false, false, false);

    assertThrows(InsufficientDatanodesException.class, () ->
        ecURH.processAndSendCommands(availableReplicas, emptyList(),
            underRep, 1));
    assertEquals(1, commandsSent.size());
    ReconstructECContainersCommand cmd = (ReconstructECContainersCommand)
        commandsSent.iterator().next().getValue();
    assertEquals(1, cmd.getTargetDatanodes().size());
    assertEquals(1, metrics.getEcPartialReconstructionNoneOverloadedTotal());
  }

  @Test
  public void testOverloadedReconstructionContinuesNextStages() {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(DECOMMISSIONING, 3));
    ECUnderReplicationHandler ecURH = new ECUnderReplicationHandler(
        policy, conf, replicationManager);

    UnderReplicatedHealthResult underRep =
        new UnderReplicatedHealthResult(container,
            0, false, false, false);

    // Setup so reconstruction fails, but we should still get a replicate
    // command for the decommissioning node and an exception thrown.
    throwOverloadedExceptionOnReconstruction.set(true);
    assertThrows(CommandTargetOverloadedException.class, () ->
        ecURH.processAndSendCommands(availableReplicas, emptyList(),
            underRep, 1));
    assertEquals(1, commandsSent.size());
    SCMCommand<?> cmd = commandsSent.iterator().next().getValue();
    assertEquals(
        SCMCommandProto.Type.replicateContainerCommand, cmd.getType());
  }

  @Test
  public void testPartialDecommissionIfNotEnoughNodes() {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(DECOMMISSIONING, 4), Pair.of(DECOMMISSIONING, 5));
    PlacementPolicy placementPolicy = ReplicationTestUtil
        .getInsufficientNodesTestPlacementPolicy(nodeManager, conf, 2);
    ECUnderReplicationHandler ecURH = new ECUnderReplicationHandler(
        placementPolicy, conf, replicationManager);

    UnderReplicatedHealthResult underRep =
        new UnderReplicatedHealthResult(container,
            0, true, false, false);

    assertThrows(InsufficientDatanodesException.class, () ->
        ecURH.processAndSendCommands(availableReplicas, emptyList(),
            underRep, 1));
    assertEquals(1, commandsSent.size());
    SCMCommand<?> cmd = commandsSent.iterator().next().getValue();
    assertEquals(
        SCMCommandProto.Type.replicateContainerCommand, cmd.getType());
    assertEquals(1,
        metrics.getEcPartialReplicationForOutOfServiceReplicasTotal());
  }

  @Test
  public void testPartialDecommissionOverloadedNodes() {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(DECOMMISSIONING, 4), Pair.of(DECOMMISSIONING, 5));
    ECUnderReplicationHandler ecURH = new ECUnderReplicationHandler(
        policy, conf, replicationManager);

    UnderReplicatedHealthResult underRep =
        new UnderReplicatedHealthResult(container,
            0, true, false, false);

    throwOverloadedExceptionOnReplication.set(true);
    assertThrows(CommandTargetOverloadedException.class, () ->
        ecURH.processAndSendCommands(availableReplicas, emptyList(),
            underRep, 1));
    assertEquals(1, commandsSent.size());
    SCMCommand<?> cmd = commandsSent.iterator().next().getValue();
    assertEquals(
        SCMCommandProto.Type.replicateContainerCommand, cmd.getType());
    // The partial recovery here is due to overloaded nodes, not insufficient
    // nodes. The "deferred" metric should be updated when all sources are
    // overloaded.
    assertEquals(0,
        metrics.getEcPartialReplicationForOutOfServiceReplicasTotal());
  }

  @Test
  public void testPartialMaintenanceIfNotEnoughNodes() {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(ENTERING_MAINTENANCE, 4),
            Pair.of(ENTERING_MAINTENANCE, 5));
    PlacementPolicy placementPolicy = ReplicationTestUtil
        .getInsufficientNodesTestPlacementPolicy(nodeManager, conf, 2);
    ECUnderReplicationHandler ecURH = new ECUnderReplicationHandler(
        placementPolicy, conf, replicationManager);

    UnderReplicatedHealthResult underRep =
        new UnderReplicatedHealthResult(container,
            0, false, false, false);

    assertThrows(InsufficientDatanodesException.class, () ->
        ecURH.processAndSendCommands(availableReplicas, emptyList(),
            underRep, 2));
    assertEquals(1, commandsSent.size());
    SCMCommand<?> cmd = commandsSent.iterator().next().getValue();
    assertEquals(
        SCMCommandProto.Type.replicateContainerCommand, cmd.getType());
    assertEquals(1,
        metrics.getEcPartialReplicationForOutOfServiceReplicasTotal());
  }

  @Test
  public void testPartialMaintenanceOverloadedNodes() {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(ENTERING_MAINTENANCE, 4),
            Pair.of(ENTERING_MAINTENANCE, 5));
    ECUnderReplicationHandler ecURH = new ECUnderReplicationHandler(
        policy, conf, replicationManager);

    UnderReplicatedHealthResult underRep =
        new UnderReplicatedHealthResult(container,
            0, false, false, false);

    throwOverloadedExceptionOnReplication.set(true);
    assertThrows(CommandTargetOverloadedException.class, () ->
        ecURH.processAndSendCommands(availableReplicas, emptyList(),
            underRep, 2));
    assertEquals(1, commandsSent.size());
    SCMCommand<?> cmd = commandsSent.iterator().next().getValue();
    assertEquals(
        SCMCommandProto.Type.replicateContainerCommand, cmd.getType());
    // The partial recovery here is due to overloaded nodes, not insufficient
    // nodes. The "deferred" metric should be updated when all sources are
    // overloaded.
    assertEquals(0,
        metrics.getEcPartialReplicationForOutOfServiceReplicasTotal());
  }

  @Test
  public void testUnderRepWithDecommissionAndNotEnoughNodes()
      throws IOException {
    DatanodeDetails newNode = MockDatanodeDetails.randomDatanodeDetails();
    PlacementPolicy sameNodePolicy = ReplicationTestUtil
        .getSameNodeTestPlacementPolicy(nodeManager, conf, newNode);
    
    ContainerReplica decomReplica =
        createContainerReplica(container.containerID(),
            5, DECOMMISSIONING, CLOSED);
    ContainerReplica maintReplica =
        createContainerReplica(container.containerID(),
            5, ENTERING_MAINTENANCE, CLOSED);

    List<ContainerReplica> replicasToAdd = new ArrayList<>();
    replicasToAdd.add(decomReplica);
    replicasToAdd.add(maintReplica);

    ECUnderReplicationHandler ecURH =
        new ECUnderReplicationHandler(
            sameNodePolicy, conf, replicationManager);
    UnderReplicatedHealthResult underRep =
        new UnderReplicatedHealthResult(container,
            1, false, false, false);

    // The underRepHandler processes in stages. First missing indexes, then
    // decommission and then maintenance. If a stage fails due to no new nodes
    // available and there are some commands created by an earlier stage, we
    // should just return those created commands even if the container is over
    // replicated.
    for (ContainerReplica toAdd : replicasToAdd) {
      // The replicas are always over replicated with 2 index 4's.
      // Index 1 is missing, then we add in either a decommissioning or
      // entering_maintenance replica 5.
      Set<ContainerReplica> availableReplicas = ReplicationTestUtil
          .createReplicas(
              Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
              Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 4));
      if (toAdd != null) {
        availableReplicas.add(toAdd);
      }

      assertThrows(SCMException.class,
          () -> ecURH.processAndSendCommands(availableReplicas,
              emptyList(), underRep, 2));

      verify(replicationManager, times(1))
          .processOverReplicatedContainer(underRep);
      assertEquals(1, commandsSent.size());
      Pair<DatanodeDetails, SCMCommand<?>> pair =
          commandsSent.iterator().next();
      assertEquals(newNode, pair.getKey());
      assertEquals(
          SCMCommandProto.Type.reconstructECContainersCommand,
          pair.getValue().getType());
      clearInvocations(replicationManager);
      commandsSent.clear();
    }
  }

  @Test
  public void testUnderRepDueToDecomAndOverRep()
      throws IOException {
    PlacementPolicy sameNodePolicy = ReplicationTestUtil
        .getNoNodesTestPlacementPolicy(nodeManager, conf);
    // First it is under-replicated, due to decom index 5, but no mode will be
    // found. This will cause an exception to be thrown out, as the container is
    // not also over replicated.
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(DECOMMISSIONING, 5));

    ECUnderReplicationHandler ecURH =
        new ECUnderReplicationHandler(
            sameNodePolicy, conf, replicationManager);

    UnderReplicatedHealthResult underRep =
        new UnderReplicatedHealthResult(container,
            1, true, false, false);

    assertThrows(SCMException.class,
        () -> ecURH.processAndSendCommands(availableReplicas,
            emptyList(), underRep, 1));

    // Now adjust replicas so it is also over replicated. This time it should
    // call the OverRepHandler and then throw
    ContainerReplica overRepReplica =
        createContainerReplica(container.containerID(),
            4, IN_SERVICE, CLOSED);
    availableReplicas.add(overRepReplica);

    Set<Pair<DatanodeDetails, SCMCommand<?>>> expectedDelete = new HashSet<>();
    expectedDelete.add(Pair.of(overRepReplica.getDatanodeDetails(),
        createDeleteContainerCommand(container,
            overRepReplica.getReplicaIndex())));

    when(replicationManager.processOverReplicatedContainer(
        underRep)).thenAnswer(invocationOnMock -> {
          commandsSent.addAll(expectedDelete);
          return expectedDelete.size();
        });
    assertThrows(SCMException.class, () -> ecURH.processAndSendCommands(
        availableReplicas, emptyList(), underRep, 1));
    verify(replicationManager, times(1))
        .processOverReplicatedContainer(underRep);
    assertEquals(expectedDelete, commandsSent);
  }

  @Test
  public void testMissingAndDecomIndexWithOnlyOneNewNodeAvailable()
      throws IOException {
    DatanodeDetails newDn = MockDatanodeDetails.randomDatanodeDetails();
    PlacementPolicy sameNodePolicy = ReplicationTestUtil
        .getSameNodeTestPlacementPolicy(nodeManager, conf, newDn);
    // Just have a missing index, this should return OK.
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4));
    // Passing zero for decommIndexes, as we don't expect the decom command to
    // get created due to the placement policy returning an already used node
    testUnderReplicationWithMissingIndexes(ImmutableList.of(5),
        availableReplicas, 0, 0, sameNodePolicy);
    commandsSent.clear();

    // Now add a decommissioning index - we will not get a replicate command
    // for it, as the placement policy will throw an exception which will
    // come up the stack and be thrown out to indicate this container must be
    // retried.
    Set<ContainerReplica> replicas = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 4));

    assertThrows(SCMException.class, () ->
        testUnderReplicationWithMissingIndexes(ImmutableList.of(5), replicas,
            0, 0, sameNodePolicy));
  }

  @Test
  public void testUnderAndOverReplication() throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1), Pair.of(IN_SERVICE, 1),
            Pair.of(IN_MAINTENANCE, 1), Pair.of(IN_MAINTENANCE, 1),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5));
    Set<Pair<DatanodeDetails, SCMCommand<?>>> cmds =
        testUnderReplicationWithMissingIndexes(ImmutableList.of(2, 3),
            availableReplicas, 0, 0, policy);
    assertEquals(1, cmds.size());
    ReconstructECContainersCommand cmd =
        (ReconstructECContainersCommand) cmds.iterator().next().getValue();
    // Ensure that all source nodes are IN_SERVICE, we should not have picked
    // the non in-service nodes for index 1.
    for (ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex s
        : cmd.getSources()) {
      assertEquals(
          IN_SERVICE, s.getDnDetails().getPersistedOpState());
    }
  }

  /**
   * HDDS-7683 was a case where the maintenance logic was calling the placement
   * policy requesting zero nodes. This test asserts that it is never called
   * with zero nodes to ensure that issue is fixed.
   */
  @Test
  public void testMaintenanceDoesNotRequestZeroNodes() throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_MAINTENANCE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5));

    when(ecPlacementPolicy.chooseDatanodes(anyList(), anyList(),
            isNull(), anyInt(), anyLong(), anyLong()))
        .thenAnswer(invocationOnMock -> {
          int numNodes = invocationOnMock.getArgument(3);
          List<DatanodeDetails> targets = new ArrayList<>();
          for (int i = 0; i < numNodes; i++) {
            targets.add(MockDatanodeDetails.randomDatanodeDetails());
          }
          return targets;
        });

    UnderReplicatedHealthResult result =
        mock(UnderReplicatedHealthResult.class);
    when(result.getContainerInfo()).thenReturn(container);
    ECUnderReplicationHandler handler = new ECUnderReplicationHandler(
        ecPlacementPolicy, conf, replicationManager);

    handler.processAndSendCommands(availableReplicas,
        emptyList(), result, 1);
    assertEquals(1, commandsSent.size());
    verify(ecPlacementPolicy, times(0))
        .chooseDatanodes(anyList(), isNull(), eq(0), anyLong(),
            anyLong());
  }

  /**
   * Create 3 replicas with 1 pending ADD. This means that 1 replica needs to
   * be reconstructed. The target DN selected for reconstruction should not be
   * the DN pending add.
   */
  @Test
  public void testDatanodesPendingAddAreNotSelectedAsTargets()
      throws IOException {
    Set<ContainerReplica> availableReplicas = createReplicas(3);
    DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
    List<ContainerReplicaOp> pendingOps = ImmutableList.of(
        new ContainerReplicaOp(ContainerReplicaOp.PendingOpType.ADD, dn, 4, null, System.currentTimeMillis(), 0));

    /*
    Mock the placement policy. If the list of nodes to be excluded does not
    contain the DN pending ADD, then chooseDatanodes will return a list
    containing that DN. Ensures the test will fail if excludeNodes does not
    contain the DN pending ADD.
     */
    when(ecPlacementPolicy.chooseDatanodes(anyList(), anyList(),
            isNull(), anyInt(), anyLong(), anyLong()))
        .thenAnswer(invocationOnMock -> {
          List<DatanodeDetails> usedList = invocationOnMock.getArgument(0);
          List<DatanodeDetails> excludeList = invocationOnMock.getArgument(1);
          List<DatanodeDetails> targets = new ArrayList<>(1);
          if (usedList.contains(dn) || excludeList.contains(dn)) {
            targets.add(MockDatanodeDetails.randomDatanodeDetails());
          } else {
            targets.add(dn);
          }
          return targets;
        });

    UnderReplicatedHealthResult result =
        mock(UnderReplicatedHealthResult.class);
    when(result.getContainerInfo()).thenReturn(container);
    ECUnderReplicationHandler handler = new ECUnderReplicationHandler(
        ecPlacementPolicy, conf, replicationManager);

    handler.processAndSendCommands(availableReplicas, pendingOps, result, 1);
    assertEquals(1, commandsSent.size());
    assertNotEquals(dn, commandsSent.iterator().next().getKey());
  }

  @Test
  public void testDecommissioningIndexCopiedWhenContainerUnRecoverable()
      throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1));
    ContainerReplica decomReplica = createContainerReplica(
        container.containerID(), 2, DECOMMISSIONING, CLOSED);
    availableReplicas.add(decomReplica);
    Set<Pair<DatanodeDetails, SCMCommand<?>>> cmds =
        testUnderReplicationWithMissingIndexes(emptyList(),
            availableReplicas, 1, 0, policy);
    assertEquals(1, cmds.size());
    // With push replication the command should always be sent to the
    // decommissioning source.
    DatanodeDetails target = cmds.iterator().next().getKey();
    assertEquals(decomReplica.getDatanodeDetails(), target);
  }

  @Test
  public void testMaintenanceIndexCopiedWhenContainerUnRecoverable()
      throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1));
    ContainerReplica maintReplica = createContainerReplica(
        container.containerID(), 2, ENTERING_MAINTENANCE, CLOSED);
    availableReplicas.add(maintReplica);

    Set<Pair<DatanodeDetails, SCMCommand<?>>> cmds =
        testUnderReplicationWithMissingIndexes(emptyList(),
            availableReplicas, 0, 1, policy);
    assertEquals(0, cmds.size());

    // Change the remaining redundancy to ensure something needs copied.
    remainingMaintenanceRedundancy = 2;
    cmds = testUnderReplicationWithMissingIndexes(emptyList(),
        availableReplicas, 0, 1, policy);

    assertEquals(1, cmds.size());
    // With push replication the command should always be sent to the
    // entering_maintenance source.
    DatanodeDetails target = cmds.iterator().next().getKey();
    assertEquals(maintReplica.getDatanodeDetails(), target);
  }

  public Set<Pair<DatanodeDetails, SCMCommand<?>>>
      testUnderReplicationWithMissingIndexes(
      List<Integer> missingIndexes, Set<ContainerReplica> availableReplicas,
      int decomIndexes, int maintenanceIndexes,
      PlacementPolicy placementPolicy) throws IOException {
    ECUnderReplicationHandler ecURH =
        new ECUnderReplicationHandler(
            placementPolicy, conf, replicationManager);
    UnderReplicatedHealthResult result =
        mock(UnderReplicatedHealthResult.class);
    when(result.isUnrecoverable()).thenReturn(false);
    when(result.getContainerInfo()).thenReturn(container);

    ecURH.processAndSendCommands(availableReplicas, ImmutableList.of(),
        result, remainingMaintenanceRedundancy);
    int replicateCommand = 0;
    int reconstructCommand = 0;
    boolean shouldReconstructCommandExist =
        !missingIndexes.isEmpty() && missingIndexes.size() <= repConfig
            .getParity();
    for (Map.Entry<DatanodeDetails, SCMCommand<?>> dnCommand : commandsSent) {
      if (dnCommand.getValue() instanceof ReplicateContainerCommand) {
        replicateCommand++;
      } else if (dnCommand
          .getValue() instanceof ReconstructECContainersCommand) {
        if (shouldReconstructCommandExist) {
          assertEquals(ECUnderReplicationHandler.integers2ByteString(missingIndexes),
              ((ReconstructECContainersCommand) dnCommand.getValue())
              .getMissingContainerIndexes());
        }
        reconstructCommand++;
      }
    }
    int maxMaintenance = PARITY - remainingMaintenanceRedundancy;
    int expectedMaintenanceCommands = Math.max(0,
        maintenanceIndexes - maxMaintenance);
    assertEquals(decomIndexes + expectedMaintenanceCommands,
        replicateCommand);
    assertEquals(shouldReconstructCommandExist ? 1 : 0,
        reconstructCommand);
    return commandsSent;
  }

  private DeleteContainerCommand createDeleteContainerCommand(
      ContainerInfo containerInfo, int replicaIndex) {
    DeleteContainerCommand deleteCommand =
        new DeleteContainerCommand(containerInfo.getContainerID(), true);
    deleteCommand.setReplicaIndex(replicaIndex);
    return deleteCommand;
  }

  /**
   * Helper to mock and verify calls to
   * {@link PlacementPolicy#chooseDatanodes(List, List, int, long, long)}.
   */
  private static class PlacementPolicySpy {

    private final List<List<DatanodeDetails>> excludedNodesLists =
        new ArrayList<>();
    private final List<List<DatanodeDetails>> usedNodesLists =
        new ArrayList<>();

    PlacementPolicySpy(PlacementPolicy placementPolicy, int totalNodes)
        throws IOException {
      when(placementPolicy.chooseDatanodes(any(), any(),
          any(), anyInt(), anyLong(), anyLong())
      ).thenAnswer(invocation -> {
        final Collection<DatanodeDetails> used = invocation.getArgument(0);
        final Collection<DatanodeDetails> excluded = invocation.getArgument(1);
        final int nodesRequired = invocation.getArgument(3);

        final int availableNodes = totalNodes - excluded.size() - used.size();
        usedNodesLists.add(new ArrayList<>(used));
        excludedNodesLists.add(new ArrayList<>(excluded));

        final List<DatanodeDetails> targets = new ArrayList<>();
        for (int i = 0; i < Math.min(nodesRequired, availableNodes); i++) {
          targets.add(MockDatanodeDetails.randomDatanodeDetails());
        }
        if (targets.isEmpty()) {
          throw new SCMException("not enough nodes",
              SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
        }
        return targets;
      });
    }

    int callCount() {
      return usedNodesLists.size();
    }

    List<DatanodeDetails> usedNodes(int i) {
      return usedNodesLists.get(i);
    }

    List<DatanodeDetails> excludedNodes(int i) {
      return excludedNodesLists.get(i);
    }
  }
}
