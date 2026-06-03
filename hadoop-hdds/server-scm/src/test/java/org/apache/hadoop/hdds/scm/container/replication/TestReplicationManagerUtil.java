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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps.SizeAndTime;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainer;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerReplica;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for ReplicationManagerUtil.
 */
public class TestReplicationManagerUtil {

  private ReplicationManager replicationManager;

  @BeforeEach
  public void setup() {
    replicationManager = mock(ReplicationManager.class);
    ContainerReplicaPendingOps pendingOpsMock = mock(ContainerReplicaPendingOps.class);
    when(replicationManager.getContainerReplicaPendingOps()).thenReturn(pendingOpsMock);
  }

  @Test
  public void testGetExcludedAndUsedNodes() throws NodeNotFoundException {
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));
    ContainerID cid = container.containerID();
    Set<ContainerReplica> replicas = new HashSet<>();
    ContainerReplica good = createContainerReplica(cid, 0,
        IN_SERVICE, ContainerReplicaProto.State.CLOSED, 1);
    replicas.add(good);

    ContainerReplica remove = createContainerReplica(cid, 0,
        IN_SERVICE, ContainerReplicaProto.State.CLOSED, 1);
    replicas.add(remove);

    ContainerReplica unhealthy = createContainerReplica(
        cid, 0, IN_SERVICE, ContainerReplicaProto.State.UNHEALTHY, 1);
    replicas.add(unhealthy);

    ContainerReplica decommissioning =
        createContainerReplica(cid, 0,
            DECOMMISSIONING, ContainerReplicaProto.State.CLOSED, 1);
    replicas.add(decommissioning);

    ContainerReplica maintenance =
        createContainerReplica(cid, 0,
            IN_MAINTENANCE, ContainerReplicaProto.State.CLOSED, 1);
    replicas.add(maintenance);

    // dead maintenance node should neither be on the used list nor on the excluded list
    ContainerReplica deadMaintenanceReplica = createContainerReplica(cid, 0,
        IN_MAINTENANCE, ContainerReplicaProto.State.CLOSED, 1);
    DatanodeDetails deadMaintenanceNode = deadMaintenanceReplica.getDatanodeDetails();
    replicas.add(deadMaintenanceReplica);

    // Take one of the replicas and set it to be removed. It should be on the
    // excluded list rather than the used list.
    Set<ContainerReplica> toBeRemoved = new HashSet<>();
    toBeRemoved.add(remove);

    // Finally, add a pending add and delete. The add should go onto the used
    // list and the delete added to the excluded nodes.
    DatanodeDetails pendingAdd = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails pendingDelete = MockDatanodeDetails.randomDatanodeDetails();
    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.ADD, pendingAdd, 0, null, Long.MAX_VALUE, 0));
    pending.add(new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.DELETE, pendingDelete, 0, null, Long.MAX_VALUE, 0));

    when(replicationManager.getNodeStatus(any())).thenAnswer(
        invocation -> {
          final DatanodeDetails dn = invocation.getArgument(0);
          if (dn.equals(deadMaintenanceNode)) {
            return NodeStatus.valueOf(dn.getPersistedOpState(), HddsProtos.NodeState.DEAD);
          }
          for (ContainerReplica r : replicas) {
            if (r.getDatanodeDetails().equals(dn)) {
              return NodeStatus.valueOf(
                  r.getDatanodeDetails().getPersistedOpState(),
                  HddsProtos.NodeState.HEALTHY);
            }
          }
          throw new NodeNotFoundException(dn.getID());
        });

    ReplicationManagerUtil.ExcludedAndUsedNodes excludedAndUsedNodes =
        ReplicationManagerUtil.getExcludedAndUsedNodes(container,
            new ArrayList<>(replicas), toBeRemoved, pending,
            replicationManager);

    assertEquals(3, excludedAndUsedNodes.getUsedNodes().size());
    assertThat(excludedAndUsedNodes.getUsedNodes())
        .contains(good.getDatanodeDetails());
    assertThat(excludedAndUsedNodes.getUsedNodes())
        .contains(maintenance.getDatanodeDetails());
    assertThat(excludedAndUsedNodes.getUsedNodes())
        .contains(pendingAdd);
    assertFalse(excludedAndUsedNodes.getUsedNodes().contains(deadMaintenanceNode));

    assertEquals(4, excludedAndUsedNodes.getExcludedNodes().size());
    assertThat(excludedAndUsedNodes.getExcludedNodes())
        .contains(unhealthy.getDatanodeDetails());
    assertThat(excludedAndUsedNodes.getExcludedNodes())
        .contains(decommissioning.getDatanodeDetails());
    assertThat(excludedAndUsedNodes.getExcludedNodes())
        .contains(remove.getDatanodeDetails());
    assertThat(excludedAndUsedNodes.getExcludedNodes())
        .contains(pendingDelete);
    assertFalse(excludedAndUsedNodes.getExcludedNodes().contains(deadMaintenanceNode));
  }

  @Test
  public void testGetUsedAndExcludedNodesForQuasiClosedContainer() throws NodeNotFoundException {
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.QUASI_CLOSED,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));
    ContainerID cid = container.containerID();
    Set<ContainerReplica> replicas = new HashSet<>();
    ContainerReplica good = createContainerReplica(cid, 0, IN_SERVICE,
        ContainerReplicaProto.State.QUASI_CLOSED, 1);
    replicas.add(good);

    ContainerReplica remove = createContainerReplica(cid, 0,
        IN_SERVICE, ContainerReplicaProto.State.QUASI_CLOSED, 1);
    replicas.add(remove);
    Set<ContainerReplica> toBeRemoved = new HashSet<>();
    toBeRemoved.add(remove);

    // this replica should be on the used nodes list
    ContainerReplica unhealthyWithUniqueOrigin = createContainerReplica(
        cid, 0, IN_SERVICE, ContainerReplicaProto.State.UNHEALTHY, 1);
    replicas.add(unhealthyWithUniqueOrigin);

    // this one should be on the excluded nodes list
    ContainerReplica unhealthyWithNonUniqueOrigin = createContainerReplica(cid, 0, IN_SERVICE,
        ContainerReplicaProto.State.UNHEALTHY, container.getNumberOfKeys(), container.getUsedBytes(),
        MockDatanodeDetails.randomDatanodeDetails(), good.getOriginDatanodeId());
    replicas.add(unhealthyWithNonUniqueOrigin);

    ContainerReplica decommissioning =
        createContainerReplica(cid, 0,
            DECOMMISSIONING, ContainerReplicaProto.State.QUASI_CLOSED, 1);
    replicas.add(decommissioning);

    ContainerReplica maintenance =
        createContainerReplica(cid, 0,
            IN_MAINTENANCE, ContainerReplicaProto.State.QUASI_CLOSED, 1);
    replicas.add(maintenance);

    // Finally, add a pending add and delete. The add should go onto the used
    // list and the delete added to the excluded nodes.
    DatanodeDetails pendingAdd = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails pendingDelete = MockDatanodeDetails.randomDatanodeDetails();
    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.ADD, pendingAdd, 0, null, Long.MAX_VALUE, 0));
    pending.add(new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.DELETE, pendingDelete, 0, null, Long.MAX_VALUE, 0));

    when(replicationManager.getNodeStatus(any())).thenAnswer(
        invocation -> {
          final DatanodeDetails dn = invocation.getArgument(0);
          for (ContainerReplica r : replicas) {
            if (r.getDatanodeDetails().equals(dn)) {
              return NodeStatus.valueOf(
                  r.getDatanodeDetails().getPersistedOpState(),
                  HddsProtos.NodeState.HEALTHY);
            }
          }
          throw new NodeNotFoundException(dn.getID());
        });

    ReplicationManagerUtil.ExcludedAndUsedNodes excludedAndUsedNodes =
        ReplicationManagerUtil.getExcludedAndUsedNodes(container,
            new ArrayList<>(replicas), toBeRemoved, pending,
            replicationManager);

    assertEquals(4, excludedAndUsedNodes.getUsedNodes().size());
    assertThat(excludedAndUsedNodes.getUsedNodes())
        .contains(good.getDatanodeDetails());
    assertThat(excludedAndUsedNodes.getUsedNodes())
        .contains(maintenance.getDatanodeDetails());
    assertThat(excludedAndUsedNodes.getUsedNodes())
        .contains(pendingAdd);
    assertThat(excludedAndUsedNodes.getUsedNodes()).contains(unhealthyWithUniqueOrigin.getDatanodeDetails());

    assertEquals(4, excludedAndUsedNodes.getExcludedNodes().size());
    assertThat(excludedAndUsedNodes.getExcludedNodes())
        .contains(unhealthyWithNonUniqueOrigin.getDatanodeDetails());
    assertThat(excludedAndUsedNodes.getExcludedNodes())
        .contains(decommissioning.getDatanodeDetails());
    assertThat(excludedAndUsedNodes.getExcludedNodes())
        .contains(remove.getDatanodeDetails());
    assertThat(excludedAndUsedNodes.getExcludedNodes())
        .contains(pendingDelete);
  }

  @Test
  public void testDatanodesWithInSufficientDiskSpaceAreExcluded() throws NodeNotFoundException {
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));
    final long oneGb = 1024L * 1024 * 1024;
    container.setUsedBytes(5 * oneGb);
    ContainerID cid = container.containerID();
    Set<ContainerReplica> replicas = new HashSet<>();
    ContainerReplica good = createContainerReplica(cid, 0,
        IN_SERVICE, ContainerReplicaProto.State.CLOSED, 1);
    replicas.add(good);

    ContainerReplica remove = createContainerReplica(cid, 0,
        IN_SERVICE, ContainerReplicaProto.State.CLOSED, 1);
    replicas.add(remove);

    ContainerReplica unhealthy = createContainerReplica(
        cid, 0, IN_SERVICE, ContainerReplicaProto.State.UNHEALTHY, 1);
    replicas.add(unhealthy);

    ContainerReplica decommissioning =
        createContainerReplica(cid, 0,
            DECOMMISSIONING, ContainerReplicaProto.State.CLOSED, 1);
    replicas.add(decommissioning);

    ContainerReplica maintenance =
        createContainerReplica(cid, 0,
            IN_MAINTENANCE, ContainerReplicaProto.State.CLOSED, 1);
    replicas.add(maintenance);

    // Take one of the replicas and set it to be removed. It should be on the
    // excluded list rather than the used list.
    Set<ContainerReplica> toBeRemoved = new HashSet<>();
    toBeRemoved.add(remove);

    // Add a pending add and delete. The add should go onto the used
    // list and the delete added to the excluded nodes.
    DatanodeDetails pendingAdd = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails pendingDelete = MockDatanodeDetails.randomDatanodeDetails();
    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.ADD, pendingAdd, 0, null, Long.MAX_VALUE, 0));
    pending.add(new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.DELETE, pendingDelete, 0, null, Long.MAX_VALUE, 0));

    // set up mocks such ContainerReplicaPendingOps returns the containerSizeScheduled map
    ReplicationManagerConfiguration rmConf = new ReplicationManagerConfiguration();
    when(replicationManager.getConfig()).thenReturn(rmConf);
    TestClock clock = new TestClock(Instant.now(), ZoneOffset.UTC);
    ConcurrentHashMap<DatanodeID, SizeAndTime> sizeScheduledMap = new ConcurrentHashMap<>();

    // fullDn has 10GB size scheduled, 30GB available and 20GB min free space, so it should be excluded
    DatanodeDetails fullDn = MockDatanodeDetails.randomDatanodeDetails();
    sizeScheduledMap.put(fullDn.getID(), new SizeAndTime(10 * oneGb, clock.millis()));
    // spaceAvailableDn should not be excluded as it has sufficient space
    DatanodeDetails spaceAvailableDn = MockDatanodeDetails.randomDatanodeDetails();
    sizeScheduledMap.put(spaceAvailableDn.getID(), new SizeAndTime(10 * oneGb, clock.millis()));
    // expiredOpDn is the same as fullDn, however its op has expired - so it should not be excluded
    DatanodeDetails expiredOpDn = MockDatanodeDetails.randomDatanodeDetails();
    sizeScheduledMap.put(expiredOpDn.getID(), new SizeAndTime(10 * oneGb,
        clock.millis() - rmConf.getEventTimeout() - 1));

    ContainerReplicaPendingOps pendingOpsMock = mock(ContainerReplicaPendingOps.class);
    when(pendingOpsMock.getContainerSizeScheduled()).thenReturn(sizeScheduledMap);
    when(pendingOpsMock.getClock()).thenReturn(clock);
    when(replicationManager.getContainerReplicaPendingOps()).thenReturn(pendingOpsMock);

    NodeManager nodeManagerMock = mock(NodeManager.class);
    when(replicationManager.getNodeManager()).thenReturn(nodeManagerMock);
    doReturn(fullDn).when(nodeManagerMock).getNode(fullDn.getID());
    doReturn(new SCMNodeMetric(50 * oneGb, 20 * oneGb, 30 * oneGb, 5 * oneGb,
        20 * oneGb, 0)).when(nodeManagerMock).getNodeStat(fullDn);
    doReturn(spaceAvailableDn).when(nodeManagerMock).getNode(spaceAvailableDn.getID());
    doReturn(new SCMNodeMetric(50 * oneGb, 10 * oneGb, 40 * oneGb, 5 * oneGb,
        20 * oneGb, 0)).when(nodeManagerMock).getNodeStat(spaceAvailableDn);
    doReturn(expiredOpDn).when(nodeManagerMock).getNode(expiredOpDn.getID());
    doReturn(new SCMNodeMetric(50 * oneGb, 20 * oneGb, 30 * oneGb, 5 * oneGb,
        20 * oneGb, 0)).when(nodeManagerMock).getNodeStat(expiredOpDn);

    when(replicationManager.getNodeStatus(any())).thenAnswer(
        invocation -> {
          final DatanodeDetails dn = invocation.getArgument(0);
          for (ContainerReplica r : replicas) {
            if (r.getDatanodeDetails().equals(dn)) {
              return NodeStatus.valueOf(
                  r.getDatanodeDetails().getPersistedOpState(),
                  HddsProtos.NodeState.HEALTHY);
            }
          }
          throw new NodeNotFoundException(dn.getID());
        });

    ReplicationManagerUtil.ExcludedAndUsedNodes excludedAndUsedNodes =
        ReplicationManagerUtil.getExcludedAndUsedNodes(container,
            new ArrayList<>(replicas), toBeRemoved, pending,
            replicationManager);

    assertEquals(3, excludedAndUsedNodes.getUsedNodes().size());
    assertThat(excludedAndUsedNodes.getUsedNodes()).contains(good.getDatanodeDetails());
    assertThat(excludedAndUsedNodes.getUsedNodes()).contains(maintenance.getDatanodeDetails());
    assertThat(excludedAndUsedNodes.getUsedNodes()).contains(pendingAdd);

    assertEquals(5, excludedAndUsedNodes.getExcludedNodes().size());
    assertThat(excludedAndUsedNodes.getExcludedNodes()).contains(unhealthy.getDatanodeDetails());
    assertThat(excludedAndUsedNodes.getExcludedNodes()).contains(decommissioning.getDatanodeDetails());
    assertThat(excludedAndUsedNodes.getExcludedNodes()).contains(remove.getDatanodeDetails());
    assertThat(excludedAndUsedNodes.getExcludedNodes()).contains(pendingDelete);
    assertThat(excludedAndUsedNodes.getExcludedNodes()).contains(fullDn);
  }

  @Test
  public void testGetTargetDatanodesRecordsAllConfirmedTargets()
      throws Exception {
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));

    DatanodeDetails target1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails target2 = MockDatanodeDetails.randomDatanodeDetails();

    PlacementPolicy policy = mock(PlacementPolicy.class);
    when(policy.chooseDatanodes(any(), any(), anyInt(), anyLong(), anyLong()))
        .thenReturn(Arrays.asList(target1, target2));

    NodeManager nodeManager = mock(NodeManager.class);
    DatanodeInfo info1 = mock(DatanodeInfo.class);
    DatanodeInfo info2 = mock(DatanodeInfo.class);
    when(nodeManager.getDatanodeInfo(target1)).thenReturn(info1);
    when(nodeManager.getDatanodeInfo(target2)).thenReturn(info2);
    when(nodeManager.checkSpaceAndRecordAllocation(any(), any())).thenReturn(true);

    List<DatanodeDetails> result = ReplicationManagerUtil.getTargetDatanodes(
        policy, 2, null, new ArrayList<>(), 5L * 1024 * 1024 * 1024, container, nodeManager);

    assertEquals(2, result.size());
    verify(nodeManager, times(1)).checkSpaceAndRecordAllocation(info1, container.containerID());
    verify(nodeManager, times(1)).checkSpaceAndRecordAllocation(info2, container.containerID());
  }

  /**
   * When checkSpaceAndRecordAllocation returns false for a candidate, that node is excluded
   * and the placement policy is called again to find a replacement.
   */
  @Test
  public void testGetTargetDatanodesRetriesWhenPendingTrackerRejects()
      throws Exception {
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));

    DatanodeDetails rejected = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails replacement = MockDatanodeDetails.randomDatanodeDetails();

    PlacementPolicy policy = mock(PlacementPolicy.class);
    when(policy.chooseDatanodes(any(), any(), anyInt(), anyLong(), anyLong()))
        .thenReturn(Arrays.asList(rejected))
        .thenReturn(Arrays.asList(replacement));

    NodeManager nodeManager = mock(NodeManager.class);
    DatanodeInfo rejectedInfo = mock(DatanodeInfo.class);
    DatanodeInfo replacementInfo = mock(DatanodeInfo.class);
    when(nodeManager.getDatanodeInfo(rejected)).thenReturn(rejectedInfo);
    when(nodeManager.getDatanodeInfo(replacement)).thenReturn(replacementInfo);
    when(nodeManager.checkSpaceAndRecordAllocation(rejectedInfo, container.containerID()))
        .thenReturn(false);
    when(nodeManager.checkSpaceAndRecordAllocation(replacementInfo, container.containerID()))
        .thenReturn(true);

    List<DatanodeDetails> result = ReplicationManagerUtil.getTargetDatanodes(
        policy, 1, null, new ArrayList<>(), 5L * 1024 * 1024 * 1024, container, nodeManager);

    assertEquals(1, result.size());
    assertEquals(replacement, result.get(0));
    verify(policy, times(2)).chooseDatanodes(any(), any(), anyInt(), anyLong(), anyLong());
    verify(nodeManager, times(1)).checkSpaceAndRecordAllocation(rejectedInfo, container.containerID());
    verify(nodeManager, times(1)).checkSpaceAndRecordAllocation(replacementInfo, container.containerID());
  }

  /**
   * When a node is not found in NodeManager, it is excluded and the policy is retried
   * for a replacement node — same behaviour as a PendingContainerTracker rejection.
   */
  @Test
  public void testGetTargetDatanodesRetriesForUnknownDatanode()
      throws Exception {
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));

    DatanodeDetails unknown = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails replacement = MockDatanodeDetails.randomDatanodeDetails();

    PlacementPolicy policy = mock(PlacementPolicy.class);
    when(policy.chooseDatanodes(any(), any(), anyInt(), anyLong(), anyLong()))
        .thenReturn(Arrays.asList(unknown))
        .thenReturn(Arrays.asList(replacement));

    NodeManager nodeManager = mock(NodeManager.class);
    DatanodeInfo replacementInfo = mock(DatanodeInfo.class);
    when(nodeManager.getDatanodeInfo(unknown)).thenReturn(null);
    when(nodeManager.getDatanodeInfo(replacement)).thenReturn(replacementInfo);
    when(nodeManager.checkSpaceAndRecordAllocation(replacementInfo, container.containerID()))
        .thenReturn(true);

    List<DatanodeDetails> result = ReplicationManagerUtil.getTargetDatanodes(
        policy, 1, null, new ArrayList<>(), 5L * 1024 * 1024 * 1024, container, nodeManager);

    assertEquals(1, result.size());
    assertEquals(replacement, result.get(0));
    verify(policy, times(2)).chooseDatanodes(any(), any(), anyInt(), anyLong(), anyLong());
  }

  /**
   * If checkSpaceAndRecordAllocation keeps rejecting nodes until the policy exhausts
   * the cluster, getTargetDatanodes throws SCMException (no nodes selected at all).
   */
  @Test
  public void testGetTargetDatanodesThrowsWhenAllNodesRejected()
      throws Exception {
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));

    DatanodeDetails alwaysRejected = MockDatanodeDetails.randomDatanodeDetails();

    PlacementPolicy policy = mock(PlacementPolicy.class);
    when(policy.chooseDatanodes(any(), any(), anyInt(), anyLong(), anyLong()))
        .thenReturn(Arrays.asList(alwaysRejected))
        .thenThrow(new IOException("No nodes available"));

    NodeManager nodeManager = mock(NodeManager.class);
    DatanodeInfo rejectedInfo = mock(DatanodeInfo.class);
    when(nodeManager.getDatanodeInfo(alwaysRejected)).thenReturn(rejectedInfo);
    when(nodeManager.checkSpaceAndRecordAllocation(any(), any())).thenReturn(false);

    assertThrows(SCMException.class, () ->
        ReplicationManagerUtil.getTargetDatanodes(
            policy, 1, null, new ArrayList<>(), 5L * 1024 * 1024 * 1024, container, nodeManager));
  }

}
