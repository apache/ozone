/**
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
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainer;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerReplica;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;

/**
 * Tests for ReplicationManagerUtil.
 */
public class TestReplicationManagerUtil {

  private ReplicationManager replicationManager;

  @BeforeEach
  public void setup() {
    replicationManager = mock(ReplicationManager.class);
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

    // Take one of the replicas and set it to be removed. It should be on the
    // excluded list rather than the used list.
    Set<ContainerReplica> toBeRemoved = new HashSet<>();
    toBeRemoved.add(remove);

    // Finally, add a pending add and delete. The add should go onto the used
    // list and the delete added to the excluded nodes.
    DatanodeDetails pendingAdd = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails pendingDelete = MockDatanodeDetails.randomDatanodeDetails();
    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(ContainerReplicaOp.create(
        ContainerReplicaOp.PendingOpType.ADD, pendingAdd, 0));
    pending.add(ContainerReplicaOp.create(
        ContainerReplicaOp.PendingOpType.DELETE, pendingDelete, 0));

    when(replicationManager.getNodeStatus(any())).thenAnswer(
        invocation -> {
          final DatanodeDetails dn = invocation.getArgument(0);
          for (ContainerReplica r : replicas) {
            if (r.getDatanodeDetails().equals(dn)) {
              return new NodeStatus(
                  r.getDatanodeDetails().getPersistedOpState(),
                  HddsProtos.NodeState.HEALTHY);
            }
          }
          throw new NodeNotFoundException(dn.getUuidString());
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

    assertEquals(4, excludedAndUsedNodes.getExcludedNodes().size());
    assertThat(excludedAndUsedNodes.getExcludedNodes())
        .contains(unhealthy.getDatanodeDetails());
    assertThat(excludedAndUsedNodes.getExcludedNodes())
        .contains(decommissioning.getDatanodeDetails());
    assertThat(excludedAndUsedNodes.getExcludedNodes())
        .contains(remove.getDatanodeDetails());
    assertThat(excludedAndUsedNodes.getExcludedNodes())
        .contains(pendingDelete);
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
    pending.add(ContainerReplicaOp.create(
        ContainerReplicaOp.PendingOpType.ADD, pendingAdd, 0));
    pending.add(ContainerReplicaOp.create(
        ContainerReplicaOp.PendingOpType.DELETE, pendingDelete, 0));

    when(replicationManager.getNodeStatus(any())).thenAnswer(
        invocation -> {
          final DatanodeDetails dn = invocation.getArgument(0);
          for (ContainerReplica r : replicas) {
            if (r.getDatanodeDetails().equals(dn)) {
              return new NodeStatus(
                  r.getDatanodeDetails().getPersistedOpState(),
                  HddsProtos.NodeState.HEALTHY);
            }
          }
          throw new NodeNotFoundException(dn.getUuidString());
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

}
