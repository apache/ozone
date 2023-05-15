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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerReplica;

/**
 * Tests for ReplicationManagerUtil.
 */
public class TestReplicationManagerUtil {

  private ReplicationManager replicationManager;

  @Before
  public void setup() {
    replicationManager = Mockito.mock(ReplicationManager.class);
  }

  @Test
  public void testGetExcludedAndUsedNodes() throws NodeNotFoundException {
    ContainerID cid = ContainerID.valueOf(1L);
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

    Mockito.when(replicationManager.getNodeStatus(Mockito.any())).thenAnswer(
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
        ReplicationManagerUtil.getExcludedAndUsedNodes(
            new ArrayList<>(replicas), toBeRemoved, pending,
            replicationManager);

    Assertions.assertEquals(3, excludedAndUsedNodes.getUsedNodes().size());
    Assertions.assertTrue(excludedAndUsedNodes.getUsedNodes()
        .contains(good.getDatanodeDetails()));
    Assertions.assertTrue(excludedAndUsedNodes.getUsedNodes()
        .contains(maintenance.getDatanodeDetails()));
    Assertions.assertTrue(excludedAndUsedNodes.getUsedNodes()
        .contains(pendingAdd));

    Assertions.assertEquals(4, excludedAndUsedNodes.getExcludedNodes().size());
    Assertions.assertTrue(excludedAndUsedNodes.getExcludedNodes()
        .contains(unhealthy.getDatanodeDetails()));
    Assertions.assertTrue(excludedAndUsedNodes.getExcludedNodes()
        .contains(decommissioning.getDatanodeDetails()));
    Assertions.assertTrue(excludedAndUsedNodes.getExcludedNodes()
        .contains(remove.getDatanodeDetails()));
    Assertions.assertTrue(excludedAndUsedNodes.getExcludedNodes()
        .contains(pendingDelete));
  }

}
