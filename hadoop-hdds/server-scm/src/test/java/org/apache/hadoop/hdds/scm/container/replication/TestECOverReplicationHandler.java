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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.DELETE;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;

/**
 * Tests the ECOverReplicationHandling functionality.
 */
public class TestECOverReplicationHandler {
  private ECReplicationConfig repConfig;
  private ContainerInfo container;
  private NodeManager nodeManager;
  private OzoneConfiguration conf;
  private PlacementPolicy policy;
  private DatanodeDetails staleNode;

  @BeforeEach
  public void setup() {
    staleNode = null;
    nodeManager = new MockNodeManager(true, 10) {
      @Override
      public NodeStatus getNodeStatus(DatanodeDetails dd)
          throws NodeNotFoundException {
        if (staleNode != null && dd.equals(staleNode)) {
          return NodeStatus.inServiceStale();
        }
        return NodeStatus.inServiceHealthy();
      }
    };
    conf = SCMTestUtils.getConf();
    repConfig = new ECReplicationConfig(3, 2);
    container = ReplicationTestUtil
        .createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    policy = ReplicationTestUtil
        .getSimpleTestPlacementPolicy(nodeManager, conf);
    NodeSchema[] schemas =
        new NodeSchema[] {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager.getInstance().init(schemas, true);
  }

  @Test
  public void testNoOverReplication() {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5));
    testOverReplicationWithIndexes(availableReplicas, Collections.emptyMap(),
        ImmutableList.of());
  }

  @Test
  public void testOverReplicationFixedByPendingDelete() {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5));
    ContainerReplica excess = ReplicationTestUtil.createContainerReplica(
        container.containerID(), 5, IN_SERVICE,
        ContainerReplicaProto.State.CLOSED);
    availableReplicas.add(excess);
    List<ContainerReplicaOp> pendingOps = new ArrayList();
    pendingOps.add(ContainerReplicaOp.create(DELETE,
        excess.getDatanodeDetails(), 5));
    testOverReplicationWithIndexes(availableReplicas, Collections.emptyMap(),
        pendingOps);
  }

  @Test
  public void testOverReplicationWithDecommissionIndexes() {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5),
            Pair.of(DECOMMISSIONING, 5));
    testOverReplicationWithIndexes(availableReplicas, Collections.emptyMap(),
        ImmutableList.of());
  }

  @Test
  public void testOverReplicationWithStaleIndexes() {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5));
    ContainerReplica stale = ReplicationTestUtil.createContainerReplica(
        container.containerID(), 5, IN_SERVICE,
        ContainerReplicaProto.State.CLOSED);
    availableReplicas.add(stale);
    // By setting stale node, it makes the mocked nodeManager return a stale
    // start for it when checked.
    staleNode = stale.getDatanodeDetails();
    testOverReplicationWithIndexes(availableReplicas, Collections.emptyMap(),
        ImmutableList.of());
  }

  @Test
  public void testOverReplicationWithOpenReplica() {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5));
    ContainerReplica open = ReplicationTestUtil.createContainerReplica(
        container.containerID(), 5, IN_SERVICE,
        ContainerReplicaProto.State.OPEN);
    availableReplicas.add(open);
    testOverReplicationWithIndexes(availableReplicas, Collections.emptyMap(),
        ImmutableList.of());
  }

  /**
   * This test mocks the placement policy so it returns invalid results. This
   * should not happen, but it tests that commands are not sent for the wrong
   * replica.
   */
  @Test
  public void testOverReplicationButPolicyReturnsWrongIndexes() {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5),
            Pair.of(IN_SERVICE, 5));
    ContainerReplica toReturn = ReplicationTestUtil.createContainerReplica(
        container.containerID(), 1, IN_SERVICE,
        ContainerReplicaProto.State.CLOSED);
    policy = Mockito.mock(PlacementPolicy.class);
    Mockito.when(policy.replicasToRemoveToFixOverreplication(
        Mockito.any(), Mockito.anyInt()))
        .thenReturn(ImmutableSet.of(toReturn));
    testOverReplicationWithIndexes(availableReplicas, Collections.emptyMap(),
        ImmutableList.of());
  }

  @Test
  public void testOverReplicationWithOneSameIndexes() {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5));

    testOverReplicationWithIndexes(availableReplicas,
        //num of index 1 is 3, but it should be 1, so 2 excess
        new ImmutableMap.Builder<Integer, Integer>().put(1, 2).build(),
            ImmutableList.of());
  }

  @Test
  public void testOverReplicationWithMultiSameIndexes() {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5), Pair.of(IN_SERVICE, 5));

    testOverReplicationWithIndexes(availableReplicas,
        //num of index 1 is 3, but it should be 1, so 2 excess
        new ImmutableMap.Builder<Integer, Integer>()
            .put(1, 2).put(2, 2).put(3, 2).put(4, 1)
            .put(5, 1).build(), ImmutableList.of());
  }

  /**
   * Even if we pass an under-replicated health state to the over-rep handler
   * it should process it OK, and if it has over replicated indexes, then
   * delete commands should be produced.
   */
  @Test
  public void testOverReplicationWithUnderReplication() {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(
            Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5));

    ContainerHealthResult.UnderReplicatedHealthResult health =
        new ContainerHealthResult.UnderReplicatedHealthResult(
            container, 1, false, false, false);

    ECOverReplicationHandler ecORH =
        new ECOverReplicationHandler(policy, nodeManager);

    Map<DatanodeDetails, SCMCommand<?>> commands = ecORH
        .processAndCreateCommands(availableReplicas, ImmutableList.of(),
            health, 1);

    Assert.assertEquals(1, commands.size());
    for (SCMCommand<?> cmd : commands.values()) {
      Assert.assertEquals(1, ((DeleteContainerCommand)cmd).getReplicaIndex());
    }
  }

  private void testOverReplicationWithIndexes(
      Set<ContainerReplica> availableReplicas,
      Map<Integer, Integer> index2excessNum,
      List<ContainerReplicaOp> pendingOps) {
    ECOverReplicationHandler ecORH =
        new ECOverReplicationHandler(policy, nodeManager);
    ContainerHealthResult.OverReplicatedHealthResult result =
        Mockito.mock(ContainerHealthResult.OverReplicatedHealthResult.class);
    Mockito.when(result.getContainerInfo()).thenReturn(container);

    Map<DatanodeDetails, SCMCommand<?>> commands = ecORH
        .processAndCreateCommands(availableReplicas, pendingOps,
            result, 1);

    // total commands send out should be equal to the sum of all
    // the excess nums
    int totalDeleteCommandNum =
        index2excessNum.values().stream().reduce(0, Integer::sum);
    Assert.assertEquals(totalDeleteCommandNum, commands.size());

    // Each command should have a non-zero replica index
    commands.forEach((datanode, command) -> Assert.assertNotEquals(0,
        ((DeleteContainerCommand)command).getReplicaIndex()));

    // command num of each index should be equal to the excess num
    // of this index
    Map<DatanodeDetails, Integer> datanodeDetails2Index =
        availableReplicas.stream().collect(Collectors.toMap(
            ContainerReplica::getDatanodeDetails,
            ContainerReplica::getReplicaIndex));
    Map<Integer, Integer> index2commandNum = new HashMap<>();
    commands.keySet().forEach(dd ->
        index2commandNum.merge(datanodeDetails2Index.get(dd), 1, Integer::sum)
    );

    index2commandNum.keySet().forEach(i -> {
      Assert.assertTrue(index2excessNum.containsKey(i));
      Assert.assertEquals(index2commandNum.get(i), index2excessNum.get(i));
    });
  }
}
