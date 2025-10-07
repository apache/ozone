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
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.DELETE;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
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
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.stubbing.Answer;

/**
 * Tests the ECOverReplicationHandling functionality.
 */
public class TestECOverReplicationHandler {
  private ContainerInfo container;
  private ReplicationManager replicationManager;
  private PlacementPolicy policy;
  private DatanodeDetails staleNode;
  private Set<Pair<DatanodeDetails, SCMCommand<?>>> commandsSent;

  @BeforeEach
  void setup(@TempDir File testDir) throws NodeNotFoundException, NotLeaderException,
      CommandTargetOverloadedException {
    staleNode = null;

    replicationManager = mock(ReplicationManager.class);
    when(replicationManager.getNodeStatus(any(DatanodeDetails.class)))
        .thenAnswer(invocation -> {
          DatanodeDetails dd = invocation.getArgument(0);
          if (staleNode != null && staleNode.equals(dd)) {
            return NodeStatus.valueOf(dd.getPersistedOpState(), HddsProtos.NodeState.STALE);
          }
          return NodeStatus.valueOf(dd.getPersistedOpState(), HddsProtos.NodeState.HEALTHY);
        });

    commandsSent = new HashSet<>();
    ReplicationTestUtil.mockRMSendThrottledDeleteCommand(replicationManager,
        commandsSent);

    NodeManager nodeManager = new MockNodeManager(true, 10);
    OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
    ECReplicationConfig repConfig = new ECReplicationConfig(3, 2);
    container = ReplicationTestUtil
        .createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    policy = ReplicationTestUtil
        .getSimpleTestPlacementPolicy(nodeManager, conf);
    NodeSchema[] schemas =
        new NodeSchema[] {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager.getInstance().init(schemas, true);
  }

  @Test
  public void testNoOverReplication()
      throws NotLeaderException, CommandTargetOverloadedException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5));
    testOverReplicationWithIndexes(availableReplicas, Collections.emptyMap(),
        ImmutableList.of());
  }

  @Test
  public void testOverReplicationFixedByPendingDelete()
      throws NotLeaderException, CommandTargetOverloadedException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5));
    ContainerReplica excess = ReplicationTestUtil.createContainerReplica(
        container.containerID(), 5, IN_SERVICE,
        ContainerReplicaProto.State.CLOSED);
    availableReplicas.add(excess);
    List<ContainerReplicaOp> pendingOps = new ArrayList<>();
    pendingOps.add(new ContainerReplicaOp(DELETE,
        excess.getDatanodeDetails(), 5, null, Long.MAX_VALUE, 0));
    testOverReplicationWithIndexes(availableReplicas, Collections.emptyMap(),
        pendingOps);
  }

  @Test
  public void testOverReplicationWithDecommissionIndexes()
      throws NotLeaderException, CommandTargetOverloadedException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5),
            Pair.of(DECOMMISSIONING, 5));
    testOverReplicationWithIndexes(availableReplicas, Collections.emptyMap(),
        ImmutableList.of());
  }

  @Test
  public void testOverReplicationWithStaleIndexes()
      throws NotLeaderException, CommandTargetOverloadedException {
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
  public void testOverReplicationWithOpenReplica()
      throws NotLeaderException, CommandTargetOverloadedException {
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
  public void testOverReplicationButPolicyReturnsWrongIndexes()
      throws NotLeaderException, CommandTargetOverloadedException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5),
            Pair.of(IN_SERVICE, 5));
    ContainerReplica toReturn = ReplicationTestUtil.createContainerReplica(
        container.containerID(), 1, IN_SERVICE,
        ContainerReplicaProto.State.CLOSED);
    policy = mock(PlacementPolicy.class);
    when(policy.replicasToRemoveToFixOverreplication(any(), anyInt()))
        .thenReturn(ImmutableSet.of(toReturn));
    testOverReplicationWithIndexes(availableReplicas, Collections.emptyMap(),
        ImmutableList.of());
  }

  @Test
  public void testOverReplicationWithOneSameIndexes()
      throws NotLeaderException, CommandTargetOverloadedException {
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
  public void testOverReplicationWithMultiSameIndexes()
      throws NotLeaderException, CommandTargetOverloadedException {
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
  public void testOverReplicationWithUnderReplication()
      throws NotLeaderException, CommandTargetOverloadedException {
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
        new ECOverReplicationHandler(policy, replicationManager);

    ecORH.processAndSendCommands(availableReplicas, ImmutableList.of(),
        health, 1);

    assertEquals(1, commandsSent.size());
    SCMCommand<?> cmd = commandsSent.iterator().next().getValue();
    assertEquals(1, ((DeleteContainerCommand)cmd).getReplicaIndex());
  }

  @Test
  public void testDeleteThrottling() throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(
            Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5));

    ContainerHealthResult.UnderReplicatedHealthResult health =
        new ContainerHealthResult.UnderReplicatedHealthResult(
            container, 2, false, false, false);

    // On the first call to throttled delete, throw an overloaded exception.
    final AtomicBoolean shouldThrow = new AtomicBoolean(true);
    // On the first call we throw, on subsequent calls we succeed.
    doAnswer((Answer<Void>) invocationOnMock -> {
      if (shouldThrow.get()) {
        shouldThrow.set(false);
        throw new CommandTargetOverloadedException("Test exception");
      }
      ContainerInfo containerInfo = invocationOnMock.getArgument(0);
      int replicaIndex = invocationOnMock.getArgument(1);
      DatanodeDetails target = invocationOnMock.getArgument(2);
      boolean forceDelete = invocationOnMock.getArgument(3);
      DeleteContainerCommand deleteCommand = new DeleteContainerCommand(
          containerInfo.getContainerID(), forceDelete);
      deleteCommand.setReplicaIndex(replicaIndex);
      commandsSent.add(Pair.of(target, deleteCommand));
      return null;
    }).when(replicationManager)
        .sendThrottledDeleteCommand(any(), anyInt(), any(), anyBoolean());

    ECOverReplicationHandler ecORH =
        new ECOverReplicationHandler(policy, replicationManager);

    assertThrows(CommandTargetOverloadedException.class,
        () -> ecORH.processAndSendCommands(availableReplicas, ImmutableList.of(), health, 1));
    assertEquals(1, commandsSent.size());
  }

  private void testOverReplicationWithIndexes(
      Set<ContainerReplica> availableReplicas,
      Map<Integer, Integer> index2excessNum,
      List<ContainerReplicaOp> pendingOps) throws NotLeaderException,
      CommandTargetOverloadedException {
    ECOverReplicationHandler ecORH =
        new ECOverReplicationHandler(policy, replicationManager);
    ContainerHealthResult.OverReplicatedHealthResult result =
        mock(ContainerHealthResult.OverReplicatedHealthResult.class);
    when(result.getContainerInfo()).thenReturn(container);

    ecORH.processAndSendCommands(availableReplicas, pendingOps,
            result, 1);

    // total commands send out should be equal to the sum of all
    // the excess nums
    int totalDeleteCommandNum =
        index2excessNum.values().stream().reduce(0, Integer::sum);
    assertEquals(totalDeleteCommandNum, commandsSent.size());

    // Each command should have a non-zero replica index
    commandsSent.forEach(pair -> assertNotEquals(0,
        ((DeleteContainerCommand) pair.getValue()).getReplicaIndex()));

    // command num of each index should be equal to the excess num
    // of this index
    Map<DatanodeDetails, Integer> datanodeDetails2Index =
        availableReplicas.stream().collect(Collectors.toMap(
            ContainerReplica::getDatanodeDetails,
            ContainerReplica::getReplicaIndex));
    Map<Integer, Integer> index2commandNum = new HashMap<>();
    commandsSent.forEach(pair -> index2commandNum.merge(
        datanodeDetails2Index.get(pair.getKey()), 1, Integer::sum)
    );

    index2commandNum.keySet().forEach(i -> {
      assertThat(index2excessNum).containsKey(i);
      assertEquals(index2commandNum.get(i), index2excessNum.get(i));
    });
  }
}
