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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.replicateContainerCommand;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;

/**
 * Tests the MisReplicationHandling functionalities to test implementations.
 */
public abstract class TestMisReplicationHandler {

  private ContainerInfo container;
  private OzoneConfiguration conf;
  private ReplicationManager replicationManager;
  private Set<Pair<DatanodeDetails, SCMCommand<?>>> commandsSent;
  private final AtomicBoolean throwThrottledException =
      new AtomicBoolean(false);
  private ReplicationManagerMetrics metrics;

  protected void setup(ReplicationConfig repConfig, File testDir)
      throws NodeNotFoundException, CommandTargetOverloadedException,
      NotLeaderException {
    conf = SCMTestUtils.getConf(testDir);

    replicationManager = mock(ReplicationManager.class);
    when(replicationManager.getNodeStatus(any(DatanodeDetails.class)))
        .thenAnswer(invocation -> {
          DatanodeDetails dd = invocation.getArgument(0);
          return NodeStatus.valueOf(dd.getPersistedOpState(),
              HddsProtos.NodeState.HEALTHY);
        });
    ReplicationManagerConfiguration rmConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    when(replicationManager.getConfig()).thenReturn(rmConf);
    metrics = ReplicationManagerMetrics.create(replicationManager);
    when(replicationManager.getMetrics()).thenReturn(metrics);
    when(replicationManager.getContainerReplicaPendingOps()).thenReturn(mock(ContainerReplicaPendingOps.class));

    commandsSent = new HashSet<>();
    ReplicationTestUtil.mockRMSendDatanodeCommand(
        replicationManager, commandsSent);
    ReplicationTestUtil.mockRMSendThrottleReplicateCommand(
        replicationManager, commandsSent, throwThrottledException);

    container = ReplicationTestUtil
            .createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    NodeSchema[] schemas =
            new NodeSchema[] {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager.getInstance().init(schemas, true);
  }

  protected ReplicationManager getReplicationManager() {
    return replicationManager;
  }

  protected ReplicationManagerMetrics getMetrics() {
    return metrics;
  }

  protected void setThrowThrottledException(boolean showThrow) {
    throwThrottledException.set(showThrow);
  }

  static PlacementPolicy mockPlacementPolicy() {
    PlacementPolicy placementPolicy = mock(PlacementPolicy.class);
    ContainerPlacementStatus mockedContainerPlacementStatus = mock(ContainerPlacementStatus.class);
    when(mockedContainerPlacementStatus.isPolicySatisfied()).thenReturn(false);
    when(placementPolicy.validateContainerPlacement(anyList(),
        anyInt())).thenReturn(mockedContainerPlacementStatus);
    return placementPolicy;
  }

  protected abstract MisReplicationHandler getMisreplicationHandler(
          PlacementPolicy placementPolicy, OzoneConfiguration configuration,
          ReplicationManager rm);

  protected void testMisReplication(Set<ContainerReplica> availableReplicas,
                                  List<ContainerReplicaOp> pendingOp,
                                  int maintenanceCnt, int misreplicationCount,
                                    int expectedNumberOfNodes)
          throws IOException {
    testMisReplication(availableReplicas, mockPlacementPolicy(), pendingOp,
            maintenanceCnt, misreplicationCount, expectedNumberOfNodes);
  }

  protected void testMisReplication(Set<ContainerReplica> availableReplicas,
      PlacementPolicy mockedPlacementPolicy,
      List<ContainerReplicaOp> pendingOp,
      int maintenanceCnt, int misreplicationCount,
      int expectedNumberOfNodes)
      throws IOException {
    testMisReplication(availableReplicas, mockedPlacementPolicy, pendingOp,
        maintenanceCnt, misreplicationCount, expectedNumberOfNodes,
        expectedNumberOfNodes);
  }

  protected void testMisReplication(Set<ContainerReplica> availableReplicas,
      PlacementPolicy mockedPlacementPolicy,
      List<ContainerReplicaOp> pendingOp,
      int maintenanceCnt, int misreplicationCount,
      int expectedNumberOfNodes,
      int expectedNumberOfCommands
  ) throws IOException {
    MisReplicationHandler misReplicationHandler = getMisreplicationHandler(
        mockedPlacementPolicy, conf, replicationManager);

    ContainerHealthResult.MisReplicatedHealthResult result =
        mock(ContainerHealthResult.MisReplicatedHealthResult.class);
    when(result.isReplicatedOkAfterPending()).thenReturn(false);
    when(result.getContainerInfo()).thenReturn(container);
    Map<ContainerReplica, Boolean> sources = availableReplicas.stream()
            .collect(Collectors.toMap(Function.identity(),
                    r -> {
                      if (r.getDatanodeDetails().getPersistedOpState()
                              == IN_SERVICE) {
                        try {
                          return replicationManager.getNodeStatus(
                                  r.getDatanodeDetails()).isHealthy();
                        } catch (NodeNotFoundException e) {
                          throw new RuntimeException(e);
                        }
                      }
                      return false;
                    }));

    Set<DatanodeDetails> sourceDns = sources.entrySet().stream()
        .filter(Map.Entry::getValue)
        .map(Map.Entry::getKey)
        .map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toSet());
    Set<ContainerReplica> copy = sources.entrySet().stream()
            .filter(Map.Entry::getValue).limit(misreplicationCount)
            .map(Map.Entry::getKey).collect(Collectors.toSet());
    when(mockedPlacementPolicy.replicasToCopyToFixMisreplication(
            anyMap())).thenAnswer(invocation -> copy);
    Set<DatanodeDetails> remainingReplicasAfterCopy =
            availableReplicas.stream().filter(r -> !copy.contains(r))
                    .map(ContainerReplica::getDatanodeDetails)
                    .collect(Collectors.toSet());
    List<DatanodeDetails> targetNodes =
            IntStream.range(0, expectedNumberOfNodes)
                    .mapToObj(i -> MockDatanodeDetails.randomDatanodeDetails())
                    .collect(Collectors.toList());
    if (expectedNumberOfNodes > 0) {
      when(mockedPlacementPolicy.chooseDatanodes(
                      any(), any(), any(),
                      eq(copy.size()), anyLong(), anyLong()))
              .thenAnswer(invocation -> {
                List<DatanodeDetails> datanodeDetails =
                        invocation.getArgument(0);
                assertThat(remainingReplicasAfterCopy)
                    .containsAll(datanodeDetails);
                return targetNodes;
              });
    }
    Map<DatanodeDetails, Integer> replicaIndexMap = copy.stream()
            .collect(Collectors.toMap(ContainerReplica::getDatanodeDetails,
                    ContainerReplica::getReplicaIndex));
    try {
      misReplicationHandler.processAndSendCommands(availableReplicas,
          pendingOp, result, maintenanceCnt);
    } finally {
      assertEquals(expectedNumberOfCommands, commandsSent.size());
      for (Pair<DatanodeDetails, SCMCommand<?>> pair : commandsSent) {
        SCMCommand<?> command = pair.getValue();
        assertSame(replicateContainerCommand, command.getType());
        ReplicateContainerCommand replicateContainerCommand =
            (ReplicateContainerCommand) command;
        assertEquals(replicateContainerCommand.getContainerID(),
            container.getContainerID());
        DatanodeDetails replicateSrcDn = pair.getKey();
        DatanodeDetails target = replicateContainerCommand.getTargetDatanode();
        assertThat(sourceDns).contains(replicateSrcDn);
        assertThat(targetNodes).contains(target);
        int replicaIndex = replicateContainerCommand.getReplicaIndex();
        assertReplicaIndex(replicaIndexMap, replicateSrcDn, replicaIndex);
      }
    }
  }

  protected abstract void assertReplicaIndex(
      Map<DatanodeDetails, Integer> expectedReplicaIndexes,
      DatanodeDetails sourceDatanode, int actualReplicaIndex);
}
