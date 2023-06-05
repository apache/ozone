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
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.replicateContainerCommand;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;

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

  protected void setup(ReplicationConfig repConfig)
      throws NodeNotFoundException, CommandTargetOverloadedException,
      NotLeaderException {
    conf = SCMTestUtils.getConf();

    replicationManager = Mockito.mock(ReplicationManager.class);
    Mockito.when(replicationManager.getNodeStatus(any(DatanodeDetails.class)))
        .thenAnswer(invocation -> {
          DatanodeDetails dd = invocation.getArgument(0);
          return new NodeStatus(dd.getPersistedOpState(),
              HddsProtos.NodeState.HEALTHY, 0);
        });
    ReplicationManagerConfiguration rmConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    Mockito.when(replicationManager.getConfig())
        .thenReturn(rmConf);

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

  protected void setThrowThrottledException(boolean showThrow) {
    throwThrottledException.set(showThrow);
  }

  static PlacementPolicy mockPlacementPolicy() {
    PlacementPolicy placementPolicy = Mockito.mock(PlacementPolicy.class);
    ContainerPlacementStatus mockedContainerPlacementStatus =
        Mockito.mock(ContainerPlacementStatus.class);
    Mockito.when(mockedContainerPlacementStatus.isPolicySatisfied())
        .thenReturn(false);
    Mockito.when(placementPolicy.validateContainerPlacement(anyList(),
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
            Mockito.mock(ContainerHealthResult.MisReplicatedHealthResult.class);
    Mockito.when(result.isReplicatedOkAfterPending()).thenReturn(false);
    Mockito.when(result.getContainerInfo()).thenReturn(container);
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
    Mockito.when(mockedPlacementPolicy.replicasToCopyToFixMisreplication(
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
      Mockito.when(mockedPlacementPolicy.chooseDatanodes(
                      any(), any(), any(),
                      eq(copy.size()), Mockito.anyLong(), Mockito.anyLong()))
              .thenAnswer(invocation -> {
                List<DatanodeDetails> datanodeDetails =
                        invocation.getArgument(0);
                Assertions.assertTrue(remainingReplicasAfterCopy
                        .containsAll(datanodeDetails));
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
      Assertions.assertEquals(expectedNumberOfCommands, commandsSent.size());
      for (Pair<DatanodeDetails, SCMCommand<?>> pair : commandsSent) {
        SCMCommand<?> command = pair.getValue();
        Assertions.assertSame(replicateContainerCommand, command.getType());
        ReplicateContainerCommand replicateContainerCommand =
            (ReplicateContainerCommand) command;
        Assertions.assertEquals(replicateContainerCommand.getContainerID(),
            container.getContainerID());
        DatanodeDetails replicateSrcDn = pair.getKey();
        DatanodeDetails target = replicateContainerCommand.getTargetDatanode();
        Assertions.assertTrue(sourceDns.contains(replicateSrcDn));
        Assertions.assertTrue(targetNodes.contains(target));
        int replicaIndex = replicateContainerCommand.getReplicaIndex();
        assertReplicaIndex(replicaIndexMap, replicateSrcDn, replicaIndex);
      }
    }
  }

  protected abstract void assertReplicaIndex(
      Map<DatanodeDetails, Integer> expectedReplicaIndexes,
      DatanodeDetails sourceDatanode, int actualReplicaIndex);
}
