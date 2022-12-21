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
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.replicateContainerCommand;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;

/**
 * Tests the ECUnderReplicationHandling functionality.
 */
public class TestECMisReplicationHandler {
  private ECReplicationConfig repConfig;
  private ContainerInfo container;
  private NodeManager nodeManager;
  private OzoneConfiguration conf;
  private static final int DATA = 3;
  private static final int PARITY = 2;


  @BeforeEach
  public void setup() {
    nodeManager = new MockNodeManager(true, 10) {
      @Override
      public NodeStatus getNodeStatus(DatanodeDetails dd) {
        return new NodeStatus(
            dd.getPersistedOpState(), HddsProtos.NodeState.HEALTHY, 0);
      }

    };
    conf = SCMTestUtils.getConf();
    repConfig = new ECReplicationConfig(DATA, PARITY);
    container = ReplicationTestUtil
        .createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    NodeSchema[] schemas =
        new NodeSchema[] {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager.getInstance().init(schemas, true);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7})
  public void testMisReplicationWithAllNodesAvailable(int misreplicationCount)
          throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
                Pair.of(IN_SERVICE, 5));
    testMisReplication(availableReplicas, Collections.emptyList(),
            0, misreplicationCount, Math.min(misreplicationCount, 5));
  }

  @Test
  public void testMisReplicationWithNoNodesReturned() throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
            .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
                    Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
                    Pair.of(IN_SERVICE, 5));
    PlacementPolicy placementPolicy = Mockito.mock(PlacementPolicy.class);
    ContainerPlacementStatus mockedContainerPlacementStatus =
            Mockito.mock(ContainerPlacementStatus.class);
    Mockito.when(mockedContainerPlacementStatus.isPolicySatisfied())
            .thenReturn(false);
    Mockito.when(placementPolicy.validateContainerPlacement(anyList(),
                    anyInt())).thenReturn(mockedContainerPlacementStatus);
    Mockito.when(placementPolicy.chooseDatanodes(
                    Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.anyInt(), Mockito.anyLong(), Mockito.anyLong()))
            .thenThrow(new IOException("No nodes found"));
    Assertions.assertThrows(SCMException.class, () -> testMisReplication(
            availableReplicas, placementPolicy, Collections.emptyList(),
            0, 2, 0));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7})
  public void testMisReplicationWithSomeNodesNotInService(
          int misreplicationCount) throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
            .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
                    Pair.of(IN_MAINTENANCE, 3), Pair.of(IN_MAINTENANCE, 4),
                    Pair.of(IN_SERVICE, 5));
    testMisReplication(availableReplicas, Collections.emptyList(),
            0, misreplicationCount, Math.min(misreplicationCount, 3));
  }

  @Test
  public void testMisReplicationWithUndereplication() throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
            .createReplicas(Pair.of(IN_SERVICE, 2),
                    Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
                    Pair.of(IN_SERVICE, 5));
    testMisReplication(availableReplicas, Collections.emptyList(), 0, 1, 0);
  }

  @Test
  public void testMisReplicationWithOvereplication() throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
            .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 1),
                    Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
                    Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5));
    testMisReplication(availableReplicas, Collections.emptyList(), 0, 1, 0);
  }

  @Test
  public void testMisReplicationWithSatisfiedPlacementPolicy()
          throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
            .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
                    Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
                    Pair.of(IN_SERVICE, 5));
    PlacementPolicy placementPolicy = Mockito.mock(PlacementPolicy.class);
    ContainerPlacementStatus mockedContainerPlacementStatus =
            Mockito.mock(ContainerPlacementStatus.class);
    Mockito.when(mockedContainerPlacementStatus.isPolicySatisfied())
            .thenReturn(true);
    Mockito.when(placementPolicy.validateContainerPlacement(anyList(),
                    anyInt())).thenReturn(mockedContainerPlacementStatus);
    testMisReplication(availableReplicas, placementPolicy,
            Collections.emptyList(), 0, 1, 0);
  }

  @Test
  public void testMisReplicationWithPendingOps()
          throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
            .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
                    Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
                    Pair.of(IN_SERVICE, 5));
    PlacementPolicy placementPolicy = Mockito.mock(PlacementPolicy.class);
    ContainerPlacementStatus mockedContainerPlacementStatus =
            Mockito.mock(ContainerPlacementStatus.class);
    Mockito.when(mockedContainerPlacementStatus.isPolicySatisfied())
            .thenReturn(true);
    Mockito.when(placementPolicy.validateContainerPlacement(anyList(),
            anyInt())).thenReturn(mockedContainerPlacementStatus);
    List<ContainerReplicaOp> pendingOp = Collections.singletonList(
            ContainerReplicaOp.create(ContainerReplicaOp.PendingOpType.ADD,
                    MockDatanodeDetails.randomDatanodeDetails(), 1));
    testMisReplication(availableReplicas, placementPolicy,
            pendingOp, 0, 1, 0);
    pendingOp = Collections.singletonList(ContainerReplicaOp
            .create(ContainerReplicaOp.PendingOpType.DELETE, availableReplicas
                    .stream().findAny().get().getDatanodeDetails(), 1));
    testMisReplication(availableReplicas, placementPolicy,
            pendingOp, 0, 1, 0);
  }

  private void testMisReplication(Set<ContainerReplica> availableReplicas,
      List<ContainerReplicaOp> pendingOp, int maintenanceCnt,
      int misreplicationCount, int expectedNumberOfNodes) throws IOException {
    PlacementPolicy placementPolicy = Mockito.mock(PlacementPolicy.class);
    ContainerPlacementStatus mockedContainerPlacementStatus =
            Mockito.mock(ContainerPlacementStatus.class);
    Mockito.when(mockedContainerPlacementStatus.isPolicySatisfied())
            .thenReturn(false);
    Mockito.when(placementPolicy.validateContainerPlacement(anyList(),
                    anyInt())).thenReturn(mockedContainerPlacementStatus);
    testMisReplication(availableReplicas, placementPolicy, pendingOp,
            maintenanceCnt, misreplicationCount, expectedNumberOfNodes);
  }

  private void testMisReplication(Set<ContainerReplica> availableReplicas,
      PlacementPolicy mockedPlacementPolicy, List<ContainerReplicaOp> pendingOp,
      int maintenanceCnt, int misreplicationCount, int expectedNumberOfNodes)
      throws IOException {
    ECMisReplicationHandler ecMRH =
        new ECMisReplicationHandler(mockedPlacementPolicy, conf, nodeManager);

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
                    return nodeManager.getNodeStatus(r.getDatanodeDetails())
                            .isHealthy();
                  } catch (NodeNotFoundException e) {
                    throw new RuntimeException(e);
                  }
                }
                return false;
              }));

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
              Mockito.any(), Mockito.any(), Mockito.any(),
                      eq(copy.size()), Mockito.anyLong(), Mockito.anyLong()))
                      .thenAnswer(invocation -> {
                        List<DatanodeDetails> datanodeDetails =
                                invocation.getArgument(0);
                        Assertions.assertTrue(remainingReplicasAfterCopy
                                .containsAll(datanodeDetails));
                        return targetNodes;
                      });
    }
    Map<DatanodeDetails, Integer> copyReplicaIdxMap = copy.stream()
            .collect(Collectors.toMap(ContainerReplica::getDatanodeDetails,
                    ContainerReplica::getReplicaIndex));
    Map<DatanodeDetails, SCMCommand<?>> datanodeDetailsSCMCommandMap = ecMRH
        .processAndCreateCommands(availableReplicas, pendingOp,
            result, maintenanceCnt);
    Assertions.assertEquals(expectedNumberOfNodes,
            datanodeDetailsSCMCommandMap.size());
    Assertions.assertTrue(datanodeDetailsSCMCommandMap.keySet()
            .containsAll(targetNodes));
    for (SCMCommand<?> command : datanodeDetailsSCMCommandMap.values()) {
      Assertions.assertTrue(command.getType() == replicateContainerCommand);
      ReplicateContainerCommand replicateContainerCommand =
              (ReplicateContainerCommand) command;
      Assertions.assertEquals(replicateContainerCommand.getContainerID(),
              container.getContainerID());
      DatanodeDetails replicateSrcDn =
              replicateContainerCommand.getSourceDatanodes().stream()
              .findFirst().get();
      Assertions.assertTrue(copyReplicaIdxMap.containsKey(replicateSrcDn));
      Assertions.assertEquals(copyReplicaIdxMap.get(replicateSrcDn),
              replicateContainerCommand.getReplicaIndex());
    }
  }

}
