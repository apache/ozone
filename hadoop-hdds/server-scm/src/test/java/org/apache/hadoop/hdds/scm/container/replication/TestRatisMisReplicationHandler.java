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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests the RatisReplicationHandling functionality.
 */
public class TestRatisMisReplicationHandler extends TestMisReplicationHandler {

  @BeforeEach
  void setup(@TempDir File testDir) throws NodeNotFoundException,
      CommandTargetOverloadedException, NotLeaderException {
    RatisReplicationConfig repConfig = RatisReplicationConfig
            .getInstance(ReplicationFactor.THREE);
    setup(repConfig, testDir);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7})
  public void testMisReplicationWithAllNodesAvailable(int misreplicationCount)
          throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0),
            Pair.of(IN_SERVICE, 0));
    testMisReplication(availableReplicas, Collections.emptyList(),
            0, misreplicationCount, Math.min(misreplicationCount, 3));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7})
  public void testMisReplicationWithAllNodesAvailableQuasiClosed(
      int misreplicationCount) throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(State.QUASI_CLOSED, Pair.of(IN_SERVICE, 0),
            Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0));
    testMisReplication(availableReplicas, Collections.emptyList(),
        0, misreplicationCount, Math.min(misreplicationCount, 3));
  }

  @Test
  public void testMisReplicationWithNoNodesReturned() throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
            .createReplicas(Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0),
                    Pair.of(IN_SERVICE, 0));
    PlacementPolicy placementPolicy = mock(PlacementPolicy.class);
    ContainerPlacementStatus mockedContainerPlacementStatus = mock(ContainerPlacementStatus.class);
    when(mockedContainerPlacementStatus.isPolicySatisfied()).thenReturn(false);
    when(placementPolicy.validateContainerPlacement(anyList(),
                    anyInt())).thenReturn(mockedContainerPlacementStatus);
    when(placementPolicy.chooseDatanodes(
                    any(), any(), any(),
                    anyInt(), anyLong(), anyLong()))
            .thenThrow(new IOException("No nodes found"));
    assertThrows(SCMException.class, () -> testMisReplication(
            availableReplicas, placementPolicy, Collections.emptyList(),
            0, 2, 0));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7})
  public void testMisReplicationWithSomeNodesNotInService(
          int misreplicationCount) throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
            .createReplicas(Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0),
                    Pair.of(IN_MAINTENANCE, 0));
    testMisReplication(availableReplicas, Collections.emptyList(),
            0, misreplicationCount, Math.min(misreplicationCount, 2));
  }

  @Test
  public void testMisReplicationWithUndereplication() throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
            .createReplicas(Pair.of(IN_SERVICE, 0),
                    Pair.of(IN_SERVICE, 0));
    testMisReplication(availableReplicas, Collections.emptyList(), 0, 1, 0);
  }

  @Test
  public void testMisReplicationWithOvereplication() throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
            .createReplicas(Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0),
                    Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0));
    testMisReplication(availableReplicas, Collections.emptyList(), 0, 1, 0);
  }

  @Test
  public void testMisReplicationWithSatisfiedPlacementPolicy()
          throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
            .createReplicas(Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0),
                    Pair.of(IN_SERVICE, 0));
    PlacementPolicy placementPolicy = mock(PlacementPolicy.class);
    ContainerPlacementStatus mockedContainerPlacementStatus = mock(ContainerPlacementStatus.class);
    when(mockedContainerPlacementStatus.isPolicySatisfied()).thenReturn(true);
    when(placementPolicy.validateContainerPlacement(anyList(),
                    anyInt())).thenReturn(mockedContainerPlacementStatus);
    testMisReplication(availableReplicas, placementPolicy,
            Collections.emptyList(), 0, 1, 0);
  }

  @Test
  public void testMisReplicationWithPendingOps()
          throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
            .createReplicas(Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0),
                    Pair.of(IN_SERVICE, 0));
    PlacementPolicy placementPolicy = mock(PlacementPolicy.class);
    ContainerPlacementStatus mockedContainerPlacementStatus = mock(ContainerPlacementStatus.class);
    when(mockedContainerPlacementStatus.isPolicySatisfied()).thenReturn(true);
    when(placementPolicy.validateContainerPlacement(anyList(),
            anyInt())).thenReturn(mockedContainerPlacementStatus);
    List<ContainerReplicaOp> pendingOp = Collections.singletonList(
            new ContainerReplicaOp(ContainerReplicaOp.PendingOpType.ADD,
                    MockDatanodeDetails.randomDatanodeDetails(), 0, null, Long.MAX_VALUE, 0));
    testMisReplication(availableReplicas, placementPolicy,
            pendingOp, 0, 1, 0);
    pendingOp = Collections.singletonList(new ContainerReplicaOp(
            ContainerReplicaOp.PendingOpType.DELETE, availableReplicas
                    .stream().findAny().get().getDatanodeDetails(), 0, null, Long.MAX_VALUE, 0));
    testMisReplication(availableReplicas, placementPolicy,
            pendingOp, 0, 1, 0);
  }

  @Test
  public void testAllSourcesOverloaded() throws IOException {
    ReplicationManager replicationManager = getReplicationManager();
    doThrow(new CommandTargetOverloadedException("Overloaded"))
        .when(replicationManager).sendThrottledReplicationCommand(any(),
            anyList(), any(), anyInt());

    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0),
            Pair.of(IN_SERVICE, 0));
    assertThrows(CommandTargetOverloadedException.class,
        () -> testMisReplication(availableReplicas, mockPlacementPolicy(),
            Collections.emptyList(), 0, 1, 1, 0));
  }

  @Override
  protected MisReplicationHandler getMisreplicationHandler(
          PlacementPolicy placementPolicy, OzoneConfiguration conf,
          ReplicationManager replicationManager) {
    return new RatisMisReplicationHandler(placementPolicy, conf,
        replicationManager);
  }

  @Override
  protected void assertReplicaIndex(
      Map<DatanodeDetails, Integer> expectedReplicaIndexes,
      DatanodeDetails sourceDatanode, int actualReplicaIndex) {
    assertEquals(0, actualReplicaIndex);
  }
}
