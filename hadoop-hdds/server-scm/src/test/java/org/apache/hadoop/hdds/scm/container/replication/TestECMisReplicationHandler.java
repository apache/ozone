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
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;

/**
 * Tests the ECMisReplicationHandling functionality.
 */
public class TestECMisReplicationHandler extends TestMisReplicationHandler {
  private static final int DATA = 3;
  private static final int PARITY = 2;


  @BeforeEach
  public void setup() {
    ECReplicationConfig repConfig = new ECReplicationConfig(DATA, PARITY);
    setup(repConfig);
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

  @Override
  protected MisReplicationHandler getMisreplicationHandler(
          PlacementPolicy placementPolicy, OzoneConfiguration conf,
          NodeManager nodeManager) {
    return new ECMisReplicationHandler(placementPolicy, conf, nodeManager);
  }
}
