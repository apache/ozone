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

package org.apache.hadoop.ozone.recon.fsck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerChecksums;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test to ensure the correct container state is return by a
 * ContainerHealthStatus instance.
 */
public class TestContainerHealthStatus {

  private PlacementPolicy placementPolicy;
  private ContainerInfo container;
  private ReconContainerMetadataManager reconContainerMetadataManager;
  private static final OzoneConfiguration CONF = new OzoneConfiguration();

  private static Stream<Arguments> outOfServiceNodeStates() {
    return Stream.of(
        Arguments.of(HddsProtos.NodeOperationalState.DECOMMISSIONING),
        Arguments.of(HddsProtos.NodeOperationalState.DECOMMISSIONED),
        Arguments.of(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE),
        Arguments.of(HddsProtos.NodeOperationalState.IN_MAINTENANCE)
    );
  }

  @BeforeEach
  public void setup() {
    placementPolicy = mock(PlacementPolicy.class);
    container = mock(ContainerInfo.class);
    reconContainerMetadataManager = mock(ReconContainerMetadataManager.class);
    when(container.getReplicationFactor())
        .thenReturn(HddsProtos.ReplicationFactor.THREE);
    when(container.getReplicationConfig())
        .thenReturn(RatisReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.THREE));
    when(container.getState()).thenReturn(HddsProtos.LifeCycleState.CLOSED);
    when(container.containerID()).thenReturn(ContainerID.valueOf(123456));
    when(container.getContainerID()).thenReturn((long)123456);
    when(placementPolicy.validateContainerPlacement(
        anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(1, 1, 1));
  }

  @Test
  public void testHealthyContainer() {
    Set<ContainerReplica> replicas = generateReplicas(container,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED);
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);
    assertTrue(status.isHealthilyReplicated());
    assertFalse(status.isOverReplicated());
    assertFalse(status.isUnderReplicated());
    assertEquals(0, status.replicaDelta());
    assertFalse(status.isMissing());
    assertFalse(status.isMisReplicated());
    assertEquals(0, status.misReplicatedDelta());

    assertEquals(container, status.getContainer());
    assertEquals((long)123456, status.getContainerID());
    assertEquals(3, status.getReplicationFactor());
    assertEquals(3, status.getReplicaCount());
  }

  @Test
  public void testHealthyContainerWithExtraUnhealthyReplica() {
    Set<ContainerReplica> replicas = generateReplicas(container,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.UNHEALTHY);
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);
    assertTrue(status.isHealthilyReplicated());
    assertFalse(status.isOverReplicated());
    assertFalse(status.isUnderReplicated());
    assertEquals(0, status.replicaDelta());
    assertFalse(status.isMissing());
    assertFalse(status.isMisReplicated());
    assertEquals(0, status.misReplicatedDelta());
  }

  @Test
  public void testMissingContainer() {
    Set<ContainerReplica> replicas = new HashSet<>();
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);
    assertFalse(status.isHealthilyReplicated());
    assertFalse(status.isOverReplicated());
    assertFalse(status.isUnderReplicated());
    assertEquals(3, status.replicaDelta());
    assertTrue(status.isMissing());
    assertFalse(status.isMisReplicated());
    assertEquals(0, status.misReplicatedDelta());
  }

  @Test
  public void testUnderReplicatedContainer() {
    Set<ContainerReplica> replicas = generateReplicas(container,
        ContainerReplicaProto.State.CLOSED);
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);
    assertFalse(status.isHealthilyReplicated());
    assertFalse(status.isMissing());
    assertFalse(status.isOverReplicated());
    assertTrue(status.isUnderReplicated());
    assertEquals(2, status.replicaDelta());
    assertFalse(status.isMisReplicated());
    assertEquals(0, status.misReplicatedDelta());
  }

  @Test
  public void testOverReplicatedContainer() {
    Set<ContainerReplica> replicas = generateReplicas(container,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED);
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);
    assertFalse(status.isHealthilyReplicated());
    assertFalse(status.isMissing());
    assertFalse(status.isUnderReplicated());
    assertTrue(status.isOverReplicated());
    assertEquals(-1, status.replicaDelta());
    assertFalse(status.isMisReplicated());
    assertEquals(0, status.misReplicatedDelta());
  }

  @Test
  public void testSameDataChecksumContainer() {
    Set<ContainerReplica> replicas = generateReplicas(container,
            ContainerReplicaProto.State.CLOSED,
            ContainerReplicaProto.State.CLOSED,
            ContainerReplicaProto.State.CLOSED);
    ContainerHealthStatus status =
            new ContainerHealthStatus(container, replicas, placementPolicy,
                    reconContainerMetadataManager, CONF);
    assertTrue(status.isHealthilyReplicated());
    assertFalse(status.isMissing());
    assertFalse(status.isUnderReplicated());
    assertFalse(status.isOverReplicated());
    assertFalse(status.isMisReplicated());
    assertFalse(status.areChecksumsMismatched());
  }

  @Test
  public void testDataChecksumMismatchContainer() {
    Set<ContainerReplica> replicas = generateMismatchedReplicas(container,
            ContainerReplicaProto.State.CLOSED,
            ContainerReplicaProto.State.CLOSED,
            ContainerReplicaProto.State.CLOSED);
    ContainerHealthStatus status =
            new ContainerHealthStatus(container, replicas, placementPolicy,
                    reconContainerMetadataManager, CONF);
    assertTrue(status.isHealthilyReplicated());
    assertFalse(status.isMissing());
    assertFalse(status.isUnderReplicated());
    assertFalse(status.isOverReplicated());
    assertFalse(status.isMisReplicated());
    assertTrue(status.areChecksumsMismatched());
  }

  /**
   * Starting with a ContainerHealthStatus of 1 over-replicated container
   * replica and then updating a datanode to one of the out-of-service states.
   * Replicas belonging to out-of-service nodes should be ignored and
   * the container should be considered properly replicated.
   */
  @ParameterizedTest
  @MethodSource("outOfServiceNodeStates")
  public void testOverReplicationWithOutOfServiceNodes(
      HddsProtos.NodeOperationalState state) {
    Set<ContainerReplica> replicas = generateReplicas(container,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED);
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);
    assertFalse(status.isHealthilyReplicated());
    assertFalse(status.isMissing());
    assertFalse(status.isUnderReplicated());
    assertFalse(status.isMisReplicated());
    assertTrue(status.isOverReplicated());

    for (ContainerReplica replica : replicas) {
      replicas.remove(replica);
      replica.getDatanodeDetails().setPersistedOpState(state);
      replicas.add(replica);
      break;
    }

    status = new ContainerHealthStatus(container, replicas, placementPolicy,
        reconContainerMetadataManager, CONF);
    assertTrue(status.isHealthilyReplicated());
    assertFalse(status.isMissing());
    assertFalse(status.isUnderReplicated());
    assertFalse(status.isMisReplicated());
    assertFalse(status.isOverReplicated());
  }

  /**
   * Nodes in Decommission aren't expected to come back.
   * If 1/3 nodes goes into decommission, the container is
   * considered under-replicated. If 1/3 nodes goes into maintenance,
   * because the node is expected to come back and there are
   * 2 available replicas (minimum required num for Ratis THREE)
   * the container isn't considered under-replicated.
   */
  @ParameterizedTest
  @MethodSource("outOfServiceNodeStates")
  public void testUnderReplicationWithOutOfServiceNodes(
      HddsProtos.NodeOperationalState state) {
    Set<ContainerReplica> replicas = generateReplicas(container,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED);
    // IN_SERVICE, IN_SERVICE, IN_SERVICE
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);
    assertTrue(status.isHealthy());
    assertTrue(status.isSufficientlyReplicated());
    assertTrue(status.isHealthilyReplicated());
    assertFalse(status.isMissing());
    assertFalse(status.isUnderReplicated());
    assertFalse(status.isMisReplicated());
    assertFalse(status.isOverReplicated());

    for (ContainerReplica replica : replicas) {
      replicas.remove(replica);
      replica.getDatanodeDetails().setPersistedOpState(state);
      replicas.add(replica);
      break;
    }

    // IN_SERVICE, IN_SERVICE, DECOMMISSION/MAINTENANCE
    status = new ContainerHealthStatus(container, replicas, placementPolicy,
        reconContainerMetadataManager, CONF);
    assertTrue(status.isHealthy());
    assertFalse(status.isHealthilyReplicated());
    assertFalse(status.isMissing());
    assertFalse(status.isMisReplicated());
    assertFalse(status.isOverReplicated());

    if (state.equals(HddsProtos.NodeOperationalState.DECOMMISSIONING) ||
        state.equals(HddsProtos.NodeOperationalState.DECOMMISSIONED)) {
      assertFalse(status.isSufficientlyReplicated());
      assertTrue(status.isUnderReplicated());
    } else {
      assertTrue(status.isSufficientlyReplicated());
      assertFalse(status.isUnderReplicated());
    }
  }

  /**
   * Starting with a healthy ContainerHealthStatus and then updating
   * a datanode to a maintenance state.
   * Any node in maintenance is expected to come back and since 2 replicas
   * in online nodes are meeting the minimum requirement for
   * proper replication, no additional replica-copy is made.
   *
   * IN_SERVICE, IN_SERVICE, IN_MAINTENANCE
   *
   * If 1 more node goes into maintenance, then 1 replica copy is made to
   * maintain the minimum requirement for proper replication.
   *
   * IN_SERVICE, IN_SERVICE, IN_MAINTENANCE, IN_MAINTENANCE
   *
   * Before the copy is made we have
   *
   * IN_SERVICE, IN_MAINTENANCE, ENTERING_MAINTENANCE
   *
   * for that short time, the container is under-replicated.
   *
   * When the copy is made, the container is again considered
   * sufficiently replicated.
   */
  @Test
  public void testReplicationWithNodesInMaintenance() {
    Set<ContainerReplica> replicas = generateReplicas(container,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED);
    // IN_SERVICE, IN_SERVICE, IN_SERVICE
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);
    assertTrue(status.isHealthy());
    assertTrue(status.isSufficientlyReplicated());
    assertTrue(status.isHealthilyReplicated());
    assertFalse(status.isMissing());
    assertFalse(status.isUnderReplicated());
    assertFalse(status.isMisReplicated());
    assertFalse(status.isOverReplicated());

    // 1/3 replicas goes into maintenance
    // IN_SERVICE, IN_SERVICE, IN_MAINTENANCE
    for (ContainerReplica replica : replicas) {
      replicas.remove(replica);
      replica.getDatanodeDetails().setPersistedOpState(
          HddsProtos.NodeOperationalState.IN_MAINTENANCE);
      replicas.add(replica);
      break;
    }

    status = new ContainerHealthStatus(container, replicas, placementPolicy,
        reconContainerMetadataManager, CONF);
    assertTrue(status.isHealthy());
    assertTrue(status.isSufficientlyReplicated());
    assertFalse(status.isHealthilyReplicated());
    assertFalse(status.isMissing());
    assertFalse(status.isUnderReplicated());
    assertFalse(status.isMisReplicated());
    assertFalse(status.isOverReplicated());

    // IN_SERVICE, IN_MAINTENANCE, ENTERING_MAINTENANCE
    for (ContainerReplica replica : replicas) {
      if (replica.getDatanodeDetails().getPersistedOpState().equals(
          HddsProtos.NodeOperationalState.IN_SERVICE)) {
        replicas.remove(replica);
        replica.getDatanodeDetails().setPersistedOpState(
            HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE);
        replicas.add(replica);
        break;
      }
    }

    // Container should be under-replicated.
    status = new ContainerHealthStatus(container, replicas, placementPolicy,
        reconContainerMetadataManager, CONF);
    assertTrue(status.isHealthy());
    assertFalse(status.isSufficientlyReplicated());
    assertFalse(status.isHealthilyReplicated());
    assertFalse(status.isMissing());
    assertTrue(status.isUnderReplicated());
    assertFalse(status.isMisReplicated());
    assertFalse(status.isOverReplicated());
  }

  @Test
  public void testMisReplicated() {
    Set<ContainerReplica> replicas = generateReplicas(container,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED);
    when(placementPolicy.validateContainerPlacement(
        anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(1, 2, 5));
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);
    assertFalse(status.isHealthilyReplicated());
    assertFalse(status.isMissing());
    assertFalse(status.isUnderReplicated());
    assertFalse(status.isOverReplicated());
    assertEquals(0, status.replicaDelta());
    assertTrue(status.isMisReplicated());
    assertEquals(1, status.misReplicatedDelta());
  }

  private Set<ContainerReplica> generateReplicas(ContainerInfo cont,
      ContainerReplicaProto.State...states) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (ContainerReplicaProto.State s : states) {
      replicas.add(new ContainerReplica.ContainerReplicaBuilder()
          .setContainerID(cont.containerID())
          .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails())
          .setChecksums(ContainerChecksums.of(1234L, 0L))
          .setContainerState(s)
          .build());
    }
    return replicas;
  }

  private Set<ContainerReplica> generateMismatchedReplicas(ContainerInfo cont,
                                                           ContainerReplicaProto.State...states) {
    Set<ContainerReplica> replicas = new HashSet<>();
    long checksum = 1234L;
    for (ContainerReplicaProto.State s : states) {
      replicas.add(new ContainerReplica.ContainerReplicaBuilder()
          .setContainerID(cont.containerID())
          .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails())
          .setContainerState(s)
          .setChecksums(ContainerChecksums.of(checksum, 0L))
          .build());
      checksum++;
    }
    return replicas;
  }
}
