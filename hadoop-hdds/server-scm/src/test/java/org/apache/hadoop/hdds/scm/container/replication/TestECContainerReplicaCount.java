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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.UNHEALTHY;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerReplica;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for EcContainerReplicaCounts.
 */
public class TestECContainerReplicaCount {

  private ECReplicationConfig repConfig;
  private ContainerInfo container;

  @BeforeEach
  public void setup() {
    repConfig = new ECReplicationConfig(3, 2);
    container = ReplicationTestUtil
        .createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
  }

  @Test
  public void testPerfectlyReplicatedContainer() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5));
    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica,
            Collections.emptyList(), 1);
    assertTrue(rcnt.isSufficientlyReplicated(false));
    assertFalse(rcnt.isUnrecoverable());
  }

  @Test
  public void testContainerMissingReplica() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4));
    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica,
            Collections.emptyList(), 1);
    assertFalse(rcnt.isSufficientlyReplicated(false));
    assertEquals(1, rcnt.unavailableIndexes(true).size());
    assertEquals(5, rcnt.unavailableIndexes(true).get(0).intValue());

    // Add a pending add op for the missing replica and ensure it no longer
    // appears missing
    ContainerReplicaOp op = new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.ADD,
        MockDatanodeDetails.randomDatanodeDetails(), 5, null, Long.MAX_VALUE, 0);
    rcnt.addPendingOp(op);
    assertTrue(rcnt.isSufficientlyReplicated(true));
    assertEquals(0, rcnt.unavailableIndexes(true).size());
  }

  @Test
  public void testContainerMissingReplicaDueToPendingDelete() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5));

    List<ContainerReplicaOp> pending =
        getContainerReplicaOps(ImmutableList.of(), ImmutableList.of(1));
    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica, pending, 1);
    assertFalse(rcnt.isSufficientlyReplicated(false));
    assertEquals(1, rcnt.unavailableIndexes(true).size());
    assertEquals(1, rcnt.unavailableIndexes(true).get(0).intValue());
  }

  @Test
  public void testUnderReplicationDueToUnhealthyReplica() {
    Set<ContainerReplica> replicas =
        ReplicationTestUtil.createReplicas(container.containerID(),
            CLOSED, 1, 2, 3, 4);
    ContainerReplica unhealthyIndex5 =
        createContainerReplica(container.containerID(), 5,
            IN_SERVICE, ContainerReplicaProto.State.UNHEALTHY);
    replicas.add(unhealthyIndex5);

    List<ContainerReplicaOp> pending =
        getContainerReplicaOps(ImmutableList.of(5), ImmutableList.of());
    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replicas, pending, 1);

    assertFalse(rcnt.isSufficientlyReplicated(false));
    assertTrue(rcnt.isSufficientlyReplicated(true));
    assertEquals(1, rcnt.unavailableIndexes(false).size());
    assertEquals(5, rcnt.unavailableIndexes(false).get(0).intValue());
    assertEquals(0, rcnt.unavailableIndexes(true).size());
  }

  @Test
  public void testContainerExcessReplicasAndPendingDelete() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5), Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2));
    List<ContainerReplicaOp> pending =
        getContainerReplicaOps(ImmutableList.of(), ImmutableList.of(1, 2));

    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica, pending, 1);
    assertTrue(rcnt.isSufficientlyReplicated(false));
    assertFalse(rcnt.isOverReplicated(true));
  }

  @Test
  public void testUnderRepContainerWithExcessReplicasAndPendingDelete() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5), Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2));

    List<ContainerReplicaOp> pending =
        getContainerReplicaOps(ImmutableList.of(), ImmutableList.of(1, 2, 2));
    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica, pending, 1);
    assertFalse(rcnt.isSufficientlyReplicated(false));
    assertEquals(1, rcnt.unavailableIndexes(true).size());
    assertEquals(2,
        rcnt.unavailableIndexes(true).get(0).intValue());
  }

  @Test
  public void testContainerWithMaintenanceReplicasSufficientlyReplicated() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_MAINTENANCE, 4),
            Pair.of(IN_MAINTENANCE, 5));
    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica,
            Collections.emptyList(), 0);
    assertTrue(rcnt.isSufficientlyReplicated(false));
    rcnt = new ECContainerReplicaCount(container, replica,
        Collections.emptyList(), 1);
    assertFalse(rcnt.isSufficientlyReplicated(false));
    List<ContainerReplicaOp> pendingOps =
        getContainerReplicaOps(ImmutableList.of(), ImmutableList.of(1));
    rcnt = new ECContainerReplicaCount(container, replica, pendingOps, 0);
    assertFalse(rcnt.isSufficientlyReplicated(false));
  }

  @Test
  public void testOverReplicatedContainer() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5), Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2));

    List<ContainerReplicaOp> pending =
        getContainerReplicaOps(ImmutableList.of(), ImmutableList.of(1));

    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica, pending, 1);
    assertTrue(rcnt.isSufficientlyReplicated(false));
    assertTrue(rcnt.isOverReplicated(true));
    assertEquals(2, rcnt.overReplicatedIndexes(true).get(0).intValue());
    assertEquals(1, rcnt.overReplicatedIndexes(true).size());
    assertTrue(rcnt.isOverReplicated(false));
    assertEquals(2, rcnt.overReplicatedIndexes(false).size());

    // Add a pending delete op for the excess replica and ensure it now reports
    // as not over replicated.
    rcnt.addPendingOp(new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.DELETE,
        MockDatanodeDetails.randomDatanodeDetails(), 2, null, Long.MAX_VALUE, 0));
    assertFalse(rcnt.isOverReplicated(true));
  }

  @Test
  public void testOverReplicatedContainerFixedWithPendingDelete() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5), Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2));

    List<ContainerReplicaOp> pending =
        getContainerReplicaOps(ImmutableList.of(), ImmutableList.of(1));
    pending.add(new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.DELETE,
        MockDatanodeDetails.randomDatanodeDetails(), 2, null, Long.MAX_VALUE, 0));

    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica, pending, 1);
    assertTrue(rcnt.isSufficientlyReplicated(false));
    assertFalse(rcnt.isOverReplicated(true));
    assertEquals(0, rcnt.overReplicatedIndexes(true).size());
    assertTrue(rcnt.isOverReplicated(false));
    assertEquals(2, rcnt.overReplicatedIndexes(false).size());
  }

  @Test
  public void testOverReplicatedAndUnderReplicatedContainer() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5), Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2));
    // this copy of index 4 is unhealthy, so it should not cause over
    // replication of index 4
    ContainerReplica unhealthyIndex4 =
        createContainerReplica(container.containerID(), 4,
            IN_SERVICE, ContainerReplicaProto.State.UNHEALTHY);
    replica.add(unhealthyIndex4);

    List<ContainerReplicaOp> pending =
        getContainerReplicaOps(ImmutableList.of(), ImmutableList.of(1));

    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica, pending, 1);
    assertFalse(rcnt.isSufficientlyReplicated(false));
    assertTrue(rcnt.isOverReplicated(true));
    assertEquals(2, rcnt.overReplicatedIndexes(true).get(0).intValue());
  }

  @Test
  public void testAdditionalMaintenanceCopiesAllMaintenance() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_MAINTENANCE, 1),
            Pair.of(ENTERING_MAINTENANCE, 2), Pair.of(IN_MAINTENANCE, 3),
            Pair.of(IN_MAINTENANCE, 4), Pair.of(IN_MAINTENANCE, 5),
            Pair.of(IN_MAINTENANCE, 1));
    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica,
            Collections.emptyList(), 1);
    assertFalse(rcnt.isSufficientlyReplicated(false));
    assertFalse(rcnt.isOverReplicated(true));
    assertEquals(4, rcnt.additionalMaintenanceCopiesNeeded(false));
    Set<Integer> maintenanceOnly = rcnt.maintenanceOnlyIndexes(false);
    for (int i = 1; i <= repConfig.getRequiredNodes(); i++) {
      assertThat(maintenanceOnly).contains(i);
    }

    // include pending adds but still have insufficient replication
    List<ContainerReplicaOp> pending =
        getContainerReplicaOps(ImmutableList.of(1, 2, 3), ImmutableList.of(1));
    rcnt = new ECContainerReplicaCount(container, replica, pending, 1);
    assertFalse(rcnt.isSufficientlyReplicated(true));
    assertFalse(rcnt.isOverReplicated(true));
    assertEquals(1, rcnt.additionalMaintenanceCopiesNeeded(true));
  }

  @Test
  public void testAdditionalMaintenanceCopiesAlreadyReplicated() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_MAINTENANCE, 5), Pair.of(IN_MAINTENANCE, 1));
    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica,
            Collections.emptyList(), 1);
    assertTrue(rcnt.isSufficientlyReplicated(false));
    assertFalse(rcnt.isOverReplicated(true));
    assertEquals(0, rcnt.additionalMaintenanceCopiesNeeded(false));
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    assertEquals(1, rcnt.maintenanceOnlyIndexes(false).size());

    // Repeat the test with redundancy of 2. Once the maintenance copies go
    // offline, we should be able to lost 2 more containers.
    rcnt = new ECContainerReplicaCount(container, replica,
        Collections.emptyList(), 2);
    assertFalse(rcnt.isSufficientlyReplicated(false));
    assertFalse(rcnt.isOverReplicated(true));
    assertEquals(1, rcnt.additionalMaintenanceCopiesNeeded(false));
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    assertEquals(1, rcnt.maintenanceOnlyIndexes(false).size());
    assertThat(rcnt.maintenanceOnlyIndexes(false)).contains(5);
  }

  @Test
  public void testAdditionalMaintenanceCopiesAlreadyReplicatedWithDelete() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_MAINTENANCE, 5), Pair.of(IN_MAINTENANCE, 1));
    List<ContainerReplicaOp> pending =
        getContainerReplicaOps(ImmutableList.of(), ImmutableList.of(1));

    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica,
            pending, 1);
    assertFalse(rcnt.isSufficientlyReplicated(false));
    assertFalse(rcnt.isOverReplicated(true));
    assertEquals(1, rcnt.additionalMaintenanceCopiesNeeded(false));
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    assertEquals(2, rcnt.maintenanceOnlyIndexes(false).size());
    assertThat(rcnt.maintenanceOnlyIndexes(false)).contains(1);
    assertThat(rcnt.maintenanceOnlyIndexes(false)).contains(5);
  }

  @Test
  public void testAdditionalMaintenanceCopiesDuplicatesInMaintenance() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_MAINTENANCE, 5), Pair.of(IN_MAINTENANCE, 1),
            Pair.of(IN_MAINTENANCE, 1), Pair.of(IN_MAINTENANCE, 5));
    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica,
            Collections.emptyList(), 1);
    assertTrue(rcnt.isSufficientlyReplicated(false));
    assertFalse(rcnt.isOverReplicated(true));
    assertEquals(0, rcnt.additionalMaintenanceCopiesNeeded(false));
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    assertEquals(1, rcnt.maintenanceOnlyIndexes(false).size());

    // Repeat the test with redundancy of 2. Once the maintenance copies go
    // offline, we should be able to lost 2 more containers.
    rcnt = new ECContainerReplicaCount(container, replica,
        Collections.emptyList(), 2);
    assertFalse(rcnt.isSufficientlyReplicated(false));
    assertFalse(rcnt.isOverReplicated(true));
    assertEquals(1, rcnt.additionalMaintenanceCopiesNeeded(false));
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    assertEquals(1, rcnt.maintenanceOnlyIndexes(false).size());
    assertThat(rcnt.maintenanceOnlyIndexes(false)).contains(5);
  }

  @Test
  public void testMaintenanceRedundancyGreaterThanParity() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_MAINTENANCE, 4),
            Pair.of(IN_MAINTENANCE, 5));
    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica,
            Collections.emptyList(), 5);
    // EC Parity is 2, which is max redundancy, but we have a
    // maintenanceRedundancy of 5, which is not possible. Only 2 more copies
    // should be needed.
    assertEquals(2, rcnt.additionalMaintenanceCopiesNeeded(false));
    // After replication, zero should be needed
    replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_MAINTENANCE, 4),
            Pair.of(IN_MAINTENANCE, 5), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5));
    rcnt = new ECContainerReplicaCount(container, replica,
        Collections.emptyList(), 5);
    assertEquals(0, rcnt.additionalMaintenanceCopiesNeeded(false));

  }

  @Test
  public void testUnderReplicatedNoMaintenance() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3));

    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica,
            Collections.emptyList(), 1);
    assertFalse(rcnt.isSufficientlyReplicated(false));
    assertFalse(rcnt.isOverReplicated(true));
    assertEquals(0, rcnt.additionalMaintenanceCopiesNeeded(false));
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    assertEquals(0, rcnt.maintenanceOnlyIndexes(false).size());

    assertEquals(2, rcnt.unavailableIndexes(true).size());
    assertThat(rcnt.unavailableIndexes(true)).contains(4);
    assertThat(rcnt.unavailableIndexes(true)).contains(5);
  }

  @Test
  public void testMaintenanceRedundancyIsMetWithPendingAdd() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_MAINTENANCE, 1),
            Pair.of(ENTERING_MAINTENANCE, 2), Pair.of(IN_MAINTENANCE, 3),
            Pair.of(IN_MAINTENANCE, 4), Pair.of(IN_MAINTENANCE, 5));
    List<ContainerReplicaOp> pending =
        getContainerReplicaOps(ImmutableList.of(1, 2, 3, 4),
            ImmutableList.of(1));

    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica, pending, 1);
    assertFalse(rcnt.isSufficientlyReplicated(false));
    assertTrue(rcnt.isSufficientlyReplicated(true));
    assertFalse(rcnt.isOverReplicated(true));
    assertEquals(4, rcnt.additionalMaintenanceCopiesNeeded(false));
    assertEquals(0, rcnt.additionalMaintenanceCopiesNeeded(true));

    Set<Integer> maintenanceOnly = rcnt.maintenanceOnlyIndexes(true);
    assertEquals(1, maintenanceOnly.size());
    assertThat(maintenanceOnly).contains(5);
  }

  @Test
  public void testUnderReplicatedFixedWithPending() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3));

    List<ContainerReplicaOp> pending =
        getContainerReplicaOps(ImmutableList.of(4, 5), ImmutableList.of());

    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica, pending, 1);
    assertFalse(rcnt.isSufficientlyReplicated(false));
    assertTrue(rcnt.isSufficientlyReplicated(true));
    assertFalse(rcnt.isOverReplicated(true));
    assertEquals(0, rcnt.additionalMaintenanceCopiesNeeded(false));
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    assertEquals(0, rcnt.maintenanceOnlyIndexes(false).size());

    // Zero unavailable, as the pending adds are scheduled as we assume they
    // will complete.
    assertEquals(0, rcnt.unavailableIndexes(true).size());
  }

  @Test
  public void testMissingNonMaintenanceReplicas() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_MAINTENANCE, 4));

    List<ContainerReplicaOp> pending =
        getContainerReplicaOps(ImmutableList.of(), ImmutableList.of(1));

    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica, pending, 1);
    assertFalse(rcnt.isSufficientlyReplicated(false));
    assertFalse(rcnt.isOverReplicated(true));

    assertEquals(2, rcnt.unavailableIndexes(true).size());
    assertThat(rcnt.unavailableIndexes(true)).contains(1);
    assertThat(rcnt.unavailableIndexes(true)).contains(5);
  }

  @Test
  public void testMissingNonMaintenanceReplicasAllMaintenance() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_MAINTENANCE, 1), Pair.of(IN_MAINTENANCE, 2),
            Pair.of(IN_MAINTENANCE, 3), Pair.of(IN_MAINTENANCE, 4),
            Pair.of(IN_MAINTENANCE, 5));

    List<ContainerReplicaOp> pending =
        getContainerReplicaOps(ImmutableList.of(1), ImmutableList.of());

    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica, pending, 1);
    assertFalse(rcnt.isSufficientlyReplicated(false));
    assertFalse(rcnt.isOverReplicated(true));
    assertEquals(3, rcnt.additionalMaintenanceCopiesNeeded(true));
    Set<Integer> maintenanceOnly = rcnt.maintenanceOnlyIndexes(true);
    assertEquals(4, maintenanceOnly.size());
    assertThat(maintenanceOnly).contains(2, 3, 4, 5);

    assertEquals(0, rcnt.unavailableIndexes(true).size());
  }

  @Test
  public void testMissingNonMaintenanceReplicasPendingAdd() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4));

    // 5 is missing, but there is a pending add.
    List<ContainerReplicaOp> pending =
        getContainerReplicaOps(ImmutableList.of(5), ImmutableList.of());

    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica, pending, 1);
    assertFalse(rcnt.isSufficientlyReplicated(false));
    assertFalse(rcnt.isOverReplicated(true));

    assertEquals(0, rcnt.unavailableIndexes(true).size());
  }

  @Nonnull
  private List<ContainerReplicaOp> getContainerReplicaOps(
      List<Integer> addIndexes, List<Integer> deleteIndexes) {
    List<ContainerReplicaOp> pending = new ArrayList<>();
    for (Integer addIndex : addIndexes) {
      pending.add(new ContainerReplicaOp(
          ContainerReplicaOp.PendingOpType.ADD,
          MockDatanodeDetails.randomDatanodeDetails(), addIndex,
          null, Long.MAX_VALUE, 0));
    }

    for (Integer deleteIndex : deleteIndexes) {
      pending.add(new ContainerReplicaOp(
          ContainerReplicaOp.PendingOpType.DELETE,
          MockDatanodeDetails.randomDatanodeDetails(), deleteIndex,
          null, Long.MAX_VALUE, 0));
    }
    return pending;
  }

  @Test
  public void testUnRecoverable() {
    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, new HashSet<>(),
            Collections.emptyList(), 1);
    assertTrue(rcnt.isUnrecoverable());
    assertEquals(5, rcnt.unavailableIndexes(true).size());

    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_MAINTENANCE, 2));
    // The unhealthy replica does not help with recovery even though we now
    // have 3 replicas.
    replica.addAll(ReplicationTestUtil.createReplicas(
        UNHEALTHY, Pair.of(IN_SERVICE, 3)));
    rcnt = new ECContainerReplicaCount(container, replica,
        Collections.emptyList(), 1);
    assertTrue(rcnt.isUnrecoverable());
    assertEquals(3, rcnt.unavailableIndexes(true).size());
    assertEquals(0, rcnt.additionalMaintenanceCopiesNeeded(false));

    replica = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONED, 1), Pair.of(DECOMMISSIONED, 2),
            Pair.of(DECOMMISSIONED, 3), Pair.of(DECOMMISSIONED, 4),
            Pair.of(DECOMMISSIONED, 5));
    rcnt = new ECContainerReplicaCount(container, replica,
        Collections.emptyList(), 1);
    // Not missing as the decommission replicas are still online
    assertFalse(rcnt.isUnrecoverable());
    assertEquals(0, rcnt.unavailableIndexes(true).size());

    // All unhealthy replicas is still un-recoverable.
    replica = ReplicationTestUtil.createReplicas(
        UNHEALTHY, Pair.of(IN_SERVICE, 1),
        Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
        Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5));
    rcnt = new ECContainerReplicaCount(container, replica,
        Collections.emptyList(), 1);
    // Not missing as the decommission replicas are still online
    assertTrue(rcnt.isUnrecoverable());
  }

  @Test
  public void testIsMissingAndUnhealthy() {
    // No Replicas
    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, new HashSet<>(),
            Collections.emptyList(), 1);
    assertTrue(rcnt.isMissing());
    assertTrue(rcnt.isUnrecoverable());

    // 1 unhealthy
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(UNHEALTHY, Pair.of(IN_SERVICE, 1));
    rcnt = new ECContainerReplicaCount(container, replica,
        Collections.emptyList(), 1);
    assertTrue(rcnt.isMissing());
    assertTrue(rcnt.isUnrecoverable());

    // 2 unhealthy
    replica = ReplicationTestUtil
        .createReplicas(UNHEALTHY, Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2));
    rcnt = new ECContainerReplicaCount(container, replica,
        Collections.emptyList(), 1);
    assertTrue(rcnt.isMissing());
    assertTrue(rcnt.isUnrecoverable());

    // 3 unhealthy
    replica = ReplicationTestUtil
        .createReplicas(UNHEALTHY, Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3));
    rcnt = new ECContainerReplicaCount(container, replica,
        Collections.emptyList(), 1);
    assertFalse(rcnt.isMissing());
    assertTrue(rcnt.isUnrecoverable());


    // 3 replicas, with 1 unhealthy
    replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2));
    replica.addAll(ReplicationTestUtil.createReplicas(
        UNHEALTHY, Pair.of(IN_SERVICE, 3)));
    rcnt = new ECContainerReplicaCount(container, replica,
        Collections.emptyList(), 1);
    assertFalse(rcnt.isMissing());
    assertTrue(rcnt.isUnrecoverable());

    // 4 replicas, with 1 unhealthy
    replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3));
    replica.addAll(ReplicationTestUtil.createReplicas(
        UNHEALTHY, Pair.of(IN_SERVICE, 4)));
    rcnt = new ECContainerReplicaCount(container, replica,
        Collections.emptyList(), 1);
    assertFalse(rcnt.isMissing());
    assertFalse(rcnt.isUnrecoverable());
  }

  @Test
  public void testDecommissioningOnlyIndexes() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5));
    List<ContainerReplicaOp> pending =
        getContainerReplicaOps(ImmutableList.of(1), ImmutableList.of());

    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica, pending, 1);
    assertEquals(ImmutableSet.of(1),
        rcnt.decommissioningOnlyIndexes(false));
    assertEquals(ImmutableSet.of(), rcnt.decommissioningOnlyIndexes(true));
  }

  @Test
  public void testSufficientlyReplicatedForOffline() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 2));

    ContainerReplica inServiceReplica =
        ReplicationTestUtil.createContainerReplica(container.containerID(),
            1, IN_SERVICE, CLOSED);
    replica.add(inServiceReplica);

    ContainerReplica offlineReplica =
        ReplicationTestUtil.createContainerReplica(container.containerID(),
            1, DECOMMISSIONING, CLOSED);
    replica.add(offlineReplica);

    ContainerReplica offlineNotReplicated =
        ReplicationTestUtil.createContainerReplica(container.containerID(),
            3, DECOMMISSIONING, CLOSED);
    replica.add(offlineNotReplicated);

    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica, Collections.emptyList(),
            1);
    assertFalse(rcnt.isSufficientlyReplicated(false));
    assertTrue(rcnt.isSufficientlyReplicatedForOffline(
        offlineReplica.getDatanodeDetails(), null));
    assertFalse(rcnt.isSufficientlyReplicatedForOffline(
        offlineNotReplicated.getDatanodeDetails(), null));

    // A random DN not hosting a replica for this container should return false.
    assertFalse(rcnt.isSufficientlyReplicatedForOffline(
        MockDatanodeDetails.randomDatanodeDetails(), null));

    // Passing the IN_SERVICE node should return false even though the
    // replica is on a healthy node
    assertFalse(rcnt.isSufficientlyReplicatedForOffline(
        inServiceReplica.getDatanodeDetails(), null));
  }

  @Test
  public void testSufficientlyReplicatedWithUnhealthyAndPendingDelete() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5));

    ContainerReplica unhealthyReplica =
        ReplicationTestUtil.createContainerReplica(container.containerID(),
            1, IN_SERVICE, UNHEALTHY);
    replica.add(unhealthyReplica);

    List<ContainerReplicaOp> pendingOps = new ArrayList<>();
    pendingOps.add(new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.DELETE,
        unhealthyReplica.getDatanodeDetails(),
        unhealthyReplica.getReplicaIndex(), null, System.currentTimeMillis(), 0));

    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica, pendingOps, 1);
    assertTrue(rcnt.isSufficientlyReplicated(false));

    // Add another pending delete to an index that is not an unhealthy index
    pendingOps.add(new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.DELETE,
        MockDatanodeDetails.randomDatanodeDetails(), 2, null, System.currentTimeMillis(), 0));

    rcnt = new ECContainerReplicaCount(container, replica, pendingOps, 1);
    assertFalse(rcnt.isSufficientlyReplicated(false));
    assertEquals(2, rcnt.unavailableIndexes(false).get(0));
  }
}
