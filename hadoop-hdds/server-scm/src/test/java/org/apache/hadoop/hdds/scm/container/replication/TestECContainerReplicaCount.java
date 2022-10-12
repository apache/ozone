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
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;

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
    Assertions.assertTrue(rcnt.isSufficientlyReplicated(false));
    Assertions.assertFalse(rcnt.isUnrecoverable());
  }

  @Test
  public void testContainerMissingReplica() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4));
    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica,
            Collections.emptyList(), 1);
    Assertions.assertFalse(rcnt.isSufficientlyReplicated(false));
    Assertions.assertEquals(1, rcnt.unavailableIndexes(true).size());
    Assertions.assertEquals(5,
        rcnt.unavailableIndexes(true).get(0).intValue());
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
    Assertions.assertFalse(rcnt.isSufficientlyReplicated(false));
    Assertions.assertEquals(1, rcnt.unavailableIndexes(true).size());
    Assertions.assertEquals(1,
        rcnt.unavailableIndexes(true).get(0).intValue());
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
    Assertions.assertTrue(rcnt.isSufficientlyReplicated(false));
    Assertions.assertFalse(rcnt.isOverReplicated(true));
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
    Assertions.assertFalse(rcnt.isSufficientlyReplicated(false));
    Assertions.assertEquals(1, rcnt.unavailableIndexes(true).size());
    Assertions.assertEquals(2,
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
    Assertions.assertTrue(rcnt.isSufficientlyReplicated(false));
    rcnt = new ECContainerReplicaCount(container, replica,
        Collections.emptyList(), 1);
    Assertions.assertFalse(rcnt.isSufficientlyReplicated(false));
    List<ContainerReplicaOp> pendingOps =
        getContainerReplicaOps(ImmutableList.of(), ImmutableList.of(1));
    rcnt = new ECContainerReplicaCount(container, replica, pendingOps, 0);
    Assertions.assertFalse(rcnt.isSufficientlyReplicated(false));
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
    Assertions.assertTrue(rcnt.isSufficientlyReplicated(false));
    Assertions.assertTrue(rcnt.isOverReplicated(true));
    Assertions.assertEquals(2,
        rcnt.overReplicatedIndexes(true).get(0).intValue());
    Assertions.assertEquals(1, rcnt.overReplicatedIndexes(true).size());
    Assertions.assertTrue(rcnt.isOverReplicated(false));
    Assertions.assertEquals(2, rcnt.overReplicatedIndexes(false).size());
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
    pending.add(ContainerReplicaOp
        .create(ContainerReplicaOp.PendingOpType.DELETE,
            MockDatanodeDetails.randomDatanodeDetails(), 2));

    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica, pending, 1);
    Assertions.assertTrue(rcnt.isSufficientlyReplicated(false));
    Assertions.assertFalse(rcnt.isOverReplicated(true));
    Assertions.assertEquals(0, rcnt.overReplicatedIndexes(true).size());
    Assertions.assertTrue(rcnt.isOverReplicated(false));
    Assertions.assertEquals(2, rcnt.overReplicatedIndexes(false).size());
  }

  @Test
  public void testOverReplicatedAndUnderReplicatedContainer() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5), Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2));
    List<ContainerReplicaOp> pending =
        getContainerReplicaOps(ImmutableList.of(), ImmutableList.of(1));

    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica, pending, 1);
    Assertions.assertFalse(rcnt.isSufficientlyReplicated(false));
    Assertions.assertTrue(rcnt.isOverReplicated(true));
    Assertions.assertEquals(2,
        rcnt.overReplicatedIndexes(true).get(0).intValue());
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
    Assertions.assertFalse(rcnt.isSufficientlyReplicated(false));
    Assertions.assertFalse(rcnt.isOverReplicated(true));
    Assertions.assertEquals(4, rcnt.additionalMaintenanceCopiesNeeded(false));
    Set<Integer> maintenanceOnly = rcnt.maintenanceOnlyIndexes(false);
    for (int i = 1; i <= repConfig.getRequiredNodes(); i++) {
      Assertions.assertTrue(maintenanceOnly.contains(i));
    }

    // include pending adds but still have insufficient replication
    List<ContainerReplicaOp> pending =
        getContainerReplicaOps(ImmutableList.of(1, 2, 3), ImmutableList.of(1));
    rcnt = new ECContainerReplicaCount(container, replica, pending, 1);
    Assertions.assertFalse(rcnt.isSufficientlyReplicated(true));
    Assertions.assertFalse(rcnt.isOverReplicated(true));
    Assertions.assertEquals(1, rcnt.additionalMaintenanceCopiesNeeded(true));
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
    Assertions.assertTrue(rcnt.isSufficientlyReplicated(false));
    Assertions.assertFalse(rcnt.isOverReplicated(true));
    Assertions.assertEquals(0, rcnt.additionalMaintenanceCopiesNeeded(false));
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    Assertions.assertEquals(1, rcnt.maintenanceOnlyIndexes(false).size());

    // Repeat the test with redundancy of 2. Once the maintenance copies go
    // offline, we should be able to lost 2 more containers.
    rcnt = new ECContainerReplicaCount(container, replica,
        Collections.emptyList(), 2);
    Assertions.assertFalse(rcnt.isSufficientlyReplicated(false));
    Assertions.assertFalse(rcnt.isOverReplicated(true));
    Assertions.assertEquals(1, rcnt.additionalMaintenanceCopiesNeeded(false));
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    Assertions.assertEquals(1, rcnt.maintenanceOnlyIndexes(false).size());
    Assertions.assertTrue(rcnt.maintenanceOnlyIndexes(false).contains(5));
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
    Assertions.assertFalse(rcnt.isSufficientlyReplicated(false));
    Assertions.assertFalse(rcnt.isOverReplicated(true));
    Assertions.assertEquals(1, rcnt.additionalMaintenanceCopiesNeeded(false));
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    Assertions.assertEquals(2, rcnt.maintenanceOnlyIndexes(false).size());
    Assertions.assertTrue(rcnt.maintenanceOnlyIndexes(false).contains(1));
    Assertions.assertTrue(rcnt.maintenanceOnlyIndexes(false).contains(5));
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
    Assertions.assertTrue(rcnt.isSufficientlyReplicated(false));
    Assertions.assertFalse(rcnt.isOverReplicated(true));
    Assertions.assertEquals(0, rcnt.additionalMaintenanceCopiesNeeded(false));
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    Assertions.assertEquals(1, rcnt.maintenanceOnlyIndexes(false).size());

    // Repeat the test with redundancy of 2. Once the maintenance copies go
    // offline, we should be able to lost 2 more containers.
    rcnt = new ECContainerReplicaCount(container, replica,
        Collections.emptyList(), 2);
    Assertions.assertFalse(rcnt.isSufficientlyReplicated(false));
    Assertions.assertFalse(rcnt.isOverReplicated(true));
    Assertions.assertEquals(1, rcnt.additionalMaintenanceCopiesNeeded(false));
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    Assertions.assertEquals(1, rcnt.maintenanceOnlyIndexes(false).size());
    Assertions.assertTrue(rcnt.maintenanceOnlyIndexes(false).contains(5));
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
    Assertions.assertEquals(2, rcnt.additionalMaintenanceCopiesNeeded(false));
    // After replication, zero should be needed
    replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_MAINTENANCE, 4),
            Pair.of(IN_MAINTENANCE, 5), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5));
    rcnt = new ECContainerReplicaCount(container, replica,
        Collections.emptyList(), 5);
    Assertions.assertEquals(0, rcnt.additionalMaintenanceCopiesNeeded(false));

  }

  @Test
  public void testUnderReplicatedNoMaintenance() {
    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3));

    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, replica,
            Collections.emptyList(), 1);
    Assertions.assertFalse(rcnt.isSufficientlyReplicated(false));
    Assertions.assertFalse(rcnt.isOverReplicated(true));
    Assertions.assertEquals(0, rcnt.additionalMaintenanceCopiesNeeded(false));
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    Assertions.assertEquals(0, rcnt.maintenanceOnlyIndexes(false).size());

    Assertions.assertEquals(2, rcnt.unavailableIndexes(true).size());
    Assertions.assertTrue(rcnt.unavailableIndexes(true).contains(4));
    Assertions.assertTrue(rcnt.unavailableIndexes(true).contains(5));
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
    Assertions.assertFalse(rcnt.isSufficientlyReplicated(false));
    Assertions.assertTrue(rcnt.isSufficientlyReplicated(true));
    Assertions.assertFalse(rcnt.isOverReplicated(true));
    Assertions.assertEquals(4, rcnt.additionalMaintenanceCopiesNeeded(false));
    Assertions.assertEquals(0, rcnt.additionalMaintenanceCopiesNeeded(true));

    Set<Integer> maintenanceOnly = rcnt.maintenanceOnlyIndexes(true);
    Assertions.assertEquals(1, maintenanceOnly.size());
    Assertions.assertTrue(maintenanceOnly.contains(5));
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
    Assertions.assertFalse(rcnt.isSufficientlyReplicated(false));
    Assertions.assertTrue(rcnt.isSufficientlyReplicated(true));
    Assertions.assertFalse(rcnt.isOverReplicated(true));
    Assertions.assertEquals(0, rcnt.additionalMaintenanceCopiesNeeded(false));
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    Assertions.assertEquals(0, rcnt.maintenanceOnlyIndexes(false).size());

    // Zero unavailable, as the pending adds are scheduled as we assume they
    // will complete.
    Assertions.assertEquals(0, rcnt.unavailableIndexes(true).size());
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
    Assertions.assertFalse(rcnt.isSufficientlyReplicated(false));
    Assertions.assertFalse(rcnt.isOverReplicated(true));

    Assertions.assertEquals(2, rcnt.unavailableIndexes(true).size());
    Assertions.assertTrue(rcnt.unavailableIndexes(true).contains(1));
    Assertions.assertTrue(rcnt.unavailableIndexes(true).contains(5));
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
    Assertions.assertFalse(rcnt.isSufficientlyReplicated(false));
    Assertions.assertFalse(rcnt.isOverReplicated(true));
    Assertions.assertEquals(3, rcnt.additionalMaintenanceCopiesNeeded(true));
    Set<Integer> maintenanceOnly = rcnt.maintenanceOnlyIndexes(true);
    Assertions.assertEquals(4, maintenanceOnly.size());
    Assertions.assertTrue(
        maintenanceOnly.contains(2) && maintenanceOnly.contains(3) &&
            maintenanceOnly.contains(4) && maintenanceOnly.contains(5));

    Assertions.assertEquals(0, rcnt.unavailableIndexes(true).size());
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
    Assertions.assertFalse(rcnt.isSufficientlyReplicated(false));
    Assertions.assertFalse(rcnt.isOverReplicated(true));

    Assertions.assertEquals(0, rcnt.unavailableIndexes(true).size());
  }

  @NotNull
  private List<ContainerReplicaOp> getContainerReplicaOps(
      List<Integer> addIndexes, List<Integer> deleteIndexes) {
    List<ContainerReplicaOp> pending = new ArrayList<>();
    for (Integer addIndex : addIndexes) {
      pending.add(ContainerReplicaOp
          .create(ContainerReplicaOp.PendingOpType.ADD,
              MockDatanodeDetails.randomDatanodeDetails(), addIndex));
    }

    for (Integer deleteIndex : deleteIndexes) {
      pending.add(ContainerReplicaOp
          .create(ContainerReplicaOp.PendingOpType.DELETE,
              MockDatanodeDetails.randomDatanodeDetails(), deleteIndex));
    }
    return pending;
  }

  @Test
  public void testMissing() {
    ECContainerReplicaCount rcnt =
        new ECContainerReplicaCount(container, new HashSet<>(),
            Collections.emptyList(), 1);
    Assertions.assertTrue(rcnt.isUnrecoverable());
    Assertions.assertEquals(5, rcnt.unavailableIndexes(true).size());

    Set<ContainerReplica> replica = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_MAINTENANCE, 2));
    rcnt = new ECContainerReplicaCount(container, replica,
        Collections.emptyList(), 1);
    Assertions.assertTrue(rcnt.isUnrecoverable());
    Assertions.assertEquals(3, rcnt.unavailableIndexes(true).size());
    Assertions.assertEquals(0, rcnt.additionalMaintenanceCopiesNeeded(false));

    replica = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONED, 1), Pair.of(DECOMMISSIONED, 2),
            Pair.of(DECOMMISSIONED, 3), Pair.of(DECOMMISSIONED, 4),
            Pair.of(DECOMMISSIONED, 5));
    rcnt = new ECContainerReplicaCount(container, replica,
        Collections.emptyList(), 1);
    // Not missing as the decommission replicas are still online
    Assertions.assertFalse(rcnt.isUnrecoverable());
    Assertions.assertEquals(0, rcnt.unavailableIndexes(true).size());
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
    Assertions.assertEquals(ImmutableSet.of(1),
        rcnt.decommissioningOnlyIndexes(false));
    Assertions
        .assertEquals(ImmutableSet.of(), rcnt.decommissioningOnlyIndexes(true));
  }
}
