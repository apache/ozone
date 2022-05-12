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
package org.apache.hadoop.hdds.scm.container.states;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ECContainerReplicaCount;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;

/**
 * Tests for EcContainerReplicaCounts.
 */
public class TestECContainerReplicaCount {

  private ECReplicationConfig repConfig;
  private ContainerInfo container;

  @Before
  public void setup() {
    repConfig = new ECReplicationConfig(3, 2);
    container = createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
  }

  @Test
  public void testPerfectlyReplicatedContainer() {
    Set<ContainerReplica> replica =
        registerNodes(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5));
    ECContainerReplicaCount rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), new ArrayList<>(), 1);
    Assert.assertTrue(rcnt.isSufficientlyReplicated());
    Assert.assertFalse(rcnt.unRecoverable());
  }

  @Test
  public void testContainerMissingReplica() {
    Set<ContainerReplica> replica =
        registerNodes(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4));
    ECContainerReplicaCount rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), new ArrayList<>(), 1);
    Assert.assertFalse(rcnt.isSufficientlyReplicated());
    Assert.assertEquals(1, rcnt.missingNonMaintenanceIndexes().size());
    Assert.assertEquals(5,
        rcnt.missingNonMaintenanceIndexes().get(0).intValue());
  }

  @Test
  public void testContainerMissingReplicaDueToPendingDelete() {
    Set<ContainerReplica> replica =
        registerNodes(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5));
    List<Integer> delete = new ArrayList<>();
    delete.add(1);
    ECContainerReplicaCount rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), delete, 1);
    Assert.assertFalse(rcnt.isSufficientlyReplicated());
    Assert.assertEquals(1, rcnt.missingNonMaintenanceIndexes().size());
    Assert.assertEquals(1,
        rcnt.missingNonMaintenanceIndexes().get(0).intValue());
  }

  @Test
  public void testContainerExcessReplicasAndPendingDelete() {
    Set<ContainerReplica> replica =
        registerNodes(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5), Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2));
    List<Integer> delete = new ArrayList<>();
    delete.add(1);
    delete.add(2);
    ECContainerReplicaCount rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), delete, 1);
    Assert.assertTrue(rcnt.isSufficientlyReplicated());
    Assert.assertFalse(rcnt.isOverReplicated());
  }

  @Test
  public void testUnderRepContainerWithExcessReplicasAndPendingDelete() {
    Set<ContainerReplica> replica =
        registerNodes(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5), Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2));
    List<Integer> delete = new ArrayList<>();
    delete.add(1);
    delete.add(2);
    delete.add(2);
    ECContainerReplicaCount rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), delete, 1);
    Assert.assertFalse(rcnt.isSufficientlyReplicated());
    Assert.assertEquals(1, rcnt.missingNonMaintenanceIndexes().size());
    Assert.assertEquals(2,
        rcnt.missingNonMaintenanceIndexes().get(0).intValue());
  }

  @Test
  public void testContainerWithMaintenanceReplicasSufficientlyReplicated() {
    Set<ContainerReplica> replica =
        registerNodes(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_MAINTENANCE, 4),
            Pair.of(IN_MAINTENANCE, 5));
    ECContainerReplicaCount rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), new ArrayList<>(), 0);
    Assert.assertTrue(rcnt.isSufficientlyReplicated());
    rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), new ArrayList<>(), 1);
    Assert.assertFalse(rcnt.isSufficientlyReplicated());
    List<Integer> delete = new ArrayList<>();
    delete.add(1);
    rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), delete, 0);
    Assert.assertFalse(rcnt.isSufficientlyReplicated());
  }

  @Test
  public void testOverReplicatedContainer() {
    Set<ContainerReplica> replica =
        registerNodes(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5), Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2));
    List<Integer> delete = new ArrayList<>();
    delete.add(1);
    ECContainerReplicaCount rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), delete, 1);
    Assert.assertTrue(rcnt.isSufficientlyReplicated());
    Assert.assertTrue(rcnt.isOverReplicated());
    Assert.assertEquals(2, rcnt.overReplicatedIndexes().get(0).intValue());
  }

  @Test
  public void testOverReplicatedAndUnderReplicatedContainer() {
    Set<ContainerReplica> replica =
        registerNodes(Pair.of(DECOMMISSIONING, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5), Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2));
    List<Integer> delete = new ArrayList<>();
    delete.add(1);
    ECContainerReplicaCount rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), delete, 1);
    Assert.assertFalse(rcnt.isSufficientlyReplicated());
    Assert.assertTrue(rcnt.isOverReplicated());
    Assert.assertEquals(2, rcnt.overReplicatedIndexes().get(0).intValue());
  }

  @Test
  public void testAdditionalMaintenanceCopiesAllMaintenance() {
    Set<ContainerReplica> replica =
        registerNodes(Pair.of(IN_MAINTENANCE, 1),
            Pair.of(ENTERING_MAINTENANCE, 2),
            Pair.of(IN_MAINTENANCE, 3), Pair.of(IN_MAINTENANCE, 4),
            Pair.of(IN_MAINTENANCE, 5), Pair.of(IN_MAINTENANCE, 1));
    ECContainerReplicaCount rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), new ArrayList<>(), 1);
    Assert.assertFalse(rcnt.isSufficientlyReplicated());
    Assert.assertFalse(rcnt.isOverReplicated());
    Assert.assertEquals(4, rcnt.additionalMaintenanceCopiesNeeded());
    for (int i = 1; i <= repConfig.getRequiredNodes(); i++) {
      Assert.assertTrue(rcnt.maintenanceOnlyIndexes().contains(i));
    }
  }

  @Test
  public void testAdditionalMaintenanceCopiesAlreadyReplicated() {
    Set<ContainerReplica> replica =
        registerNodes(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_MAINTENANCE, 5), Pair.of(IN_MAINTENANCE, 1));
    ECContainerReplicaCount rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), new ArrayList<>(), 1);
    Assert.assertTrue(rcnt.isSufficientlyReplicated());
    Assert.assertFalse(rcnt.isOverReplicated());
    Assert.assertEquals(0, rcnt.additionalMaintenanceCopiesNeeded());
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    Assert.assertEquals(1, rcnt.maintenanceOnlyIndexes().size());

    // Repeat the test with redundancy of 2. Once the maintenance copies go
    // offline, we should be able to lost 2 more containers.
    rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), new ArrayList<>(), 2);
    Assert.assertFalse(rcnt.isSufficientlyReplicated());
    Assert.assertFalse(rcnt.isOverReplicated());
    Assert.assertEquals(1, rcnt.additionalMaintenanceCopiesNeeded());
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    Assert.assertEquals(1, rcnt.maintenanceOnlyIndexes().size());
    Assert.assertEquals(5, rcnt.maintenanceOnlyIndexes().get(0).intValue());
  }

  @Test
  public void testAdditionalMaintenanceCopiesAlreadyReplicatedWithDelete() {
    Set<ContainerReplica> replica =
        registerNodes(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_MAINTENANCE, 5), Pair.of(IN_MAINTENANCE, 1));
    List<Integer> delete = new ArrayList<>();
    delete.add(1);
    ECContainerReplicaCount rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), delete, 1);
    Assert.assertFalse(rcnt.isSufficientlyReplicated());
    Assert.assertFalse(rcnt.isOverReplicated());
    Assert.assertEquals(1, rcnt.additionalMaintenanceCopiesNeeded());
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    Assert.assertEquals(2, rcnt.maintenanceOnlyIndexes().size());
    Assert.assertTrue(rcnt.maintenanceOnlyIndexes().contains(1));
    Assert.assertTrue(rcnt.maintenanceOnlyIndexes().contains(5));
  }

  @Test
  public void testAdditionalMaintenanceCopiesDuplicatesInMaintenance() {
    Set<ContainerReplica> replica =
        registerNodes(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_MAINTENANCE, 5), Pair.of(IN_MAINTENANCE, 1),
            Pair.of(IN_MAINTENANCE, 1), Pair.of(IN_MAINTENANCE, 5));
    ECContainerReplicaCount rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), new ArrayList<>(), 1);
    Assert.assertTrue(rcnt.isSufficientlyReplicated());
    Assert.assertFalse(rcnt.isOverReplicated());
    Assert.assertEquals(0, rcnt.additionalMaintenanceCopiesNeeded());
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    Assert.assertEquals(1, rcnt.maintenanceOnlyIndexes().size());

    // Repeat the test with redundancy of 2. Once the maintenance copies go
    // offline, we should be able to lost 2 more containers.
    rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), new ArrayList<>(), 2);
    Assert.assertFalse(rcnt.isSufficientlyReplicated());
    Assert.assertFalse(rcnt.isOverReplicated());
    Assert.assertEquals(1, rcnt.additionalMaintenanceCopiesNeeded());
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    Assert.assertEquals(1, rcnt.maintenanceOnlyIndexes().size());
    Assert.assertEquals(5, rcnt.maintenanceOnlyIndexes().get(0).intValue());
  }

  @Test
  public void testMaintenanceRedundancyGreaterThanParity() {
    Set<ContainerReplica> replica =
        registerNodes(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_MAINTENANCE, 4),
            Pair.of(IN_MAINTENANCE, 5));
    ECContainerReplicaCount rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), new ArrayList<>(), 5);
    // EC Parity is 2, which is max redundancy, but we have a
    // maintenanceRedundancy of 5, which is not possible. Only 2 more copies
    // should be needed.
    Assert.assertEquals(2, rcnt.additionalMaintenanceCopiesNeeded());
    // After replication, zero should be needed
    replica =
        registerNodes(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_MAINTENANCE, 4),
            Pair.of(IN_MAINTENANCE, 5), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5));
    rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), new ArrayList<>(), 5);
    Assert.assertEquals(0, rcnt.additionalMaintenanceCopiesNeeded());

  }

  @Test
  public void testUnderReplicatedNoMaintenance() {
    Set<ContainerReplica> replica =
        registerNodes(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3));

    ECContainerReplicaCount rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), new ArrayList<>(), 1);
    Assert.assertFalse(rcnt.isSufficientlyReplicated());
    Assert.assertFalse(rcnt.isOverReplicated());
    Assert.assertEquals(0, rcnt.additionalMaintenanceCopiesNeeded());
    // Even though we don't need new copies, the following call will return
    // any indexes only have a maintenance copy.
    Assert.assertEquals(0, rcnt.maintenanceOnlyIndexes().size());

    Assert.assertEquals(2, rcnt.missingNonMaintenanceIndexes().size());
    Assert.assertTrue(rcnt.missingNonMaintenanceIndexes().contains(4));
    Assert.assertTrue(rcnt.missingNonMaintenanceIndexes().contains(5));
  }

  @Test
  public void testMissingNonMaintenanceReplicas() {
    Set<ContainerReplica> replica =
        registerNodes(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_MAINTENANCE, 4));

    List<Integer> delete = new ArrayList<>();
    delete.add(1);

    ECContainerReplicaCount rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), delete, 1);
    Assert.assertFalse(rcnt.isSufficientlyReplicated());
    Assert.assertFalse(rcnt.isOverReplicated());

    Assert.assertEquals(2, rcnt.missingNonMaintenanceIndexes().size());
    Assert.assertTrue(rcnt.missingNonMaintenanceIndexes().contains(1));
    Assert.assertTrue(rcnt.missingNonMaintenanceIndexes().contains(5));
  }

  @Test
  public void testMissingNonMaintenanceReplicasAllMaintenance() {
    Set<ContainerReplica> replica =
        registerNodes(Pair.of(IN_MAINTENANCE, 1), Pair.of(IN_MAINTENANCE, 2),
            Pair.of(IN_MAINTENANCE, 3), Pair.of(IN_MAINTENANCE, 4),
            Pair.of(IN_MAINTENANCE, 5));

    List<Integer> delete = new ArrayList<>();
    delete.add(1);

    ECContainerReplicaCount rcnt = new ECContainerReplicaCount(container,
        replica, new ArrayList<>(), delete, 1);
    Assert.assertFalse(rcnt.isSufficientlyReplicated());
    Assert.assertFalse(rcnt.isOverReplicated());

    Assert.assertEquals(0, rcnt.missingNonMaintenanceIndexes().size());
  }

  @Test
  public void testMissingNonMaintenanceReplicasPendingAdd() {
    Set<ContainerReplica> replica =
        registerNodes(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4));

    // 5 is missing, but there is a pending add.
    List<Integer> add = new ArrayList<>();
    add.add(5);

    ECContainerReplicaCount rcnt = new ECContainerReplicaCount(container,
        replica, add, new ArrayList<>(), 1);
    Assert.assertFalse(rcnt.isSufficientlyReplicated());
    Assert.assertFalse(rcnt.isOverReplicated());

    Assert.assertEquals(0, rcnt.missingNonMaintenanceIndexes().size());
  }

  @Test
  public void testMissing() {
    ECContainerReplicaCount rcnt = new ECContainerReplicaCount(container,
        new HashSet<>(), new ArrayList<>(), new ArrayList<>(), 1);
    Assert.assertTrue(rcnt.unRecoverable());
    Assert.assertEquals(5, rcnt.missingNonMaintenanceIndexes().size());

    Set<ContainerReplica> replica =
        registerNodes(Pair.of(IN_SERVICE, 1), Pair.of(IN_MAINTENANCE, 2));
    rcnt = new ECContainerReplicaCount(container, replica, new ArrayList<>(),
        new ArrayList<>(), 1);
    Assert.assertTrue(rcnt.unRecoverable());
    Assert.assertEquals(3, rcnt.missingNonMaintenanceIndexes().size());
    Assert.assertEquals(0, rcnt.additionalMaintenanceCopiesNeeded());

    replica =
        registerNodes(Pair.of(DECOMMISSIONED, 1), Pair.of(DECOMMISSIONED, 2),
            Pair.of(DECOMMISSIONED, 3), Pair.of(DECOMMISSIONED, 4),
            Pair.of(DECOMMISSIONED, 5));
    rcnt = new ECContainerReplicaCount(container, replica, new ArrayList<>(),
        new ArrayList<>(), 1);
    // Not missing as the decommission replicas are still online
    Assert.assertFalse(rcnt.unRecoverable());
    Assert.assertEquals(5, rcnt.missingNonMaintenanceIndexes().size());
  }

  private Set<ContainerReplica> registerNodes(
      Pair<HddsProtos.NodeOperationalState, Integer>... states) {
    Set<ContainerReplica> replica = new HashSet<>();
    for (Pair<HddsProtos.NodeOperationalState, Integer> s : states) {
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      dn.setPersistedOpState(s.getLeft());
      replica.add(new ContainerReplica.ContainerReplicaBuilder()
          .setContainerID(ContainerID.valueOf(1))
          .setContainerState(CLOSED)
          .setDatanodeDetails(dn)
          .setOriginNodeId(dn.getUuid())
          .setSequenceId(1)
          .setReplicaIndex(s.getRight())
          .build());
    }
    return replica;
  }

  private ContainerInfo createContainer(HddsProtos.LifeCycleState state,
      ReplicationConfig replicationConfig) {
    return new ContainerInfo.Builder()
        .setContainerID(ContainerID.valueOf(1).getId())
        .setState(state)
        .setReplicationConfig(replicationConfig)
        .build();
  }
}
