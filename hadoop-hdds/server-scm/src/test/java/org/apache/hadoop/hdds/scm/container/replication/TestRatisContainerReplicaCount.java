/*
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

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.junit.Assert;
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
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSING;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.OPEN;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.UNHEALTHY;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerInfo;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerReplica;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createReplicas;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Class used to test the RatisContainerReplicaCount class.
 */
class TestRatisContainerReplicaCount {

  @Test
  void testThreeHealthyReplica() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, true, 0, false);
  }

  @Test
  void testTwoHealthyReplica() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 1, false);
  }

  @Test
  void testOneHealthyReplica() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 2, false);
  }

  @Test
  void testTwoHealthyAndInflightAdd() {

    Set<ContainerReplica> replica = registerNodes(IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 1, 0, 3, 2);
    validate(rcnt, false, 0, false);
  }

  /**
   * This does not schedule a container to be removed, as the inFlight add may
   * fail and then the delete would make things under-replicated. Once the add
   * completes there will be 4 healthy and it will get taken care of then.
   */
  @Test
  void testThreeHealthyAndInflightAdd() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 1, 0, 3, 2);
    validate(rcnt, true, 0, false);
  }

  /**
   * As the inflight delete may fail, but as it will make the the container
   * under replicated, we go ahead and schedule another replica to be added.
   */
  @Test
  void testThreeHealthyAndInflightDelete() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 1, 3, 2);
    validate(rcnt, false, 1, false);
  }

  /**
   * This is NOT sufficiently replicated as the inflight add may fail and the
   * inflight del could succeed, leaving only 2 healthy replicas.
   */
  @Test
  void testThreeHealthyAndInflightAddAndInFlightDelete() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 1, 1, 3, 2);
    validate(rcnt, false, 0, false);
  }

  @Test
  void testFourHealthyReplicas() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, true, -1, true);
  }

  @Test
  void testFourHealthyReplicasAndInFlightDelete() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 1, 3, 2);
    validate(rcnt, true, 0, false);
  }

  @Test
  void testFourHealthyReplicasAndTwoInFlightDelete() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 2, 3, 2);
    validate(rcnt, false, 1, false);
  }

  @Test
  void testOneHealthyReplicaRepFactorOne() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 1, 2);
    validate(rcnt, true, 0, false);
  }

  @Test
  void testOneHealthyReplicaRepFactorOneInFlightDelete() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 1, 1, 2);
    validate(rcnt, false, 1, false);
  }

  @Test
  void testTwoHealthyReplicaTwoInflightAdd() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 2, 0, 3, 2);
    validate(rcnt, false, 0, false);
  }

  /**
   * From here consider decommission replicas.
   */

  @Test
  void testThreeHealthyAndTwoDecommission() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE, IN_SERVICE,
        IN_SERVICE, DECOMMISSIONING, DECOMMISSIONING);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, true, 0, false);
  }

  @Test
  void testOneDecommissionedReplica() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, DECOMMISSIONING);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 1, false);
  }

  @Test
  void testTwoHealthyOneDecommissionedneInFlightAdd() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, DECOMMISSIONED);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 1, 0, 3, 2);
    validate(rcnt, false, 0, false);
  }

  @Test
  void testAllDecommissioned() {
    Set<ContainerReplica> replica =
        registerNodes(DECOMMISSIONED, DECOMMISSIONED, DECOMMISSIONED);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 3, false);
  }

  @Test
  void testAllDecommissionedRepFactorOne() {
    Set<ContainerReplica> replica = registerNodes(DECOMMISSIONED);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 1, 2);
    validate(rcnt, false, 1, false);

  }

  @Test
  void testAllDecommissionedRepFactorOneInFlightAdd() {
    Set<ContainerReplica> replica = registerNodes(DECOMMISSIONED);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 1, 0, 1, 2);
    validate(rcnt, false, 0, false);
  }

  @Test
  void testOneHealthyOneDecommissioningRepFactorOne() {
    Set<ContainerReplica> replica = registerNodes(DECOMMISSIONED, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 1, 2);
    validate(rcnt, true, 0, false);
  }

  /**
   * Maintenance tests from here.
   */

  @Test
  void testOneHealthyTwoMaintenanceMinRepOfTwo() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_MAINTENANCE, IN_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 1, false);
  }

  @Test
  void testOneHealthyThreeMaintenanceMinRepOfTwo() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE,
        IN_MAINTENANCE, IN_MAINTENANCE, ENTERING_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 1, false);
  }

  @Test
  void testOneHealthyTwoMaintenanceMinRepOfOne() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_MAINTENANCE, ENTERING_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 1);
    validate(rcnt, true, 0, false);
  }

  @Test
  void testOneHealthyThreeMaintenanceMinRepOfTwoInFlightAdd() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE,
        IN_MAINTENANCE, ENTERING_MAINTENANCE, IN_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 1, 0, 3, 2);
    validate(rcnt, false, 0, false);
  }

  @Test
  void testAllMaintenance() {
    Set<ContainerReplica> replica =
        registerNodes(IN_MAINTENANCE, ENTERING_MAINTENANCE, IN_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 2, false);
  }

  /**
   * As we have exactly 3 healthy, but then an excess of maintenance copies
   * we ignore the over-replication caused by the maintenance copies until they
   * come back online, and then deal with them.
   */
  @Test
  void testThreeHealthyTwoInMaintenance() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE, IN_SERVICE,
        IN_SERVICE, IN_MAINTENANCE, ENTERING_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, true, 0, false);
  }

  /**
   * This is somewhat similar to testThreeHealthyTwoInMaintenance() except now
   * one of the maintenance copies has become healthy and we will need to remove
   * the over-replicated healthy container.
   */
  @Test
  void testFourHealthyOneInMaintenance() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE, IN_SERVICE,
            IN_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, true, -1, true);
    assertTrue(rcnt.isSafelyOverReplicated());
  }

  @Test
  void testOneMaintenanceMinRepOfTwoRepFactorOne() {
    Set<ContainerReplica> replica = registerNodes(IN_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 1, 2);
    validate(rcnt, false, 1, false);
  }

  @Test
  void testOneMaintenanceMinRepOfTwoRepFactorOneInFlightAdd() {
    Set<ContainerReplica> replica = registerNodes(IN_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 1, 0, 1, 2);
    validate(rcnt, false, 0, false);
  }

  @Test
  void testOneHealthyOneMaintenanceRepFactorOne() {
    Set<ContainerReplica> replica = registerNodes(IN_MAINTENANCE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 1, 2);
    validate(rcnt, true, 0, false);
  }

  @Test
  void testTwoDecomTwoMaintenanceOneInflightAdd() {
    Set<ContainerReplica> replica =
        registerNodes(DECOMMISSIONED, DECOMMISSIONING,
            IN_MAINTENANCE, ENTERING_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 1, 0, 3, 2);
    validate(rcnt, false, 1, false);
  }

  @Test
  void testHealthyContainerIsHealthy() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    assertTrue(rcnt.isHealthy());
  }

  @Test
  void testIsHealthyWithDifferentReplicaStateNotHealthy() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE);
    for (ContainerReplica r : replica) {
      DatanodeDetails dn = r.getDatanodeDetails();

      ContainerReplica replace = new ContainerReplica.ContainerReplicaBuilder()
          .setContainerID(ContainerID.valueOf(1))
          .setContainerState(OPEN)
          .setDatanodeDetails(dn)
          .setOriginNodeId(dn.getUuid())
          .setSequenceId(1)
          .build();
      replica.remove(r);
      replica.add(replace);
      break;
    }
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    assertFalse(rcnt.isHealthy());
  }

  @Test
  void testSufficientReplicationWithMismatchedReplicaState() {
    ContainerInfo container =
        createContainerInfo(RatisReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.THREE), 1L,
            HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas =
        createReplicas(ContainerID.valueOf(1L), CLOSED, 0, 0);
    replicas.add(createContainerReplica(ContainerID.valueOf(1L), 0,
        IN_SERVICE, CLOSING));

    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replicas,
            Collections.emptyList(), 2, false);
    validate(rcnt, true, 0, false);
  }

  @Test
  void testReplicaCounts() {
    ContainerInfo container =
        createContainerInfo(RatisReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.THREE), 1L,
            HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas =
        createReplicas(ContainerID.valueOf(1L), CLOSED, 0, 0);
    replicas.add(createContainerReplica(ContainerID.valueOf(1L), 0,
        IN_SERVICE, CLOSING));
    replicas.add(createContainerReplica(ContainerID.valueOf(1L), 0,
        IN_SERVICE, UNHEALTHY));

    // First, test by not considering UNHEALTHY replicas.
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replicas,
            Collections.emptyList(), 2, false);
    assertTrue(rcnt.isSufficientlyReplicated());
    assertFalse(rcnt.isOverReplicated());
    assertEquals(0, rcnt.getExcessRedundancy(true));

    // CLOSED + CLOSED + CLOSING = 3
    assertEquals(3, rcnt.getHealthyReplicaCount());
    // CLOSING = 1
    assertEquals(1, rcnt.getMisMatchedReplicaCount());
    // CLOSED + CLOSED = 2
    assertEquals(2, rcnt.getMatchingReplicaCount());
    // UNHEALTHY = 1
    assertEquals(1, rcnt.getUnhealthyReplicaCount());

    // Now, test by considering UNHEALTHY replicas
    rcnt = new RatisContainerReplicaCount(container, replicas,
        Collections.emptyList(), 2, true);
    assertTrue(rcnt.isSufficientlyReplicated());
    assertTrue(rcnt.isOverReplicated());
    assertEquals(1, rcnt.getExcessRedundancy(true));

    // CLOSED + CLOSED + CLOSING = 3
    assertEquals(3, rcnt.getHealthyReplicaCount());
    // CLOSING = 1
    assertEquals(1, rcnt.getMisMatchedReplicaCount());
    // CLOSED + CLOSED = 2
    assertEquals(2, rcnt.getMatchingReplicaCount());
    // UNHEALTHY = 1
    assertEquals(1, rcnt.getUnhealthyReplicaCount());
  }

  @Test
  void testUnhealthyReplicaOnDecommissionedNodeWithPendingDelete() {
    ContainerInfo container =
        createContainerInfo(RatisReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.THREE), 1L,
            HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas =
        createReplicas(ContainerID.valueOf(1L), CLOSED, 0, 0);
    replicas.add(createContainerReplica(ContainerID.valueOf(1L), 0,
        IN_SERVICE, CLOSING));
    ContainerReplica unhealthyReplica =
        createContainerReplica(ContainerID.valueOf(1L), 0,
            DECOMMISSIONED, UNHEALTHY);
    replicas.add(unhealthyReplica);

    // First, test by not considering UNHEALTHY replicas.
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replicas,
            Collections.emptyList(), 2, false);
    assertTrue(rcnt.isSufficientlyReplicated());
    assertFalse(rcnt.isOverReplicated());
    assertEquals(0, rcnt.getExcessRedundancy(true));

    // CLOSED + CLOSED + CLOSING = 3
    assertEquals(3, rcnt.getHealthyReplicaCount());
    // CLOSING = 1
    assertEquals(1, rcnt.getMisMatchedReplicaCount());
    // CLOSED + CLOSED = 2
    assertEquals(2, rcnt.getMatchingReplicaCount());
    // UNHEALTHY should be 0 because it is counted as decommissioned
    assertEquals(0, rcnt.getUnhealthyReplicaCount());
    // 1 because the UNHEALTHY replica is on a decommissioned node
    assertEquals(1, rcnt.getDecommissionCount());

    // Now, test by considering UNHEALTHY replicas
    rcnt = new RatisContainerReplicaCount(container, replicas,
        Collections.emptyList(), 2, true);
    assertTrue(rcnt.isSufficientlyReplicated());
    assertFalse(rcnt.isOverReplicated());
    assertEquals(0, rcnt.getExcessRedundancy(true));

    // CLOSED + CLOSED + CLOSING = 3
    assertEquals(3, rcnt.getHealthyReplicaCount());
    // CLOSING = 1
    assertEquals(1, rcnt.getMisMatchedReplicaCount());
    // CLOSED + CLOSED = 2
    assertEquals(2, rcnt.getMatchingReplicaCount());
    // UNHEALTHY should be 0 because it is counted as decommissioned
    assertEquals(0, rcnt.getUnhealthyReplicaCount());
    assertEquals(1, rcnt.getDecommissionCount());
  }

  /**
   * There is a CLOSED container with 3 CLOSED replicas and 1 UNHEALTHY
   * replica. There is a pending delete on the UNHEALTHY replica.
   * Expectation: If considerUnhealthy in RatisContainerReplicaCount is
   * false, the pending delete on the UNHEALTHY replica should be ignored.
   * The container should be sufficiently replicated.
   * If considerUnhealthy is true, the pending delete should be considered,
   * but the container is still sufficiently replicated because we have
   * enough CLOSED replicas.
   */
  @Test
  void testSufficientReplicationWithPendingDeleteOnUnhealthyReplica() {
    ContainerInfo container =
        createContainerInfo(RatisReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.THREE), 1L,
            HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas =
        createReplicas(container.containerID(), CLOSED, 0, 0, 0);
    ContainerReplica unhealthyReplica = createContainerReplica(
        ContainerID.valueOf(1L), 0, IN_SERVICE, UNHEALTHY);
    replicas.add(unhealthyReplica);

    List<ContainerReplicaOp> ops = new ArrayList<>();
    ops.add(ContainerReplicaOp.create(ContainerReplicaOp.PendingOpType.DELETE,
        unhealthyReplica.getDatanodeDetails(), 0));
    RatisContainerReplicaCount withoutUnhealthy =
        new RatisContainerReplicaCount(container, replicas, ops, 2, false);
    validate(withoutUnhealthy, true, 0, false);

    RatisContainerReplicaCount withUnhealthy =
        new RatisContainerReplicaCount(container, replicas, ops, 2, true);
    validate(withUnhealthy, true, 0, false);
  }

  @Test
  void testIsHealthyWithMaintReplicaIsHealthy() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_MAINTENANCE,
            ENTERING_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    assertTrue(rcnt.isHealthy());
  }

  @Test
  void testContainerWithNoReplicasIsMissing() {
    Set<ContainerReplica> replica = new HashSet<>();
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    assertTrue(rcnt.isUnrecoverable());
    assertFalse(rcnt.isSufficientlyReplicated());
  }

  @Test
  void testOverReplicatedWithAndWithoutPending() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE, IN_SERVICE,
        IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 2, 3, 2);
    assertTrue(rcnt.isOverReplicated(false));
    assertFalse(rcnt.isOverReplicated(true));
    assertEquals(2, rcnt.getExcessRedundancy(false));
    assertEquals(0, rcnt.getExcessRedundancy(true));
  }

  /**
   * A container is safely over replicated if:
   * 1. It is over replicated.
   * 2. Has at least replication factor number of matching replicas.
   * 3. # matching replicas - replication factor >= pending deletes.
   */
  @Test
  void testSafelyOverReplicated() {
    /*
    First case: 3 CLOSED, 2 UNHEALTHY, 1 pending delete.
    Expectation: Not safely over replicated because rule 3 is violated.
     */
    ContainerInfo container =
        createContainerInfo(RatisReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.THREE), 1L,
            HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas =
        createReplicas(container.containerID(), CLOSED, 0, 0, 0);
    Set<ContainerReplica> unhealthyReplicas =
        createReplicas(container.containerID(), UNHEALTHY, 0, 0);
    replicas.addAll(unhealthyReplicas);
    List<ContainerReplicaOp> ops = new ArrayList<>();
    ops.add(ContainerReplicaOp.create(ContainerReplicaOp.PendingOpType.DELETE,
        unhealthyReplicas.iterator().next().getDatanodeDetails(), 0));

    RatisContainerReplicaCount withoutUnhealthy =
        new RatisContainerReplicaCount(container, replicas, ops, 2, false);
    validate(withoutUnhealthy, true, 0, false);
    assertFalse(withoutUnhealthy.isSafelyOverReplicated());

    RatisContainerReplicaCount withUnhealthy =
        new RatisContainerReplicaCount(container, replicas, ops, 2, true);
    validate(withUnhealthy, true, -1, true);
    assertFalse(withUnhealthy.isSafelyOverReplicated());

    /*
    Second case: 2 CLOSED, 1 CLOSING, 1 UNHEALTHY
     */
    container = createContainerInfo(RatisReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.THREE), 1L,
            HddsProtos.LifeCycleState.CLOSED);
    replicas = createReplicas(container.containerID(), CLOSED, 0, 0);
    ContainerReplica unhealthyReplica =
        createContainerReplica(container.containerID(), 0, IN_SERVICE,
            UNHEALTHY);
    ContainerReplica misMatchedReplica =
        createContainerReplica(container.containerID(), 0, IN_SERVICE, CLOSING);
    replicas.add(unhealthyReplica);
    replicas.add(misMatchedReplica);

    withoutUnhealthy =
        new RatisContainerReplicaCount(container, replicas,
            Collections.emptyList(), 2,
            false);
    validate(withoutUnhealthy, true, 0, false);
    assertFalse(withoutUnhealthy.isSafelyOverReplicated());

    withUnhealthy =
        new RatisContainerReplicaCount(container, replicas,
            Collections.emptyList(), 2,
            true);
    validate(withUnhealthy, true, -1, true);
    // Violates rule 2
    assertFalse(withUnhealthy.isSafelyOverReplicated());
    // now check by adding a CLOSED replica
    replicas.add(createContainerReplica(container.containerID(), 0,
        IN_SERVICE, CLOSED));
    withUnhealthy =
        new RatisContainerReplicaCount(container, replicas,
            Collections.emptyList(), 2,
            true);
    validate(withUnhealthy, true, -2, true);
    assertTrue(withUnhealthy.isSafelyOverReplicated());
  }

  @Test
  void testRemainingRedundancy() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE, IN_SERVICE,
        IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 1, 3, 2);
    Assert.assertEquals(2, rcnt.getRemainingRedundancy());
    replica = registerNodes(IN_SERVICE);
    rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    Assert.assertEquals(0, rcnt.getRemainingRedundancy());
    rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 1, 3, 2);
    Assert.assertEquals(0, rcnt.getRemainingRedundancy());
  }

  @Test
  void testSufficientlyReplicatedWithAndWithoutPending() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    assertFalse(rcnt.isSufficientlyReplicated(true));
    assertFalse(rcnt.isSufficientlyReplicated(false));

    rcnt =
        new RatisContainerReplicaCount(container, replica, 1, 0, 3, 2);
    assertTrue(rcnt.isSufficientlyReplicated(true));
    assertFalse(rcnt.isSufficientlyReplicated(false));

    replica = registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE);
    rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 1, 3, 2);
    assertFalse(rcnt.isSufficientlyReplicated(false));
    assertFalse(rcnt.isSufficientlyReplicated(true));
    rcnt =
        new RatisContainerReplicaCount(container, replica, 1, 1, 3, 2);
    assertFalse(rcnt.isSufficientlyReplicated(false));
    assertTrue(rcnt.isSufficientlyReplicated(true));

  }

  private void validate(RatisContainerReplicaCount rcnt,
      boolean sufficientlyReplicated, int replicaDelta,
      boolean overReplicated) {
    assertEquals(sufficientlyReplicated, rcnt.isSufficientlyReplicated());
    assertEquals(overReplicated, rcnt.isOverReplicated());
    assertEquals(replicaDelta, rcnt.additionalReplicaNeeded());
  }

  private Set<ContainerReplica> registerNodes(
      HddsProtos.NodeOperationalState... states) {
    Set<ContainerReplica> replica = new HashSet<>();
    for (HddsProtos.NodeOperationalState s : states) {
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      dn.setPersistedOpState(s);
      replica.add(new ContainerReplica.ContainerReplicaBuilder()
          .setContainerID(ContainerID.valueOf(1))
          .setContainerState(CLOSED)
          .setDatanodeDetails(dn)
          .setOriginNodeId(dn.getUuid())
          .setSequenceId(1)
          .build());
    }
    return replica;
  }

  private ContainerInfo createContainer(HddsProtos.LifeCycleState state) {
    return new ContainerInfo.Builder()
        .setContainerID(ContainerID.valueOf(1).getId())
        .setState(state)
        .build();
  }
}
