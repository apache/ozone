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
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSING;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.OPEN;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.QUASI_CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.UNHEALTHY;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerInfo;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerReplica;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createReplicas;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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
    validate(rcnt, true, 0, false, false);
  }

  @Test
  void testTwoHealthyReplica() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 1, false, false);
  }

  @Test
  void testOneHealthyReplica() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 2, false, false);
  }

  @Test
  void testTwoHealthyAndInflightAdd() {

    Set<ContainerReplica> replica = registerNodes(IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 1, 0, 3, 2);
    validate(rcnt, false, 0, false, false);
  }

  /**
   * This does not schedule a container to be removed, as the inFlight add may
   * fail and then the delete would make things under-replicated. Once the add
   * completes there will be 4 healthy, and it will get taken care of then.
   */
  @Test
  void testThreeHealthyAndInflightAdd() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 1, 0, 3, 2);
    validate(rcnt, true, 0, false, false);
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
    validate(rcnt, false, 1, false, false);
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
    validate(rcnt, false, 0, false, false);
  }

  @Test
  void testFourHealthyReplicas() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, true, -1, true, false);
  }

  @Test
  void testFourHealthyReplicasAndInFlightDelete() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 1, 3, 2);
    validate(rcnt, true, 0, false, false);
  }

  @Test
  void testFourHealthyReplicasAndTwoInFlightDelete() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 2, 3, 2);
    validate(rcnt, false, 1, false, false);
  }

  @Test
  void testOneHealthyReplicaRepFactorOne() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 1, 2);
    validate(rcnt, true, 0, false, false);
  }

  @Test
  void testOneHealthyReplicaRepFactorOneInFlightDelete() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 1, 1, 2);
    validate(rcnt, false, 1, false, false);
  }

  @Test
  void testTwoHealthyReplicaTwoInflightAdd() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 2, 0, 3, 2);
    validate(rcnt, false, 0, false, false);
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
    validate(rcnt, true, 0, false, false);
  }

  @ParameterizedTest
  @MethodSource("org.apache.hadoop.hdds.scm.node.NodeStatus#decommissionStates")
  void testOneDecommissionReplica(HddsProtos.NodeOperationalState state) {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, state);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 1, false, true);
  }

  @Test
  void testTwoHealthyOneDecommissionedneInFlightAdd() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_SERVICE, DECOMMISSIONED);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 1, 0, 3, 2);
    validate(rcnt, false, 0, false, true);
  }

  @ParameterizedTest
  @MethodSource("org.apache.hadoop.hdds.scm.node.NodeStatus#decommissionStates")
  void testAllDecommissioned(HddsProtos.NodeOperationalState state) {
    Set<ContainerReplica> replica =
        registerNodes(state, state, state);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 3, false, true);
  }

  @Test
  void testAllDecommissionedRepFactorOne() {
    Set<ContainerReplica> replica = registerNodes(DECOMMISSIONED);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 1, 2);
    validate(rcnt, false, 1, false, true);

  }

  @Test
  void testAllDecommissionedRepFactorOneInFlightAdd() {
    Set<ContainerReplica> replica = registerNodes(DECOMMISSIONED);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 1, 0, 1, 2);
    validate(rcnt, false, 0, false, true);
  }

  @Test
  void testOneHealthyOneDecommissioningRepFactorOne() {
    Set<ContainerReplica> replica = registerNodes(DECOMMISSIONED, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 1, 2);
    validate(rcnt, true, 0, false, false);
  }

  /**
   * Maintenance tests from here.
   */

  @ParameterizedTest
  @MethodSource("org.apache.hadoop.hdds.scm.node.NodeStatus#maintenanceStates")
  void testOneHealthyTwoMaintenanceMinRepOfTwo(
      HddsProtos.NodeOperationalState state) {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, state, state);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 1, false, true);
  }

  @Test
  void testOneHealthyThreeMaintenanceMinRepOfTwo() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE,
        IN_MAINTENANCE, IN_MAINTENANCE, ENTERING_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 1, false, true);
  }

  @Test
  void testOneHealthyTwoMaintenanceMinRepOfOne() {
    Set<ContainerReplica> replica =
        registerNodes(IN_SERVICE, IN_MAINTENANCE, ENTERING_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 1);
    validate(rcnt, true, 0, false, false);
  }

  @Test
  void testOneHealthyThreeMaintenanceMinRepOfTwoInFlightAdd() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE,
        IN_MAINTENANCE, ENTERING_MAINTENANCE, IN_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 1, 0, 3, 2);
    validate(rcnt, false, 0, false, true);
  }

  @Test
  void testAllMaintenance() {
    Set<ContainerReplica> replica =
        registerNodes(IN_MAINTENANCE, ENTERING_MAINTENANCE, IN_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    validate(rcnt, false, 2, false, true);
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
    validate(rcnt, true, 0, false, false);
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
    validate(rcnt, true, -1, true, false);
    assertTrue(rcnt.isSafelyOverReplicated());
  }

  @Test
  void testOneMaintenanceMinRepOfTwoRepFactorOne() {
    Set<ContainerReplica> replica = registerNodes(IN_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 1, 2);
    validate(rcnt, false, 1, false, true);
  }

  @Test
  void testOneMaintenanceMinRepOfTwoRepFactorOneInFlightAdd() {
    Set<ContainerReplica> replica = registerNodes(IN_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 1, 0, 1, 2);
    validate(rcnt, false, 0, false, true);
  }

  @Test
  void testOneHealthyOneMaintenanceRepFactorOne() {
    Set<ContainerReplica> replica = registerNodes(IN_MAINTENANCE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 1, 2);
    validate(rcnt, true, 0, false, false);
  }

  @Test
  void testTwoDecomTwoMaintenanceOneInflightAdd() {
    Set<ContainerReplica> replica =
        registerNodes(DECOMMISSIONED, DECOMMISSIONING,
            IN_MAINTENANCE, ENTERING_MAINTENANCE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 1, 0, 3, 2);
    validate(rcnt, false, 1, false, true);
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
          .setOriginNodeId(dn.getID())
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
            THREE), 1L,
            HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas =
        createReplicas(ContainerID.valueOf(1L), State.CLOSED, 0, 0);
    replicas.add(createContainerReplica(ContainerID.valueOf(1L), 0,
        IN_SERVICE, CLOSING));

    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replicas,
            Collections.emptyList(), 2, false);
    validate(rcnt, true, 0, false, false);
  }

  @Test
  void testReplicaCounts() {
    ContainerInfo container =
        createContainerInfo(RatisReplicationConfig.getInstance(
                THREE), 1L,
            HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas =
        createReplicas(ContainerID.valueOf(1L), State.CLOSED, 0, 0);
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
                THREE), 1L,
            HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas =
        createReplicas(ContainerID.valueOf(1L), State.CLOSED, 0, 0);
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
    // UNHEALTHY decommissioned is counted, too
    assertEquals(1, rcnt.getUnhealthyReplicaCount());
    // due to considerUnhealthy=false
    assertEquals(0, rcnt.getDecommissionCount());

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
    // UNHEALTHY decommissioned is counted as unhealthy
    assertEquals(1, rcnt.getUnhealthyReplicaCount());
    // due to considerUnhealthy=true
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
            THREE), 1L,
            HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas =
        createReplicas(container.containerID(), State.CLOSED, 0, 0, 0);
    ContainerReplica unhealthyReplica = createContainerReplica(
        ContainerID.valueOf(1L), 0, IN_SERVICE, UNHEALTHY);
    replicas.add(unhealthyReplica);

    List<ContainerReplicaOp> ops = new ArrayList<>();
    ops.add(new ContainerReplicaOp(ContainerReplicaOp.PendingOpType.DELETE,
        unhealthyReplica.getDatanodeDetails(), 0, null, System.currentTimeMillis(), 0));
    RatisContainerReplicaCount withoutUnhealthy =
        new RatisContainerReplicaCount(container, replicas, ops, 2, false);
    validate(withoutUnhealthy, true, 0, false, false);

    RatisContainerReplicaCount withUnhealthy =
        new RatisContainerReplicaCount(container, replicas, ops, 2, true);
    validate(withUnhealthy, true, 0, false, false);
  }

  /**
   * Scenario: A CLOSED RATIS container with 2 CLOSED and 1 UNHEALTHY replicas.
   * Expectation: If considerUnhealthy is false, this container is not
   * sufficiently replicated and replicaDelta is 1.
   */
  @Test
  public void testUnderReplicationBecauseOfUnhealthyReplica() {
    ContainerInfo container =
        createContainerInfo(RatisReplicationConfig.getInstance(
                THREE), 1L,
            HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas =
        createReplicas(container.containerID(), State.CLOSED, 0, 0);
    ContainerReplica unhealthyReplica = createContainerReplica(
        ContainerID.valueOf(1L), 0, IN_SERVICE, UNHEALTHY);
    replicas.add(unhealthyReplica);
    RatisContainerReplicaCount withoutUnhealthy =
        new RatisContainerReplicaCount(container, replicas,
            Collections.emptyList(), 2,
            false);
    validate(withoutUnhealthy, false, 1, false, false);
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
   */
  @Test
  void testSafelyOverReplicated() {
    /*
    First case: 3 CLOSED, 2 UNHEALTHY, 1 pending delete.
     */
    ContainerInfo container =
        createContainerInfo(RatisReplicationConfig.getInstance(
                THREE), 1L,
            HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas =
        createReplicas(container.containerID(), State.CLOSED, 0, 0, 0);
    Set<ContainerReplica> unhealthyReplicas =
        createReplicas(container.containerID(), UNHEALTHY, 0, 0);
    replicas.addAll(unhealthyReplicas);
    List<ContainerReplicaOp> ops = new ArrayList<>();
    ops.add(new ContainerReplicaOp(ContainerReplicaOp.PendingOpType.DELETE,
        unhealthyReplicas.iterator().next().getDatanodeDetails(), 0, null, System.currentTimeMillis(), 0));

    RatisContainerReplicaCount withoutUnhealthy =
        new RatisContainerReplicaCount(container, replicas, ops, 2, false);
    validate(withoutUnhealthy, true, 0, false, false);
    // not safely over replicated (3 CLOSED - 1 pending delete)
    assertFalse(withoutUnhealthy.isSafelyOverReplicated());

    RatisContainerReplicaCount withUnhealthy =
        new RatisContainerReplicaCount(container, replicas, ops, 2, true);
    validate(withUnhealthy, true, -1, true, false);
    assertTrue(withUnhealthy.isSafelyOverReplicated());

    /*
    Second case: 2 CLOSED, 1 CLOSING, 1 UNHEALTHY
     */
    container = createContainerInfo(RatisReplicationConfig.getInstance(
                THREE), 1L,
        HddsProtos.LifeCycleState.CLOSED);
    replicas = createReplicas(container.containerID(), State.CLOSED, 0, 0);
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
    validate(withoutUnhealthy, true, 0, false, false);
    assertFalse(withoutUnhealthy.isSafelyOverReplicated());

    withUnhealthy =
        new RatisContainerReplicaCount(container, replicas,
            Collections.emptyList(), 2,
            true);
    validate(withUnhealthy, true, -1, true, false);
    // Violates rule 2
    assertFalse(withUnhealthy.isSafelyOverReplicated());
    // now check by adding a CLOSED replica
    replicas.add(createContainerReplica(container.containerID(), 0,
        IN_SERVICE, State.CLOSED));
    withUnhealthy = new RatisContainerReplicaCount(container, replicas,
        Collections.emptyList(), 2, true);
    validate(withUnhealthy, true, -2, true, false);
    assertTrue(withUnhealthy.isSafelyOverReplicated());
  }

  @Test
  void testRemainingRedundancy() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE, IN_SERVICE,
        IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 1, 3, 2);
    assertEquals(2, rcnt.getRemainingRedundancy());
    replica = registerNodes(IN_SERVICE);
    rcnt = new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    assertEquals(0, rcnt.getRemainingRedundancy());
    rcnt = new RatisContainerReplicaCount(container, replica, 0, 1, 3, 2);
    assertEquals(0, rcnt.getRemainingRedundancy());
  }

  @Test
  void testSufficientlyReplicatedWithAndWithoutPending() {
    Set<ContainerReplica> replica = registerNodes(IN_SERVICE, IN_SERVICE);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    RatisContainerReplicaCount rcnt =
        new RatisContainerReplicaCount(container, replica, 0, 0, 3, 2);
    assertFalse(rcnt.isSufficientlyReplicated(true));
    assertFalse(rcnt.isSufficientlyReplicated(false));

    rcnt = new RatisContainerReplicaCount(container, replica, 1, 0, 3, 2);
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

  @Test
  void testQuasiClosedReplicaWithCorrectSequenceID() {
    final ContainerInfo container =
        createContainerInfo(RatisReplicationConfig.getInstance(THREE), 1L,
            HddsProtos.LifeCycleState.CLOSED);
    final Set<ContainerReplica> replicas =
        createReplicas(container.containerID(), State.CLOSED, 0, 0);
    final Set<ContainerReplica> quasiClosedReplica =
        createReplicas(container.containerID(), QUASI_CLOSED, 0);
    replicas.addAll(quasiClosedReplica);

    final RatisContainerReplicaCount crc =
        new RatisContainerReplicaCount(container, replicas,
            Collections.emptyList(), 2, false);
    validate(crc, true, 0, false, false);
    assertTrue(crc.isSufficientlyReplicated(true));
    assertEquals(0, crc.getUnhealthyReplicaCount());

    // With additional unhealthy replica
    final Set<ContainerReplica> unhealthyReplica =
        createReplicas(container.containerID(), UNHEALTHY, 0);
    replicas.addAll(unhealthyReplica);

    final RatisContainerReplicaCount crcWithUnhealthy =
        new RatisContainerReplicaCount(container, replicas,
            Collections.emptyList(), 2, false);
    validate(crcWithUnhealthy, true, 0, false, false);
    assertTrue(crcWithUnhealthy.isSufficientlyReplicated(true));
    assertEquals(1, crcWithUnhealthy.getUnhealthyReplicaCount());
  }

  @Test
  void testQuasiClosedReplicaWithInCorrectSequenceID() {
    final long sequenceID = 101;
    final ContainerInfo container =
        createContainerInfo(RatisReplicationConfig.getInstance(THREE), 1L,
            HddsProtos.LifeCycleState.CLOSED, sequenceID);
    final ContainerID containerID = container.containerID();

    final ContainerReplica replicaOne = createContainerReplica(
        containerID, 0, IN_SERVICE, State.CLOSED, sequenceID);
    final ContainerReplica replicaTwo = createContainerReplica(
        containerID, 0, IN_SERVICE, State.CLOSED, sequenceID);

    final ContainerReplica quasiCloseReplica = createContainerReplica(
        containerID, 0, IN_SERVICE, State.QUASI_CLOSED, sequenceID - 5);

    final Set<ContainerReplica> replicas = new HashSet<>();
    replicas.add(replicaOne);
    replicas.add(replicaTwo);
    replicas.add(quasiCloseReplica);

    final RatisContainerReplicaCount crc =
        new RatisContainerReplicaCount(container, replicas,
            Collections.emptyList(), 2, false);
    validate(crc, false, 1, false, false);
    assertFalse(crc.isSufficientlyReplicated(true));
    assertEquals(1, crc.getUnhealthyReplicaCount());

    // With additional unhealthy replica

    final Set<ContainerReplica> unhealthyReplica =
        createReplicas(container.containerID(), UNHEALTHY, 0);
    replicas.addAll(unhealthyReplica);

    final RatisContainerReplicaCount crcWithUnhealthy =
        new RatisContainerReplicaCount(container, replicas,
            Collections.emptyList(), 2, false);
    validate(crcWithUnhealthy, false, 1, false, false);
    assertFalse(crcWithUnhealthy.isSufficientlyReplicated(true));
    assertEquals(2, crcWithUnhealthy.getUnhealthyReplicaCount());
  }

  private void validate(RatisContainerReplicaCount rcnt,
      boolean sufficientlyReplicated, int replicaDelta,
      boolean overReplicated, boolean insufficientDueToOutOfService) {
    assertEquals(sufficientlyReplicated, rcnt.isSufficientlyReplicated());
    assertEquals(overReplicated, rcnt.isOverReplicated());
    assertEquals(replicaDelta, rcnt.additionalReplicaNeeded());
    assertEquals(insufficientDueToOutOfService,
        rcnt.insufficientDueToOutOfService());
  }

  private Set<ContainerReplica> registerNodes(
      HddsProtos.NodeOperationalState... states) {
    Set<ContainerReplica> replica = new HashSet<>();
    for (HddsProtos.NodeOperationalState s : states) {
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      dn.setPersistedOpState(s);
      replica.add(new ContainerReplica.ContainerReplicaBuilder()
          .setContainerID(ContainerID.valueOf(1))
          .setContainerState(State.CLOSED)
          .setDatanodeDetails(dn)
          .setOriginNodeId(dn.getID())
          .setSequenceId(1)
          .build());
    }
    return replica;
  }

  private ContainerInfo createContainer(HddsProtos.LifeCycleState state) {
    return new ContainerInfo.Builder()
        .setContainerID(1)
        .setState(state)
        .build();
  }
}
