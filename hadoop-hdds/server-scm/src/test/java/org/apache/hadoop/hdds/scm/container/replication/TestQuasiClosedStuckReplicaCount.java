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
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.QUASI_CLOSED;
import static org.apache.ratis.util.Preconditions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.junit.jupiter.api.Test;

/**
 * Tests for the QuasiClosedStuckReplicaCount class.
 */
public class TestQuasiClosedStuckReplicaCount {

  private final DatanodeID origin1 = DatanodeID.randomID();
  private final DatanodeID origin2 = DatanodeID.randomID();
  private final DatanodeID origin3 = DatanodeID.randomID();

  @Test
  public void testCorrectlyReplicationWithThreeOrigins() {
    // origin1 is best (BCSID=10) with 3 copies at target; origin2 and origin3 are other (BCSID=5)
    // with 2 copies each at target.
    Set<ContainerReplica> replicas = new HashSet<>();
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin1,
        IN_SERVICE, QUASI_CLOSED, 10, 3);
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin2,
        IN_SERVICE, QUASI_CLOSED, 5, 2);
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin3,
        IN_SERVICE, QUASI_CLOSED, 5, 2);

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    assertTrue(replicaCount.getUnderReplicatedReplicas().isEmpty());
  }

  @Test
  public void testCorrectReplicationWithTwoOrigins() {
    // origin1 is best (BCSID=10) with 3 copies at target; origin2 is other (BCSID=5) with 2 copies at target.
    Set<ContainerReplica> replicas = new HashSet<>();
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin1,
        IN_SERVICE, QUASI_CLOSED, 10, 3);
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin2,
        IN_SERVICE, QUASI_CLOSED, 5, 2);

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    assertTrue(replicaCount.getUnderReplicatedReplicas().isEmpty());
  }

  @Test
  public void testCorrectReplicationWithOneOrigin() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    assertTrue(replicaCount.getUnderReplicatedReplicas().isEmpty());
  }

  @Test
  public void testUnderReplicationWithThreeOrigins() {
    // origin1 is best (BCSID=10) with 3 copies at target.
    // origin2 is other (BCSID=5) with 2 copies at target.
    // origin3 is other (BCSID=5) with only 1 copy, so it is under-replicated by 1.
    Set<ContainerReplica> replicas = new HashSet<>();
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin1,
        IN_SERVICE, QUASI_CLOSED, 10, 3);
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin2,
        IN_SERVICE, QUASI_CLOSED, 5, 2);
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin3,
        IN_SERVICE, QUASI_CLOSED, 5, 1);

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertTrue(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getUnderReplicatedReplicas(), 1, 1, 1, origin3);
  }

  @Test
  public void testUnderReplicationWithThreeOriginsTwoUnderReplicated() {
    // origin2 is best (BCSID=10) with 3 copies at target.
    // origin1 and origin3 are other (BCSID=5) with only 1 copy each, so both are under-replicated.
    Set<ContainerReplica> replicas = new HashSet<>();
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin1,
        IN_SERVICE, QUASI_CLOSED, 5, 1);
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin2,
        IN_SERVICE, QUASI_CLOSED, 10, 3);
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin3,
        IN_SERVICE, QUASI_CLOSED, 5, 1);

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertTrue(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());

    List<QuasiClosedStuckReplicaCount.MisReplicatedOrigin> misReplicatedOrigins =
        replicaCount.getUnderReplicatedReplicas();
    assertEquals(2, misReplicatedOrigins.size());

    for (QuasiClosedStuckReplicaCount.MisReplicatedOrigin misReplicatedOrigin : misReplicatedOrigins) {
      final DatanodeID source = misReplicatedOrigin.getSources().iterator().next().getOriginDatanodeId();
      assertTrue(source.equals(origin1) || source.equals(origin3));
    }
  }

  @Test
  public void testUnderReplicationWithTwoOrigins() {
    // origin1 is best (BCSID=10) with 3 copies at target.
    // origin2 is other (BCSID=5) with only 1 copy, so it is under-replicated by 1.
    Set<ContainerReplica> replicas = new HashSet<>();
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin1,
        IN_SERVICE, QUASI_CLOSED, 10, 3);
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin2,
        IN_SERVICE, QUASI_CLOSED, 5, 1);

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertTrue(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getUnderReplicatedReplicas(), 1, 1, 1, origin2);
  }

  @Test
  public void testUnderReplicationWithOneOrigin() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertTrue(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getUnderReplicatedReplicas(), 1, 1, 2, origin1);
  }

  @Test
  public void testOverReplicationWithThreeOrigins() {
    // origin1 is best (BCSID=10) with 3 copies at target.
    // origin2 is other (BCSID=5) with 2 copies at target.
    // origin3 is other (BCSID=5) with 3 copies, exceeding its target of 2 by 1.
    Set<ContainerReplica> replicas = new HashSet<>();
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin1,
        IN_SERVICE, QUASI_CLOSED, 10, 3);
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin2,
        IN_SERVICE, QUASI_CLOSED, 5, 2);
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin3,
        IN_SERVICE, QUASI_CLOSED, 5, 3);

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertFalse(replicaCount.isUnderReplicated());
    assertTrue(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getOverReplicatedOrigins(), 1, 3, 1, origin3);
  }

  @Test
  public void testOverReplicationWithTwoOrigins() {
    // origin1 is best (BCSID=10) with 3 copies at target.
    // origin2 is other (BCSID=5) with 3 copies, exceeding its target of 2 by 1.
    Set<ContainerReplica> replicas = new HashSet<>();
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin1,
        IN_SERVICE, QUASI_CLOSED, 10, 3);
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin2,
        IN_SERVICE, QUASI_CLOSED, 5, 3);

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertFalse(replicaCount.isUnderReplicated());
    assertTrue(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getOverReplicatedOrigins(), 1, 3, 1, origin2);
  }

  @Test
  public void testOverReplicationWithOneOrigin() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE),
        Pair.of(origin1, IN_SERVICE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertFalse(replicaCount.isUnderReplicated());
    assertTrue(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getOverReplicatedOrigins(), 1, 4, 1, origin1);
  }

  @Test
  public void testUnderReplicationDueToDecommissionWithThreeOrigins() {
    // origin1 is best (BCSID=10) with 2 decommissioning copies and 0 in-service, so it is under-replicated by 3.
    // origin2 and origin3 are other (BCSID=5) with 2 in-service copies each at target 2.
    Set<ContainerReplica> replicas = new HashSet<>();
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin1,
        DECOMMISSIONING, QUASI_CLOSED, 10, 2);
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin2,
        IN_SERVICE, QUASI_CLOSED, 5, 2);
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin3,
        IN_SERVICE, QUASI_CLOSED, 5, 2);

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertTrue(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getUnderReplicatedReplicas(), 1, 2, 3, origin1);
  }

  @Test
  public void testUnderReplicationDueToDecommissionWithTwoOrigins() {
    // origin1 is best (BCSID=10) with 1 in-service and 1 decommissioning copy; it needs 2 more to reach target 3.
    // origin2 is other (BCSID=5) with 2 in-service copies at target 2.
    Set<ContainerReplica> replicas = new HashSet<>();
    replicas.add(ReplicationTestUtil.createReplicaWithOriginAndSeqId(ContainerID.valueOf(1), origin1,
        IN_SERVICE, QUASI_CLOSED, 10));
    replicas.add(ReplicationTestUtil.createReplicaWithOriginAndSeqId(ContainerID.valueOf(1), origin1,
        DECOMMISSIONING, QUASI_CLOSED, 10));
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin2,
        IN_SERVICE, QUASI_CLOSED, 5, 2);

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertTrue(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getUnderReplicatedReplicas(), 1, 2, 2, origin1);
  }

  @Test
  public void testUnderReplicationDueToDecommissionWithOneOrigin() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, DECOMMISSIONING), Pair.of(origin1, DECOMMISSIONING));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertTrue(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getUnderReplicatedReplicas(), 1, 3, 2, origin1);
  }

  @Test
  public void testNoOverReplicationWithOutOfServiceReplicasWithThreeOrigins() {
    // origin1 is best (BCSID=10) with 3 in-service copies at target plus 1 decommissioned.
    // The decommissioned replica is out-of-service and must not be counted toward over-replication.
    // origin2 and origin3 are other (BCSID=5) with 2 in-service copies each at target 2.
    Set<ContainerReplica> replicas = new HashSet<>();
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin1,
        IN_SERVICE, QUASI_CLOSED, 10, 3);
    replicas.add(ReplicationTestUtil.createReplicaWithOriginAndSeqId(ContainerID.valueOf(1), origin1,
        DECOMMISSIONED, QUASI_CLOSED, 10));
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin2,
        IN_SERVICE, QUASI_CLOSED, 5, 2);
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin3,
        IN_SERVICE, QUASI_CLOSED, 5, 2);

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
  }

  @Test
  public void testNoOverReplicationWithOutOfServiceReplicasWithTwoOrigins() {
    // origin1 is best (BCSID=10) with 3 in-service copies at target plus 1 decommissioned.
    // origin2 is other (BCSID=5) with 2 in-service copies at target 2.
    Set<ContainerReplica> replicas = new HashSet<>();
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin1,
        IN_SERVICE, QUASI_CLOSED, 10, 3);
    replicas.add(ReplicationTestUtil.createReplicaWithOriginAndSeqId(ContainerID.valueOf(1), origin1,
        DECOMMISSIONED, QUASI_CLOSED, 10));
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin2,
        IN_SERVICE, QUASI_CLOSED, 5, 2);

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
  }

  @Test
  public void testNoOverReplicationWithOutOfServiceReplicasWithOneOrigin() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE),
        Pair.of(origin1, DECOMMISSIONED));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
  }

  @Test
  public void testUnderReplicationWithMaintenanceWithOneOrigin() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE), Pair.of(origin1, ENTERING_MAINTENANCE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());

    replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, ENTERING_MAINTENANCE), Pair.of(origin1, ENTERING_MAINTENANCE));

    replicaCount = new QuasiClosedStuckReplicaCount(replicas, 2, 3, 2);
    assertTrue(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getUnderReplicatedReplicas(), 1, 3, 1, origin1);
  }

  @Test
  public void testUnderReplicationWithMaintenanceWithTwoOrigins() {
    // origin1 is best (BCSID=10) with 1 in-service and 1 entering-maintenance copy.
    // Because origin1 has at least 1 in-service replica while under maintenance, it is not under-replicated.
    // origin2 is other (BCSID=5) with 2 in-service copies at target 2.
    Set<ContainerReplica> replicas = new HashSet<>();
    replicas.add(ReplicationTestUtil.createReplicaWithOriginAndSeqId(ContainerID.valueOf(1), origin1,
        IN_SERVICE, QUASI_CLOSED, 10));
    replicas.add(ReplicationTestUtil.createReplicaWithOriginAndSeqId(ContainerID.valueOf(1), origin1,
        ENTERING_MAINTENANCE, QUASI_CLOSED, 10));
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin2,
        IN_SERVICE, QUASI_CLOSED, 5, 2);

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());

    // When all of origin1's replicas are entering-maintenance it has no in-service copies,
    // so it is under-replicated by 1 (the maintenance path requires at least 1 online copy).
    replicas = new HashSet<>();
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin1,
        ENTERING_MAINTENANCE, QUASI_CLOSED, 10, 2);
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin2,
        IN_SERVICE, QUASI_CLOSED, 5, 2);

    replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertTrue(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getUnderReplicatedReplicas(), 1, 2, 1, origin1);
  }

  @Test
  public void testNoOverReplicationWithExcessMaintenanceReplicasTwoOrigins() {
    // origin1 is best (BCSID=10) with 2 in-service copies and 1 in-maintenance copy.
    // The maintenance replica is excluded from the over-replication check.
    // origin2 is other (BCSID=5) with 2 in-service copies at target 2.
    Set<ContainerReplica> replicas = new HashSet<>();
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin1,
        IN_SERVICE, QUASI_CLOSED, 10, 2);
    replicas.add(ReplicationTestUtil.createReplicaWithOriginAndSeqId(ContainerID.valueOf(1), origin1,
        IN_MAINTENANCE, QUASI_CLOSED, 10));
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin2,
        IN_SERVICE, QUASI_CLOSED, 5, 2);

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
  }

  @Test
  public void testNoOverReplicationWithExcessMaintenanceReplicasOneOrigin() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE),
        Pair.of(origin1, IN_MAINTENANCE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
  }

  @Test
  public void testShiftInBestOriginOnNewHigherBcsid() {
    // Initial state: origin1 is best (BCSID=10, target=3) with 3 copies;
    // origin2 is other (BCSID=5, target=2) with 2 copies. Both are at target.
    Set<ContainerReplica> replicas = new HashSet<>();
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin1,
        IN_SERVICE, QUASI_CLOSED, 10, 3);
    ReplicationTestUtil.addReplicasWithOriginAndSeqId(replicas, ContainerID.valueOf(1), origin2,
        IN_SERVICE, QUASI_CLOSED, 5, 2);

    QuasiClosedStuckReplicaCount initialCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);
    assertFalse(initialCount.isUnderReplicated());
    assertFalse(initialCount.isOverReplicated());

    // A new replica arrives from origin3 with BCSID=20, higher than the previous best of 10.
    // origin3 is now the best origin (BCSID=20, target=3) but has only 1 copy: under-replicated by 2.
    // origin1 is now an other origin (BCSID=10, target=2) but still has 3 copies: over-replicated by 1.
    replicas.add(ReplicationTestUtil.createReplicaWithOriginAndSeqId(ContainerID.valueOf(1), origin3,
        IN_SERVICE, QUASI_CLOSED, 20));

    QuasiClosedStuckReplicaCount updatedCount = new QuasiClosedStuckReplicaCount(replicas, 1, 3, 2);

    // The new best origin (origin3, BCSID=20) is under-replicated and must receive more copies.
    assertTrue(updatedCount.isUnderReplicated());
    List<QuasiClosedStuckReplicaCount.MisReplicatedOrigin> underRep = updatedCount.getUnderReplicatedReplicas();
    assertEquals(1, underRep.size());
    assertEquals(2, underRep.get(0).getReplicaDelta());
    assertTrue(underRep.get(0).getSources().stream()
        .allMatch(r -> r.getOriginDatanodeId().equals(origin3)));

    // The old best origin (origin1, BCSID=10) is now over-replicated and one copy must be deleted.
    assertTrue(updatedCount.isOverReplicated());
    List<QuasiClosedStuckReplicaCount.MisReplicatedOrigin> overRep = updatedCount.getOverReplicatedOrigins();
    assertEquals(1, overRep.size());
    assertEquals(1, overRep.get(0).getReplicaDelta());
    assertTrue(overRep.get(0).getSources().stream()
        .allMatch(r -> r.getOriginDatanodeId().equals(origin1)));
  }

  private void validateMisReplicatedOrigins(
      List<QuasiClosedStuckReplicaCount.MisReplicatedOrigin> misReplicatedOrigins,
      int expectedUnderRepOrigins, int expectedSources, int expectedDelta, DatanodeID expectedOrigin) {

    assertTrue(misReplicatedOrigins.size() == expectedUnderRepOrigins);
    Set<ContainerReplica> sources = misReplicatedOrigins.get(0).getSources();
    assertEquals(sources.size(), expectedSources);
    for (ContainerReplica source : sources) {
      assertTrue(source.getOriginDatanodeId().equals(expectedOrigin));
    }
    assertTrue(misReplicatedOrigins.get(0).getReplicaDelta() == expectedDelta);
  }

}
