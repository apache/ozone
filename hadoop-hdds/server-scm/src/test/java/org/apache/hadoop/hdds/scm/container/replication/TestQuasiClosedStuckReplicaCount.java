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
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE),
        Pair.of(origin2, IN_SERVICE), Pair.of(origin2, IN_SERVICE),
        Pair.of(origin3, IN_SERVICE), Pair.of(origin3, IN_SERVICE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    assertTrue(replicaCount.getUnderReplicatedReplicas().isEmpty());
  }

  @Test
  public void testCorrectReplicationWithTwoOrigins() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE),
        Pair.of(origin2, IN_SERVICE), Pair.of(origin2, IN_SERVICE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    assertTrue(replicaCount.getUnderReplicatedReplicas().isEmpty());
  }

  @Test
  public void testCorrectReplicationWithOneOrigin() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    assertTrue(replicaCount.getUnderReplicatedReplicas().isEmpty());
  }

  @Test
  public void testUnderReplicationWithThreeOrigins() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE),
        Pair.of(origin2, IN_SERVICE), Pair.of(origin2, IN_SERVICE),
        Pair.of(origin3, IN_SERVICE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertTrue(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getUnderReplicatedReplicas(), 1, 1, 1, origin3);
  }

  @Test
  public void testUnderReplicationWithThreeOriginsTwoUnderReplicated() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE),
        Pair.of(origin2, IN_SERVICE), Pair.of(origin2, IN_SERVICE),
        Pair.of(origin3, IN_SERVICE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertTrue(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());

    List<QuasiClosedStuckReplicaCount.MisReplicatedOrigin> misReplicatedOrigins =
        replicaCount.getUnderReplicatedReplicas();
    assertTrue(misReplicatedOrigins.size() == 2);

    for (QuasiClosedStuckReplicaCount.MisReplicatedOrigin misReplicatedOrigin : misReplicatedOrigins) {
      final DatanodeID source = misReplicatedOrigin.getSources().iterator().next().getOriginDatanodeId();
      assertTrue(source.equals(origin1) || source.equals(origin3));
    }
  }

  @Test
  public void testUnderReplicationWithTwoOrigins() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE),
        Pair.of(origin2, IN_SERVICE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertTrue(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getUnderReplicatedReplicas(), 1, 1, 1, origin2);
  }

  @Test
  public void testUnderReplicationWithOneOrigin() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertTrue(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getUnderReplicatedReplicas(), 1, 1, 2, origin1);
  }

  @Test
  public void testOverReplicationWithThreeOrigins() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE),
        Pair.of(origin2, IN_SERVICE), Pair.of(origin2, IN_SERVICE),
        Pair.of(origin3, IN_SERVICE), Pair.of(origin3, IN_SERVICE), Pair.of(origin3, IN_SERVICE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertFalse(replicaCount.isUnderReplicated());
    assertTrue(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getOverReplicatedOrigins(), 1, 3, 1, origin3);
  }

  @Test
  public void testOverReplicationWithTwoOrigins() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE),
        Pair.of(origin2, IN_SERVICE), Pair.of(origin2, IN_SERVICE), Pair.of(origin2, IN_SERVICE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
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

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertFalse(replicaCount.isUnderReplicated());
    assertTrue(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getOverReplicatedOrigins(), 1, 4, 1, origin1);
  }

  @Test
  public void testUnderReplicationDueToDecommissionWithThreeOrigins() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, DECOMMISSIONING), Pair.of(origin1, DECOMMISSIONING),
        Pair.of(origin2, IN_SERVICE), Pair.of(origin2, IN_SERVICE),
        Pair.of(origin3, IN_SERVICE), Pair.of(origin3, IN_SERVICE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertTrue(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getUnderReplicatedReplicas(), 1, 2, 2, origin1);
  }

  @Test
  public void testUnderReplicationDueToDecommissionWithTwoOrigins() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, DECOMMISSIONING),
        Pair.of(origin2, IN_SERVICE), Pair.of(origin2, IN_SERVICE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertTrue(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getUnderReplicatedReplicas(), 1, 2, 1, origin1);
  }

  @Test
  public void testUnderReplicationDueToDecommissionWithOneOrigin() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, DECOMMISSIONING), Pair.of(origin1, DECOMMISSIONING));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertTrue(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getUnderReplicatedReplicas(), 1, 3, 2, origin1);
  }

  @Test
  public void testNoOverReplicationWithOutOfServiceReplicasWithThreeOrigins() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE), Pair.of(origin1, DECOMMISSIONED),
        Pair.of(origin2, IN_SERVICE), Pair.of(origin2, IN_SERVICE),
        Pair.of(origin3, IN_SERVICE), Pair.of(origin3, IN_SERVICE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
  }

  @Test
  public void testNoOverReplicationWithOutOfServiceReplicasWithTwoOrigins() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE), Pair.of(origin1, DECOMMISSIONED),
        Pair.of(origin2, IN_SERVICE), Pair.of(origin2, IN_SERVICE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
  }

  @Test
  public void testNoOverReplicationWithOutOfServiceReplicasWithOneOrigin() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE),
        Pair.of(origin1, DECOMMISSIONED));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
  }

  @Test
  public void testUnderReplicationWithMaintenanceWithOneOrigin() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE), Pair.of(origin1, ENTERING_MAINTENANCE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());

    replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, ENTERING_MAINTENANCE), Pair.of(origin1, ENTERING_MAINTENANCE));

    replicaCount = new QuasiClosedStuckReplicaCount(replicas, 2);
    assertTrue(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getUnderReplicatedReplicas(), 1, 3, 1, origin1);
  }

  @Test
  public void testUnderReplicationWithMaintenanceWithTwoOrigins() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, ENTERING_MAINTENANCE),
        Pair.of(origin2, IN_SERVICE), Pair.of(origin2, IN_SERVICE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());

    replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, ENTERING_MAINTENANCE), Pair.of(origin1, ENTERING_MAINTENANCE),
        Pair.of(origin2, IN_SERVICE), Pair.of(origin2, IN_SERVICE));

    replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertTrue(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
    validateMisReplicatedOrigins(replicaCount.getUnderReplicatedReplicas(), 1, 2, 1, origin1);
  }

  @Test
  public void testNoOverReplicationWithExcessMaintenanceReplicasTwoOrigins() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_MAINTENANCE),
        Pair.of(origin2, IN_SERVICE), Pair.of(origin2, IN_SERVICE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
  }

  @Test
  public void testNoOverReplicationWithExcessMaintenanceReplicasOneOrigin() {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(
        ContainerID.valueOf(1), QUASI_CLOSED,
        Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE),
        Pair.of(origin1, IN_MAINTENANCE));

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 1);
    assertFalse(replicaCount.isUnderReplicated());
    assertFalse(replicaCount.isOverReplicated());
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
