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

package org.apache.hadoop.hdds.scm.container.replication.health;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.QUASI_CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationQueue;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the QuasiClosedStuckReplicationCheck class.
 */
public class TestQuasiClosedStuckReplicationCheck {

  private QuasiClosedStuckReplicationCheck handler;
  private final DatanodeID origin1 = DatanodeID.randomID();
  private final DatanodeID origin2 = DatanodeID.randomID();
  private final DatanodeID origin3 = DatanodeID.randomID();

  private ReplicationManager.ReplicationManagerConfiguration rmConf;
  private ReplicationManagerReport report;
  private ReplicationQueue queue;

  @BeforeEach
  public void setup() {
    rmConf = mock(ReplicationManager.ReplicationManagerConfiguration.class);
    handler = new QuasiClosedStuckReplicationCheck();
    report = new ReplicationManagerReport(rmConf.getContainerSampleLimit());
    report.resetContainerHealthState();  // Reset before each test
    queue = new ReplicationQueue();
  }

  @Test
  public void testClosedContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        RatisReplicationConfig.getInstance(THREE), 1, CLOSED);

    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicasWithOriginAndOpState(containerInfo.containerID(), State.QUASI_CLOSED,
            Pair.of(origin1, IN_SERVICE));
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReplicationQueue(queue)
        .build();

    assertFalse(handler.handle(request));
    assertEquals(0, report.getStat(ContainerHealthState.UNDER_REPLICATED));
    assertEquals(0, report.getStat(ContainerHealthState.OVER_REPLICATED));
    assertEquals(0, queue.underReplicatedQueueSize());
    assertEquals(0, queue.overReplicatedQueueSize());
  }

  @Test
  public void testQuasiClosedNotStuckReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        RatisReplicationConfig.getInstance(THREE), 1, QUASI_CLOSED);

    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicasWithOriginAndOpState(containerInfo.containerID(), State.QUASI_CLOSED,
            Pair.of(origin1, IN_SERVICE), Pair.of(origin2, IN_SERVICE), Pair.of(origin3, IN_SERVICE));
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(report)
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReplicationQueue(queue)
        .build();

    assertFalse(handler.handle(request));
    assertEquals(0, report.getStat(ContainerHealthState.UNDER_REPLICATED));
    assertEquals(0, report.getStat(ContainerHealthState.OVER_REPLICATED));
    assertEquals(0, queue.underReplicatedQueueSize());
    assertEquals(0, queue.overReplicatedQueueSize());
  }

  @Test
  public void testQuasiClosedStuckWithOpenReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        RatisReplicationConfig.getInstance(THREE), 1, QUASI_CLOSED);

    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicasWithOriginAndOpState(containerInfo.containerID(), State.QUASI_CLOSED,
            Pair.of(origin1, IN_SERVICE), Pair.of(origin2, IN_SERVICE));
    containerReplicas.addAll(ReplicationTestUtil
        .createReplicasWithOriginAndOpState(containerInfo.containerID(), State.OPEN,
            Pair.of(origin3, IN_SERVICE)));
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(report)
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReplicationQueue(queue)
        .build();

    assertFalse(handler.handle(request));
    assertEquals(0, report.getStat(ContainerHealthState.UNDER_REPLICATED));
    assertEquals(0, report.getStat(ContainerHealthState.OVER_REPLICATED));
    assertEquals(0, queue.underReplicatedQueueSize());
    assertEquals(0, queue.overReplicatedQueueSize());
  }

  @Test
  public void testCorrectlyReplicated() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        RatisReplicationConfig.getInstance(THREE), 1, QUASI_CLOSED);

    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicasWithOriginAndOpState(containerInfo.containerID(), State.QUASI_CLOSED,
            Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE),
            Pair.of(origin2, IN_SERVICE), Pair.of(origin2, IN_SERVICE));
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(report)
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReplicationQueue(queue)
        .build();

    assertFalse(handler.handle(request));
    assertEquals(0, report.getStat(ContainerHealthState.UNDER_REPLICATED));
    assertEquals(0, report.getStat(ContainerHealthState.OVER_REPLICATED));
    assertEquals(0, queue.underReplicatedQueueSize());
    assertEquals(0, queue.overReplicatedQueueSize());
  }

  @Test
  public void testNoReplicasReturnsTrue() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        RatisReplicationConfig.getInstance(THREE), 1, QUASI_CLOSED);

    Set<ContainerReplica> containerReplicas = new HashSet<>();
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(report)
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReplicationQueue(queue)
        .build();

    assertTrue(handler.handle(request));
    // Container with no replicas = QUASI_CLOSED_STUCK_MISSING combination
    assertEquals(0, report.getStat(ContainerHealthState.QUASI_CLOSED_STUCK_UNDER_REPLICATED));
    assertEquals(0, report.getStat(ContainerHealthState.QUASI_CLOSED_STUCK_OVER_REPLICATED));
    assertEquals(1, report.getStat(ContainerHealthState.QUASI_CLOSED_STUCK_MISSING));
    assertEquals(0, queue.underReplicatedQueueSize());
    assertEquals(0, queue.overReplicatedQueueSize());
  }

  @Test
  public void testUnderReplicatedOneOriginNotHandled() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        RatisReplicationConfig.getInstance(THREE), 1, QUASI_CLOSED);

    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicasWithOriginAndOpState(containerInfo.containerID(), State.QUASI_CLOSED,
            Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE));

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(report)
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReplicationQueue(queue)
        .build();

    assertFalse(handler.handle(request));
  }

  @Test
  public void testUnderReplicatedWithPendingAddIsNotQueued() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        RatisReplicationConfig.getInstance(THREE), 1, QUASI_CLOSED);

    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicasWithOriginAndOpState(containerInfo.containerID(), State.QUASI_CLOSED,
            Pair.of(origin1, IN_SERVICE), Pair.of(origin2, IN_SERVICE));

    List<ContainerReplicaOp> pendingOps = new ArrayList<>();
    pendingOps.add(new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.ADD, MockDatanodeDetails.randomDatanodeDetails(), 0, null, Long.MAX_VALUE, 0));

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(report)
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReplicationQueue(queue)
        .setPendingOps(pendingOps)
        .build();

    assertTrue(handler.handle(request));
    // QuasiClosedStuckReplicationCheck sets combination state
    assertEquals(1, report.getStat(ContainerHealthState.QUASI_CLOSED_STUCK_UNDER_REPLICATED));
    assertEquals(0, report.getStat(ContainerHealthState.QUASI_CLOSED_STUCK_OVER_REPLICATED));
    assertEquals(0, report.getStat(ContainerHealthState.QUASI_CLOSED_STUCK_MISSING));
    assertEquals(0, queue.underReplicatedQueueSize());
    assertEquals(0, queue.overReplicatedQueueSize());
  }

  @Test
  public void testOverReplicatedIsQueued() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        RatisReplicationConfig.getInstance(THREE), 1, QUASI_CLOSED);

    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicasWithOriginAndOpState(containerInfo.containerID(), State.QUASI_CLOSED,
            Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE),
            Pair.of(origin2, IN_SERVICE), Pair.of(origin2, IN_SERVICE));

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(report)
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReplicationQueue(queue)
        .build();

    assertTrue(handler.handle(request));
    // QuasiClosedStuckReplicationCheck sets combination state
    assertEquals(0, report.getStat(ContainerHealthState.QUASI_CLOSED_STUCK_UNDER_REPLICATED));
    assertEquals(1, report.getStat(ContainerHealthState.QUASI_CLOSED_STUCK_OVER_REPLICATED));
    assertEquals(0, report.getStat(ContainerHealthState.QUASI_CLOSED_STUCK_MISSING));
    assertEquals(0, queue.underReplicatedQueueSize());
    assertEquals(1, queue.overReplicatedQueueSize());
  }

  @Test
  public void testOverReplicatedWithPendingDeleteIsNotQueued() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        RatisReplicationConfig.getInstance(THREE), 1, QUASI_CLOSED);

    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicasWithOriginAndOpState(containerInfo.containerID(), State.QUASI_CLOSED,
            Pair.of(origin1, IN_SERVICE), Pair.of(origin1, IN_SERVICE),
            Pair.of(origin2, IN_SERVICE), Pair.of(origin2, IN_SERVICE), Pair.of(origin2, IN_SERVICE));

    List<ContainerReplicaOp> pendingOps = new ArrayList<>();
    pendingOps.add(new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.DELETE,
        MockDatanodeDetails.randomDatanodeDetails(),
        0,
        null,
        Long.MAX_VALUE,
        0));

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(report)
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReplicationQueue(queue)
        .setPendingOps(pendingOps)
        .build();

    assertTrue(handler.handle(request));
    // QuasiClosedStuckReplicationCheck sets combination state
    assertEquals(0, report.getStat(ContainerHealthState.QUASI_CLOSED_STUCK_UNDER_REPLICATED));
    assertEquals(1, report.getStat(ContainerHealthState.QUASI_CLOSED_STUCK_OVER_REPLICATED));
    assertEquals(0, report.getStat(ContainerHealthState.QUASI_CLOSED_STUCK_MISSING));
    assertEquals(0, queue.underReplicatedQueueSize());
    assertEquals(0, queue.overReplicatedQueueSize());
  }

}
