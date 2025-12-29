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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerInfo;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerReplica;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createReplicas;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationQueue;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RatisUnhealthyReplicationCheckHandler}.
 */
public class TestRatisUnhealthyReplicationCheckHandler {
  private RatisUnhealthyReplicationCheckHandler handler;
  private ReplicationConfig repConfig;
  private ReplicationQueue repQueue;
  private ContainerCheckRequest.Builder requestBuilder;
  private ReplicationManagerReport report;
  private int maintenanceRedundancy = 2;

  @BeforeEach
  public void setup() throws IOException {
    handler = new RatisUnhealthyReplicationCheckHandler();
    repConfig = RatisReplicationConfig.getInstance(THREE);
    repQueue = new ReplicationQueue();
    ReplicationManager.ReplicationManagerConfiguration rmConf =
        mock(ReplicationManager.ReplicationManagerConfiguration.class);
    report = new ReplicationManagerReport(rmConf.getContainerSampleLimit());
    requestBuilder = new ContainerCheckRequest.Builder()
        .setReplicationQueue(repQueue)
        .setMaintenanceRedundancy(maintenanceRedundancy)
        .setPendingOps(Collections.emptyList())
        .setReport(report);
  }

  @Test
  public void testReturnFalseForNonRatis() {
    ContainerInfo container =
        createContainerInfo(new ECReplicationConfig(3, 2));
    Set<ContainerReplica> replicas =
        createReplicas(container.containerID(), 1, 2, 3, 4);

    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);
    assertFalse(handler.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void shouldReturnFalseForPerfectReplicationConsideringHealthy() {
    ContainerInfo container =
        createContainerInfo(repConfig, 1L, HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(),
        ContainerReplicaProto.State.CLOSED, 0, 0, 0);
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);

    assertFalse(handler.handle(requestBuilder.build()));
  }

  @Test
  public void shouldReturnFalseForUnderReplicatedHealthyReplicas() {
    ContainerInfo container =
        createContainerInfo(repConfig, 1L, HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(),
        ContainerReplicaProto.State.CLOSED, 0, 0);
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);

    assertFalse(handler.handle(requestBuilder.build()));
  }

  @Test
  public void shouldReturnFalseForOverReplicatedHealthyReplicas() {
    ContainerInfo container =
        createContainerInfo(repConfig, 1L, HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(),
        ContainerReplicaProto.State.CLOSED, 0, 0, 0, 0);
    ContainerReplica unhealthyReplica =
        createContainerReplica(container.containerID(), 0, IN_SERVICE,
            ContainerReplicaProto.State.UNHEALTHY);
    replicas.add(unhealthyReplica);

    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);

    assertFalse(handler.handle(requestBuilder.build()));
  }

  /**
   * Handling this is the responsibility of
   * {@link RatisReplicationCheckHandler}, so this handler should return false.
   */
  @Test
  public void shouldReturnFalseForExcessUnhealthyReplicas() {
    ContainerInfo container =
        createContainerInfo(repConfig, 1L, HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(),
        ContainerReplicaProto.State.CLOSED, 0, 0, 0);
    ContainerReplica unhealthyReplica =
        createContainerReplica(container.containerID(), 0, IN_SERVICE,
            ContainerReplicaProto.State.UNHEALTHY);
    replicas.add(unhealthyReplica);
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);

    assertFalse(handler.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
    assertEquals(0,
        report.getStat(ReplicationManagerReport.HealthState.UNHEALTHY));
  }

  @Test
  public void shouldReturnTrueForUnderReplicatedUnhealthyReplicas() {
    ContainerInfo container =
        createContainerInfo(repConfig, 1L, HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(),
        ContainerReplicaProto.State.UNHEALTHY, 0);
    List<ContainerReplicaOp> pendingOps =
        ImmutableList.of(new ContainerReplicaOp(
            ContainerReplicaOp.PendingOpType.ADD,
            MockDatanodeDetails.randomDatanodeDetails(), 0, null, Long.MAX_VALUE, 0));
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setPendingOps(pendingOps);

    ContainerHealthResult.UnderReplicatedHealthResult
        result = (ContainerHealthResult.UnderReplicatedHealthResult)
        handler.checkReplication(requestBuilder.build());

    assertEquals(ContainerHealthResult.HealthState.UNDER_REPLICATED,
        result.getHealthState());
    assertEquals(0, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());

    assertTrue(handler.handle(requestBuilder.build()));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(1,
        report.getStat(ReplicationManagerReport.HealthState.UNHEALTHY));
  }

  @Test
  public void testUnderReplicatedFixedByPendingAdd() {
    ContainerInfo container =
        createContainerInfo(repConfig, 1L, HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(),
        ContainerReplicaProto.State.UNHEALTHY, 0, 0);
    List<ContainerReplicaOp> pendingOps =
        ImmutableList.of(new ContainerReplicaOp(
            ContainerReplicaOp.PendingOpType.ADD,
            MockDatanodeDetails.randomDatanodeDetails(), 0, null, Long.MAX_VALUE, 0));
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setPendingOps(pendingOps);

    ContainerHealthResult.UnderReplicatedHealthResult
        result = (ContainerHealthResult.UnderReplicatedHealthResult)
        handler.checkReplication(requestBuilder.build());

    assertEquals(ContainerHealthResult.HealthState.UNDER_REPLICATED,
        result.getHealthState());
    assertEquals(1, result.getRemainingRedundancy());
    assertTrue(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());

    assertTrue(handler.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(1,
        report.getStat(ReplicationManagerReport.HealthState.UNHEALTHY));
  }

  @Test
  public void testUnderReplicatedDueToPendingDelete() {
    ContainerInfo container =
        createContainerInfo(repConfig, 1L, HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(),
        ContainerReplicaProto.State.UNHEALTHY, 0, 0, 0);
    List<ContainerReplicaOp> pendingOps =
        ImmutableList.of(new ContainerReplicaOp(
            ContainerReplicaOp.PendingOpType.DELETE,
            replicas.stream().findFirst().get().getDatanodeDetails(), 0, null, Long.MAX_VALUE, 0));
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setPendingOps(pendingOps);

    ContainerHealthResult.UnderReplicatedHealthResult
        result = (ContainerHealthResult.UnderReplicatedHealthResult)
        handler.checkReplication(requestBuilder.build());

    assertEquals(ContainerHealthResult.HealthState.UNDER_REPLICATED,
        result.getHealthState());
    assertEquals(1, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());

    assertTrue(handler.handle(requestBuilder.build()));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(1,
        report.getStat(ReplicationManagerReport.HealthState.UNHEALTHY));
  }

  @Test
  public void testOverReplicationWithAllUnhealthyReplicas() {
    ContainerInfo container =
        createContainerInfo(repConfig, 1L, HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(),
        ContainerReplicaProto.State.UNHEALTHY, 0, 0, 0, 0);

    requestBuilder.setContainerInfo(container)
        .setContainerReplicas(replicas);

    ContainerHealthResult.OverReplicatedHealthResult
        result = (ContainerHealthResult.OverReplicatedHealthResult)
        handler.checkReplication(requestBuilder.build());

    assertEquals(ContainerHealthResult.HealthState.OVER_REPLICATED,
        result.getHealthState());
    assertEquals(1, result.getExcessRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());

    assertTrue(handler.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(1, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
    assertEquals(1,
        report.getStat(ReplicationManagerReport.HealthState.UNHEALTHY));
  }

  @Test
  public void testOverReplicationFixedByPendingDelete() {
    ContainerInfo container =
        createContainerInfo(repConfig, 1L, HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(),
        ContainerReplicaProto.State.UNHEALTHY, 0, 0, 0, 0);
    List<ContainerReplicaOp> pendingOps =
        ImmutableList.of(new ContainerReplicaOp(
            ContainerReplicaOp.PendingOpType.DELETE,
            replicas.stream().findFirst().get().getDatanodeDetails(), 0, null, Long.MAX_VALUE, 0));
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setPendingOps(pendingOps);

    ContainerHealthResult.OverReplicatedHealthResult
        result = (ContainerHealthResult.OverReplicatedHealthResult)
        handler.checkReplication(requestBuilder.build());

    assertEquals(ContainerHealthResult.HealthState.OVER_REPLICATED,
        result.getHealthState());
    assertEquals(1, result.getExcessRedundancy());
    assertTrue(result.isReplicatedOkAfterPending());

    assertTrue(handler.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
    assertEquals(1,
        report.getStat(ReplicationManagerReport.HealthState.UNHEALTHY));
  }

  @Test
  public void testWithQuasiClosedReplica() {
    final long sequenceID = 20;
    final ContainerInfo container = ReplicationTestUtil.createContainerInfo(
        repConfig, 1, HddsProtos.LifeCycleState.CLOSED, sequenceID);

    final Set<ContainerReplica> replicas = new HashSet<>(2);
    replicas.add(createContainerReplica(container.containerID(), 0,
        IN_SERVICE, State.CLOSED, sequenceID));
    replicas.add(createContainerReplica(container.containerID(), 0,
        IN_SERVICE, State.CLOSED, sequenceID));

    final ContainerReplica quasiClosedReplica =
        createContainerReplica(container.containerID(), 0,
            IN_SERVICE, State.QUASI_CLOSED, sequenceID);
    replicas.add(quasiClosedReplica);

    requestBuilder.setContainerReplicas(replicas).setContainerInfo(container);
    assertFalse(handler.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }
  
  @Test
  public void overReplicationCheckWithQuasiClosedReplica() {
    final long sequenceID = 20;
    final ContainerInfo container = ReplicationTestUtil.createContainerInfo(
        repConfig, 1, HddsProtos.LifeCycleState.CLOSED, sequenceID);

    Set<ContainerReplica> replicas = new HashSet<>(4);
    for (int i = 0; i < 4; i++) {
      replicas.add(createContainerReplica(container.containerID(), 0,
          IN_SERVICE, State.QUASI_CLOSED, sequenceID - 1));
    }

    requestBuilder.setContainerReplicas(replicas).setContainerInfo(container);
    assertTrue(handler.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(1, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
    assertEquals(0,
        report.getStat(ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(1,
        report.getStat(ReplicationManagerReport.HealthState.UNHEALTHY));
  }

  @Test
  public void testUnderReplicatedWithQuasiClosedReplicasHavingLessSequenceId() {
    final long sequenceID = 20;
    final ContainerInfo container = ReplicationTestUtil.createContainerInfo(
        repConfig, 1, HddsProtos.LifeCycleState.CLOSED, sequenceID);

    Set<ContainerReplica> replicas = new HashSet<>(2);
    for (int i = 0; i < 2; i++) {
      replicas.add(createContainerReplica(container.containerID(), 0,
          IN_SERVICE, State.QUASI_CLOSED, sequenceID - 1));
    }

    requestBuilder.setContainerReplicas(replicas).setContainerInfo(container);
    assertTrue(handler.handle(requestBuilder.build()));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
    assertEquals(1,
        report.getStat(ReplicationManagerReport.HealthState.UNHEALTHY));
  }
}
