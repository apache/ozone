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
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.UNHEALTHY;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.ADD;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.DELETE;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerInfo;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerReplica;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createReplicas;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult.HealthState;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult.OverReplicatedHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult.UnderReplicatedHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationQueue;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the ECContainerHealthCheck class.
 */
public class TestECReplicationCheckHandler {

  private ECReplicationCheckHandler healthCheck;
  private ECReplicationConfig repConfig;
  private ReplicationQueue repQueue;
  private int maintenanceRedundancy = 2;
  private ContainerCheckRequest.Builder requestBuilder;
  private ReplicationManagerReport report;
  private PlacementPolicy placementPolicy;

  @BeforeEach
  public void setup() {
    placementPolicy = mock(PlacementPolicy.class);
    when(placementPolicy.validateContainerPlacement(anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(2, 2, 3));
    healthCheck = new ECReplicationCheckHandler();
    repConfig = new ECReplicationConfig(3, 2);
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
  public void testHealthyContainerIsHealthy() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), 1, 2, 3, 4, 5);
    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .build();

    ContainerHealthResult result = healthCheck.checkHealth(request);
    assertEquals(HealthState.HEALTHY, result.getHealthState());

    assertFalse(healthCheck.handle(request));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testUnderReplicatedContainerIsUnderReplicated() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), 1, 2, 4, 5);
    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .build();
    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(request);
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(1, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());

    assertTrue(healthCheck.handle(request));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
  }

  @Test
  public void testUnderReplicatedContainerFixedWithPending() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), 1, 2, 4, 5);
    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(new ContainerReplicaOp(
        ADD, MockDatanodeDetails.randomDatanodeDetails(), 3, null, Long.MAX_VALUE, 0));
    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setPendingOps(pending)
        .build();
    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(request);
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(1, result.getRemainingRedundancy());
    assertTrue(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());

    assertTrue(healthCheck.handle(request));
    // Fixed with pending so nothing added to the queue
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    // Still under replicated until the pending complete
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
  }

  @Test
  public void testUnderReplicatedDueToOutOfService() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
        Pair.of(IN_SERVICE, 3), Pair.of(DECOMMISSIONING, 4),
        Pair.of(DECOMMISSIONED, 5));
    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .build();

    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(request);
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(2, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertTrue(result.underReplicatedDueToOutOfService());

    assertTrue(healthCheck.handle(request));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    // Still under replicated until the pending complete
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
  }

  @Test
  public void testUnderReplicatedDueToOutOfServiceFixedWithPending() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
        Pair.of(IN_SERVICE, 3), Pair.of(DECOMMISSIONING, 4),
        Pair.of(IN_SERVICE, 4), Pair.of(DECOMMISSIONED, 5));
    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(new ContainerReplicaOp(
        ADD, MockDatanodeDetails.randomDatanodeDetails(), 5, null, Long.MAX_VALUE, 0));
    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setPendingOps(pending)
        .build();

    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(request);
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(2, result.getRemainingRedundancy());
    assertTrue(result.isReplicatedOkAfterPending());
    assertTrue(result.underReplicatedDueToOutOfService());

    assertTrue(healthCheck.handle(request));
    // Fixed with pending so nothing added to the queue
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    // Still under replicated until the pending complete
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
  }

  @Test
  public void testUnderReplicatedDueToOutOfServiceAndMissingReplica() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
        Pair.of(DECOMMISSIONING, 4), Pair.of(DECOMMISSIONED, 5));
    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(new ContainerReplicaOp(
        ADD, MockDatanodeDetails.randomDatanodeDetails(), 3, null, Long.MAX_VALUE, 0));

    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setPendingOps(pending)
        .build();
    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(request);
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(1, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());

    assertTrue(healthCheck.handle(request));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
  }

  @Test
  public void testUnderReplicatedAndUnrecoverable() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2));
    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .build();

    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(request);
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(-1, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());
    assertTrue(result.isUnrecoverable());

    assertTrue(healthCheck.handle(request));
    // Unrecoverable so not added to the queue
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.MISSING));
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.UNHEALTHY));
  }

  @Test
  public void testUnderReplicatedAndUnhealthy() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2));
    replicas.addAll(createReplicas(UNHEALTHY, Pair.of(IN_SERVICE, 3),
        Pair.of(IN_SERVICE, 4)));
    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .build();

    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(request);
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(-1, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());
    assertTrue(result.isUnrecoverable());
    assertFalse(result.isMissing());

    assertTrue(healthCheck.handle(request));
    // Unrecoverable so not added to the queue
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.MISSING));
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNHEALTHY));
  }

  @Test
  public void testUnderReplicatedAndUnrecoverableWithDecommission() {
    testUnderReplicatedAndUnrecoverableWithOffline(DECOMMISSIONING);
  }

  @Test
  public void testUnderReplicatedAndUnrecoverableWithMaintenance() {
    testUnderReplicatedAndUnrecoverableWithOffline(ENTERING_MAINTENANCE);
  }

  private void testUnderReplicatedAndUnrecoverableWithOffline(
      HddsProtos.NodeOperationalState offlineState) {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(offlineState, 2));
    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .build();

    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(request);
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(-1, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());
    assertTrue(result.isUnrecoverable());
    assertTrue(result.hasUnreplicatedOfflineIndexes());
    assertFalse(result.offlineIndexesOkAfterPending());

    assertTrue(healthCheck.handle(request));
    // Unrecoverable so not added to the queue
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.MISSING));
  }

  @Test
  public void testUnderReplicatedAndUnrecoverableWithDecommissionPending() {
    testUnderReplicatedAndUnrecoverableWithOfflinePending(DECOMMISSIONING);
  }

  @Test
  public void testUnderReplicatedAndUnrecoverableWithMaintenancePending() {
    testUnderReplicatedAndUnrecoverableWithOfflinePending(ENTERING_MAINTENANCE);
  }

  private void testUnderReplicatedAndUnrecoverableWithOfflinePending(
      HddsProtos.NodeOperationalState offlineState) {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(offlineState, 2));
    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(new ContainerReplicaOp(
        ADD, MockDatanodeDetails.randomDatanodeDetails(), 2, null, Long.MAX_VALUE, 0));
    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setPendingOps(pending)
        .build();

    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(request);
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(-1, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());
    assertTrue(result.isUnrecoverable());
    // The pending has not completed yet, so the container is still under-replicated.
    assertTrue(result.hasUnreplicatedOfflineIndexes());
    assertTrue(result.offlineIndexesOkAfterPending());

    assertTrue(healthCheck.handle(request));
    // Unrecoverable, but there are offline indexes which need replicated. As there is a pending
    // add on the offline index, it is not added to the queue.
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.MISSING));
  }

  /**
   * Tests that a closed EC 3-2 container with 3 closed and 2 unhealthy
   * replicas is under replicated.
   */
  @Test
  public void testUnderReplicatedDueToUnhealthyReplicas() {
    ContainerInfo container = createContainerInfo(repConfig, 1, CLOSED);

    // create 3 closed and 2 unhealthy replicas
    Set<ContainerReplica> containerReplicas =
        ReplicationTestUtil.createReplicas(container.containerID(),
            ContainerReplicaProto.State.CLOSED, 1, 2, 3);
    ContainerReplica unhealthyIndex4 =
        createContainerReplica(container.containerID(), 4,
            IN_MAINTENANCE, ContainerReplicaProto.State.UNHEALTHY);
    ContainerReplica unhealthyIndex5 =
        createContainerReplica(container.containerID(), 5,
            IN_SERVICE, ContainerReplicaProto.State.UNHEALTHY);
    containerReplicas.add(unhealthyIndex4);
    containerReplicas.add(unhealthyIndex5);

    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(containerReplicas)
        .setContainerInfo(container)
        .build();

    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(request);
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(0, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());

    assertTrue(healthCheck.handle(request));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
  }

  /**
   * Tests that a closed EC 3-2 container with 5 closed replicas and 1 more
   * unhealthy replica for an index is sufficiently replicated. This is not over
   * replication because the unhealthy replica is unavailable and should not be
   * considered.
   */
  @Test
  public void testSufficientlyReplicatedDespiteUnhealthyReplicas() {
    ContainerInfo container = createContainerInfo(repConfig, 1, CLOSED);

    // create 5 closed replicas and 1 unhealthy replica for index 5
    Set<ContainerReplica> containerReplicas =
        ReplicationTestUtil.createReplicas(container.containerID(),
            ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4, 5);
    ContainerReplica unhealthyIndex5 =
        createContainerReplica(container.containerID(), 5,
            IN_SERVICE, ContainerReplicaProto.State.UNHEALTHY);
    containerReplicas.add(unhealthyIndex5);

    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(containerReplicas)
        .setContainerInfo(container)
        .build();

    ContainerHealthResult result = healthCheck.checkHealth(request);
    assertEquals(HealthState.HEALTHY, result.getHealthState());

    assertFalse(healthCheck.handle(request));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testOverReplicatedContainer() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas =  createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
        Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
        Pair.of(IN_SERVICE, 5),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2));

    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .build();

    OverReplicatedHealthResult result = (OverReplicatedHealthResult)
        healthCheck.checkHealth(request);
    assertEquals(HealthState.OVER_REPLICATED, result.getHealthState());
    assertEquals(2, result.getExcessRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());

    assertTrue(healthCheck.handle(request));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(1, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
  }

  @Test
  public void testOverReplicatedContainerFixedByPending() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas =  createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
        Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
        Pair.of(IN_SERVICE, 5),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2));

    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(new ContainerReplicaOp(
        DELETE, MockDatanodeDetails.randomDatanodeDetails(), 1, null, Long.MAX_VALUE, 0));
    pending.add(new ContainerReplicaOp(
        DELETE, MockDatanodeDetails.randomDatanodeDetails(), 2, null, Long.MAX_VALUE, 0));
    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setPendingOps(pending)
        .build();

    OverReplicatedHealthResult result = (OverReplicatedHealthResult)
        healthCheck.checkHealth(request);
    assertEquals(HealthState.OVER_REPLICATED, result.getHealthState());
    assertEquals(2, result.getExcessRedundancy());
    assertTrue(result.isReplicatedOkAfterPending());

    assertTrue(healthCheck.handle(request));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
  }

  @Test
  public void testOverReplicatedContainerDueToMaintenance() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas =  createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
        Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
        Pair.of(IN_SERVICE, 5),
        Pair.of(IN_MAINTENANCE, 1), Pair.of(IN_MAINTENANCE, 2));
    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .build();
    ContainerHealthResult result = healthCheck.checkHealth(request);
    assertEquals(HealthState.HEALTHY, result.getHealthState());

    // As it is maintenance replicas causing the over replication, the container
    // is not really over-replicated.
    assertFalse(healthCheck.handle(request));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
  }

  @Test
  public void testOverAndUnderReplicated() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas =  createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
        Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2));
    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .build();
    ContainerHealthResult result = healthCheck.checkHealth(request);
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(1,
        ((UnderReplicatedHealthResult)result).getRemainingRedundancy());

    // Under-replicated takes precedence and the over-replication is ignored
    // for now.
    assertTrue(healthCheck.handle(request));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
  }

  @Test
  public void testUnderAndMisReplicatedContainer() {
    ContainerInfo container = createContainerInfo(repConfig);

    // Placement policy is always violated
    when(placementPolicy.validateContainerPlacement(any(), anyInt()
    )).thenAnswer(invocation ->
        new ContainerPlacementStatusDefault(4, 5, 9));

    Set<ContainerReplica> replicas =  createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
        Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4));
    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .build();

    ContainerHealthResult result = healthCheck.checkHealth(request);
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());

    // Under-replicated takes precedence and the over-replication is ignored
    // for now.
    assertTrue(healthCheck.handle(request));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.MIS_REPLICATED));
  }

  @Test
  public void testOverAndMisReplicatedContainer() {
    ContainerInfo container = createContainerInfo(repConfig);

    // Placement policy is always violated
    when(placementPolicy.validateContainerPlacement(
        any(),
        anyInt()
    )).thenAnswer(invocation ->
        new ContainerPlacementStatusDefault(4, 5, 9));

    Set<ContainerReplica> replicas =  createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
        Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
        Pair.of(IN_SERVICE, 5), Pair.of(IN_SERVICE, 5));
    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .build();

    ContainerHealthResult result = healthCheck.checkHealth(request);
    assertEquals(HealthState.OVER_REPLICATED, result.getHealthState());

    // Under-replicated takes precedence and the over-replication is ignored
    // for now.
    assertTrue(healthCheck.handle(request));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(1, repQueue.overReplicatedQueueSize());
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.MIS_REPLICATED));
  }

  @Test
  public void testUnhealthyReplicaWithOtherCopyAndPendingDelete() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas =  createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
        Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
        Pair.of(IN_SERVICE, 5));

    ContainerReplica unhealthyReplica = ReplicationTestUtil
        .createContainerReplica(container.containerID(), 1, IN_SERVICE,
            UNHEALTHY);
    replicas.add(unhealthyReplica);

    List<ContainerReplicaOp> pendingOps = new ArrayList<>();
    pendingOps.add(new ContainerReplicaOp(DELETE,
        unhealthyReplica.getDatanodeDetails(),
        unhealthyReplica.getReplicaIndex(), null, Long.MAX_VALUE, 0));

    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setPendingOps(pendingOps)
        .build();
    ContainerHealthResult result = healthCheck.checkHealth(request);
    assertEquals(HealthState.HEALTHY, result.getHealthState());

    assertFalse(healthCheck.handle(request));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
  }

}
