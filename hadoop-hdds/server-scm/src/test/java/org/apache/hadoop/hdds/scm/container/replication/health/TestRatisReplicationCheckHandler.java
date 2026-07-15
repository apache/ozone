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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult.HealthState;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult.MisReplicatedHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult.OverReplicatedHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult.UnderReplicatedHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationQueue;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for the RatisReplicationCheckHandler class.
 */
public class TestRatisReplicationCheckHandler {

  private RatisReplicationCheckHandler healthCheck;
  private ReplicationConfig repConfig;
  private PlacementPolicy containerPlacementPolicy;
  private ReplicationQueue repQueue;
  private ContainerCheckRequest.Builder requestBuilder;
  private ReplicationManagerReport report;
  private int maintenanceRedundancy = 2;

  @BeforeEach
  public void setup() throws IOException, NodeNotFoundException {
    containerPlacementPolicy = mock(PlacementPolicy.class);
    when(containerPlacementPolicy.validateContainerPlacement(
        any(),
        anyInt()
    )).thenAnswer(invocation ->
        new ContainerPlacementStatusDefault(2, 2, 3));

    ReplicationManager replicationManager = mock(ReplicationManager.class);
    when(replicationManager.getNodeStatus(any()))
        .thenReturn(NodeStatus.inServiceHealthy());
    healthCheck = new RatisReplicationCheckHandler(containerPlacementPolicy,
        replicationManager);
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
    assertFalse(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testHealthyContainerIsHealthy() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), 0, 0, 0);
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);
    ContainerHealthResult result =
        healthCheck.checkHealth(requestBuilder.build());
    assertEquals(HealthState.HEALTHY, result.getHealthState());

    assertFalse(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testUnderReplicatedContainerIsUnderReplicated() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), 0, 0);
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);
    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(requestBuilder.build());
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(1, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
  }

  @Test
  public void testUnderReplicatedContainerDueToPendingDelete() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), 0, 0, 0);
    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(new ContainerReplicaOp(
        DELETE, MockDatanodeDetails.randomDatanodeDetails(), 0, null, Long.MAX_VALUE, 0));
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setPendingOps(pending);
    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(requestBuilder.build());
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(1, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
  }

  @Test
  public void testUnderReplicatedContainerFixedWithPending() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), 0, 0);
    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(new ContainerReplicaOp(
        ADD, MockDatanodeDetails.randomDatanodeDetails(), 0, null, Long.MAX_VALUE, 0));
    requestBuilder.setContainerReplicas(replicas)
        .setPendingOps(pending)
        .setContainerInfo(container);
    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(requestBuilder.build());
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(1, result.getRemainingRedundancy());
    assertTrue(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    // Fixed with pending, so nothing added to the queue
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    // Still under replicated until the pending complete
    assertEquals(1, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
  }

  @Test
  public void testUnderReplicatedDueToOutOfService() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 0), Pair.of(DECOMMISSIONING, 0),
        Pair.of(DECOMMISSIONED, 0));

    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);
    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(requestBuilder.build());
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(2, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertTrue(result.underReplicatedDueToOutOfService());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
  }

  @ParameterizedTest
  @MethodSource("org.apache.hadoop.hdds.scm.node.NodeStatus#outOfServiceStates")
  void testUnderReplicatedDueToAllOutOfService(
      HddsProtos.NodeOperationalState state) {
    Pair<HddsProtos.NodeOperationalState, Integer> pair = Pair.of(state, 0);

    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        pair, pair, pair);

    ContainerCheckRequest checkRequest = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .build();

    ContainerHealthResult healthResult = healthCheck.checkHealth(checkRequest);
    assertEquals(HealthState.UNDER_REPLICATED, healthResult.getHealthState());
    assertEquals(UnderReplicatedHealthResult.class, healthResult.getClass());
    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthResult;

    assertEquals(2, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertTrue(result.underReplicatedDueToOutOfService());

    assertTrue(healthCheck.handle(checkRequest));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
  }

  @Test
  public void testUnderReplicatedDueToOutOfServiceFixedWithPending() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0),
        Pair.of(DECOMMISSIONED, 0));
    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(new ContainerReplicaOp(
        ADD, MockDatanodeDetails.randomDatanodeDetails(), 0, null, Long.MAX_VALUE, 0));

    requestBuilder.setContainerReplicas(replicas)
        .setPendingOps(pending)
        .setContainerInfo(container);
    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(requestBuilder.build());
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(2, result.getRemainingRedundancy());
    assertTrue(result.isReplicatedOkAfterPending());
    assertTrue(result.underReplicatedDueToOutOfService());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    // Nothing queued as inflight replicas will fix it.
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    // Still under replicated in the report until pending complete
    assertEquals(1, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
  }

  @Test
  public void testUnderReplicatedDueToOutOfServiceAndMissing() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 0), Pair.of(DECOMMISSIONED, 0));
    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(new ContainerReplicaOp(
        ADD, MockDatanodeDetails.randomDatanodeDetails(), 0, null, Long.MAX_VALUE, 0));

    requestBuilder.setContainerReplicas(replicas)
        .setPendingOps(pending)
        .setContainerInfo(container);
    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(requestBuilder.build());
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(1, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
  }

  @Test
  public void testUnderReplicatedAndUnrecoverable() {
    ContainerInfo container = createContainerInfo(repConfig);

    requestBuilder.setContainerReplicas(Collections.emptySet())
        .setContainerInfo(container);
    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(requestBuilder.build());
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(0, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());
    assertTrue(result.isUnrecoverable());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    // Unrecoverable, so not added to the queue.
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(1, report.getStat(
        ContainerHealthState.MISSING));
  }

  /**
   * Replicas with ContainerReplicaProto#State UNHEALTHY don't contribute to
   * the redundancy of a container. This tests that a CLOSED container with {
   * CLOSED, CLOSED, UNHEALTHY, UNHEALTHY} replicas is under replicated.
   */
  @Test
  public void testUnderReplicatedWithUnhealthyReplicas() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), State.CLOSED, 0, 0);
    Set<ContainerReplica> unhealthyReplicas =
        createReplicas(container.containerID(), State.UNHEALTHY, 0, 0);
    replicas.addAll(unhealthyReplicas);
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);
    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(requestBuilder.build());

    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(1, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
  }

  @Test
  public void testSufficientReplicationWithMismatchedReplicas() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), State.CLOSING, 0, 0, 0);

    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);
    ContainerHealthResult result =
        healthCheck.checkHealth(requestBuilder.build());
    assertEquals(HealthState.HEALTHY, result.getHealthState());

    assertFalse(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testHandlerReturnsFalseWhenAllReplicasAreUnhealthy() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas =
        createReplicas(container.containerID(), State.UNHEALTHY, 0, 0, 0, 0);
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);
    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(requestBuilder.build());

    /*
    Here, UNDER_REPLICATED health state simply means there aren't enough
    healthy replicas. This handler cannot make a decision about
    replication/deleting replicas when all of them are unhealthy.
     */
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(0, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());

    assertFalse(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));

    /*
    Now, check when there are less than replication factor UNHEALTHY replicas.
    The handler should still return false.
     */
    replicas =
        createReplicas(container.containerID(), State.UNHEALTHY, 0, 0);
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);
    result = (UnderReplicatedHealthResult) healthCheck.checkHealth(
        requestBuilder.build());

    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(0, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());

    assertFalse(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
  }

  @Test
  public void testOverReplicatedContainer() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas =  createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0),
        Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0),
        Pair.of(IN_SERVICE, 0),
        Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0));

    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(new ContainerReplicaOp(
        DELETE, MockDatanodeDetails.randomDatanodeDetails(), 0, null, Long.MAX_VALUE, 0));
    pending.add(new ContainerReplicaOp(
        DELETE, MockDatanodeDetails.randomDatanodeDetails(), 0, null, Long.MAX_VALUE, 0));

    requestBuilder.setContainerReplicas(replicas)
        .setPendingOps(pending)
        .setContainerInfo(container);
    OverReplicatedHealthResult result = (OverReplicatedHealthResult)
        healthCheck.checkHealth(requestBuilder.build());
    assertEquals(HealthState.OVER_REPLICATED, result.getHealthState());
    assertEquals(4, result.getExcessRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(1, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ContainerHealthState.OVER_REPLICATED));
  }

  @Test
  public void testOverReplicatedContainerWithMismatchedReplicas() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), State.QUASI_CLOSED, 0, 0);
    Set<ContainerReplica> misMatchedReplicas =
        createReplicas(container.containerID(), State.CLOSING, 0, 0);
    replicas.addAll(misMatchedReplicas);
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);

    OverReplicatedHealthResult result = (OverReplicatedHealthResult)
        healthCheck.checkHealth(requestBuilder.build());
    assertEquals(HealthState.OVER_REPLICATED, result.getHealthState());
    assertEquals(1, result.getExcessRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    /*
    We have an excess replica, but we hold off on adding to the over
    replication queue until all the mismatched replicas match the container
    state.
     */
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ContainerHealthState.OVER_REPLICATED));
  }

  @Test
  public void shouldQueueForOverReplicationOnlyWhenSafe() {
    ContainerInfo container =
        createContainerInfo(repConfig, 1L, HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        ContainerReplicaProto.State.CLOSED, 0, 0);
    ContainerReplica unhealthyReplica =
        createContainerReplica(container.containerID(), 0, IN_SERVICE,
            ContainerReplicaProto.State.UNHEALTHY);
    ContainerReplica mismatchedReplica =
        createContainerReplica(container.containerID(), 0, IN_SERVICE,
            ContainerReplicaProto.State.QUASI_CLOSED);
    replicas.add(mismatchedReplica);
    replicas.add(unhealthyReplica);

    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);

    ContainerHealthResult.OverReplicatedHealthResult
        result = (ContainerHealthResult.OverReplicatedHealthResult)
        healthCheck.checkHealth(requestBuilder.build());

    assertEquals(ContainerHealthResult.HealthState.OVER_REPLICATED,
        result.getHealthState());
    assertEquals(1, result.getExcessRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());

    // not safe for over replication because we don't have 3 matching replicas
    assertFalse(result.isSafelyOverReplicated());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ContainerHealthState.OVER_REPLICATED));
  }

  /**
   * Scenario: CLOSED container with 2 CLOSED, 1 CLOSING and 3 UNHEALTHY
   * replicas.
   * Expectation: This container is over replicated because there's an excess
   * of UNHEALTHY replicas. The handler should return true but this container
   * should not be queued for over replication because there's a mis matched
   * replica.
   */
  @Test
  public void testHandlerReturnsTrueForExcessUnhealthyReplicas() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), State.CLOSED, 0, 0);
    ContainerReplica mismatchedReplica =
        createContainerReplica(container.containerID(), 0, IN_SERVICE,
            State.CLOSING);
    Set<ContainerReplica> unhealthyReplicas =
        createReplicas(container.containerID(), State.UNHEALTHY, 0, 0, 0);
    replicas.add(mismatchedReplica);
    replicas.addAll(unhealthyReplicas);
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);
    ContainerHealthResult result =
        healthCheck.checkHealth(requestBuilder.build());

    // there's an excess of 3 UNHEALTHY replicas, so it's over replicated
    assertEquals(HealthState.OVER_REPLICATED, result.getHealthState());
    OverReplicatedHealthResult overRepResult =
        (OverReplicatedHealthResult) result;
    assertEquals(3, overRepResult.getExcessRedundancy());
    assertTrue(overRepResult.hasMismatchedReplicas());
    assertFalse(overRepResult.isReplicatedOkAfterPending());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    // it should not be queued for over replication because there's a mis
    // matched replica
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ContainerHealthState.OVER_REPLICATED));
    assertEquals(0, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
  }

  /**
   * There is a CLOSED container with 3 CLOSED replicas and 1 QUASI_CLOSED
   * replica with incorrect sequence ID. This container is over replicated
   * because of the QUASI_CLOSED replica and should be queued for over
   * replication.
   */
  @Test
  public void testExcessQuasiClosedWithIncorrectSequenceID() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), State.CLOSED, 0, 0, 0);
    ContainerReplica quasiClosed =
        createContainerReplica(container.containerID(), 0,
            IN_SERVICE, State.QUASI_CLOSED, container.getSequenceId() - 1);
    replicas.add(quasiClosed);
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);
    ContainerHealthResult result =
        healthCheck.checkHealth(requestBuilder.build());

    // there's an excess QUASI_CLOSED replica with incorrect sequence ID, so
    // it's over replicated
    assertEquals(HealthState.OVER_REPLICATED, result.getHealthState());
    OverReplicatedHealthResult overRepResult =
        (OverReplicatedHealthResult) result;
    assertEquals(1, overRepResult.getExcessRedundancy());
    assertFalse(overRepResult.hasMismatchedReplicas());
    assertFalse(overRepResult.isReplicatedOkAfterPending());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(1, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ContainerHealthState.OVER_REPLICATED));
    assertEquals(0, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
  }

  /**
   * Scenario: CLOSED container with 3 CLOSED and 3 UNHEALTHY replicas.
   * Expectation: This container should be queued for over replication
   * because there's an excess of UNHEALTHY replicas and there are no mis
   * matched replicas.
   */
  @Test
  public void testHandlerAddsToQueueWhenExcessUnhealthyReplicas() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), State.CLOSED, 0, 0, 0);
    Set<ContainerReplica> unhealthyReplicas =
        createReplicas(container.containerID(), State.UNHEALTHY, 0, 0, 0);
    replicas.addAll(unhealthyReplicas);
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);
    ContainerHealthResult result =
        healthCheck.checkHealth(requestBuilder.build());

    // there's an excess of 3 UNHEALTHY replicas, so it's over replicated
    assertEquals(HealthState.OVER_REPLICATED, result.getHealthState());
    OverReplicatedHealthResult overRepResult =
        (OverReplicatedHealthResult) result;
    assertEquals(3, overRepResult.getExcessRedundancy());
    assertFalse(overRepResult.hasMismatchedReplicas());
    assertFalse(overRepResult.isReplicatedOkAfterPending());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(1, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ContainerHealthState.OVER_REPLICATED));
    assertEquals(0, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));

  }

  @Test
  public void testOverReplicatedContainerFixedByPending() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas =  createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0),
        Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0));

    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(new ContainerReplicaOp(
        DELETE, MockDatanodeDetails.randomDatanodeDetails(), 0, null, Long.MAX_VALUE, 0));

    requestBuilder.setContainerReplicas(replicas)
        .setPendingOps(pending)
        .setContainerInfo(container);
    OverReplicatedHealthResult result = (OverReplicatedHealthResult)
        healthCheck.checkHealth(requestBuilder.build());
    assertEquals(HealthState.OVER_REPLICATED, result.getHealthState());
    assertEquals(1, result.getExcessRedundancy());
    assertTrue(result.isReplicatedOkAfterPending());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    // Fixed by pending so nothing queued.
    assertEquals(0, repQueue.overReplicatedQueueSize());
    // Still over replicated, so the report should contain it
    assertEquals(1, report.getStat(
        ContainerHealthState.OVER_REPLICATED));
  }

  @Test
  public void testOverReplicatedContainerWithMaintenance() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas =  createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0),
        Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0),
        Pair.of(IN_MAINTENANCE, 0), Pair.of(DECOMMISSIONED, 0));

    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);
    OverReplicatedHealthResult result = (OverReplicatedHealthResult)
        healthCheck.checkHealth(requestBuilder.build());
    assertEquals(HealthState.OVER_REPLICATED, result.getHealthState());
    assertEquals(1, result.getExcessRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(1, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ContainerHealthState.OVER_REPLICATED));
  }

  @Test
  public void testOverReplicatedContainerDueToMaintenanceIsHealthy() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0),
        Pair.of(IN_SERVICE, 0), Pair.of(IN_MAINTENANCE, 0),
        Pair.of(IN_MAINTENANCE, 0));

    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);
    ContainerHealthResult result =
        healthCheck.checkHealth(requestBuilder.build());
    assertEquals(HealthState.HEALTHY, result.getHealthState());

    assertFalse(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, report.getStat(
        ContainerHealthState.OVER_REPLICATED));
    assertEquals(0, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
  }

  /**
   * Scenario: Container is both over replicated and mis replicated.
   * Expectation: It should be queued for over replication.
   */
  @Test
  public void testOverReplicatedWithMisReplication() {
    when(containerPlacementPolicy.validateContainerPlacement(
        any(),
        anyInt()
    )).thenAnswer(invocation ->
        new ContainerPlacementStatusDefault(1, 2, 3));

    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), State.CLOSED, 0, 0, 0);
    Set<ContainerReplica> unhealthyReplicas =
        createReplicas(container.containerID(), State.UNHEALTHY, 0, 0, 0);
    replicas.addAll(unhealthyReplicas);
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);
    ContainerHealthResult result =
        healthCheck.checkHealth(requestBuilder.build());

    // there's an excess of 3 UNHEALTHY replicas, so it's over replicated
    assertEquals(HealthState.OVER_REPLICATED, result.getHealthState());
    OverReplicatedHealthResult overRepResult =
        (OverReplicatedHealthResult) result;
    assertEquals(3, overRepResult.getExcessRedundancy());
    assertFalse(overRepResult.hasMismatchedReplicas());
    assertFalse(overRepResult.isReplicatedOkAfterPending());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(1, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ContainerHealthState.OVER_REPLICATED));
    assertEquals(0, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(0, report.getStat(
        ContainerHealthState.MIS_REPLICATED));
  }

  @Test
  public void testUnderReplicatedWithMisReplication() {
    when(containerPlacementPolicy.validateContainerPlacement(
        any(),
        anyInt()
    )).thenAnswer(invocation ->
        new ContainerPlacementStatusDefault(1, 2, 3));

    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), 0, 0);
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);
    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(requestBuilder.build());
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(1, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(0, report.getStat(
        ContainerHealthState.MIS_REPLICATED));
  }

  @Test
  public void testUnderReplicatedWithMisReplicationFixedByPending() {
    when(containerPlacementPolicy.validateContainerPlacement(
        any(),
        anyInt()
    )).thenAnswer(invocation -> {
      List<DatanodeDetails> dns = invocation.getArgument(0);
      // If the number of DNs is 3 or less make it be mis-replicated
      if (dns.size() <= 3) {
        return new ContainerPlacementStatusDefault(1, 2, 3);
      } else {
        return new ContainerPlacementStatusDefault(2, 2, 3);
      }
    });

    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), 0, 0);

    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(new ContainerReplicaOp(
        ADD, MockDatanodeDetails.randomDatanodeDetails(), 0, null, Long.MAX_VALUE, 0));
    pending.add(new ContainerReplicaOp(
        ADD, MockDatanodeDetails.randomDatanodeDetails(), 0, null, Long.MAX_VALUE, 0));

    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setPendingOps(pending);
    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(requestBuilder.build());
    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(1, result.getRemainingRedundancy());
    assertTrue(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(0, report.getStat(
        ContainerHealthState.MIS_REPLICATED));
  }

  @Test
  public void testMisReplicated() {
    when(containerPlacementPolicy.validateContainerPlacement(
        any(),
        anyInt()
    )).thenAnswer(invocation ->
        new ContainerPlacementStatusDefault(1, 2, 3));

    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), 0, 0, 0);
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);
    MisReplicatedHealthResult result = (MisReplicatedHealthResult)
        healthCheck.checkHealth(requestBuilder.build());
    assertEquals(HealthState.MIS_REPLICATED, result.getHealthState());
    assertFalse(result.isReplicatedOkAfterPending());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(1, report.getStat(
        ContainerHealthState.MIS_REPLICATED));
  }

  @Test
  public void testMisReplicatedFixedByPending() {
    when(containerPlacementPolicy.validateContainerPlacement(
        any(),
        anyInt()
    )).thenAnswer(invocation -> {
      List<DatanodeDetails> dns = invocation.getArgument(0);
      // If the number of DNs is 3 or less make it be mis-replicated
      if (dns.size() <= 3) {
        return new ContainerPlacementStatusDefault(1, 2, 3);
      } else {
        return new ContainerPlacementStatusDefault(2, 2, 3);
      }
    });

    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), 0, 0, 0);

    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(new ContainerReplicaOp(
        ADD, MockDatanodeDetails.randomDatanodeDetails(), 0, null, Long.MAX_VALUE, 0));
    pending.add(new ContainerReplicaOp(
        ADD, MockDatanodeDetails.randomDatanodeDetails(), 0, null, Long.MAX_VALUE, 0));

    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setPendingOps(pending);
    MisReplicatedHealthResult result = (MisReplicatedHealthResult)
        healthCheck.checkHealth(requestBuilder.build());
    assertEquals(HealthState.MIS_REPLICATED, result.getHealthState());
    assertTrue(result.isReplicatedOkAfterPending());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(1, report.getStat(
        ContainerHealthState.MIS_REPLICATED));
  }

  @Test
  public void testWithQuasiClosedReplicas() {
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
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);
    final ContainerHealthResult result =
        healthCheck.checkHealth(requestBuilder.build());

    assertEquals(HealthState.HEALTHY, result.getHealthState());

    assertFalse(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testWithQuasiClosedReplicasWithWrongSequenceID() {
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
            IN_SERVICE, State.QUASI_CLOSED, sequenceID - 1);
    replicas.add(quasiClosedReplica);

    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);
    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(requestBuilder.build());

    assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    assertEquals(1, result.getRemainingRedundancy());
    assertFalse(result.isReplicatedOkAfterPending());
    assertFalse(result.underReplicatedDueToOutOfService());

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(1, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
  }

  @Test
  public void testExcessReplicasButNotOverReplicatedDuetoUniqueOrigins() {
    final long sequenceID = 20;
    final ContainerInfo container = ReplicationTestUtil.createContainerInfo(
        repConfig, 1, HddsProtos.LifeCycleState.QUASI_CLOSED,
        sequenceID);

    final Set<ContainerReplica> replicas = new HashSet<>(2);
    replicas.add(createContainerReplica(container.containerID(), 0,
        IN_SERVICE, State.QUASI_CLOSED, 1, 1,
        MockDatanodeDetails.randomDatanodeDetails(),
        DatanodeID.randomID(),
        sequenceID - 1));
    replicas.add(createContainerReplica(container.containerID(), 0,
        IN_SERVICE, State.QUASI_CLOSED, 1, 1,
        MockDatanodeDetails.randomDatanodeDetails(),
        DatanodeID.randomID(),
        sequenceID - 1));
    replicas.add(createContainerReplica(container.containerID(), 0,
        IN_SERVICE, State.QUASI_CLOSED, 1, 1,
        MockDatanodeDetails.randomDatanodeDetails(),
        DatanodeID.randomID(),
        sequenceID - 1));

    replicas.add(createContainerReplica(container.containerID(), 0,
        IN_SERVICE, State.UNHEALTHY, 1, 1,
        MockDatanodeDetails.randomDatanodeDetails(),
        DatanodeID.randomID(),
        sequenceID));

    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);

    assertFalse(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(0, report.getStat(
        ContainerHealthState.OVER_REPLICATED));
  }

  @Test
  public void testExcessReplicasAndOverReplicatedDuetoNonUniqueOrigins() {
    final long sequenceID = 20;
    final ContainerInfo container = ReplicationTestUtil.createContainerInfo(
        repConfig, 1, HddsProtos.LifeCycleState.QUASI_CLOSED,
        sequenceID);

    final DatanodeID origin = DatanodeID.randomID();
    final Set<ContainerReplica> replicas = new HashSet<>(2);
    replicas.add(createContainerReplica(container.containerID(), 0,
        IN_SERVICE, State.QUASI_CLOSED, 1, 1,
        MockDatanodeDetails.randomDatanodeDetails(),
        DatanodeID.randomID(),
        sequenceID - 1));
    replicas.add(createContainerReplica(container.containerID(), 0,
        IN_SERVICE, State.QUASI_CLOSED, 1, 1,
        MockDatanodeDetails.randomDatanodeDetails(),
        DatanodeID.randomID(),
        sequenceID - 1));
    replicas.add(createContainerReplica(container.containerID(), 0,
        IN_SERVICE, State.QUASI_CLOSED, 1, 1,
        MockDatanodeDetails.randomDatanodeDetails(), origin,
        sequenceID - 1));

    replicas.add(createContainerReplica(container.containerID(), 0,
        IN_SERVICE, State.UNHEALTHY, 1, 1,
        MockDatanodeDetails.randomDatanodeDetails(), origin,
        sequenceID - 1));

    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container);

    assertTrue(healthCheck.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(1, repQueue.overReplicatedQueueSize());
    assertEquals(0, report.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(1, report.getStat(
        ContainerHealthState.OVER_REPLICATED));
  }

}
