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
import static org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult.HealthState.MIS_REPLICATED;
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
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ECMisReplicationCheckHandler}.
 */
public class TestECMisReplicationCheckHandler {
  private ECMisReplicationCheckHandler handler;
  private ECReplicationConfig repConfig;
  private ReplicationQueue repQueue;
  private ContainerCheckRequest.Builder requestBuilder;
  private ReplicationManagerReport report;
  private PlacementPolicy placementPolicy;

  @BeforeEach
  public void setup() {
    placementPolicy = mock(PlacementPolicy.class);
    when(placementPolicy.validateContainerPlacement(anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(5, 5, 5));
    handler = new ECMisReplicationCheckHandler(placementPolicy);
    repConfig = new ECReplicationConfig(3, 2);
    repQueue = new ReplicationQueue();
    ReplicationManager.ReplicationManagerConfiguration rmConf =
        mock(ReplicationManager.ReplicationManagerConfiguration.class);
    report = new ReplicationManagerReport(rmConf.getContainerSampleLimit());
    int maintenanceRedundancy = 2;
    requestBuilder = new ContainerCheckRequest.Builder()
        .setReplicationQueue(repQueue)
        .setMaintenanceRedundancy(maintenanceRedundancy)
        .setPendingOps(Collections.emptyList())
        .setReport(report);
  }

  @Test
  public void shouldReturnFalseForHealthyContainer() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), 1, 2, 3, 4, 5);
    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .build();

    ContainerHealthResult result = handler.checkMisReplication(request);
    assertEquals(ContainerHealthResult.HealthState.HEALTHY,
        result.getHealthState());

    assertFalse(handler.handle(request));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void shouldReturnFalseForNonECContainer() {
    ContainerInfo container =
        createContainerInfo(RatisReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.THREE));
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), 1, 2, 3, 4, 5);
    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .build();

    assertFalse(handler.handle(request));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void shouldHandleMisReplicatedContainer() {
    ContainerInfo container = createContainerInfo(repConfig);

    // Placement policy is always violated
    when(placementPolicy.validateContainerPlacement(any(), anyInt()
    )).thenAnswer(invocation ->
        new ContainerPlacementStatusDefault(4, 5, 9));

    Set<ContainerReplica> replicas =  createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
        Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
        Pair.of(IN_SERVICE, 5));
    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .build();

    ContainerHealthResult result = handler.checkMisReplication(request);
    assertEquals(MIS_REPLICATED, result.getHealthState());

    assertTrue(handler.handle(request));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.MIS_REPLICATED));
  }

  /**
   * If mis replication is fixed by a pending ADD, then the container should
   * not be queued.
   */
  @Test
  public void shouldReturnFalseForMisReplicatedContainerFixedByPending() {
    ContainerInfo container = createContainerInfo(repConfig);

    when(placementPolicy.validateContainerPlacement(any(), anyInt()
    )).thenAnswer(invocation -> {
      List<DatanodeDetails> dns = invocation.getArgument(0);
      // If the number of DNs is 5 or less make it be mis-replicated
      if (dns.size() <= 5) {
        return new ContainerPlacementStatusDefault(4, 5, 9);
      } else {
        return new ContainerPlacementStatusDefault(5, 5, 9);
      }
    });

    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(new ContainerReplicaOp(
        ADD, MockDatanodeDetails.randomDatanodeDetails(), 1, null, Long.MAX_VALUE, 0));

    Set<ContainerReplica> replicas =  createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
        Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
        Pair.of(IN_SERVICE, 5));
    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setPendingOps(pending)
        .build();

    ContainerHealthResult result = handler.checkMisReplication(request);
    assertEquals(MIS_REPLICATED, result.getHealthState());

    assertTrue(handler.handle(request));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.MIS_REPLICATED));
  }

  /**
   * This tests what happens when there's a pending DELETE. Suppose there's a
   * mis replicated container with an excess unhealthy replica. And, there's a
   * pending delete scheduled for the unhealthy replica. Then this container
   * should not be queued.
   */
  @Test
  public void testMisReplicationWithUnhealthyReplica() {
    ContainerInfo container = createContainerInfo(repConfig);

    when(placementPolicy.validateContainerPlacement(any(), anyInt()
    )).thenAnswer(invocation -> {
      List<DatanodeDetails> dns = invocation.getArgument(0);
      // If the number of DNs is 6 or more make it mis-replicated
      if (dns.size() > 5) {
        return new ContainerPlacementStatusDefault(5, 6, 9);
      } else {
        return new ContainerPlacementStatusDefault(5, 5, 9);
      }
    });

    Set<ContainerReplica> replicas =  createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
        Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
        Pair.of(IN_SERVICE, 5));
    ContainerReplica unhealthyReplica =
        createContainerReplica(container.containerID(), 1, IN_SERVICE,
            State.UNHEALTHY);
    replicas.add(unhealthyReplica);
    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(new ContainerReplicaOp(
        DELETE, unhealthyReplica.getDatanodeDetails(), 1, null, Long.MAX_VALUE, 0));

    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setPendingOps(pending)
        .build();

    ContainerHealthResult result = handler.checkMisReplication(request);
    assertEquals(MIS_REPLICATED, result.getHealthState());

    assertTrue(handler.handle(request));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(0, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
    assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.MIS_REPLICATED));
  }

}
