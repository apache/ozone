/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container.replication.health;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationQueue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerInfo;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerReplica;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createReplicas;

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

  @Before
  public void setup() throws IOException {
    handler = new RatisUnhealthyReplicationCheckHandler();
    repConfig = RatisReplicationConfig.getInstance(THREE);
    repQueue = new ReplicationQueue();
    report = new ReplicationManagerReport();
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
    Assert.assertFalse(handler.handle(requestBuilder.build()));
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
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

    Assert.assertFalse(handler.handle(requestBuilder.build()));
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

    Assert.assertFalse(handler.handle(requestBuilder.build()));
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

    Assert.assertFalse(handler.handle(requestBuilder.build()));
  }

  @Test
  public void shouldReturnTrueForExcessUnhealthyReplicas() {
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

    ContainerHealthResult.OverReplicatedHealthResult result =
        (ContainerHealthResult.OverReplicatedHealthResult)
            handler.checkReplication(requestBuilder.build());
    Assert.assertEquals(ContainerHealthResult.HealthState.OVER_REPLICATED,
        result.getHealthState());
    Assert.assertEquals(1, result.getExcessRedundancy());
    Assert.assertFalse(result.isReplicatedOkAfterPending());

    Assert.assertTrue(handler.handle(requestBuilder.build()));
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(1, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
    Assert.assertEquals(1,
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
        ImmutableList.of(ContainerReplicaOp.create(
            ContainerReplicaOp.PendingOpType.ADD,
            MockDatanodeDetails.randomDatanodeDetails(), 0));
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setPendingOps(pendingOps);

    ContainerHealthResult.UnderReplicatedHealthResult
        result = (ContainerHealthResult.UnderReplicatedHealthResult)
        handler.checkReplication(requestBuilder.build());

    Assert.assertEquals(ContainerHealthResult.HealthState.UNDER_REPLICATED,
        result.getHealthState());
    Assert.assertEquals(0, result.getRemainingRedundancy());
    Assert.assertFalse(result.isReplicatedOkAfterPending());
    Assert.assertFalse(result.underReplicatedDueToDecommission());

    Assert.assertTrue(handler.handle(requestBuilder.build()));
    Assert.assertEquals(1, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(1,
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
        ImmutableList.of(ContainerReplicaOp.create(
            ContainerReplicaOp.PendingOpType.ADD,
            MockDatanodeDetails.randomDatanodeDetails(), 0));
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setPendingOps(pendingOps);

    ContainerHealthResult.UnderReplicatedHealthResult
        result = (ContainerHealthResult.UnderReplicatedHealthResult)
        handler.checkReplication(requestBuilder.build());

    Assert.assertEquals(ContainerHealthResult.HealthState.UNDER_REPLICATED,
        result.getHealthState());
    Assert.assertEquals(1, result.getRemainingRedundancy());
    Assert.assertTrue(result.isReplicatedOkAfterPending());
    Assert.assertFalse(result.underReplicatedDueToDecommission());

    Assert.assertTrue(handler.handle(requestBuilder.build()));
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(1,
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
        ImmutableList.of(ContainerReplicaOp.create(
            ContainerReplicaOp.PendingOpType.DELETE,
            replicas.stream().findFirst().get().getDatanodeDetails(), 0));
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setPendingOps(pendingOps);

    ContainerHealthResult.UnderReplicatedHealthResult
        result = (ContainerHealthResult.UnderReplicatedHealthResult)
        handler.checkReplication(requestBuilder.build());

    Assert.assertEquals(ContainerHealthResult.HealthState.UNDER_REPLICATED,
        result.getHealthState());
    Assert.assertEquals(1, result.getRemainingRedundancy());
    Assert.assertFalse(result.isReplicatedOkAfterPending());

    Assert.assertTrue(handler.handle(requestBuilder.build()));
    Assert.assertEquals(1, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(1,
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
        ImmutableList.of(ContainerReplicaOp.create(
            ContainerReplicaOp.PendingOpType.DELETE,
            replicas.stream().findFirst().get().getDatanodeDetails(), 0));
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setPendingOps(pendingOps);

    ContainerHealthResult.OverReplicatedHealthResult
        result = (ContainerHealthResult.OverReplicatedHealthResult)
        handler.checkReplication(requestBuilder.build());

    Assert.assertEquals(ContainerHealthResult.HealthState.OVER_REPLICATED,
        result.getHealthState());
    Assert.assertEquals(1, result.getExcessRedundancy());
    Assert.assertTrue(result.isReplicatedOkAfterPending());

    Assert.assertTrue(handler.handle(requestBuilder.build()));
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
    Assert.assertEquals(1,
        report.getStat(ReplicationManagerReport.HealthState.UNHEALTHY));
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
        handler.checkReplication(requestBuilder.build());

    Assert.assertEquals(ContainerHealthResult.HealthState.OVER_REPLICATED,
        result.getHealthState());
    Assert.assertEquals(1, result.getExcessRedundancy());
    Assert.assertFalse(result.isReplicatedOkAfterPending());

    // not safe for over replication because we don't have 3 matching replicas
    Assert.assertFalse(result.isSafelyOverReplicated());

    Assert.assertTrue(handler.handle(requestBuilder.build()));
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(1, report.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
    Assert.assertEquals(1,
        report.getStat(ReplicationManagerReport.HealthState.UNHEALTHY));
  }
}
