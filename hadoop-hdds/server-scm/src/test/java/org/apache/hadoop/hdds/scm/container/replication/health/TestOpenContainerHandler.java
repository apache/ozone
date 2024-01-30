/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.replication.health;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport.HealthState;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

/**
 * Tests for the OpenContainerHandler class.
 */
public class TestOpenContainerHandler {

  private ReplicationManager replicationManager;
  private OpenContainerHandler openContainerHandler;
  private ECReplicationConfig ecReplicationConfig;
  private RatisReplicationConfig ratisReplicationConfig;

  @BeforeEach
  public void setup() {
    ecReplicationConfig = new ECReplicationConfig(3, 2);
    ratisReplicationConfig = RatisReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.THREE);
    replicationManager = Mockito.mock(ReplicationManager.class);
    Mockito.when(replicationManager.hasHealthyPipeline(any())).thenReturn(true);
    openContainerHandler = new OpenContainerHandler(replicationManager);
  }

  @Test
  public void testClosedContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, CLOSED);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4, 5);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();
    Assertions.assertFalse(openContainerHandler.handle(request));
    Mockito.verify(replicationManager, times(0))
        .sendCloseContainerEvent(any());
  }

  @Test
  public void testOpenContainerReturnsTrue() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, OPEN);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.OPEN, 1, 2, 3, 4, 5);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();
    Assertions.assertTrue(openContainerHandler.handle(request));
    Mockito.verify(replicationManager, times(0))
        .sendCloseContainerEvent(any());
  }

  @Test
  public void testOpenUnhealthyContainerIsClosed() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, OPEN);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();
    ContainerCheckRequest readRequest = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReadOnly(true)
        .build();
    Assertions.assertTrue(openContainerHandler.handle(request));
    Assertions.assertTrue(openContainerHandler.handle(readRequest));
    Mockito.verify(replicationManager, times(1))
        .sendCloseContainerEvent(containerInfo.containerID());
    Assertions.assertEquals(1, request.getReport().getStat(HealthState.OPEN_UNHEALTHY));
  }

  @Test
  public void testOpenContainerWithoutPipelineIsClosed() {
    Mockito.when(replicationManager.hasHealthyPipeline(any())).thenReturn(false);
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, OPEN);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.OPEN, 1, 2, 3, 4);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();
    ContainerCheckRequest readRequest = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReadOnly(true)
        .build();
    Assertions.assertTrue(openContainerHandler.handle(request));
    Assertions.assertTrue(openContainerHandler.handle(readRequest));
    Mockito.verify(replicationManager, times(1))
        .sendCloseContainerEvent(containerInfo.containerID());
    Assertions.assertEquals(1, request.getReport().getStat(HealthState.OPEN_WITHOUT_PIPELINE));
  }

  @Test
  public void testClosedRatisContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, CLOSED);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 0, 0, 0);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();
    Assertions.assertFalse(openContainerHandler.handle(request));
    Mockito.verify(replicationManager, times(0))
        .sendCloseContainerEvent(any());
  }

  @Test
  public void testOpenRatisContainerReturnsTrue() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, OPEN);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.OPEN, 0, 0, 0);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();
    Assertions.assertTrue(openContainerHandler.handle(request));
    Mockito.verify(replicationManager, times(0))
        .sendCloseContainerEvent(any());
  }

  @Test
  public void testOpenUnhealthyRatisContainerIsClosed() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, OPEN);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 0, 0, 0);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();
    ContainerCheckRequest readRequest = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReadOnly(true)
        .build();
    Assertions.assertTrue(openContainerHandler.handle(request));
    Assertions.assertTrue(openContainerHandler.handle(readRequest));
    Mockito.verify(replicationManager, times(1))
        .sendCloseContainerEvent(any());
  }

  @Test
  public void testOpenRatisContainerWithoutPipelineIsClosed() {
    Mockito.when(replicationManager.hasHealthyPipeline(any())).thenReturn(false);
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, OPEN);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.OPEN, 0, 0, 0);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();
    ContainerCheckRequest readRequest = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReadOnly(true)
        .build();
    Assertions.assertTrue(openContainerHandler.handle(request));
    Assertions.assertTrue(openContainerHandler.handle(readRequest));
    Mockito.verify(replicationManager, times(1)).sendCloseContainerEvent(any());
    Assertions.assertEquals(1, request.getReport().getStat(ReplicationManagerReport.HealthState.OPEN_WITHOUT_PIPELINE));
  }
}
