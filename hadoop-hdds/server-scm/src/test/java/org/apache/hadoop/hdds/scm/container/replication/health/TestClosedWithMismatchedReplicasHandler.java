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
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.times;

/**
 * Tests for the ClosedWithMismatchedReplicasHandler.
 */
public class TestClosedWithMismatchedReplicasHandler {

  private ReplicationManager replicationManager;
  private ClosedWithMismatchedReplicasHandler handler;
  private ECReplicationConfig ecReplicationConfig;
  private RatisReplicationConfig ratisReplicationConfig;

  @BeforeEach
  public void setup() {
    ecReplicationConfig = new ECReplicationConfig(3, 2);
    ratisReplicationConfig = RatisReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.THREE);
    replicationManager = Mockito.mock(ReplicationManager.class);
    handler = new ClosedWithMismatchedReplicasHandler(replicationManager);
  }

  @Test
  public void testOpenContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, OPEN);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.EMPTY_LIST)
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(Collections.emptySet())
        .build();

    Assertions.assertFalse(handler.handle(request));
    Mockito.verify(replicationManager, times(0))
        .sendCloseContainerReplicaCommand(
            any(), any(), anyBoolean());
  }

  @Test
  public void testClosedHealthyContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, CLOSED);
    Set<ContainerReplica> containerReplicas =
        ReplicationTestUtil.createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4, 5);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.EMPTY_LIST)
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();
    Assertions.assertFalse(handler.handle(request));

    Mockito.verify(replicationManager, times(0))
        .sendCloseContainerReplicaCommand(
          any(), any(), anyBoolean());
  }

  @Test
  public void testCloseCommandSentForMismatchedReplicas() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, CLOSED);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 1, 2);
    ContainerReplica mismatch1 = ReplicationTestUtil.createContainerReplica(
        containerInfo.containerID(), 4,
        HddsProtos.NodeOperationalState.IN_SERVICE,
        ContainerReplicaProto.State.OPEN);
    ContainerReplica mismatch2 = ReplicationTestUtil.createContainerReplica(
        containerInfo.containerID(), 5,
        HddsProtos.NodeOperationalState.IN_SERVICE,
        ContainerReplicaProto.State.CLOSING);
    ContainerReplica mismatch3 = ReplicationTestUtil.createContainerReplica(
        containerInfo.containerID(), 3,
        HddsProtos.NodeOperationalState.IN_SERVICE,
        ContainerReplicaProto.State.UNHEALTHY);
    containerReplicas.add(mismatch1);
    containerReplicas.add(mismatch2);
    containerReplicas.add(mismatch3);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.EMPTY_LIST)
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    // this handler always returns false so other handlers can fix issues
    // such as under replication
    Assertions.assertFalse(handler.handle(request));

    Mockito.verify(replicationManager, times(1))
        .sendCloseContainerReplicaCommand(
            containerInfo, mismatch1.getDatanodeDetails(), true);
    Mockito.verify(replicationManager, times(1))
        .sendCloseContainerReplicaCommand(
            containerInfo, mismatch2.getDatanodeDetails(), true);
    // close command should not be sent for unhealthy replica
    Mockito.verify(replicationManager, times(0))
        .sendCloseContainerReplicaCommand(
            containerInfo, mismatch3.getDatanodeDetails(), true);
  }

  @Test
  public void testOpenRatisContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, OPEN);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.EMPTY_LIST)
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(Collections.emptySet())
        .build();

    Assertions.assertFalse(handler.handle(request));
    Mockito.verify(replicationManager, times(0))
        .sendCloseContainerReplicaCommand(
            any(), any(), anyBoolean());
  }

  @Test
  public void testClosedHealthyRatisContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, CLOSED);
    Set<ContainerReplica> containerReplicas =
        ReplicationTestUtil.createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 0, 0, 0);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.EMPTY_LIST)
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();
    Assertions.assertFalse(handler.handle(request));

    Mockito.verify(replicationManager, times(0))
        .sendCloseContainerReplicaCommand(
            any(), any(), anyBoolean());
  }

  @Test
  public void testCloseCommandSentForMismatchedRatisReplicas() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, CLOSED);
    ContainerReplica mismatch1 = ReplicationTestUtil.createContainerReplica(
        containerInfo.containerID(), 0,
        HddsProtos.NodeOperationalState.IN_SERVICE,
        ContainerReplicaProto.State.OPEN);
    ContainerReplica mismatch2 = ReplicationTestUtil.createContainerReplica(
        containerInfo.containerID(), 0,
        HddsProtos.NodeOperationalState.IN_SERVICE,
        ContainerReplicaProto.State.CLOSING);
    ContainerReplica mismatch3 = ReplicationTestUtil.createContainerReplica(
        containerInfo.containerID(), 0,
        HddsProtos.NodeOperationalState.IN_SERVICE,
        ContainerReplicaProto.State.UNHEALTHY);
    Set<ContainerReplica> containerReplicas = new HashSet<>();
    containerReplicas.add(mismatch1);
    containerReplicas.add(mismatch2);
    containerReplicas.add(mismatch3);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.EMPTY_LIST)
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    // this handler always returns false so other handlers can fix issues
    // such as under replication
    Assertions.assertFalse(handler.handle(request));

    Mockito.verify(replicationManager, times(1))
        .sendCloseContainerReplicaCommand(
            containerInfo, mismatch1.getDatanodeDetails(), true);
    Mockito.verify(replicationManager, times(1))
        .sendCloseContainerReplicaCommand(
            containerInfo, mismatch2.getDatanodeDetails(), true);
    // close command should not be sent for unhealthy replica
    Mockito.verify(replicationManager, times(0))
        .sendCloseContainerReplicaCommand(
            containerInfo, mismatch3.getDatanodeDetails(), true);
  }
}
