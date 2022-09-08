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
  private ECReplicationConfig replicationConfig;

  @BeforeEach
  public void setup() {
    replicationConfig = new ECReplicationConfig(3, 2);
    replicationManager = Mockito.mock(ReplicationManager.class);
    handler = new ClosedWithMismatchedReplicasHandler(replicationManager);
  }

  @Test
  public void testOpenContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        replicationConfig, 1, OPEN);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .pendingOps(Collections.EMPTY_LIST)
        .report(new ReplicationManagerReport())
        .containerInfo(containerInfo)
        .containerReplicas(Collections.emptySet())
        .build();

    Assertions.assertFalse(handler.handle(request));
    Mockito.verify(replicationManager, times(0))
        .sendCloseContainerReplicaCommand(
            any(), any(), anyBoolean());
  }

  @Test
  public void testClosedHealthyContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        replicationConfig, 1, CLOSED);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil.createReplicas(
        containerInfo.containerID(), ContainerReplicaProto.State.CLOSED,
        1, 2, 3, 4, 5);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .pendingOps(Collections.EMPTY_LIST)
        .report(new ReplicationManagerReport())
        .containerInfo(containerInfo)
        .containerReplicas(containerReplicas)
        .build();
    Assertions.assertFalse(handler.handle(request));

    Mockito.verify(replicationManager, times(0))
        .sendCloseContainerReplicaCommand(
          any(), any(), anyBoolean());
  }

  @Test
  public void testClosedMissMatchContainerReturnsTrue() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        replicationConfig, 1, CLOSED);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil.createReplicas(
        containerInfo.containerID(), ContainerReplicaProto.State.CLOSED,
        1, 2);
    ContainerReplica mismatch1 = ReplicationTestUtil.createContainerReplica(
        containerInfo.containerID(),4,
        HddsProtos.NodeOperationalState.IN_SERVICE,
        ContainerReplicaProto.State.OPEN);
    ContainerReplica mismatch2 = ReplicationTestUtil.createContainerReplica(
        containerInfo.containerID(),5,
        HddsProtos.NodeOperationalState.IN_SERVICE,
        ContainerReplicaProto.State.CLOSING);
    ContainerReplica mismatch3 = ReplicationTestUtil.createContainerReplica(
        containerInfo.containerID(),3,
        HddsProtos.NodeOperationalState.IN_SERVICE,
        ContainerReplicaProto.State.UNHEALTHY);
    containerReplicas.add(mismatch1);
    containerReplicas.add(mismatch2);
    containerReplicas.add(mismatch3);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .pendingOps(Collections.EMPTY_LIST)
        .report(new ReplicationManagerReport())
        .containerInfo(containerInfo)
        .containerReplicas(containerReplicas)
        .build();
    Assertions.assertTrue(handler.handle(request));

    Mockito.verify(replicationManager, times(1))
        .sendCloseContainerReplicaCommand(
            containerInfo, mismatch1.getDatanodeDetails(), true);
    Mockito.verify(replicationManager, times(1))
        .sendCloseContainerReplicaCommand(
            containerInfo, mismatch2.getDatanodeDetails(), true);
    Mockito.verify(replicationManager, times(0))
        .sendCloseContainerReplicaCommand(
            containerInfo, mismatch3.getDatanodeDetails(), true);
  }

}
