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
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSING;

/**
 * Tests for {@link EmptyContainerHandler}.
 */
public class TestEmptyContainerHandler {
  private ReplicationManager replicationManager;
  private ContainerManager containerManager;
  private EmptyContainerHandler emptyContainerHandler;
  private ECReplicationConfig ecReplicationConfig;
  private RatisReplicationConfig ratisReplicationConfig;

  @BeforeEach
  public void setup()
      throws IOException, InvalidStateTransitionException, TimeoutException {
    ecReplicationConfig = new ECReplicationConfig(3, 2);
    ratisReplicationConfig = RatisReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.THREE);
    replicationManager = Mockito.mock(ReplicationManager.class);
    containerManager = Mockito.mock(ContainerManager.class);

    emptyContainerHandler =
        new EmptyContainerHandler(replicationManager, containerManager);
  }

  /**
   * A container is considered empty if it has 0 key count. Handler should
   * return true for empty and CLOSED EC containers.
   */
  @Test
  public void testEmptyAndClosedECContainerReturnsTrue() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, CLOSED, 0L, 123L);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 0L, 123L, 1, 2, 3, 4, 5);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertAndVerify(request, true, 5);
  }

  @Test
  public void testEmptyAndClosedRatisContainerReturnsTrue() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, CLOSED, 0L, 123L);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 0L, 123L, 0, 0, 0);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertAndVerify(request, true, 3);
  }

  /**
   * Handler should return false when key count is 0 but the container is not
   * in CLOSED state.
   */
  @Test
  public void testEmptyAndNonClosedECContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, CLOSING, 0L, 123L);

    // though key count is 0, the container and its replicas are not CLOSED
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.OPEN, 0L, 123L, 1, 2, 3, 4, 5);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertAndVerify(request, false, 0);
  }

  /**
   * This test exists to verify that the definition of an empty container is
   * 0 key count. Number of used bytes are not considered.
   */
  @Test
  public void testNonEmptyRatisContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, CLOSED, 5L, 123L);

    // though container and its replicas are CLOSED, key count is not 0
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 5L, 123L, 0, 0, 0);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertAndVerify(request, false, 0);
  }

  @Test
  public void testEmptyContainerWithNonZeroBytesUsedReturnsTrue() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, CLOSED, 0L, 123L);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 0L, 123L, 0, 0, 0);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertAndVerify(request, true, 3);
  }

  private void assertAndVerify(ContainerCheckRequest request,
      boolean assertion, int times) {
    Assertions.assertEquals(assertion, emptyContainerHandler.handle(request));
    try {
      Mockito.verify(replicationManager, Mockito.times(times))
          .sendDeleteCommand(Mockito.any(ContainerInfo.class), Mockito.anyInt(),
              Mockito.any(DatanodeDetails.class));

      if (times > 0) {
        Mockito.verify(containerManager, Mockito.times(1))
            .updateContainerState(Mockito.any(ContainerID.class),
                Mockito.any(HddsProtos.LifeCycleEvent.class));
      }
    } catch (Exception e) {
      // meaningless; do nothing
    }
  }

}
