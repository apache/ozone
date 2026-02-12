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

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.QUASI_CLOSED;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getContainer;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getReplicas;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link QuasiClosedContainerHandler}. This handler is only meant
 * to handle Ratis containers.
 */
public class TestQuasiClosedContainerHandler {
  private ReplicationManager replicationManager;
  private ReplicationManager.ReplicationManagerConfiguration rmConf;
  private QuasiClosedContainerHandler quasiClosedContainerHandler;
  private RatisReplicationConfig ratisReplicationConfig;

  @BeforeEach
  public void setup() {
    ratisReplicationConfig = RatisReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.THREE);
    replicationManager = mock(ReplicationManager.class);
    rmConf = mock(ReplicationManager.ReplicationManagerConfiguration.class);
    quasiClosedContainerHandler =
        new QuasiClosedContainerHandler(replicationManager);
  }

  @Test
  public void testECContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        new ECReplicationConfig(3, 2), 1, QUASI_CLOSED);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            State.QUASI_CLOSED, 1, 2, 3);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertFalse(quasiClosedContainerHandler.handle(request));
    verify(replicationManager, times(0))
        .sendCloseContainerReplicaCommand(any(), any(), anyBoolean());
  }

  @Test
  public void testOpenContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, OPEN);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            State.OPEN, 0, 0, 0);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertFalse(quasiClosedContainerHandler.handle(request));
    verify(replicationManager, times(0))
        .sendCloseContainerReplicaCommand(any(), any(), anyBoolean());
  }

  /**
   * When a container is QUASI_CLOSED, and it has only 2
   * replicas in QUASI_CLOSED state with unique origin node id,
   * the handler should not force close it as all 3 unique replicas are needed.
   */
  @Test
  public void testQuasiClosedWithQuorumReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, QUASI_CLOSED);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            State.QUASI_CLOSED, 0, 0);
    ContainerReplica openReplica = ReplicationTestUtil.createContainerReplica(
        containerInfo.containerID(), 0,
        HddsProtos.NodeOperationalState.IN_SERVICE, State.OPEN);
    containerReplicas.add(openReplica);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();
    ContainerCheckRequest readRequest = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReadOnly(true)
        .build();

    assertFalse(quasiClosedContainerHandler.handle(request));
    assertFalse(quasiClosedContainerHandler.handle(readRequest));
    verify(replicationManager, times(0))
        .sendCloseContainerReplicaCommand(any(), any(), anyBoolean());
    assertEquals(1, request.getReport().getStat(
        ContainerHealthState.QUASI_CLOSED_STUCK));
  }

  /**
   * When a container is QUASI_CLOSED, and all 3 replicas are reported with unique
   * origins, it should be forced closed.
   */
  @Test
  public void testQuasiClosedWithAllUniqueOriginSendsForceClose() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, QUASI_CLOSED);
    // These 3 replicas will have the same BCSID and unique origin node ids
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            State.QUASI_CLOSED, 0, 0, 0);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();
    ContainerCheckRequest readRequest = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReadOnly(true)
        .build();

    assertFalse(quasiClosedContainerHandler.handle(request));
    assertFalse(quasiClosedContainerHandler.handle(readRequest));
    verify(replicationManager, times(3))
        .sendCloseContainerReplicaCommand(any(), any(), anyBoolean());
  }

  /**
   * When a container is QUASI_CLOSED with some unhealthy and all 3 are reported with unique
   * origins, it should be forced closed.
   */
  @Test
  public void testQuasiClosedWithAllUniqueOriginAndUnhealthySendsForceClose() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, QUASI_CLOSED);
    // These 3 replicas will have the same BCSID and unique origin node ids
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            State.QUASI_CLOSED, 0, 0);
    containerReplicas.addAll(ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            State.UNHEALTHY, 0));
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();
    ContainerCheckRequest readRequest = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReadOnly(true)
        .build();

    assertFalse(quasiClosedContainerHandler.handle(request));
    assertFalse(quasiClosedContainerHandler.handle(readRequest));
    verify(replicationManager, times(2))
        .sendCloseContainerReplicaCommand(any(), any(), anyBoolean());
  }

  /**
   * If it's possible to force close replicas then only replicas with the
   * highest Sequence ID (also known as BCSID) should be closed.
   */
  @Test
  public void testQuasiClosedWithUnhealthyHavingHighestSeq() {
    final ContainerInfo containerInfo =
        getContainer(HddsProtos.LifeCycleState.QUASI_CLOSED);
    containerInfo.setUsedBytes(99);
    final ContainerID id = containerInfo.containerID();

    // create replicas with unique origin DNs
    DatanodeDetails dnOne = randomDatanodeDetails();
    DatanodeDetails dnTwo = randomDatanodeDetails();
    DatanodeDetails dnThree = randomDatanodeDetails();

    // 1001 is the highest sequence id
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, dnOne.getID(), dnOne);
    final ContainerReplica replicaTwo = getReplicas(
        id, State.QUASI_CLOSED, 1000L, dnTwo.getID(), dnTwo);
    final ContainerReplica replicaThree = getReplicas(
        id, State.UNHEALTHY, 1001L, dnThree.getID(), dnThree);
    Set<ContainerReplica> containerReplicas = new HashSet<>();
    containerReplicas.add(replicaOne);
    containerReplicas.add(replicaTwo);
    containerReplicas.add(replicaThree);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();
    ContainerCheckRequest readRequest = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReadOnly(true)
        .build();

    assertFalse(quasiClosedContainerHandler.handle(request));
    assertFalse(quasiClosedContainerHandler.handle(readRequest));
    // verify no close commands are sent as the container cannot be closed.
    verify(replicationManager, times(0))
        .sendCloseContainerReplicaCommand(eq(containerInfo), any(), anyBoolean());
  }

  /**
   * The replicas are QUASI_CLOSED, but all of them have the same origin node
   * id. Since all replicas must have unique origin node ids, the handler
   * should not force close it.
   */
  @Test
  public void testHealthyQuasiClosedContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, QUASI_CLOSED);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicasWithSameOrigin(containerInfo.containerID(),
            State.QUASI_CLOSED, 0, 0, 0);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertFalse(quasiClosedContainerHandler.handle(request));
    verify(replicationManager, times(0))
        .sendCloseContainerReplicaCommand(any(), any(), anyBoolean());
    assertEquals(1, request.getReport().getStat(
        ContainerHealthState.QUASI_CLOSED_STUCK));
  }

  /**
   * Only one replica is in QUASI_CLOSED state. This fails the condition of
   * having all replicas with unique origin nodes in QUASI_CLOSED state.
   */
  @Test
  public void testQuasiClosedWithTwoOpenReplicasReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, QUASI_CLOSED);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicasWithSameOrigin(containerInfo.containerID(),
            State.OPEN, 0, 0);
    ContainerReplica quasiClosed = ReplicationTestUtil.createContainerReplica(
        containerInfo.containerID(), 0,
        HddsProtos.NodeOperationalState.IN_SERVICE, State.QUASI_CLOSED);
    containerReplicas.add(quasiClosed);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertFalse(quasiClosedContainerHandler.handle(request));
    verify(replicationManager, times(0))
        .sendCloseContainerReplicaCommand(any(), any(), anyBoolean());
    assertEquals(1, request.getReport().getStat(
        ContainerHealthState.QUASI_CLOSED_STUCK));
  }

  /**
   * If it's possible to force close replicas then only replicas with the
   * highest Sequence ID (also known as BCSID) should be closed.
   */
  @Test
  public void testReplicasWithHighestBCSIDAreClosed() {
    final ContainerInfo containerInfo =
        getContainer(HddsProtos.LifeCycleState.QUASI_CLOSED);
    containerInfo.setUsedBytes(99);
    final ContainerID id = containerInfo.containerID();

    // create replicas with unique origin DNs
    DatanodeDetails dnOne = randomDatanodeDetails();
    DatanodeDetails dnTwo = randomDatanodeDetails();
    DatanodeDetails dnThree = randomDatanodeDetails();

    // 1001 is the highest sequence id
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, dnOne.getID(), dnOne);
    final ContainerReplica replicaTwo = getReplicas(
        id, State.QUASI_CLOSED, 1001L, dnTwo.getID(), dnTwo);
    final ContainerReplica replicaThree = getReplicas(
        id, State.QUASI_CLOSED, 1001L, dnThree.getID(), dnThree);
    Set<ContainerReplica> containerReplicas = new HashSet<>();
    containerReplicas.add(replicaOne);
    containerReplicas.add(replicaTwo);
    containerReplicas.add(replicaThree);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();
    ContainerCheckRequest readRequest = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReadOnly(true)
        .build();

    assertFalse(quasiClosedContainerHandler.handle(request));
    assertFalse(quasiClosedContainerHandler.handle(readRequest));
    // verify close command was sent for replicas with sequence ID 1001, that
    // is dnTwo and dnThree
    verify(replicationManager, times(1))
        .sendCloseContainerReplicaCommand(eq(containerInfo), eq(dnTwo), anyBoolean());
    verify(replicationManager, times(1))
        .sendCloseContainerReplicaCommand(eq(containerInfo), eq(dnThree), anyBoolean());
  }
}
