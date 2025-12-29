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
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.DELETING;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DeletingContainerHandler}.
 */
public class TestDeletingContainerHandler {
  private ReplicationManager replicationManager;
  private DeletingContainerHandler deletingContainerHandler;
  private ECReplicationConfig ecReplicationConfig;
  private RatisReplicationConfig ratisReplicationConfig;
  private ReplicationManager.ReplicationManagerConfiguration rmConf;

  @BeforeEach
  public void setup() throws IOException {

    ecReplicationConfig = new ECReplicationConfig(3, 2);
    ratisReplicationConfig = RatisReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.THREE);
    replicationManager = mock(ReplicationManager.class);
    rmConf = mock(ReplicationManager.ReplicationManagerConfiguration.class);
    doNothing().when(replicationManager).updateContainerState(any(ContainerID.class),
        any(HddsProtos.LifeCycleEvent.class));

    deletingContainerHandler =
        new DeletingContainerHandler(replicationManager);
  }

  /**
   * If a container is not in Deleting state, it should not be handled by
   * DeletingContainerHandler. It should return false so the request can be
   * passed to the next handler in the chain.
   */
  @Test
  public void testNonDeletingContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, CLOSED);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSING, 1, 2, 3, 4, 5);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertFalse(deletingContainerHandler.handle(request));
  }

  @Test
  public void testNonDeletingRatisContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, CLOSED);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSING, 0, 0, 0);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertFalse(deletingContainerHandler.handle(request));
  }

  /**
   * If a container is in Deleting state and no replica exists,
   * change the state of the container to DELETED.
   */
  @Test
  public void testCleanupIfNoReplicaExist() {
    //ratis container
    cleanupIfNoReplicaExist(RatisReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.THREE), 1);

    //ec container
    cleanupIfNoReplicaExist(ecReplicationConfig, 1);
  }

  private void cleanupIfNoReplicaExist(
      ReplicationConfig replicationConfig, int times) {
    clearInvocations(replicationManager);
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        replicationConfig, 1, DELETING);

    Set<ContainerReplica> containerReplicas = new HashSet<>();
    ContainerCheckRequest.Builder builder = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas);

    ContainerCheckRequest request = builder.build();

    builder.setReadOnly(true);
    ContainerCheckRequest readRequest = builder.build();

    assertTrue(deletingContainerHandler.handle(readRequest));
    verify(replicationManager, times(0)).updateContainerState(any(ContainerID.class),
        any(HddsProtos.LifeCycleEvent.class));

    assertTrue(deletingContainerHandler.handle(request));
    verify(replicationManager, times(times)).updateContainerState(any(ContainerID.class),
        any(HddsProtos.LifeCycleEvent.class));
  }

  /**
   * If a container is in Deleting state , some replicas exist and
   * for each replica there is a pending delete, then do nothing.
   */
  @Test
  public void testNoNeedResendDeleteCommand() throws NotLeaderException {
    //ratis container
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, DELETING);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 0, 0, 0);
    List<ContainerReplicaOp> pendingOps = new ArrayList<>();
    containerReplicas.forEach(r -> pendingOps.add(
        new ContainerReplicaOp(ContainerReplicaOp.PendingOpType.DELETE,
            r.getDatanodeDetails(), r.getReplicaIndex(), null, Long.MAX_VALUE, 0)));
    verifyDeleteCommandCount(containerInfo, containerReplicas, pendingOps, 0);

    //EC container
    containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, DELETING);
    containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4, 5);
    pendingOps.clear();
    containerReplicas.forEach(r -> pendingOps.add(
        new ContainerReplicaOp(ContainerReplicaOp.PendingOpType.DELETE,
            r.getDatanodeDetails(), r.getReplicaIndex(), null, Long.MAX_VALUE, 0)));
    verifyDeleteCommandCount(containerInfo, containerReplicas, pendingOps, 0);

  }

  /**
   * If a container is in Deleting state , some replicas exist and
   * for some replica there is no pending delete, then resending delete
   * command.
   */
  @Test
  public void testResendDeleteCommand() throws NotLeaderException {
    //ratis container
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, DELETING);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createEmptyReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 0, 0, 0);
    List<ContainerReplicaOp> pendingOps = new ArrayList<>();
    containerReplicas.stream().limit(2).forEach(replica -> pendingOps.add(
        new ContainerReplicaOp(ContainerReplicaOp.PendingOpType.DELETE,
            replica.getDatanodeDetails(), replica.getReplicaIndex(), null, Long.MAX_VALUE, 0)));
    verifyDeleteCommandCount(containerInfo, containerReplicas, pendingOps, 1);

    //EC container
    containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, DELETING);
    containerReplicas = ReplicationTestUtil
        .createEmptyReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4, 5);
    pendingOps.clear();
    containerReplicas.stream().limit(3).forEach(replica -> pendingOps.add(
        new ContainerReplicaOp(ContainerReplicaOp.PendingOpType.DELETE,
            replica.getDatanodeDetails(), replica.getReplicaIndex(), null, Long.MAX_VALUE, 0)));
    //since one delete command is end when testing ratis container, so
    //here should be 1+2 = 3 times
    verifyDeleteCommandCount(containerInfo, containerReplicas, pendingOps, 3);

  }

  @Test
  public void testDeleteCommandIsNotSentForNonEmptyReplica() throws NotLeaderException {
    // Ratis container
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, DELETING);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 0, 0, 0);
    verifyDeleteCommandCount(containerInfo, containerReplicas, Collections.emptyList(), 0);

    // EC container
    containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, DELETING);
    containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4, 5);
    verifyDeleteCommandCount(containerInfo, containerReplicas, Collections.emptyList(), 0);
  }

  private void verifyDeleteCommandCount(ContainerInfo containerInfo,
                                   Set<ContainerReplica> containerReplicas,
                                   List<ContainerReplicaOp> pendingOps,
                                   int times) throws NotLeaderException {
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(pendingOps)
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertTrue(deletingContainerHandler.handle(request));

    verify(replicationManager, times(times)).sendDeleteCommand(any(ContainerInfo.class), anyInt(),
        any(DatanodeDetails.class), eq(false));
  }
}
