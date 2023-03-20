/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaNotFoundException;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.ozone.test.TestClock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.ADD;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.DELETE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;

/**
 * Tests for the MoveManager class.
 */
public class TestMoveManager {

  private TestClock clock;
  private ReplicationManager replicationManager;
  private ContainerManager containerManager;
  private MoveManager moveManager;
  private ContainerInfo containerInfo;
  private Set<ContainerReplica> replicas;
  private Map<DatanodeDetails, NodeStatus> nodes;
  private List<ContainerReplicaOp> pendingOps;
  private DatanodeDetails src;
  private DatanodeDetails tgt;

  @Before
  public void setup() throws ContainerNotFoundException,
      NodeNotFoundException {
    clock = new TestClock(Instant.now(), ZoneId.systemDefault());
    containerInfo = ReplicationTestUtil.createContainerInfo(
        RatisReplicationConfig.getInstance(THREE), 1,
        HddsProtos.LifeCycleState.CLOSED);
    replicas = new HashSet<>();
    nodes = new HashMap<>();
    pendingOps = new ArrayList<>();
    setupMocks();
  }

  private void setupMocks() throws ContainerNotFoundException,
      NodeNotFoundException {
    replicationManager = Mockito.mock(ReplicationManager.class);
    containerManager = Mockito.mock(ContainerManager.class);
    Mockito.when(containerManager.getContainer(eq(containerInfo.containerID())))
        .thenReturn(containerInfo);
    Mockito.when(containerManager.getContainerReplicas(
        containerInfo.containerID()))
        .thenReturn(replicas);
    Mockito.when(replicationManager.getNodeStatus(any()))
        .thenAnswer(i -> nodes.get(i.getArgument(0)));
    Mockito.when(replicationManager.getPendingReplicationOps(any()))
        .thenReturn(pendingOps);
    Mockito.when(replicationManager.getContainerReplicationHealth(any(), any()))
        .thenReturn(new ContainerHealthResult.HealthyResult(containerInfo));
    Mockito.when(replicationManager.getClock()).thenReturn(clock);

    moveManager = new MoveManager(replicationManager, containerManager);
  }

  @Test
  public void testMoveSourceOrDestNotInService() throws NodeNotFoundException,
      ContainerReplicaNotFoundException, ContainerNotFoundException,
      TimeoutException, ExecutionException, InterruptedException {
    replicas.addAll(ReplicationTestUtil
        .createReplicas(containerInfo.containerID(), 0, 0, 0));
    Iterator<ContainerReplica> iterator = replicas.iterator();
    src = iterator.next().getDatanodeDetails();
    tgt = iterator.next().getDatanodeDetails();

    nodes.put(src, NodeStatus.inServiceStale());
    nodes.put(tgt, NodeStatus.inServiceHealthy());
    assertMoveFailsWith(MoveManager.MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY,
        containerInfo.containerID());

    nodes.put(src, NodeStatus.inServiceHealthy());
    nodes.put(tgt, NodeStatus.inServiceStale());
    assertMoveFailsWith(MoveManager.MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY,
        containerInfo.containerID());

    nodes.put(src, new NodeStatus(
        HddsProtos.NodeOperationalState.DECOMMISSIONING,
        HddsProtos.NodeState.HEALTHY));
    nodes.put(tgt, NodeStatus.inServiceHealthy());
    assertMoveFailsWith(
        MoveManager.MoveResult.REPLICATION_FAIL_NODE_NOT_IN_SERVICE,
        containerInfo.containerID());

    nodes.put(src, NodeStatus.inServiceHealthy());
    nodes.put(tgt, new NodeStatus(
        HddsProtos.NodeOperationalState.DECOMMISSIONING,
        HddsProtos.NodeState.HEALTHY));
    assertMoveFailsWith(
        MoveManager.MoveResult.REPLICATION_FAIL_NODE_NOT_IN_SERVICE,
        containerInfo.containerID());
  }

  @Test
  public void testMoveSourceDoesNotExist() throws Exception {
    src = MockDatanodeDetails.randomDatanodeDetails();
    tgt = MockDatanodeDetails.randomDatanodeDetails();
    nodes.put(src, NodeStatus.inServiceHealthy());
    nodes.put(tgt, NodeStatus.inServiceHealthy());

    assertMoveFailsWith(
        MoveManager.MoveResult.REPLICATION_FAIL_NOT_EXIST_IN_SOURCE,
        containerInfo.containerID());
  }

  @Test
  public void testMoveTargetAlreadyExists() throws Exception {
    replicas.addAll(ReplicationTestUtil
        .createReplicas(containerInfo.containerID(), 0, 0, 0));
    Iterator<ContainerReplica> iterator = replicas.iterator();
    src = iterator.next().getDatanodeDetails();
    tgt = iterator.next().getDatanodeDetails();
    nodes.put(src, NodeStatus.inServiceHealthy());
    nodes.put(tgt, NodeStatus.inServiceHealthy());

    assertMoveFailsWith(
        MoveManager.MoveResult.REPLICATION_FAIL_EXIST_IN_TARGET,
        containerInfo.containerID());
  }

  @Test
  public void testMovePendingOpsExist() throws Exception {
    replicas.addAll(ReplicationTestUtil
        .createReplicas(containerInfo.containerID(), 0, 0, 0));
    Iterator<ContainerReplica> iterator = replicas.iterator();
    src = iterator.next().getDatanodeDetails();
    tgt = MockDatanodeDetails.randomDatanodeDetails();
    nodes.put(src, NodeStatus.inServiceHealthy());
    nodes.put(tgt, NodeStatus.inServiceHealthy());

    pendingOps.add(new ContainerReplicaOp(ADD, tgt, 0, clock.millis()));

    assertMoveFailsWith(
        MoveManager.MoveResult.REPLICATION_FAIL_INFLIGHT_REPLICATION,
        containerInfo.containerID());

    pendingOps.clear();
    pendingOps.add(new ContainerReplicaOp(DELETE, src, 0, clock.millis()));
    assertMoveFailsWith(
        MoveManager.MoveResult.REPLICATION_FAIL_INFLIGHT_DELETION,
        containerInfo.containerID());
  }

  @Test
  public void testMoveContainerIsNotClosed() throws Exception {
    containerInfo = ReplicationTestUtil.createContainerInfo(
        RatisReplicationConfig.getInstance(THREE), 1,
        HddsProtos.LifeCycleState.OPEN);
    setupMocks();
    replicas.addAll(ReplicationTestUtil
        .createReplicas(containerInfo.containerID(), 0, 0, 0));
    Iterator<ContainerReplica> iterator = replicas.iterator();
    src = iterator.next().getDatanodeDetails();
    tgt = MockDatanodeDetails.randomDatanodeDetails();
    nodes.put(src, NodeStatus.inServiceHealthy());
    nodes.put(tgt, NodeStatus.inServiceHealthy());

    assertMoveFailsWith(
        MoveManager.MoveResult.REPLICATION_FAIL_CONTAINER_NOT_CLOSED,
        containerInfo.containerID());
  }

  /**
   * Move should fail if the container will not be healthy after move.
   * Creates a situation where container is healthy before move but mis
   * replicated after move. Set of replicas after move will not contain the
   * source replica but will contain the target replica.
   */
  @Test
  public void testContainerIsNotHealthyAfterMove() throws Exception {
    replicas.addAll(ReplicationTestUtil
        .createReplicas(containerInfo.containerID(), 0, 0, 0));
    ContainerReplica sourceReplica = replicas.iterator().next();
    src = sourceReplica.getDatanodeDetails();
    tgt = MockDatanodeDetails.randomDatanodeDetails();
    nodes.put(src, NodeStatus.inServiceHealthy());
    nodes.put(tgt, NodeStatus.inServiceHealthy());

    // Return healthy before move but mis replicated after move
    Mockito.when(replicationManager.getContainerReplicationHealth(any(), any()))
        .thenAnswer(invocationOnMock -> {
          Set<ContainerReplica> replicasBeingChecked =
              invocationOnMock.getArgument(1);
          if (replicasBeingChecked.contains(sourceReplica)) {
            // before move
            return new ContainerHealthResult.HealthyResult(containerInfo);
          } else {
            // after move
            return new ContainerHealthResult.MisReplicatedHealthResult(
                containerInfo, false);
          }
        });

    assertMoveFailsWith(
        MoveManager.MoveResult.REPLICATION_NOT_HEALTHY_AFTER_MOVE,
        containerInfo.containerID());
  }

  /**
   * If the container has issues such as under, over or mis replication
   * before moving then move should fail.
   */
  @Test
  public void testContainerIsNotHealthyBeforeMove() throws Exception {
    // return an under replicated health result from replication manager
    Mockito.when(replicationManager.getContainerReplicationHealth(any(), any()))
        .thenReturn(new ContainerHealthResult.UnderReplicatedHealthResult(
            containerInfo, 1, false, false,
            false));

    // Check for an under replicated ratis container
    replicas.addAll(ReplicationTestUtil
        .createReplicas(containerInfo.containerID(), 0, 0));
    src = replicas.iterator().next().getDatanodeDetails();
    tgt = MockDatanodeDetails.randomDatanodeDetails();
    nodes.put(src, NodeStatus.inServiceHealthy());
    nodes.put(tgt, NodeStatus.inServiceHealthy());
    assertMoveFailsWith(
        MoveManager.MoveResult.REPLICATION_NOT_HEALTHY_BEFORE_MOVE,
        containerInfo.containerID());

    // check for an under replicated EC container
    containerInfo = ReplicationTestUtil.createContainer(
        HddsProtos.LifeCycleState.CLOSED, new ECReplicationConfig(3, 2));
    replicas.clear();
    replicas.addAll(ReplicationTestUtil.createReplicas(
        containerInfo.containerID(), ContainerReplicaProto.State.CLOSED,
        1, 2, 3, 4));
    src = replicas.iterator().next().getDatanodeDetails();
    nodes.put(src, NodeStatus.inServiceHealthy());
    assertMoveFailsWith(
        MoveManager.MoveResult.REPLICATION_NOT_HEALTHY_BEFORE_MOVE,
        containerInfo.containerID());
  }

  @Test
  public void testExistingMoveScheduled() throws Exception {
    setupSuccessfulMove();
    // Try to make the same move again
    CompletableFuture<MoveManager.MoveResult> res =
        moveManager.move(containerInfo.containerID(), src, tgt);
    Assert.assertEquals(
        MoveManager.MoveResult.FAIL_CONTAINER_ALREADY_BEING_MOVED, res.get());
  }

  @Test
  public void testReplicationCommandFails() throws Exception {
    Mockito.doThrow(new RuntimeException("test")).when(replicationManager)
            .sendLowPriorityReplicateContainerCommand(
        any(), anyInt(), any(), any(), anyLong(), anyLong());
    CompletableFuture<MoveManager.MoveResult> res = setupSuccessfulMove();
    Assert.assertEquals(
        MoveManager.MoveResult.FAIL_UNEXPECTED_ERROR, res.get());
  }

  @Test
  public void testSuccessfulMove() throws Exception {
    CompletableFuture<MoveManager.MoveResult> res = setupSuccessfulMove();

    ContainerReplicaOp op = new ContainerReplicaOp(
        ADD, tgt, 0, clock.millis() + 1000);
    moveManager.opCompleted(op, containerInfo.containerID(), false);

    Mockito.verify(replicationManager).sendDeleteCommand(
        eq(containerInfo), eq(0), eq(src), eq(true));

    op = new ContainerReplicaOp(
        DELETE, src, 0, clock.millis() + 1000);
    moveManager.opCompleted(op, containerInfo.containerID(), false);

    MoveManager.MoveResult finalResult = res.get();
    Assertions.assertEquals(MoveManager.MoveResult.COMPLETED, finalResult);
  }

  @Test
  public void testSuccessfulMoveNonZeroRepIndex() throws Exception {
    containerInfo = ReplicationTestUtil.createContainer(
        HddsProtos.LifeCycleState.CLOSED, new ECReplicationConfig(3, 2));
    setupMocks();

    replicas.addAll(ReplicationTestUtil
        .createReplicas(containerInfo.containerID(), 1, 2, 3, 4, 5));
    Iterator<ContainerReplica> iterator = replicas.iterator();
    ContainerReplica srcReplica = iterator.next();
    src = srcReplica.getDatanodeDetails();
    tgt = MockDatanodeDetails.randomDatanodeDetails();
    nodes.put(src, NodeStatus.inServiceHealthy());
    nodes.put(tgt, NodeStatus.inServiceHealthy());

    CompletableFuture<MoveManager.MoveResult> res =
        moveManager.move(containerInfo.containerID(), src, tgt);

    Mockito.verify(replicationManager).sendLowPriorityReplicateContainerCommand(
        eq(containerInfo), eq(srcReplica.getReplicaIndex()), eq(src), eq(tgt),
        anyLong(), anyLong());

    ContainerReplicaOp op = new ContainerReplicaOp(
        ADD, tgt, srcReplica.getReplicaIndex(), clock.millis() + 1000);
    moveManager.opCompleted(op, containerInfo.containerID(), false);

    Mockito.verify(replicationManager).sendDeleteCommand(
        eq(containerInfo), eq(srcReplica.getReplicaIndex()), eq(src),
        eq(true));

    op = new ContainerReplicaOp(
        DELETE, src, srcReplica.getReplicaIndex(), clock.millis() + 1000);
    moveManager.opCompleted(op, containerInfo.containerID(), false);

    MoveManager.MoveResult finalResult = res.get();
    Assertions.assertEquals(MoveManager.MoveResult.COMPLETED, finalResult);
  }

  @Test
  public void testMoveTimeoutOnAdd() throws Exception {
    CompletableFuture<MoveManager.MoveResult> res = setupSuccessfulMove();

    ContainerReplicaOp op = new ContainerReplicaOp(
        ADD, tgt, 0, clock.millis() + 1000);
    moveManager.opCompleted(op, containerInfo.containerID(), true);

    MoveManager.MoveResult finalResult = res.get();
    Assertions.assertEquals(MoveManager.MoveResult.REPLICATION_FAIL_TIME_OUT,
        finalResult);
  }

  @Test
  public void testMoveTimeoutOnDelete() throws Exception {
    CompletableFuture<MoveManager.MoveResult> res = setupSuccessfulMove();

    ContainerReplicaOp op = new ContainerReplicaOp(
        ADD, tgt, 0, clock.millis() + 1000);
    moveManager.opCompleted(op, containerInfo.containerID(), false);

    Mockito.verify(replicationManager).sendDeleteCommand(
        eq(containerInfo), eq(0), eq(src), eq(true));

    op = new ContainerReplicaOp(
        DELETE, src, 0, clock.millis() + 1000);
    moveManager.opCompleted(op, containerInfo.containerID(), true);

    MoveManager.MoveResult finalResult = res.get();
    Assertions.assertEquals(MoveManager.MoveResult.DELETION_FAIL_TIME_OUT,
        finalResult);
  }

  @Test
  public void testMoveCompleteSrcNoLongerPresent() throws Exception {
    CompletableFuture<MoveManager.MoveResult> res = setupSuccessfulMove();

    // Remove the source replica from the map
    Iterator<ContainerReplica>  iterator = replicas.iterator();
    while (iterator.hasNext()) {
      ContainerReplica r = iterator.next();
      if (r.getDatanodeDetails().equals(src)) {
        iterator.remove();
        break;
      }
    }
    ContainerReplicaOp op = new ContainerReplicaOp(
        ADD, tgt, 0, clock.millis() + 1000);
    moveManager.opCompleted(op, containerInfo.containerID(), false);

    MoveManager.MoveResult finalResult = res.get();
    Assertions.assertEquals(MoveManager.MoveResult.COMPLETED, finalResult);

    Mockito.verify(replicationManager, Mockito.times(0))
        .sendDeleteCommand(eq(containerInfo), eq(0), eq(src), eq(true));
  }

  @Test
  public void testMoveCompleteSrcNotHealthy() throws Exception {
    CompletableFuture<MoveManager.MoveResult> res = setupSuccessfulMove();

    nodes.put(src, NodeStatus.inServiceStale());
    ContainerReplicaOp op = new ContainerReplicaOp(
        ADD, tgt, 0, clock.millis() + 1000);
    moveManager.opCompleted(op, containerInfo.containerID(), false);

    MoveManager.MoveResult finalResult = res.get();
    Assertions.assertEquals(MoveManager.MoveResult.DELETION_FAIL_NODE_UNHEALTHY,
        finalResult);

    Mockito.verify(replicationManager, Mockito.times(0))
        .sendDeleteCommand(eq(containerInfo), eq(0), eq(src), eq(true));
  }

  @Test
  public void testMoveCompleteSrcNotInService() throws Exception {
    CompletableFuture<MoveManager.MoveResult> res = setupSuccessfulMove();

    nodes.put(src, new NodeStatus(
        HddsProtos.NodeOperationalState.DECOMMISSIONING,
        HddsProtos.NodeState.HEALTHY));
    ContainerReplicaOp op = new ContainerReplicaOp(
        ADD, tgt, 0, clock.millis() + 1000);
    moveManager.opCompleted(op, containerInfo.containerID(), false);

    MoveManager.MoveResult finalResult = res.get();
    Assertions.assertEquals(
        MoveManager.MoveResult.DELETION_FAIL_NODE_NOT_IN_SERVICE, finalResult);

    Mockito.verify(replicationManager, Mockito.times(0))
        .sendDeleteCommand(eq(containerInfo), eq(0), eq(src), eq(true));
  }

  @Test
  public void testMoveCompleteFutureReplicasUnhealthy() throws Exception {
    CompletableFuture<MoveManager.MoveResult> res = setupSuccessfulMove();

    Mockito.when(replicationManager.getContainerReplicationHealth(any(), any()))
        .thenReturn(new ContainerHealthResult
            .MisReplicatedHealthResult(containerInfo, false));

    ContainerReplicaOp op = new ContainerReplicaOp(
        ADD, tgt, 0, clock.millis() + 1000);
    moveManager.opCompleted(op, containerInfo.containerID(), false);

    MoveManager.MoveResult finalResult = res.get();
    Assertions.assertEquals(
        MoveManager.MoveResult.DELETE_FAIL_POLICY, finalResult);

    Mockito.verify(replicationManager, Mockito.times(0))
        .sendDeleteCommand(eq(containerInfo), eq(0), eq(src), eq(true));
  }

  private CompletableFuture<MoveManager.MoveResult> setupSuccessfulMove()
      throws Exception {
    replicas.addAll(ReplicationTestUtil
        .createReplicas(containerInfo.containerID(), 0, 0, 0));
    Iterator<ContainerReplica> iterator = replicas.iterator();
    src = iterator.next().getDatanodeDetails();
    tgt = MockDatanodeDetails.randomDatanodeDetails();
    nodes.put(src, NodeStatus.inServiceHealthy());
    nodes.put(tgt, NodeStatus.inServiceHealthy());

    CompletableFuture<MoveManager.MoveResult> res =
        moveManager.move(containerInfo.containerID(), src, tgt);

    Mockito.verify(replicationManager).sendLowPriorityReplicateContainerCommand(
        eq(containerInfo), eq(0), eq(src), eq(tgt), anyLong(), anyLong());

    return res;
  }

  private void assertMoveFailsWith(MoveManager.MoveResult expectedResult,
      ContainerID containerId) throws
      NodeNotFoundException, ContainerReplicaNotFoundException,
      ContainerNotFoundException, TimeoutException, ExecutionException,
      InterruptedException {
    CompletableFuture<MoveManager.MoveResult> res = moveManager.move(
        containerId, src, tgt);
    MoveManager.MoveResult actualResult = res.get();
    Assertions.assertEquals(expectedResult, actualResult);
  }
}
