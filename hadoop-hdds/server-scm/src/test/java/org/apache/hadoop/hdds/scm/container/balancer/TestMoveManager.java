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

package org.apache.hadoop.hdds.scm.container.balancer;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.container.balancer.MoveManager.MoveResult.COMPLETED;
import static org.apache.hadoop.hdds.scm.container.balancer.MoveManager.MoveResult.DELETE_FAIL_POLICY;
import static org.apache.hadoop.hdds.scm.container.balancer.MoveManager.MoveResult.DELETION_FAIL_NODE_NOT_IN_SERVICE;
import static org.apache.hadoop.hdds.scm.container.balancer.MoveManager.MoveResult.DELETION_FAIL_NODE_UNHEALTHY;
import static org.apache.hadoop.hdds.scm.container.balancer.MoveManager.MoveResult.DELETION_FAIL_TIME_OUT;
import static org.apache.hadoop.hdds.scm.container.balancer.MoveManager.MoveResult.FAIL_CONTAINER_ALREADY_BEING_MOVED;
import static org.apache.hadoop.hdds.scm.container.balancer.MoveManager.MoveResult.FAIL_UNEXPECTED_ERROR;
import static org.apache.hadoop.hdds.scm.container.balancer.MoveManager.MoveResult.REPLICATION_FAIL_CONTAINER_NOT_CLOSED;
import static org.apache.hadoop.hdds.scm.container.balancer.MoveManager.MoveResult.REPLICATION_FAIL_EXIST_IN_TARGET;
import static org.apache.hadoop.hdds.scm.container.balancer.MoveManager.MoveResult.REPLICATION_FAIL_INFLIGHT_DELETION;
import static org.apache.hadoop.hdds.scm.container.balancer.MoveManager.MoveResult.REPLICATION_FAIL_INFLIGHT_REPLICATION;
import static org.apache.hadoop.hdds.scm.container.balancer.MoveManager.MoveResult.REPLICATION_FAIL_NODE_NOT_IN_SERVICE;
import static org.apache.hadoop.hdds.scm.container.balancer.MoveManager.MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY;
import static org.apache.hadoop.hdds.scm.container.balancer.MoveManager.MoveResult.REPLICATION_FAIL_NOT_EXIST_IN_SOURCE;
import static org.apache.hadoop.hdds.scm.container.balancer.MoveManager.MoveResult.REPLICATION_FAIL_TIME_OUT;
import static org.apache.hadoop.hdds.scm.container.balancer.MoveManager.MoveResult.REPLICATION_NOT_HEALTHY_AFTER_MOVE;
import static org.apache.hadoop.hdds.scm.container.balancer.MoveManager.MoveResult.REPLICATION_NOT_HEALTHY_BEFORE_MOVE;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.ADD;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.DELETE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

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

  @BeforeEach
  public void setup() throws ContainerNotFoundException,
      NodeNotFoundException {
    clock = TestClock.newInstance();
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
    replicationManager = mock(ReplicationManager.class);
    containerManager = mock(ContainerManager.class);
    when(containerManager.getContainer(eq(containerInfo.containerID())))
        .thenReturn(containerInfo);
    when(containerManager.getContainerReplicas(
        containerInfo.containerID()))
        .thenReturn(replicas);
    when(replicationManager.getNodeStatus(any()))
        .thenAnswer(i -> nodes.get(i.getArgument(0)));
    when(replicationManager.getPendingReplicationOps(any()))
        .thenReturn(pendingOps);
    when(replicationManager.getContainerReplicationHealth(any(), any()))
        .thenReturn(new ContainerHealthResult.HealthyResult(containerInfo));
    when(replicationManager.getClock()).thenReturn(clock);

    moveManager = new MoveManager(replicationManager, containerManager);
  }

  @Test
  public void testMoveSourceOrDestNotInService() throws NodeNotFoundException,
      ContainerReplicaNotFoundException, ContainerNotFoundException,
      ExecutionException, InterruptedException {
    replicas.addAll(ReplicationTestUtil
        .createReplicas(containerInfo.containerID(), 0, 0, 0));
    Iterator<ContainerReplica> iterator = replicas.iterator();
    src = iterator.next().getDatanodeDetails();
    tgt = iterator.next().getDatanodeDetails();

    nodes.put(src, NodeStatus.inServiceStale());
    nodes.put(tgt, NodeStatus.inServiceHealthy());
    assertMoveFailsWith(REPLICATION_FAIL_NODE_UNHEALTHY,
        containerInfo.containerID());

    nodes.put(src, NodeStatus.inServiceHealthy());
    nodes.put(tgt, NodeStatus.inServiceStale());
    assertMoveFailsWith(REPLICATION_FAIL_NODE_UNHEALTHY,
        containerInfo.containerID());

    nodes.put(src, NodeStatus.valueOf(
        HddsProtos.NodeOperationalState.DECOMMISSIONING,
        HddsProtos.NodeState.HEALTHY));
    nodes.put(tgt, NodeStatus.inServiceHealthy());
    assertMoveFailsWith(REPLICATION_FAIL_NODE_NOT_IN_SERVICE,
        containerInfo.containerID());

    nodes.put(src, NodeStatus.inServiceHealthy());
    nodes.put(tgt, NodeStatus.valueOf(
        HddsProtos.NodeOperationalState.DECOMMISSIONING,
        HddsProtos.NodeState.HEALTHY));
    assertMoveFailsWith(REPLICATION_FAIL_NODE_NOT_IN_SERVICE,
        containerInfo.containerID());
  }

  @Test
  public void testMoveSourceDoesNotExist() throws Exception {
    src = MockDatanodeDetails.randomDatanodeDetails();
    tgt = MockDatanodeDetails.randomDatanodeDetails();
    nodes.put(src, NodeStatus.inServiceHealthy());
    nodes.put(tgt, NodeStatus.inServiceHealthy());

    assertMoveFailsWith(REPLICATION_FAIL_NOT_EXIST_IN_SOURCE,
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

    assertMoveFailsWith(REPLICATION_FAIL_EXIST_IN_TARGET,
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

    pendingOps.add(new ContainerReplicaOp(ADD, tgt, 0, null, clock.millis(), 0));

    assertMoveFailsWith(REPLICATION_FAIL_INFLIGHT_REPLICATION,
        containerInfo.containerID());

    pendingOps.clear();
    pendingOps.add(new ContainerReplicaOp(DELETE, src, 0, null, clock.millis(), 0));
    assertMoveFailsWith(REPLICATION_FAIL_INFLIGHT_DELETION,
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

    assertMoveFailsWith(REPLICATION_FAIL_CONTAINER_NOT_CLOSED,
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
    when(replicationManager.getContainerReplicationHealth(any(), any()))
        .thenAnswer(invocationOnMock -> {
          Set<ContainerReplica> replicasBeingChecked =
              invocationOnMock.getArgument(1);
          if (replicasBeingChecked.contains(sourceReplica)) {
            // before move
            return new ContainerHealthResult.HealthyResult(containerInfo);
          } else {
            // after move
            return new ContainerHealthResult.MisReplicatedHealthResult(
                containerInfo, false, null);
          }
        });

    assertMoveFailsWith(REPLICATION_NOT_HEALTHY_AFTER_MOVE,
        containerInfo.containerID());
  }

  /**
   * If the container has issues such as under, over or mis replication
   * before moving then move should fail.
   */
  @Test
  public void testContainerIsNotHealthyBeforeMove() throws Exception {
    // return an under replicated health result from replication manager
    when(replicationManager.getContainerReplicationHealth(any(), any()))
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
    assertMoveFailsWith(REPLICATION_NOT_HEALTHY_BEFORE_MOVE,
        containerInfo.containerID());

    // check for an under replicated EC container
    containerInfo = ReplicationTestUtil.createContainerInfo(
        new ECReplicationConfig(3, 2), 1,
        HddsProtos.LifeCycleState.CLOSED);
    replicas.clear();
    replicas.addAll(ReplicationTestUtil.createReplicas(
        containerInfo.containerID(), ContainerReplicaProto.State.CLOSED,
        1, 2, 3, 4));
    src = replicas.iterator().next().getDatanodeDetails();
    nodes.put(src, NodeStatus.inServiceHealthy());
    assertMoveFailsWith(REPLICATION_NOT_HEALTHY_BEFORE_MOVE,
        containerInfo.containerID());
  }

  @Test
  public void testExistingMoveScheduled() throws Exception {
    setupSuccessfulMove();
    // Try to make the same move again
    CompletableFuture<MoveManager.MoveResult> res =
        moveManager.move(containerInfo.containerID(), src, tgt);
    assertEquals(FAIL_CONTAINER_ALREADY_BEING_MOVED, res.get());
  }

  @Test
  public void testReplicationCommandFails() throws Exception {
    doThrow(new RuntimeException("test")).when(replicationManager)
            .sendLowPriorityReplicateContainerCommand(
        any(), anyInt(), any(), any(), anyLong());
    CompletableFuture<MoveManager.MoveResult> res = setupSuccessfulMove();
    assertEquals(FAIL_UNEXPECTED_ERROR, res.get());
  }

  @Test
  public void testDeleteCommandFails() throws Exception {
    CompletableFuture<MoveManager.MoveResult> res = setupSuccessfulMove();

    doThrow(ContainerNotFoundException.newInstanceForTesting())
        .when(containerManager).getContainer(any(ContainerID.class));

    ContainerReplicaOp op = new ContainerReplicaOp(
        ADD, tgt, 0, null, clock.millis() + 1000, 0);
    moveManager.opCompleted(op, containerInfo.containerID(), false);

    MoveManager.MoveResult moveResult = res.get();
    assertEquals(FAIL_UNEXPECTED_ERROR, moveResult);
  }

  @Test
  public void testSuccessfulMove() throws Exception {
    CompletableFuture<MoveManager.MoveResult> res = setupSuccessfulMove();

    ContainerReplicaOp op = new ContainerReplicaOp(
        ADD, tgt, 0, null, clock.millis() + 1000, 0);
    moveManager.opCompleted(op, containerInfo.containerID(), false);

    verify(replicationManager).sendDeleteCommand(
        eq(containerInfo), eq(0), eq(src), eq(true), anyLong());

    op = new ContainerReplicaOp(
        DELETE, src, 0, null, clock.millis() + 1000, 0);
    moveManager.opCompleted(op, containerInfo.containerID(), false);

    MoveManager.MoveResult finalResult = res.get();
    assertEquals(COMPLETED, finalResult);
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

    verify(replicationManager).sendLowPriorityReplicateContainerCommand(
        eq(containerInfo), eq(srcReplica.getReplicaIndex()), eq(src), eq(tgt),
        anyLong());

    ContainerReplicaOp op = new ContainerReplicaOp(
        ADD, tgt, srcReplica.getReplicaIndex(), null, clock.millis() + 1000, 0);
    moveManager.opCompleted(op, containerInfo.containerID(), false);

    verify(replicationManager).sendDeleteCommand(
        eq(containerInfo), eq(srcReplica.getReplicaIndex()), eq(src),
        eq(true), anyLong());

    op = new ContainerReplicaOp(
        DELETE, src, srcReplica.getReplicaIndex(), null, clock.millis() + 1000, 0);
    moveManager.opCompleted(op, containerInfo.containerID(), false);

    MoveManager.MoveResult finalResult = res.get();
    assertEquals(COMPLETED, finalResult);
  }

  @Test
  public void testMoveTimeoutOnAdd() throws Exception {
    CompletableFuture<MoveManager.MoveResult> res = setupSuccessfulMove();

    ContainerReplicaOp op = new ContainerReplicaOp(
        ADD, tgt, 0, null, clock.millis() + 1000, 0);
    moveManager.opCompleted(op, containerInfo.containerID(), true);

    MoveManager.MoveResult finalResult = res.get();
    assertEquals(REPLICATION_FAIL_TIME_OUT, finalResult);
  }

  @Test
  public void testMoveTimeoutOnDelete() throws Exception {
    CompletableFuture<MoveManager.MoveResult> res = setupSuccessfulMove();

    ContainerReplicaOp op = new ContainerReplicaOp(
        ADD, tgt, 0, null, clock.millis() + 1000, 0);
    moveManager.opCompleted(op, containerInfo.containerID(), false);

    verify(replicationManager).sendDeleteCommand(
        eq(containerInfo), eq(0), eq(src), eq(true), anyLong());

    op = new ContainerReplicaOp(
        DELETE, src, 0, null, clock.millis() + 1000, 0);
    moveManager.opCompleted(op, containerInfo.containerID(), true);

    MoveManager.MoveResult finalResult = res.get();
    assertEquals(DELETION_FAIL_TIME_OUT, finalResult);
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
        ADD, tgt, 0, null, clock.millis() + 1000, 0);
    moveManager.opCompleted(op, containerInfo.containerID(), false);

    MoveManager.MoveResult finalResult = res.get();
    assertEquals(COMPLETED, finalResult);

    verify(replicationManager, times(0))
        .sendDeleteCommand(eq(containerInfo), eq(0), eq(src), eq(true));
  }

  @Test
  public void testMoveCompleteSrcNotHealthy() throws Exception {
    CompletableFuture<MoveManager.MoveResult> res = setupSuccessfulMove();

    nodes.put(src, NodeStatus.inServiceStale());
    ContainerReplicaOp op = new ContainerReplicaOp(
        ADD, tgt, 0, null, clock.millis() + 1000, 0);
    moveManager.opCompleted(op, containerInfo.containerID(), false);

    MoveManager.MoveResult finalResult = res.get();
    assertEquals(DELETION_FAIL_NODE_UNHEALTHY, finalResult);

    verify(replicationManager, times(0))
        .sendDeleteCommand(eq(containerInfo), eq(0), eq(src), eq(true));
  }

  @Test
  public void testMoveCompleteSrcNotInService() throws Exception {
    CompletableFuture<MoveManager.MoveResult> res = setupSuccessfulMove();

    nodes.put(src, NodeStatus.valueOf(
        HddsProtos.NodeOperationalState.DECOMMISSIONING,
        HddsProtos.NodeState.HEALTHY));
    ContainerReplicaOp op = new ContainerReplicaOp(
        ADD, tgt, 0, null, clock.millis() + 1000, 0);
    moveManager.opCompleted(op, containerInfo.containerID(), false);

    MoveManager.MoveResult finalResult = res.get();
    assertEquals(DELETION_FAIL_NODE_NOT_IN_SERVICE, finalResult);

    verify(replicationManager, times(0))
        .sendDeleteCommand(eq(containerInfo), eq(0), eq(src), eq(true));
  }

  @Test
  public void testMoveCompleteFutureReplicasUnhealthy() throws Exception {
    CompletableFuture<MoveManager.MoveResult> res = setupSuccessfulMove();

    when(replicationManager.getContainerReplicationHealth(any(), any()))
        .thenReturn(new ContainerHealthResult
            .MisReplicatedHealthResult(containerInfo, false, null));

    ContainerReplicaOp op = new ContainerReplicaOp(
        ADD, tgt, 0, null, clock.millis() + 1000, 0);
    moveManager.opCompleted(op, containerInfo.containerID(), false);

    MoveManager.MoveResult finalResult = res.get();
    assertEquals(DELETE_FAIL_POLICY, finalResult);

    verify(replicationManager, times(0))
        .sendDeleteCommand(eq(containerInfo), eq(0), eq(src), eq(true));
  }

  @Test
  public void testDeleteNotSentWithExpirationTimeInPast() throws Exception {
    containerInfo = ReplicationTestUtil.createContainer(
        HddsProtos.LifeCycleState.CLOSED, new ECReplicationConfig(3, 2));
    setupMocks();
    long moveTimeout = 55 * 60 * 1000, replicationTimeout = 50 * 60 * 1000;
    moveManager.setMoveTimeout(moveTimeout);
    moveManager.setReplicationTimeout(replicationTimeout);

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
    ArgumentCaptor<Long> longCaptorReplicate = ArgumentCaptor.forClass(Long.class);
    verify(replicationManager).sendLowPriorityReplicateContainerCommand(
        eq(containerInfo), eq(srcReplica.getReplicaIndex()), eq(src),
        eq(tgt), longCaptorReplicate.capture());

    ContainerReplicaOp op = new ContainerReplicaOp(
        ADD, tgt, srcReplica.getReplicaIndex(), null, clock.millis() + 1000, 0);
    moveManager.opCompleted(op, containerInfo.containerID(), false);
    ArgumentCaptor<Long> longCaptorDelete = ArgumentCaptor.forClass(Long.class);
    verify(replicationManager).sendDeleteCommand(
        eq(containerInfo), eq(srcReplica.getReplicaIndex()), eq(src),
        eq(true), longCaptorDelete.capture());

    // verify that command is sent with deadline as (moveStartTime + moveTimeout)
    // moveStartTime can be calculated as (expirationTime set for replication - replicationTimeout)
    assertEquals(longCaptorReplicate.getValue() - replicationTimeout + moveTimeout, longCaptorDelete.getValue());
    // replicationManager sends a datanode command with the deadline as
    // (scmDeadlineEpochMs - rmConf.getDatanodeTimeoutOffset()). The offset is 6 minutes by default.
    // For the datanode deadline to not be in the past, the below condition is checked.
    assertTrue((longCaptorDelete.getValue() - Duration.ofMinutes(6).toMillis()) > clock.millis());

    op = new ContainerReplicaOp(
        DELETE, src, srcReplica.getReplicaIndex(), null, clock.millis() + 1000, 0);
    moveManager.opCompleted(op, containerInfo.containerID(), false);
    MoveManager.MoveResult finalResult = res.get();
    assertEquals(COMPLETED, finalResult);
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

    verify(replicationManager).sendLowPriorityReplicateContainerCommand(
        eq(containerInfo), eq(0), eq(src), eq(tgt), anyLong());

    return res;
  }

  private void assertMoveFailsWith(MoveManager.MoveResult expectedResult,
                                   ContainerID containerId) throws
      NodeNotFoundException, ContainerReplicaNotFoundException,
      ContainerNotFoundException, ExecutionException,
      InterruptedException {
    CompletableFuture<MoveManager.MoveResult> res = moveManager.move(
        containerId, src, tgt);
    MoveManager.MoveResult actualResult = res.get();
    assertEquals(expectedResult, actualResult);
  }

  /**
   * Test that moving an over-replicated CLOSED container fails when config is disabled.
   */
  @Test
  public void testMoveOverReplicatedClosedContainerWithConfigDisabled() throws Exception {
    setupOverReplicatedContainer();
    
    moveManager.setIncludeNonStandardContainers(false);
    assertMoveFailsWith(REPLICATION_NOT_HEALTHY_BEFORE_MOVE, containerInfo.containerID());
  }

  /**
   * Test that moving an over-replicated CLOSED container succeeds when config is enabled.
   */
  @Test
  public void testMoveOverReplicatedClosedContainerWithConfigEnabled() throws Exception {
    setupOverReplicatedContainer();
    when(replicationManager.getPendingReplicationOps(containerInfo.containerID())).thenReturn(new ArrayList<>());

    moveManager.setIncludeNonStandardContainers(true);
    CompletableFuture<MoveManager.MoveResult> successRes =
        moveManager.move(containerInfo.containerID(), src, tgt);
    verify(replicationManager).sendLowPriorityReplicateContainerCommand(
        eq(containerInfo), eq(0), eq(src), eq(tgt), anyLong());
    completeMove(containerInfo, src, tgt, successRes);
  }

  /**
   * Test that moving a QUASI_CLOSED container fails when config is disabled.
   */
  @Test
  public void testMoveQuasiClosedContainerWithConfigDisabled() throws Exception {
    ContainerInfo qcContainer = setupQuasiClosedContainer(1);
    src = replicas.iterator().next().getDatanodeDetails();
    tgt = MockDatanodeDetails.randomDatanodeDetails();
    nodes.put(src, NodeStatus.inServiceHealthy());
    nodes.put(tgt, NodeStatus.inServiceHealthy());

    moveManager.setIncludeNonStandardContainers(false);
    assertMoveFailsWith(REPLICATION_FAIL_CONTAINER_NOT_CLOSED, qcContainer.containerID());
  }

  /**
   * Test that moving a QUASI_CLOSED container succeeds when config is enabled.
   */
  @Test
  public void testMoveQuasiClosedContainerWithConfigEnabled() throws Exception {
    ContainerInfo qcContainer = setupQuasiClosedContainer(2);
    src = replicas.iterator().next().getDatanodeDetails();
    tgt = MockDatanodeDetails.randomDatanodeDetails();
    nodes.put(src, NodeStatus.inServiceHealthy());
    nodes.put(tgt, NodeStatus.inServiceHealthy());

    moveManager.setIncludeNonStandardContainers(true);
    CompletableFuture<MoveManager.MoveResult> successRes =
        moveManager.move(qcContainer.containerID(), src, tgt);
    verify(replicationManager).sendLowPriorityReplicateContainerCommand(
        eq(qcContainer), eq(0), eq(src), eq(tgt), anyLong());
    completeMove(qcContainer, src, tgt, successRes);
  }

  /**
   * OVER_REPLICATED QUASI_CLOSED lifecycle container can be moved when includeNonStandardContainers is enabled.
   */
  @Test
  public void testMoveQuasiClosedContainerOverReplicatedWithConfigEnabled() throws Exception {
    ContainerInfo qcContainer = setupQuasiClosedContainer(3);
    when(replicationManager.getContainerReplicationHealth(eq(qcContainer), anySet()))
        .thenReturn(new ContainerHealthResult.OverReplicatedHealthResult(qcContainer, 1, false));
    when(replicationManager.getPendingReplicationOps(qcContainer.containerID()))
        .thenReturn(new ArrayList<>());
    src = replicas.iterator().next().getDatanodeDetails();
    tgt = MockDatanodeDetails.randomDatanodeDetails();
    nodes.put(src, NodeStatus.inServiceHealthy());
    nodes.put(tgt, NodeStatus.inServiceHealthy());

    moveManager.setIncludeNonStandardContainers(true);
    CompletableFuture<MoveManager.MoveResult> successRes =
        moveManager.move(qcContainer.containerID(), src, tgt);
    verify(replicationManager).sendLowPriorityReplicateContainerCommand(
        eq(qcContainer), eq(0), eq(src), eq(tgt), anyLong());
    completeMove(qcContainer, src, tgt, successRes);
  }

  /**
   * Empty QUASI_CLOSED replica on source must not be moved.
   */
  @Test
  public void testMoveQuasiClosedContainerRejectsEmptySourceReplica() throws Exception {
    ContainerInfo qcContainer = ReplicationTestUtil.createContainerInfo(
        RatisReplicationConfig.getInstance(THREE), 5L,
        HddsProtos.LifeCycleState.QUASI_CLOSED, 1L, OzoneConsts.GB);
    ContainerID cid = qcContainer.containerID();

    DatanodeDetails dnWithEmpty = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails dn2 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails dn3 = MockDatanodeDetails.randomDatanodeDetails();

    replicas.clear();
    replicas.add(ContainerReplica.newBuilder()
        .setContainerID(cid)
        .setContainerState(ContainerReplicaProto.State.QUASI_CLOSED)
        .setSequenceId(0L)
        .setKeyCount(0)
        .setBytesUsed(0)
        .setReplicaIndex(0)
        .setDatanodeDetails(dnWithEmpty)
        .setEmpty(true)
        .build());
    replicas.add(ReplicationTestUtil.createContainerReplica(cid, 0, IN_SERVICE,
        ContainerReplicaProto.State.QUASI_CLOSED, 1L, OzoneConsts.GB, dn2, dn2.getID()));
    replicas.add(ReplicationTestUtil.createContainerReplica(cid, 0, IN_SERVICE,
        ContainerReplicaProto.State.QUASI_CLOSED, 1L, OzoneConsts.GB, dn3, dn3.getID()));

    when(containerManager.getContainer(eq(cid))).thenReturn(qcContainer);
    when(containerManager.getContainerReplicas(cid)).thenReturn(replicas);
    when(replicationManager.getContainerReplicationHealth(eq(qcContainer), anySet()))
        .thenReturn(new ContainerHealthResult.HealthyResult(qcContainer));

    src = dnWithEmpty;
    tgt = MockDatanodeDetails.randomDatanodeDetails();
    nodes.put(src, NodeStatus.inServiceHealthy());
    nodes.put(tgt, NodeStatus.inServiceHealthy());

    moveManager.setIncludeNonStandardContainers(true);
    assertMoveFailsWith(REPLICATION_NOT_HEALTHY_BEFORE_MOVE, cid);
  }

  private void setupOverReplicatedContainer() {
    replicas.clear();
    replicas.addAll(ReplicationTestUtil.createReplicas(containerInfo.containerID(), 0, 0, 0, 0));
    src = replicas.iterator().next().getDatanodeDetails();
    tgt = MockDatanodeDetails.randomDatanodeDetails();
    nodes.put(src, NodeStatus.inServiceHealthy());
    nodes.put(tgt, NodeStatus.inServiceHealthy());

    when(replicationManager.getContainerReplicationHealth(any(), anySet()))
        .thenReturn(new ContainerHealthResult.OverReplicatedHealthResult(containerInfo, 1, false));
  }

  private ContainerInfo setupQuasiClosedContainer(long containerId) throws Exception {
    ContainerInfo qcContainer = ReplicationTestUtil.createContainerInfo(
        RatisReplicationConfig.getInstance(THREE), containerId,
        HddsProtos.LifeCycleState.QUASI_CLOSED);
    replicas.clear();
    replicas.addAll(ReplicationTestUtil.createReplicas(qcContainer.containerID(),
        ContainerReplicaProto.State.QUASI_CLOSED, 0, 0, 0));
    
    when(containerManager.getContainer(eq(qcContainer.containerID()))).thenReturn(qcContainer);
    when(containerManager.getContainerReplicas(qcContainer.containerID())).thenReturn(replicas);
    when(replicationManager.getContainerReplicationHealth(eq(qcContainer), anySet()))
        .thenReturn(new ContainerHealthResult.HealthyResult(qcContainer));

    return qcContainer;
  }

  private void completeMove(ContainerInfo container, DatanodeDetails source,
      DatanodeDetails target, CompletableFuture<MoveManager.MoveResult> moveResult) throws Exception {
    ContainerReplicaOp addOp = new ContainerReplicaOp(
        ADD, target, 0, null, clock.millis() + 1000, 0);
    moveManager.opCompleted(addOp, container.containerID(), false);

    verify(replicationManager).sendDeleteCommand(
        eq(container), eq(0), eq(source), eq(true), anyLong());

    ContainerReplicaOp deleteOp = new ContainerReplicaOp(
        DELETE, source, 0, null, clock.millis() + 1000, 0);
    moveManager.opCompleted(deleteOp, container.containerID(), false);

    assertEquals(COMPLETED, moveResult.get());
  }
}
