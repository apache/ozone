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

import java.time.Clock;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaNotFoundException;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOpsSubscriber;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class which schedules, tracks and completes moves scheduled by the
 * balancer.
 */
public final class MoveManager implements
    ContainerReplicaPendingOpsSubscriber {

  private static final Logger LOG =
      LoggerFactory.getLogger(MoveManager.class);

  // TODO - Should pending ops notify under lock to allow MM to schedule a
  //        delete after the move, but before anything else can, eg RM?

  /*
  moveTimeout and replicationTimeout are set by ContainerBalancer.
   */
  private long moveTimeout = 1000 * 65 * 60;
  private long replicationTimeout = 1000 * 50 * 60;

  private final ReplicationManager replicationManager;
  private final ContainerManager containerManager;
  private final Clock clock;

  private final Map<ContainerID, MoveOperation> pendingMoves =
      new ConcurrentHashMap<>();

  public MoveManager(final ReplicationManager replicationManager,
      final ContainerManager containerManager) {
    this.replicationManager = replicationManager;
    this.containerManager = containerManager;
    this.clock = replicationManager.getClock();
  }

  /**
   * get all the pending move operations.
   */
  public Map<ContainerID, MoveOperation> getPendingMove() {
    return pendingMoves;
  }

  void resetState() {
    pendingMoves.clear();
  }

  /**
   * completeMove a move action for a given container.
   *
   * @param cid Container id to which the move option is finished
   */
  private void completeMove(final ContainerID cid, final MoveResult mr) {
    MoveOperation move = pendingMoves.remove(cid);
    if (move != null) {
      CompletableFuture<MoveResult> future = move.getResult();
      if (future != null && mr != null) {
        // when we know the future is null, and we want to complete
        // the move , then we set mr to null.
        // for example , after becoming a new leader, we might need
        // to complete some moves and we know these futures does not
        // exist.
        LOG.debug("Completing container move for container {} with result {}.",
            cid, mr);
        future.complete(mr);
      }
    }
  }

  /**
   * start a move for a given container.
   *
   * @param containerInfo The container for which to move a replica
   * @param src source datanode to copy the replica from
   * @param tgt target datanode to copy the replica to
   */
  private void startMove(
      final ContainerInfo containerInfo, final DatanodeDetails src,
      final DatanodeDetails tgt, final CompletableFuture<MoveResult> ret) {
    MoveOperation move = pendingMoves.putIfAbsent(containerInfo.containerID(),
        new MoveOperation(ret, new MoveDataNodePair(src, tgt)));
    if (move == null) {
      // A move for this container did not exist, so send a replicate command
      try {
        sendReplicateCommand(containerInfo, tgt, src);
      } catch (Exception e) {
        LOG.error("Unable to schedule the replication command for container {}",
            containerInfo, e);
        ret.complete(MoveResult.FAIL_UNEXPECTED_ERROR);
        pendingMoves.remove(containerInfo.containerID());
      }
    } else {
      // A move for this container is already scheduled, so we cannot schedule
      // another one. Failing this move.
      ret.complete(MoveResult.FAIL_CONTAINER_ALREADY_BEING_MOVED);
    }
  }

  /**
   * move a container replica from source datanode to
   * target datanode. A move is a two part operation. First a replication
   * command is scheduled to create a new copy of the replica. Later, when the
   * replication completes a delete is scheduled to remove the original replica.
   *
   * @param cid Container to move
   * @param src source datanode
   * @param tgt target datanode
   */
  CompletableFuture<MoveResult> move(
      ContainerID cid, DatanodeDetails src, DatanodeDetails tgt)
      throws ContainerNotFoundException, NodeNotFoundException,
      ContainerReplicaNotFoundException {
    CompletableFuture<MoveResult> ret = new CompletableFuture<>();

    // Ensure src and tgt are IN_SERVICE and HEALTHY
    for (DatanodeDetails dn : Arrays.asList(src, tgt)) {
      NodeStatus currentNodeStatus = replicationManager.getNodeStatus(dn);
      if (currentNodeStatus.getHealth() != HddsProtos.NodeState.HEALTHY) {
        ret.complete(MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY);
        return ret;
      }
      if (currentNodeStatus.getOperationalState()
          != HddsProtos.NodeOperationalState.IN_SERVICE) {
        ret.complete(MoveResult.REPLICATION_FAIL_NODE_NOT_IN_SERVICE);
        return ret;
      }
    }

    // Ensure the container exists on the src and is not present on the target
    ContainerInfo containerInfo = containerManager.getContainer(cid);
    synchronized (containerInfo) {
      final Set<ContainerReplica> currentReplicas = containerManager
          .getContainerReplicas(cid);

      boolean srcExists = false;
      for (ContainerReplica r : currentReplicas) {
        if (r.getDatanodeDetails().equals(src)) {
          srcExists = true;
        }
        if (r.getDatanodeDetails().equals(tgt)) {
          ret.complete(MoveResult.REPLICATION_FAIL_EXIST_IN_TARGET);
          return ret;
        }
      }
      if (!srcExists) {
        ret.complete(MoveResult.REPLICATION_FAIL_NOT_EXIST_IN_SOURCE);
        return ret;
      }

      /*
      If the container is under, over, or mis replicated, we should let
      replication manager solve these issues first. Fail move for such a
      container.
       */
      ContainerHealthResult healthBeforeMove =
          replicationManager.getContainerReplicationHealth(containerInfo,
              currentReplicas);
      if (healthBeforeMove.getHealthState() !=
          ContainerHealthResult.HealthState.HEALTHY) {
        ret.complete(MoveResult.REPLICATION_NOT_HEALTHY_BEFORE_MOVE);
        return ret;
      }

      /*
       * Ensure the container has no inflight actions.
       * The reason why the given container should not be taking any inflight
       * action is that: if the given container is being replicated or deleted,
       * the num of its replica is not deterministic, so move operation issued
       * by balancer may cause a nondeterministic result, so we should drop
       * this option for this time.
       */
      List<ContainerReplicaOp> pendingOps =
          replicationManager.getPendingReplicationOps(cid);
      for (ContainerReplicaOp op : pendingOps) {
        if (op.getOpType() == ContainerReplicaOp.PendingOpType.ADD) {
          ret.complete(MoveResult.REPLICATION_FAIL_INFLIGHT_REPLICATION);
          return ret;
        } else if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
          ret.complete(MoveResult.REPLICATION_FAIL_INFLIGHT_DELETION);
          return ret;
        }
      }

      // Ensure the container is CLOSED
      HddsProtos.LifeCycleState currentContainerStat = containerInfo.getState();
      if (currentContainerStat != HddsProtos.LifeCycleState.CLOSED) {
        ret.complete(MoveResult.REPLICATION_FAIL_CONTAINER_NOT_CLOSED);
        return ret;
      }

      // Create a set or replicas that indicates how the container will look
      // after the move and ensure it is healthy - ie not under, over or mis
      // replicated.
      Set<ContainerReplica> replicasAfterMove = createReplicaSetAfterMove(
          src, tgt, currentReplicas);
      ContainerHealthResult healthResult = replicationManager
          .getContainerReplicationHealth(containerInfo, replicasAfterMove);
      if (healthResult.getHealthState()
          != ContainerHealthResult.HealthState.HEALTHY) {
        ret.complete(MoveResult.REPLICATION_NOT_HEALTHY_AFTER_MOVE);
        return ret;
      }
      startMove(containerInfo, src, tgt, ret);
      LOG.debug("Processed a move request for container {}, from {} to {}",
          cid, src, tgt);
      return ret;
    }
  }

  /**
   * Notify Move Manager that a container op has been completed.
   *
   * @param containerReplicaOp ContainerReplicaOp which has completed
   * @param containerID ContainerID for which to complete
   */
  private void notifyContainerOpCompleted(ContainerReplicaOp containerReplicaOp,
      ContainerID containerID) {
    MoveOperation move = pendingMoves.get(containerID);
    if (move != null) {
      MoveDataNodePair mdnp = move.getMoveDataNodePair();
      PendingOpType opType = containerReplicaOp.getOpType();
      DatanodeDetails dn = containerReplicaOp.getTarget();
      if (opType.equals(PendingOpType.ADD) && mdnp.getTgt().equals(dn)) {
        try {
          handleSuccessfulAdd(containerID);
        } catch (ContainerNotFoundException | NodeNotFoundException |
                 ContainerReplicaNotFoundException | NotLeaderException e) {
          LOG.warn("Failed to handle successful Add for container {} being " +
              "moved from source {} to target {}.", containerID,
              mdnp.getSrc(), mdnp.getTgt(), e);
          move.getResult().complete(MoveResult.FAIL_UNEXPECTED_ERROR);
        }
      } else if (
            opType.equals(PendingOpType.DELETE) && mdnp.getSrc().equals(dn)) {
        completeMove(containerID, MoveResult.COMPLETED);
      }
    }
  }

   /**
   * Notify Move Manager that a container op has been Expired.
   *
   * @param containerReplicaOp ContainerReplicaOp
   * @param containerID ContainerID for which to complete
   */
  private void notifyContainerOpExpired(ContainerReplicaOp containerReplicaOp,
      ContainerID containerID) {
    MoveOperation pair = pendingMoves.get(containerID);
    if (pair != null) {
      MoveDataNodePair mdnp = pair.getMoveDataNodePair();
      PendingOpType opType = containerReplicaOp.getOpType();
      DatanodeDetails dn = containerReplicaOp.getTarget();
      if (opType.equals(PendingOpType.ADD) && mdnp.getTgt().equals(dn)) {
        completeMove(containerID, MoveResult.REPLICATION_FAIL_TIME_OUT);
      } else if (opType.equals(PendingOpType.DELETE) &&
          mdnp.getSrc().equals(dn)) {
        completeMove(containerID, MoveResult.DELETION_FAIL_TIME_OUT);
      }
    }
  }

  private void handleSuccessfulAdd(final ContainerID cid)
      throws ContainerNotFoundException,
      ContainerReplicaNotFoundException, NodeNotFoundException,
      NotLeaderException {
    MoveOperation moveOp = pendingMoves.get(cid);
    if (moveOp == null) {
      return;
    }
    MoveDataNodePair movePair = moveOp.getMoveDataNodePair();
    final DatanodeDetails src = movePair.getSrc();
    final DatanodeDetails tgt = movePair.getTgt();
    LOG.debug("Handling successful addition of Container {} from" +
        " source {} to target {}.", cid, src, tgt);

    ContainerInfo containerInfo = containerManager.getContainer(cid);
    synchronized (containerInfo) {
      Set<ContainerReplica> currentReplicas = containerManager
          .getContainerReplicas(cid);

      Set<ContainerReplica> futureReplicas = new HashSet<>(currentReplicas);
      boolean found = futureReplicas.removeIf(
          r -> r.getDatanodeDetails().equals(src));
      if (!found) {
        // if the target is present but source disappears somehow,
        // we can consider move is successful.
        completeMove(cid, MoveResult.COMPLETED);
        return;
      }

      final NodeStatus nodeStatus = replicationManager.getNodeStatus(src);
      if (nodeStatus.getOperationalState()
          != HddsProtos.NodeOperationalState.IN_SERVICE) {
        completeMove(cid, MoveResult.DELETION_FAIL_NODE_NOT_IN_SERVICE);
        return;
      }
      if (!nodeStatus.isHealthy()) {
        completeMove(cid, MoveResult.DELETION_FAIL_NODE_UNHEALTHY);
        return;
      }

      ContainerHealthResult healthResult = replicationManager
          .getContainerReplicationHealth(containerInfo, futureReplicas);

      if (healthResult.getHealthState() ==
          ContainerHealthResult.HealthState.HEALTHY) {
        sendDeleteCommand(containerInfo, src, moveOp.getMoveStartTime());
      } else {
        LOG.info("Cannot remove source replica as the container health would " +
            "be {}", healthResult.getHealthState());
        completeMove(cid, MoveResult.DELETE_FAIL_POLICY);
      }
    }
  }

  private Set<ContainerReplica> createReplicaSetAfterMove(DatanodeDetails src,
      DatanodeDetails tgt, Set<ContainerReplica> existing) {
    Set<ContainerReplica> replicas = new HashSet<>(existing);
    ContainerReplica srcReplica = null;
    for (ContainerReplica r : replicas) {
      if (r.getDatanodeDetails().equals(src)) {
        srcReplica = r;
        break;
      }
    }
    if (srcReplica == null) {
      throw new IllegalArgumentException("The source replica is not present");
    }
    replicas.remove(srcReplica);
    replicas.add(srcReplica.toBuilder()
        .setDatanodeDetails(tgt)
        .build());
    return replicas;
  }

  /**
   * Sends replicate container command for the given container to the given
   * datanode.
   *
   * @param containerInfo ContainerInfo to be replicated
   * @param tgt The destination datanode to replicate to
   * @param src The source node to replicate from
   */
  private void sendReplicateCommand(
      final ContainerInfo containerInfo, final DatanodeDetails tgt,
      final DatanodeDetails src)
      throws ContainerReplicaNotFoundException, ContainerNotFoundException,
      NotLeaderException {
    int replicaIndex = getContainerReplicaIndex(
        containerInfo.containerID(), src);
    long now = clock.millis();
    replicationManager.sendLowPriorityReplicateContainerCommand(containerInfo,
        replicaIndex, src, tgt, now + replicationTimeout);
    pendingMoves.get(containerInfo.containerID()).setMoveStartTime(now);
  }

  /**
   * Sends delete container command for the given container to the given
   * datanode.
   *
   * @param containerInfo Container to be deleted
   * @param datanode The datanode on which the replica should be deleted
   * @param moveStartTime The time at which the replicate command for the container was scheduled
   */
  private void sendDeleteCommand(
      final ContainerInfo containerInfo, final DatanodeDetails datanode,
      long moveStartTime)
      throws ContainerReplicaNotFoundException, ContainerNotFoundException,
      NotLeaderException {
    int replicaIndex = getContainerReplicaIndex(
        containerInfo.containerID(), datanode);
    replicationManager.sendDeleteCommand(
        containerInfo, replicaIndex, datanode, true, moveStartTime + moveTimeout);
  }

  private int getContainerReplicaIndex(
      final ContainerID id, final DatanodeDetails dn)
      throws ContainerNotFoundException, ContainerReplicaNotFoundException {
    Set<ContainerReplica> replicas = containerManager.getContainerReplicas(id);
    return replicas.stream().filter(r -> r.getDatanodeDetails().equals(dn))
        //there should not be more than one replica of a container on the same
        //datanode. handle this if found in the future.
        .findFirst().orElseThrow(() ->
            new ContainerReplicaNotFoundException(id, dn))
        .getReplicaIndex();
  }

  @Override
  public void opCompleted(ContainerReplicaOp op, ContainerID containerID,
      boolean timedOut) {
    if (timedOut) {
      notifyContainerOpExpired(op, containerID);
    } else {
      notifyContainerOpCompleted(op, containerID);
    }
  }

  void setMoveTimeout(long moveTimeout) {
    this.moveTimeout = moveTimeout;
  }

  void setReplicationTimeout(long replicationTimeout) {
    this.replicationTimeout = replicationTimeout;
  }

  /**
   * All details about a move operation.
   */
  static class MoveOperation {
    private CompletableFuture<MoveResult> result;
    private MoveDataNodePair moveDataNodePair;
    private long moveStartTime;

    MoveOperation(CompletableFuture<MoveResult> result, MoveDataNodePair srcTgt) {
      this.result = result;
      this.moveDataNodePair = srcTgt;
    }

    public CompletableFuture<MoveResult> getResult() {
      return result;
    }

    public MoveDataNodePair getMoveDataNodePair() {
      return moveDataNodePair;
    }

    public long getMoveStartTime() {
      return moveStartTime;
    }

    public void setResult(
        CompletableFuture<MoveResult> result) {
      this.result = result;
    }

    public void setMoveDataNodePair(MoveDataNodePair srcTgt) {
      this.moveDataNodePair = srcTgt;
    }

    public void setMoveStartTime(long time) {
      this.moveStartTime = time;
    }
  }

  /**
   * Various move return results.
   */
  public enum MoveResult {
    // both replication and deletion are completed
    COMPLETED,
    // RM is not ratis leader
    FAIL_LEADER_NOT_READY,
    // replication fail because the container does not exist in src
    REPLICATION_FAIL_NOT_EXIST_IN_SOURCE,
    // replication fail because the container exists in target
    REPLICATION_FAIL_EXIST_IN_TARGET,
    // replication fail because the container is not cloesed
    REPLICATION_FAIL_CONTAINER_NOT_CLOSED,
    // replication fail because the container is in inflightDeletion
    REPLICATION_FAIL_INFLIGHT_DELETION,
    // replication fail because the container is in inflightReplication
    REPLICATION_FAIL_INFLIGHT_REPLICATION,
    // replication fail because of timeout
    REPLICATION_FAIL_TIME_OUT,
    // replication fail because of node is not in service
    REPLICATION_FAIL_NODE_NOT_IN_SERVICE,
    // replication fail because node is unhealthy
    REPLICATION_FAIL_NODE_UNHEALTHY,
    // replication succeed, but deletion fail because of timeout
    DELETION_FAIL_TIME_OUT,
    // deletion fail because of node is not in service
    DELETION_FAIL_NODE_NOT_IN_SERVICE,
    // replication succeed, but deletion fail because because
    // node is unhealthy
    DELETION_FAIL_NODE_UNHEALTHY,
    // replication succeed, but if we delete the container from
    // the source datanode , the policy(eg, replica num or
    // rack location) will not be satisfied, so we should not delete
    // the container
    DELETE_FAIL_POLICY,
    /*
    Container is not healthy if it has issues such as under, over, or mis
    replication. We don't try to move replicas of such containers.
     */
    REPLICATION_NOT_HEALTHY_BEFORE_MOVE,
    //  replicas + target - src does not satisfy placement policy
    REPLICATION_NOT_HEALTHY_AFTER_MOVE,
    // A move is already scheduled for this container
    FAIL_CONTAINER_ALREADY_BEING_MOVED,
    // Unexpected error
    FAIL_UNEXPECTED_ERROR
  }
}
