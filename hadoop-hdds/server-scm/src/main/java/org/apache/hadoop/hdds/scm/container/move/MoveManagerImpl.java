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
package org.apache.hadoop.hdds.scm.container.move;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicyValidateProxy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaNotFoundException;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAInvocationHandler;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType.MOVE;

/**
 * An HA enabled implementation of MoveManager.
 */
public final class MoveManagerImpl implements MoveManager {
  public static final Logger LOG =
      LoggerFactory.getLogger(MoveManagerImpl.class);

  private final SCMContext scmContext;
  private final NodeManager nodeManager;
  private final ContainerManager containerManager;
  private final PlacementPolicyValidateProxy placementPolicyValidateProxy;
  private final EventPublisher eventPublisher;
  private final StorageContainerManager scm;
  private final Map<ContainerID, CompletableFuture<MoveResult>>
      pendingMoveFuture = new ConcurrentHashMap<>();
  private final Map<ContainerID, MoveDataNodePair>
      pendingMoveOps = new ConcurrentHashMap<>();
  private final MoveDBHandler moveDBHandler;
  private volatile boolean running = false;

  public MoveManagerImpl(final StorageContainerManager scm) throws IOException {
    this.nodeManager = scm.getScmNodeManager();
    this.containerManager = scm.getContainerManager();
    this.scmContext = scm.getScmContext();
    this.placementPolicyValidateProxy = scm.getPlacementPolicyValidateProxy();
    this.eventPublisher = scm.getEventQueue();
    this.scm = scm;
    this.moveDBHandler = new MoveDBHandlerImpl.Builder()
        .setMoveTable(scm.getScmMetadataStore().getMoveTable())
        .setRatisServer(scm.getScmHAManager().getRatisServer())
        .setDBTransactionBuffer(scm.getScmHAManager().getDBTransactionBuffer())
        .build();
  }

  /**
   * get all the pending move operations .
   */
  public Map<ContainerID, MoveDataNodePair> getPendingMove() {
    return pendingMoveOps;
  }

  /**
   * completeMove a move action for a given container.
   *
   * @param cid Container id to which the move option is finished
   */
  private void completeMove(final ContainerID cid, final MoveResult mr) {
    if (pendingMoveOps.containsKey(cid)) {
      boolean successful = false;
      try {
        successful = moveDBHandler.deleteFromDB(cid);
      } catch (TimeoutException t) {
        //TimeoutException here is caused by ratis request timeout.
        //we do not throw TimeoutException out here since completeMove is called
        //in many places. Handling this exception inside itself seems
        //a better way to have lower complexity.
        LOG.warn("fail to delete move from DB");
      }

      // no matter deletion is successful or not, we could complete the
      // corresponding pending CompletableFuture, because :
      // 1 if completeMove is called , it means move is actually completed.
      // 2 a CompletableFuture here means a `move` method call from
      // container balancer, and balancer will block at CompletableFuture.get(),
      // so we would better let it return as soon as possible.
      CompletableFuture<MoveResult> future = pendingMoveFuture.get(cid);
      pendingMoveFuture.remove(cid);
      if (successful) {
        pendingMoveOps.remove(cid);
        if (future != null && mr != null) {
          // when we know the future is null, and we want to complete
          // the move , then we set mr to null.
          // for example , after becoming a new leader, we might need
          // to complete some moves and we know these futures does not
          // exist.
          future.complete(mr);
        }
      } else {
        //TODO: if unsuccessful(actually rarely), cid will not be removed
        // from pendingMoveOps. we need to try to delete cid from db and
        // remove it from pendingMoveOps again. we could start a background
        // service to handle those failed cases.
        // Note that, when scm leader switches, new leader will also try
        // to delete those stale completed move from db.
        if (future != null) {
          future.complete(MoveResult.FAIL_CAN_NOT_RECORD_TO_DB);
        }
      }
    }
  }

  /**
   * start a move for a given container.
   *
   * @param cid Container id
   * @param src source datanode
   * @param tgt target datanode
   */
  private void startMove(
      final ContainerID cid, final DatanodeDetails src,
      final DatanodeDetails tgt, final CompletableFuture<MoveResult> ret)
      throws TimeoutException, ContainerReplicaNotFoundException,
      ContainerNotFoundException {
    if (!pendingMoveOps.containsKey(cid)) {
      MoveDataNodePair mp = new MoveDataNodePair(src, tgt);
      if (moveDBHandler.addToDB(cid, mp)) {
        sendReplicateCommand(cid, tgt, src);
        pendingMoveOps.putIfAbsent(cid, mp);
        pendingMoveFuture.putIfAbsent(cid, ret);
      } else {
        ret.complete(MoveResult.FAIL_CAN_NOT_RECORD_TO_DB);
      }
    }
  }

  /**
   * notify MoveManager that the current scm has become leader and ready.
   */
  @Override
  public void onLeaderReady() {
    //discard all stale records
    pendingMoveOps.clear();
    pendingMoveFuture.clear();

    //initialize with db record
    try (TableIterator<ContainerID,
        ? extends Table.KeyValue<ContainerID, MoveDataNodePair>> iterator =
             moveDBHandler.getMoveTable().iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<ContainerID, MoveDataNodePair> kv = iterator.next();
        final ContainerID cid = kv.getKey();
        final MoveDataNodePair mp = kv.getValue();
        Preconditions.assertNotNull(cid,
            "moved container id should not be null");
        Preconditions.assertNotNull(mp,
            "MoveDataNodePair container id should not be null");
        pendingMoveOps.put(cid, mp);
      }
    } catch (IOException ex) {
      ExitUtils.terminate(1,
          "Resuming MoveManager failed on SCM leader change.",
          ex, true, LOG);
    }

    pendingMoveOps.forEach((cid, mdmp) -> {
      Set<ContainerReplica> replicas;
      try {
        replicas = containerManager.getContainerReplicas(cid);
      } catch (ContainerNotFoundException e) {
        LOG.error("can not find container {} " +
            "while processing replicated move", cid);
        completeMove(cid, null);
        return;
      }
      DatanodeDetails src = mdmp.getSrc();
      DatanodeDetails tgt = mdmp.getTgt();

      boolean isSrcExist = replicas.stream()
          .anyMatch(r -> r.getDatanodeDetails().equals(src));
      boolean isTgtExist = replicas.stream()
          .anyMatch(r -> r.getDatanodeDetails().equals(tgt));
      if (isSrcExist) {
        if (isTgtExist) {
          //the former scm leader may or may not send the deletion command
          //before reelection.here, we just try to send the command again.
          try {
            handleSuccessfulAdd(cid);
          } catch (ContainerReplicaNotFoundException |
                   NodeNotFoundException |
                   ContainerNotFoundException e) {
            LOG.warn("Can not handle successful Add for move : {}", e);
            completeMove(cid, null);
          }
        } else {
          // resenting replication command is ok , no matter whether there is an
          // on-going replication
          try {
            sendReplicateCommand(cid, tgt, src);
          } catch (ContainerReplicaNotFoundException |
                   ContainerNotFoundException e) {
            LOG.warn("can not send replication command, {}", e);
            completeMove(cid, null);
          }
        }
      } else {
        // if container does not exist in src datanode, no matter it exists
        // in target datanode, we can not take more actions to this option,
        // so just remove it through ratis
        completeMove(cid, null);
      }
    });

    running = true;
  }

  /**
   * notify MoveManager that the current scm leader steps down.
   */
  @Override
  public void onNotLeader() {
    running = false;
  }

  /**
   * move a container replica from source datanode to
   * target datanode.
   *
   * @param cid Container to move
   * @param src source datanode
   * @param tgt target datanode
   */
  @Override
  public CompletableFuture<MoveResult> move(
      ContainerID cid, DatanodeDetails src, DatanodeDetails tgt)
      throws ContainerNotFoundException, NodeNotFoundException,
      TimeoutException, ContainerReplicaNotFoundException {
    CompletableFuture<MoveResult> ret = new CompletableFuture<>();

    if (!running) {
      ret.complete(MoveResult.FAIL_LEADER_NOT_READY);
      return ret;
    }

    /*
     * make sure the flowing conditions are met:
     *  1 the given two datanodes are in healthy state
     *  2 the given container exists on the given source datanode
     *  3 the given container does not exist on the given target datanode
     *  4 the given container is in closed state
     *  5 the giver container is not taking any inflight action
     *  6 the given two datanodes are in IN_SERVICE state
     *  7 {Existing replicas + Target_Dn - Source_Dn} satisfies
     *     the placement policy
     *
     * move is a combination of two steps : replication and deletion.
     * if the conditions above are all met, then we take a conservative
     * strategy here : replication can always be executed, but the execution
     * of deletion always depends on placement policy
     */

    NodeStatus currentNodeStat = nodeManager.getNodeStatus(src);
    HddsProtos.NodeState healthStat = currentNodeStat.getHealth();
    HddsProtos.NodeOperationalState operationalState =
        currentNodeStat.getOperationalState();
    if (healthStat != HddsProtos.NodeState.HEALTHY) {
      ret.complete(MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY);
      return ret;
    }
    if (operationalState != HddsProtos.NodeOperationalState.IN_SERVICE) {
      ret.complete(MoveResult.REPLICATION_FAIL_NODE_NOT_IN_SERVICE);
      return ret;
    }

    currentNodeStat = nodeManager.getNodeStatus(tgt);
    healthStat = currentNodeStat.getHealth();
    operationalState = currentNodeStat.getOperationalState();
    if (healthStat != HddsProtos.NodeState.HEALTHY) {
      ret.complete(MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY);
      return ret;
    }
    if (operationalState != HddsProtos.NodeOperationalState.IN_SERVICE) {
      ret.complete(MoveResult.REPLICATION_FAIL_NODE_NOT_IN_SERVICE);
      return ret;
    }

    // we need to synchronize on ContainerInfo, since it is
    // shared by ICR/FCR handler and this.processContainer
    // TODO: use a Read lock after introducing a RW lock into ContainerInfo
    ContainerInfo cif = containerManager.getContainer(cid);
    final Set<ContainerReplica> currentReplicas = containerManager
        .getContainerReplicas(cid);
    final Set<DatanodeDetails> replicas = currentReplicas.stream()
        .map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toSet());
    if (replicas.contains(tgt)) {
      ret.complete(MoveResult.REPLICATION_FAIL_EXIST_IN_TARGET);
      return ret;
    }
    if (!replicas.contains(src)) {
      ret.complete(MoveResult.REPLICATION_FAIL_NOT_EXIST_IN_SOURCE);
      return ret;
    }

    /*
     * the reason why the given container should not be taking any inflight
     * action is that: if the given container is being replicated or deleted,
     * the num of its replica is not deterministic, so move operation issued
     * by balancer may cause a nondeterministic result, so we should drop
     * this option for this time.
     * */
    List<ContainerReplicaOp> pendingOps =
        scm.getContainerReplicaPendingOps().getPendingOps(cid);

    if (pendingOps.stream().anyMatch(op ->
        op.getOpType() == ContainerReplicaOp.PendingOpType.ADD)) {
      ret.complete(MoveResult.REPLICATION_FAIL_INFLIGHT_REPLICATION);
      return ret;
    }

    if (pendingOps.stream().anyMatch(op ->
        op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE)) {
      ret.complete(MoveResult.REPLICATION_FAIL_INFLIGHT_DELETION);
      return ret;
    }

    /*
     * here, no need to see whether cid is in inflightMove, because
     * these three map are all synchronized on ContainerInfo, if cid
     * is in infligtMove , it must now being replicated or deleted,
     * so it must be in inflightReplication or in infligthDeletion.
     * thus, if we can not find cid in both of them , this cid must
     * not be in inflightMove.
     */

    HddsProtos.LifeCycleState currentContainerStat = cif.getState();
    if (currentContainerStat != HddsProtos.LifeCycleState.CLOSED) {
      ret.complete(MoveResult.REPLICATION_FAIL_CONTAINER_NOT_CLOSED);
      return ret;
    }

    // check whether {Existing replicas + Target_Dn - Source_Dn}
    // satisfies current placement policy
    if (!isPolicySatisfiedAfterMove(cif, src, tgt)) {
      ret.complete(MoveResult.PLACEMENT_POLICY_NOT_SATISFIED);
      return ret;
    }
    startMove(cid, src, tgt, ret);
    LOG.info("receive a move request about container {} , from {} to {}",
        cid, src.getUuid(), tgt.getUuid());
    return ret;
  }

  /**
   * notify move manager that a container op has been completed.
   *
   * @param cop ContainerReplicaOp
   * @param containerID ContainerID for which to complete
   */
  @Override
  public void notifyContainerOpCompleted(ContainerReplicaOp cop,
      ContainerID containerID) {
    if (!running) {
      return;
    }

    MoveDataNodePair mdnp = pendingMoveOps.get(containerID);
    if (mdnp != null) {
      PendingOpType opType = cop.getOpType();
      DatanodeDetails dn = cop.getTarget();
      if (opType.equals(PendingOpType.ADD) && mdnp.getTgt().equals(dn)) {
        if (pendingMoveFuture.containsKey(containerID)) {
          try {
            handleSuccessfulAdd(containerID);
          } catch (ContainerNotFoundException | NodeNotFoundException |
                   ContainerReplicaNotFoundException e) {
            LOG.warn("Can not handle successful Add for move : {}", e);
          }
        } else {
          //if MoveManager is reinitialize, pendingMoveOps will be restored,
          //but pendingMoveFuture not. so there will be a case that
          //container is in pendingMoveOps, but not in pendingMoveFuture.
          // we just complete this move for now.
          // also, we can continue this move, to be discussed.
          completeMove(containerID, null);
        }
      } else if (opType.equals(PendingOpType.DELETE)
          && mdnp.getSrc().equals(dn)) {
        completeMove(containerID, MoveResult.COMPLETED);
      }
    }
  }

   /**
   * notify move manager that a container op has been Expired.
   *
   * @param cop ContainerReplicaOp
   * @param containerID ContainerID for which to complete
   */
  @Override
  public void notifyContainerOpExpired(ContainerReplicaOp cop,
      ContainerID containerID) {
    if (!running) {
      return;
    }

    MoveDataNodePair mdnp = pendingMoveOps.get(containerID);
    if (mdnp != null) {
      PendingOpType opType = cop.getOpType();
      DatanodeDetails dn = cop.getTarget();
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
      ContainerReplicaNotFoundException, NodeNotFoundException {
    MoveDataNodePair movePair = pendingMoveOps.get(cid);
    if (movePair == null) {
      return;
    }
    final DatanodeDetails src = movePair.getSrc();
    final Set<ContainerReplica> replicaSet =
        containerManager.getContainerReplicas(cid);
    if (!replicaSet.stream()
        .anyMatch(r -> r.getDatanodeDetails().equals(src))) {
      // if the target is present but source disappears somehow,
      // we can consider move is successful.
      completeMove(cid, MoveResult.COMPLETED);
      return;
    }

    final NodeStatus nodeStatus = nodeManager.getNodeStatus(src);
    if (!nodeStatus.isInService()) {
      completeMove(cid, MoveResult.DELETION_FAIL_NODE_NOT_IN_SERVICE);
      return;
    }
    if (!nodeStatus.isHealthy()) {
      completeMove(cid, MoveResult.DELETION_FAIL_NODE_UNHEALTHY);
      return;
    }

    final ContainerInfo cif = containerManager.getContainer(cid);
    ContainerPlacementStatus currentCPS =
        getPlacementStatus(replicaSet, cif);
    Set<ContainerReplica> newReplicaSet = replicaSet.
        stream().collect(Collectors.toSet());
    newReplicaSet.removeIf(r -> r.getDatanodeDetails().equals(src));
    ContainerPlacementStatus newCPS =
        getPlacementStatus(newReplicaSet, cif);

    ContainerReplicaCount replicaCount =
        containerManager.getContainerReplicaCount(cid);
    int index = containerManager.getContainerReplicaIndex(cid, src);
    if (replicaCount.isOverReplicatedWithIndex(index) &&
        isPlacementStatusActuallyEqual(currentCPS, newCPS)) {
      sendDeleteCommand(cid, src);
    } else {
      // if source and target datanode are both in the replicaset,
      // but we can not delete source datanode for now (e.g.,
      // there is only 3 replicas or not policy-statisfied , etc.),
      // we just complete the future without sending a delete command.
      LOG.info("can not remove source replica after successfully " +
          "replicated to target datanode");
      completeMove(cid, MoveResult.DELETE_FAIL_POLICY);
    }
  }

  /**
   * whether the given two ContainerPlacementStatus are actually equal.
   *
   * @param cps1 ContainerPlacementStatus
   * @param cps2 ContainerPlacementStatus
   */
  private boolean isPlacementStatusActuallyEqual(
      ContainerPlacementStatus cps1,
      ContainerPlacementStatus cps2) {
    return (!cps1.isPolicySatisfied() &&
        cps1.actualPlacementCount() == cps2.actualPlacementCount()) ||
        cps1.isPolicySatisfied() && cps2.isPolicySatisfied();
  }

  /**
   * Returns whether {Existing replicas + Target_Dn - Source_Dn}
   * satisfies current placement policy.
   * @param srcDn DatanodeDetails of source data node
   * @param targetDn DatanodeDetails of target data node
   * @param cInfo container replicas
   * @return whether the placement policy is satisfied after move
   */
  private boolean isPolicySatisfiedAfterMove(
      ContainerInfo cInfo, DatanodeDetails srcDn,
      DatanodeDetails targetDn) throws ContainerNotFoundException {
    ContainerID cid = cInfo.containerID();
    final Set<ContainerReplica> replicas =
        containerManager.getContainerReplicas(cid);
    Set<ContainerReplica> movedReplicas =
        replicas.stream().collect(Collectors.toSet());
    movedReplicas.removeIf(r -> r.getDatanodeDetails().equals(srcDn));
    movedReplicas.add(ContainerReplica.newBuilder()
        .setDatanodeDetails(targetDn)
        .setContainerID(cid)
        .setContainerState(StorageContainerDatanodeProtocolProtos
            .ContainerReplicaProto.State.CLOSED).build());
    ContainerPlacementStatus placementStatus = getPlacementStatus(
        movedReplicas, cInfo);
    return placementStatus.isPolicySatisfied();
  }

  /**
   * Given a set of ContainerReplica, transform it to a list of DatanodeDetails
   * and then check if the list meets the container placement policy.
   * @param replicas List of containerReplica
   * @param cInfo container info
   * @return ContainerPlacementStatus indicating if the policy is met or not
   */
  private ContainerPlacementStatus getPlacementStatus(
      Set<ContainerReplica> replicas, ContainerInfo cInfo) {
    List<DatanodeDetails> replicaDns = replicas.stream()
        .map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList());
    return placementPolicyValidateProxy.validateContainerPlacement(
        replicaDns, cInfo);
  }

  /**
   * Sends replicate container command for the given container to the given
   * datanode.
   *
   * @param cid Container id to be replicated
   * @param tgt The destination datanode to replicate
   * @param src List of source nodes from where we can replicate
   */
  private void sendReplicateCommand(
      final ContainerID cid, final DatanodeDetails tgt,
      final DatanodeDetails src)
      throws ContainerReplicaNotFoundException, ContainerNotFoundException {
    int replicaIndex = containerManager.getContainerReplicaIndex(cid, src);
    final ReplicateContainerCommand replicateCommand =
        new ReplicateContainerCommand(cid.getId(),
            Collections.singletonList(src));
    try {
      replicateCommand.setTerm(scmContext.getTermOfLeader());
    } catch (NotLeaderException nle) {
      LOG.warn("Skip sending datanode command,"
          + " since current SCM is not leader.", nle);
      return;
    }

    LOG.info("Sending replicate container command for container {}" +
            " to datanode {} from datanodes {}", cid, tgt, src);
    eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND,
        new CommandForDatanode<>(tgt.getUuid(), replicateCommand));
    scm.getContainerReplicaPendingOps()
        .scheduleAddReplica(cid, tgt, replicaIndex);
  }

  /**
   * Sends delete container command for the given container to the given
   * datanode.
   *
   * @param cid Container to be deleted
   * @param datanode  The datanode on which the replica should be deleted
   */
  public void sendDeleteCommand(
      final ContainerID cid, final DatanodeDetails datanode)
      throws ContainerReplicaNotFoundException, ContainerNotFoundException {
    int replicaIndex = containerManager.getContainerReplicaIndex(cid, datanode);
    LOG.info("Sending delete container command for container {}" +
        " to datanode {}", cid, datanode);

    final DeleteContainerCommand deleteCommand =
        new DeleteContainerCommand(cid, true);
    try {
      deleteCommand.setTerm(scmContext.getTermOfLeader());
    } catch (NotLeaderException nle) {
      LOG.warn("Skip sending datanode command,"
          + " since current SCM is not leader.", nle);
      return;
    }

    eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND,
        new CommandForDatanode<>(datanode.getUuid(), deleteCommand));
    scm.getContainerReplicaPendingOps()
        .scheduleDeleteReplica(cid, datanode, replicaIndex);
  }

  /**
   * MoveDBHandler handles DB operations in move process.
   */
  public interface MoveDBHandler {
    @Replicate
    boolean deleteFromDB(ContainerID cid)
        throws TimeoutException;

    @Replicate
    boolean addToDB(ContainerID cid, MoveDataNodePair mp)
        throws TimeoutException;

    Table<ContainerID, MoveDataNodePair> getMoveTable();
  }

  /**
   * HA implementation of MoveDBHandler.
   */
  public static final class MoveDBHandlerImpl implements MoveDBHandler {
    private final DBTransactionBuffer transactionBuffer;
    private final Table<ContainerID, MoveDataNodePair> moveTable;

    private MoveDBHandlerImpl(
        final DBTransactionBuffer transactionBuffer,
        final Table<ContainerID, MoveDataNodePair> moveTable) {
      this.transactionBuffer = transactionBuffer;
      this.moveTable = moveTable;
    }


    @Override
    public boolean deleteFromDB(final ContainerID cid)
        throws TimeoutException {
      boolean success = true;
      try {
        transactionBuffer.removeFromBuffer(moveTable, cid);
      } catch (IOException e) {
        // this is a fatal error.
        // should we terminate scm here if this happens?
        LOG.error("Exception while deleting {} from DB: {}", cid, e);
        success = false;
      }
      return success;
    }

    @Override
    public boolean addToDB(final ContainerID cid, MoveDataNodePair mp)
        throws TimeoutException {
      boolean success = true;
      try {
        transactionBuffer.addToBuffer(moveTable, cid, mp);
      } catch (IOException e) {
        // this is a fatal error.
        // should we terminate scm here if this happens?
        LOG.error("Exception while adding {} to DB: {}", cid, e);
        success = false;
      }
      return success;
    }

    @Override
    public Table<ContainerID, MoveDataNodePair> getMoveTable() {
      return moveTable;
    }

    /**
     * Builder for Ratis based MoveManagerImpl.
     */
    public static class Builder {
      private SCMRatisServer ratisServer;
      private DBTransactionBuffer transactionBuffer;
      private Table<ContainerID, MoveDataNodePair> moveTable;

      public Builder setRatisServer(
          final SCMRatisServer ratisServer) {
        this.ratisServer = ratisServer;
        return this;
      }

      public Builder setDBTransactionBuffer(
          final DBTransactionBuffer buffer) {
        this.transactionBuffer = buffer;
        return this;
      }

      public Builder setMoveTable(
          final Table<ContainerID, MoveDataNodePair> moveTable) {
        this.moveTable = moveTable;
        return this;
      }

      public MoveDBHandler build() throws IOException {
        Preconditions.assertNotNull(transactionBuffer,
            "transactionBuffer is null");
        Preconditions.assertNotNull(moveTable, "moveTable is null");

        final MoveDBHandler impl =
            new MoveDBHandlerImpl(transactionBuffer, moveTable);

        final SCMHAInvocationHandler invocationHandler
            = new SCMHAInvocationHandler(MOVE, impl, ratisServer);

        return (MoveDBHandler) Proxy.newProxyInstance(
            SCMHAInvocationHandler.class.getClassLoader(),
            new Class<?>[]{MoveDBHandler.class},
            invocationHandler);
      }
    }
  }
}
