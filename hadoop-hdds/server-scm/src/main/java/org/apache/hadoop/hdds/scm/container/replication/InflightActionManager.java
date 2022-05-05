/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import com.google.protobuf.GeneratedMessage;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.container.replication.MoveScheduler.MoveResult;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * InflightActionManager manages all the inflight actions, including
 * inflightDeletion, inflightReplication and InflightECReconstruct.
 */
public class InflightActionManager {

  public static final Logger LOG =
      LoggerFactory.getLogger(InflightActionManager.class);

  /**
   * This is used for tracking container replication commands which are issued
   * by ReplicationManager and not yet complete.
   */
  private final Map<ContainerID, List<InflightAction>> inflightReplication;

  /**
   * This is used for tracking container deletion commands which are issued
   * by ReplicationManager and not yet complete.
   */
  private final Map<ContainerID, List<InflightAction>> inflightDeletion;

  /**
   * This is used for tracking EC recovery commands which are issued
   * by ReplicationManager and not yet complete.
   */
  private final Map<ContainerID, List<InflightAction>> inflightECReconstruct;

  /**
   * Reference to the ContainerManager.
   */
  private final ContainerManager containerManager;


  /**
   * Used to lookup the health of a nodes or the nodes operational state.
   */
  private final NodeManager nodeManager;

  /**
   * inflight action timeout.
   */
  private final long eventTimeout;

  private final Clock clock;

  /**
   * Minimum number of replica in a healthy state for maintenance.
   */
  private int minHealthyForMaintenance;

  /**
   * PlacementPolicy which is used to identify where a container
   * should be replicated.
   */
  private final PlacementPolicy containerPlacement;

  /**
   * EventPublisher to fire Replicate and Delete container events.
   */
  private final EventPublisher eventPublisher;

  /**
   * SCMContext from StorageContainerManager.
   */
  private final SCMContext scmContext;

  /**
   * Replication progress related metrics.
   */
  private ReplicationManagerMetrics metrics;

  private final MoveScheduler moveScheduler;

  @SuppressWarnings("parameternumber")
  public InflightActionManager(final ContainerManager containerManager,
                               final NodeManager nodeManager,
                               final long eventTimeout, Clock clock,
                               final MoveScheduler moveScheduler,
                               final int minHealthyForMaintenance,
                               final PlacementPolicy containerPlacement,
                               final EventPublisher eventPublisher,
                               final SCMContext scmContext) {
    inflightReplication = new ConcurrentHashMap<>();
    inflightDeletion = new ConcurrentHashMap<>();
    inflightECReconstruct = new ConcurrentHashMap<>();
    this.containerManager = containerManager;
    this.nodeManager = nodeManager;
    this.eventTimeout = eventTimeout;
    this.clock = clock;
    this.metrics = null;
    this.moveScheduler = moveScheduler;
    this.minHealthyForMaintenance = minHealthyForMaintenance;
    this.containerPlacement = containerPlacement;
    this.eventPublisher = eventPublisher;
    this.scmContext = scmContext;
  }

  protected void setMetrics(ReplicationManagerMetrics metrics) {
    this.metrics = metrics;
  }

  public Map<ContainerID, List<InflightAction>> getInflightReplication() {
    return inflightReplication;
  }

  public Map<ContainerID, List<InflightAction>> getInflightDeletion() {
    return inflightDeletion;
  }

  public Map<ContainerID, List<InflightAction>> getInflightECReconstruct() {
    return inflightECReconstruct;
  }

  public void addInflightReplication(
      ContainerID id, InflightAction inflightAction) {
    inflightReplication.computeIfAbsent(id, k -> new ArrayList<>());
    inflightReplication.get(id).add(inflightAction);
  }

  public void addInflightDeletion(
      ContainerID id, InflightAction inflightAction) {
    inflightDeletion.computeIfAbsent(id, k -> new ArrayList<>());
    inflightDeletion.get(id).add(inflightAction);
  }

  public void addInflightECReconstruct(
      ContainerID id, InflightAction inflightAction) {
    inflightECReconstruct.computeIfAbsent(id, k -> new ArrayList<>());
    inflightECReconstruct.get(id).add(inflightAction);
  }

  public void clearInflightActions() {
    inflightDeletion.clear();
    inflightECReconstruct.clear();
    inflightReplication.clear();
  }

  /**
  * Reconciles the InflightActions for a given container.
  */
  public void updateContainerInflightActions(final ContainerInfo container) {
    updateContainerInflightReplication(container);
    updateContainerInflightDeletion(container);
    updateContainerInflightECReconstruct(container);
  }

  private void updateContainerInflightReplication(
      final ContainerInfo container) {
    try {
      final Set<ContainerReplica> replicas =
          containerManager.getContainerReplicas(container.containerID());
      updateInflightActionInternal(container, inflightReplication,
          action -> replicas.stream().anyMatch(
              r -> r.getDatanodeDetails().equals(action.getDatanode())),
          () -> metrics.incrNumReplicationCmdsTimeout(),
          action -> updateCompletedReplicationMetrics(container, action));
    } catch (ContainerNotFoundException e) {
      LOG.warn("fail to update InflightReplication, " +
          "container not found : {}", container);
    }
  }

  private void updateContainerInflightDeletion(final ContainerInfo container) {
    try {
      final Set<ContainerReplica> replicas =
          containerManager.getContainerReplicas(container.containerID());
      updateInflightActionInternal(container, inflightDeletion,
          action -> replicas.stream().noneMatch(
              r -> r.getDatanodeDetails().equals(action.getDatanode())),
          () -> metrics.incrNumDeletionCmdsTimeout(),
          action -> updateCompletedDeletionMetrics(container, action));
    } catch (ContainerNotFoundException e) {
      LOG.warn("fail to update InflightReplication, " +
          "container not found : {}", container);
    }
  }

  private void updateContainerInflightECReconstruct(
      final ContainerInfo container) {
    //no op for now, implement later
  }

  private void updateCompletedReplicationMetrics(
      ContainerInfo container, InflightAction action) {
    metrics.incrNumReplicationCmdsCompleted();
    metrics.incrNumReplicationBytesCompleted(container.getUsedBytes());
    metrics.addReplicationTime(clock.millis() - action.getTime());
  }

  private void updateCompletedDeletionMetrics(
      ContainerInfo container, InflightAction action) {
    metrics.incrNumDeletionCmdsCompleted();
    metrics.incrNumDeletionBytesCompleted(container.getUsedBytes());
    metrics.addDeletionTime(clock.millis() - action.getTime());
  }

  /**
   * Given a ContainerID, lookup the ContainerInfo and then return a
   * ContainerReplicaCount object for the container.
   * @param containerID The ID of the container
   * @return ContainerReplicaCount for the given container
   * @throws ContainerNotFoundException
   */
  public ContainerReplicaCount getContainerReplicaCount(ContainerID containerID)
      throws ContainerNotFoundException {
    ContainerInfo container = containerManager.getContainer(containerID);
    return getContainerReplicaCount(container);
  }

  /**
   * Given a container, obtain the set of known replica for it, and return a
   * ContainerReplicaCount object. This object will contain the set of replica
   * as well as all information required to determine if the container is over
   * or under replicated, including the delta of replica required to repair the
   * over or under replication.
   *
   * @param container The container to create a ContainerReplicaCount for
   * @return ContainerReplicaCount representing the replicated state of the
   *         container.
   * @throws ContainerNotFoundException
   */
  private ContainerReplicaCount getContainerReplicaCount(
      ContainerInfo container) throws ContainerNotFoundException {
    // TODO: using a RW lock for only read
    synchronized (container) {
      final Set<ContainerReplica> replica = containerManager
          .getContainerReplicas(container.containerID());
      return getContainerReplicaCount(container, replica);
    }
  }

  /**
   * Given a container and its set of replicas, create and return a
   * ContainerReplicaCount representing the container.
   *
   * @param container The container for which to construct a
   *                  ContainerReplicaCount
   * @param replica The set of existing replica for this container
   * @return ContainerReplicaCount representing the current state of the
   *         container
   */
  protected ContainerReplicaCount getContainerReplicaCount(
      ContainerInfo container, Set<ContainerReplica> replica) {
    return new ContainerReplicaCount(
        container,
        replica,
        getInflightAdd(container.containerID()),
        getInflightDel(container.containerID()),
        container.getReplicationConfig().getRequiredNodes(),
        minHealthyForMaintenance);
  }

  /**
   * Returns the number replica which are pending creation for the given
   * container ID.
   * @param id The ContainerID for which to check the pending replica
   * @return The number of inflight additions or zero if none
   */
  private int getInflightAdd(final ContainerID id) {
    return inflightReplication.getOrDefault(id, Collections.emptyList()).size();
  }

  /**
   * Returns the number replica which are pending delete for the given
   * container ID.
   * @param id The ContainerID for which to check the pending replica
   * @return The number of inflight deletes or zero if none
   */
  private int getInflightDel(final ContainerID id) {
    return inflightDeletion.getOrDefault(id, Collections.emptyList()).size();
  }

  /**
   * Reconciles the specified InflightActions for a given container.
   *
   * @param container Container to update
   * @param inflightActions inflightReplication (or) inflightDeletion
   * @param filter filter to check if the operation is completed
   * @param timeoutCounter update timeout metrics
   * @param completedCounter update completed metrics
   */
  private void updateInflightActionInternal(
      final ContainerInfo container,
      final Map<ContainerID, List<InflightAction>> inflightActions,
      final Predicate<InflightAction> filter,
      final Runnable timeoutCounter,
      final Consumer<InflightAction> completedCounter) {
    final ContainerID id = container.containerID();
    final long deadline = clock.millis() - eventTimeout;
    if (inflightActions.containsKey(id)) {
      final List<InflightAction> actions = inflightActions.get(id);

      Iterator<InflightAction> iter = actions.iterator();
      while (iter.hasNext()) {
        try {
          InflightAction a = iter.next();
          NodeStatus status = nodeManager.getNodeStatus(a.getDatanode());
          boolean isUnhealthy =
              status.getHealth() != HddsProtos.NodeState.HEALTHY;
          boolean isCompleted = filter.test(a);
          boolean isTimeout = a.getTime() < deadline;
          boolean isNotInService = status.getOperationalState() !=
              HddsProtos.NodeOperationalState.IN_SERVICE;
          if (isCompleted || isUnhealthy || isTimeout || isNotInService) {
            iter.remove();

            if (isTimeout) {
              timeoutCounter.run();
            } else if (isCompleted) {
              completedCounter.accept(a);
            }

            updateMoveIfNeeded(isUnhealthy, isCompleted, isTimeout,
                isNotInService, container, a.getDatanode(), inflightActions);
          }
        } catch (NodeNotFoundException | ContainerNotFoundException e) {
          // Should not happen, but if it does, just remove the action as the
          // node somehow does not exist;
          iter.remove();
        }
      }
      if (actions.isEmpty()) {
        inflightActions.remove(id);
      }
    }
  }

  /**
   * update inflight move if needed.
   *
   * @param isUnhealthy is the datanode unhealthy
   * @param isCompleted is the action completed
   * @param isTimeout is the action timeout
   * @param container Container to update
   * @param dn datanode which is removed from the inflightActions
   * @param inflightActions inflightReplication (or) inflightDeletion
   */
  private void updateMoveIfNeeded(
      final boolean isUnhealthy,
      final boolean isCompleted, final boolean isTimeout,
      final boolean isNotInService,
      final ContainerInfo container, final DatanodeDetails dn,
      final Map<ContainerID, List<InflightAction>> inflightActions)
      throws ContainerNotFoundException {
    // make sure inflightMove contains the container
    ContainerID id = container.containerID();

    // make sure the datanode , which is removed from inflightActions,
    // is source or target datanode.
    MoveDataNodePair kv = moveScheduler.getMoveDataNodePair(id);
    if (kv == null) {
      return;
    }
    final boolean isSource = kv.getSrc().equals(dn);
    final boolean isTarget = kv.getTgt().equals(dn);
    if (!isSource && !isTarget) {
      return;
    }
    final boolean isInflightReplication =
        inflightActions.equals(inflightReplication);

    /*
     * there are some case:
     **********************************************************
     *              * InflightReplication  * InflightDeletion *
     **********************************************************
     *source removed*     unexpected       *    expected      *
     **********************************************************
     *target removed*      expected        *   unexpected     *
     **********************************************************
     * unexpected action may happen somehow. to make it deterministic,
     * if unexpected action happens, we just fail the completableFuture.
     */

    if (isSource && isInflightReplication) {
      //if RM is reinitialize, inflightMove will be restored,
      //but inflightMoveFuture not. so there will be a case that
      //container is in inflightMove, but not in inflightMoveFuture.
      moveScheduler.compleleteMoveFutureWithResult(id,
          MoveResult.UNEXPECTED_REMOVE_SOURCE_AT_INFLIGHT_REPLICATION);
      moveScheduler.completeMove(id.getProtobuf());
      return;
    }

    if (isTarget && !isInflightReplication) {
      moveScheduler.compleleteMoveFutureWithResult(id,
          MoveResult.UNEXPECTED_REMOVE_TARGET_AT_INFLIGHT_DELETION);
      moveScheduler.completeMove(id.getProtobuf());
      return;
    }

    if (!(isInflightReplication && isCompleted)) {
      if (isInflightReplication) {
        if (isUnhealthy) {
          moveScheduler.compleleteMoveFutureWithResult(id,
              MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY);
        } else if (isNotInService) {
          moveScheduler.compleleteMoveFutureWithResult(id,
              MoveResult.REPLICATION_FAIL_NODE_NOT_IN_SERVICE);
        } else {
          moveScheduler.compleleteMoveFutureWithResult(id,
              MoveResult.REPLICATION_FAIL_TIME_OUT);
        }
      } else {
        if (isUnhealthy) {
          moveScheduler.compleleteMoveFutureWithResult(id,
              MoveResult.DELETION_FAIL_NODE_UNHEALTHY);
        } else if (isTimeout) {
          moveScheduler.compleleteMoveFutureWithResult(id,
              MoveResult.DELETION_FAIL_TIME_OUT);
        } else if (isNotInService) {
          moveScheduler.compleleteMoveFutureWithResult(id,
              MoveResult.DELETION_FAIL_NODE_NOT_IN_SERVICE);
        } else {
          moveScheduler.compleleteMoveFutureWithResult(id,
              MoveResult.COMPLETED);
        }
      }
      moveScheduler.completeMove(id.getProtobuf());
    } else {
      deleteSrcDnForMove(container,
          containerManager.getContainerReplicas(id));
    }
  }

  /**
   * if the container is in inflightMove, handle move.
   * This function assumes replication has been completed
   *
   * @param cif ContainerInfo
   * @param replicaSet An Set of replicas, which may have excess replicas
   */
  protected void deleteSrcDnForMove(final ContainerInfo cif,
                                  final Set<ContainerReplica> replicaSet) {
    final ContainerID cid = cif.containerID();
    MoveDataNodePair movePair = moveScheduler.getMoveDataNodePair(cid);
    if (movePair == null) {
      return;
    }
    final DatanodeDetails srcDn = movePair.getSrc();
    ContainerReplicaCount replicaCount =
        getContainerReplicaCount(cif, replicaSet);

    if (!replicaSet.stream()
        .anyMatch(r -> r.getDatanodeDetails().equals(srcDn))) {
      // if the target is present but source disappears somehow,
      // we can consider move is successful.
      moveScheduler.compleleteMoveFutureWithResult(cid, MoveResult.COMPLETED);
      moveScheduler.completeMove(cid.getProtobuf());
      return;
    }

    int replicationFactor =
        cif.getReplicationConfig().getRequiredNodes();
    ContainerPlacementStatus currentCPS =
        getPlacementStatus(replicaSet, replicationFactor);
    Set<ContainerReplica> newReplicaSet = replicaSet.
        stream().collect(Collectors.toSet());
    newReplicaSet.removeIf(r -> r.getDatanodeDetails().equals(srcDn));
    ContainerPlacementStatus newCPS =
        getPlacementStatus(newReplicaSet, replicationFactor);

    if (replicaCount.isOverReplicated() &&
        isPlacementStatusActuallyEqual(currentCPS, newCPS)) {
      sendDeleteCommand(cif, srcDn, true);
    } else {
      // if source and target datanode are both in the replicaset,
      // but we can not delete source datanode for now (e.g.,
      // there is only 3 replicas or not policy-statisfied , etc.),
      // we just complete the future without sending a delete command.
      LOG.info("can not remove source replica after successfully " +
          "replicated to target datanode");
      moveScheduler.compleleteMoveFutureWithResult(
          cid, MoveResult.DELETE_FAIL_POLICY);
      moveScheduler.completeMove(cid.getProtobuf());
    }
  }

  /**
   * Given a set of ContainerReplica, transform it to a list of DatanodeDetails
   * and then check if the list meets the container placement policy.
   * @param replicas List of containerReplica
   * @param replicationFactor Expected Replication Factor of the containe
   * @return ContainerPlacementStatus indicating if the policy is met or not
   */
  protected ContainerPlacementStatus getPlacementStatus(
      Set<ContainerReplica> replicas, int replicationFactor) {
    List<DatanodeDetails> replicaDns = replicas.stream()
        .map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList());
    return containerPlacement.validateContainerPlacement(
        replicaDns, replicationFactor);
  }

  /**
   * whether the given two ContainerPlacementStatus are actually equal.
   *
   * @param cps1 ContainerPlacementStatus
   * @param cps2 ContainerPlacementStatus
   */
  protected boolean isPlacementStatusActuallyEqual(
      ContainerPlacementStatus cps1,
      ContainerPlacementStatus cps2) {
    return (!cps1.isPolicySatisfied() &&
        cps1.actualPlacementCount() == cps2.actualPlacementCount()) ||
        cps1.isPolicySatisfied() && cps2.isPolicySatisfied();
  }

  public boolean isContainerReplicatingOrDeleting(ContainerID containerID) {
    return inflightReplication.containsKey(containerID) ||
        inflightDeletion.containsKey(containerID);
  }



  //todo : following command-sending functions will be removed
  // after wrap command sending actions to a standalone class or function.

  /**
   * Sends replicate container command for the given container to the given
   * datanode.
   *
   * @param container Container to be replicated
   * @param datanode The destination datanode to replicate
   * @param sources List of source nodes from where we can replicate
   */
  protected void sendReplicateCommand(final ContainerInfo container,
                                    final DatanodeDetails datanode,
                                    final List<DatanodeDetails> sources) {

    LOG.info("Sending replicate container command for container {}" +
            " to datanode {} from datanodes {}",
        container.containerID(), datanode, sources);

    final ContainerID id = container.containerID();
    final ReplicateContainerCommand replicateCommand =
        new ReplicateContainerCommand(id.getId(), sources);
    inflightReplication.computeIfAbsent(id, k -> new ArrayList<>());
    sendAndTrackDatanodeCommand(datanode, replicateCommand,
        action -> inflightReplication.get(id).add(action));

    metrics.incrNumReplicationCmdsSent();
    metrics.incrNumReplicationBytesTotal(container.getUsedBytes());
  }

  /**
   * Sends delete container command for the given container to the given
   * datanode.
   *
   * @param container Container to be deleted
   * @param datanode The datanode on which the replica should be deleted
   * @param force Should be set to true to delete an OPEN replica
   */
  protected void sendDeleteCommand(final ContainerInfo container,
                                 final DatanodeDetails datanode,
                                 final boolean force) {

    LOG.info("Sending delete container command for container {}" +
        " to datanode {}", container.containerID(), datanode);

    final ContainerID id = container.containerID();
    final DeleteContainerCommand deleteCommand =
        new DeleteContainerCommand(id.getId(), force);
    inflightDeletion.computeIfAbsent(id, k -> new ArrayList<>());
    sendAndTrackDatanodeCommand(datanode, deleteCommand,
        action -> inflightDeletion.get(id).add(action));

    metrics.incrNumDeletionCmdsSent();
    metrics.incrNumDeletionBytesTotal(container.getUsedBytes());
  }

  /**
   * Creates CommandForDatanode with the given SCMCommand and fires
   * DATANODE_COMMAND event to event queue.
   *
   * Tracks the command using the given tracker.
   *
   * @param datanode Datanode to which the command has to be sent
   * @param command SCMCommand to be sent
   * @param tracker Tracker which tracks the inflight actions
   * @param <T> Type of SCMCommand
   */
  private <T extends GeneratedMessage> void sendAndTrackDatanodeCommand(
      final DatanodeDetails datanode,
      final SCMCommand<T> command,
      final Consumer<InflightAction> tracker) {
    try {
      command.setTerm(scmContext.getTermOfLeader());
    } catch (NotLeaderException nle) {
      LOG.warn("Skip sending datanode command,"
          + " since current SCM is not leader.", nle);
      return;
    }
    final CommandForDatanode<T> datanodeCommand =
        new CommandForDatanode<>(datanode.getUuid(), command);
    eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND, datanodeCommand);
    tracker.accept(new InflightAction(datanode, clock.millis()));
  }
}

