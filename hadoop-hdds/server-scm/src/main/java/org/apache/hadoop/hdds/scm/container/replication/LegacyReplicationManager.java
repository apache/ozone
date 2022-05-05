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

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport.HealthState;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.scm.container.replication.MoveScheduler.MoveResult;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Legacy Replication Manager (RM) is a legacy , which is used to process
 * non-EC container, and hopefully to be replaced int the future.
 */
public class LegacyReplicationManager {

  public static final Logger LOG =
      LoggerFactory.getLogger(LegacyReplicationManager.class);

  /**
   * Reference to the ContainerManager.
   */
  private final ContainerManager containerManager;

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
   * Used to lookup the health of a nodes or the nodes operational state.
   */
  private final NodeManager nodeManager;


  private final InflightActionManager inflightActionManager;

  /**
   * Current container size as a bound for choosing datanodes with
   * enough space for a replica.
   */
  private long currentContainerSize;

  /**
   * scheduler move option.
   */
  private final MoveScheduler moveScheduler;


  /**
   * Constructs ReplicationManager instance with the given configuration.
   *  @param conf OzoneConfiguration
   * @param containerManager ContainerManager
   * @param containerPlacement PlacementPolicy
   * @param eventPublisher EventPublisher
   */
  @SuppressWarnings("parameternumber")
  public LegacyReplicationManager(
      final ConfigurationSource conf,
      final ContainerManager containerManager,
      final PlacementPolicy containerPlacement,
      final EventPublisher eventPublisher,
      final SCMContext scmContext,
      final NodeManager nodeManager,
      final MoveScheduler moveScheduler,
      final InflightActionManager inflightActionManager) throws IOException {
    this.containerManager = containerManager;
    this.containerPlacement = containerPlacement;
    this.eventPublisher = eventPublisher;
    this.scmContext = scmContext;
    this.nodeManager = nodeManager;
    this.inflightActionManager = inflightActionManager;

    this.currentContainerSize = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT,
        StorageUnit.BYTES);

    this.moveScheduler = moveScheduler;
  }

  /**
   * Process the given container.
   *
   * @param container ContainerInfo
   */
  @SuppressWarnings("checkstyle:methodlength")
  protected void processContainer(ContainerInfo container,
      ReplicationManagerReport report) {
    final ContainerID id = container.containerID();
    try {
      // synchronize on the containerInfo object to solve container
      // race conditions with ICR/FCR handlers
      synchronized (container) {
        final Set<ContainerReplica> replicas = containerManager
            .getContainerReplicas(id);
        final LifeCycleState state = container.getState();
        report.increment(state);

        /*
         * We don't take any action if the container is in OPEN state and
         * the container is healthy. If the container is not healthy, i.e.
         * the replicas are not in OPEN state, send CLOSE_CONTAINER command.
         */
        if (state == LifeCycleState.OPEN) {
          if (!isOpenContainerHealthy(container, replicas)) {
            report.incrementAndSample(
                HealthState.OPEN_UNHEALTHY, container.containerID());
            eventPublisher.fireEvent(SCMEvents.CLOSE_CONTAINER, id);
          }
          return;
        }

        /*
         * If the container is in CLOSING state, the replicas can either
         * be in OPEN or in CLOSING state. In both of this cases
         * we have to resend close container command to the datanodes.
         */
        if (state == LifeCycleState.CLOSING) {
          for (ContainerReplica replica: replicas) {
            if (replica.getState() != State.UNHEALTHY) {
              sendCloseCommand(
                  container, replica.getDatanodeDetails(), false);
            }
          }
          return;
        }

        /*
         * If the container is in QUASI_CLOSED state, check and close the
         * container if possible.
         */
        if (state == LifeCycleState.QUASI_CLOSED) {
          if (canForceCloseContainer(container, replicas)) {
            forceCloseContainer(container, replicas);
            return;
          } else {
            report.incrementAndSample(HealthState.QUASI_CLOSED_STUCK,
                container.containerID());
          }
        }

        if (container.getReplicationType() == HddsProtos.ReplicationType.EC) {
          // TODO We do not support replicating EC containers as yet, so at this
          //      point, after handing the closing etc states, we just return.
          //      EC Support will be added later.
          return;
        }

        /*
         * Before processing the container we have to reconcile the
         * inflightReplication and inflightDeletion actions.
         *
         * We remove the entry from inflightReplication and inflightDeletion
         * list, if the operation is completed or if it has timed out.
         */
        inflightActionManager.updateContainerInflightActions(container);

        /*
         * If container is under deleting and all it's replicas are deleted,
         * then make the container as CLEANED,
         * or resend the delete replica command if needed.
         */
        if (state == LifeCycleState.DELETING) {
          handleContainerUnderDelete(container, replicas);
          return;
        }

        /**
         * We don't need to take any action for a DELETE container - eventually
         * it will be removed from SCM.
         */
        if (state == LifeCycleState.DELETED) {
          return;
        }

        ContainerReplicaCount replicaSet =
            inflightActionManager.getContainerReplicaCount(container, replicas);
        ContainerPlacementStatus placementStatus = inflightActionManager
            .getPlacementStatus(replicas,
                container.getReplicationConfig().getRequiredNodes());

        /*
         * We don't have to take any action if the container is healthy.
         *
         * According to ReplicationMonitor container is considered healthy if
         * the container is either in QUASI_CLOSED or in CLOSED state and has
         * exact number of replicas in the same state.
         */
        if (isContainerEmpty(container, replicas)) {
          report.incrementAndSample(
              HealthState.EMPTY, container.containerID());
          /*
           *  If container is empty, schedule task to delete the container.
           */
          deleteContainerReplicas(container, replicas);
          return;
        }

        /*
         * Check if the container is under replicated and take appropriate
         * action.
         */
        boolean sufficientlyReplicated = replicaSet.isSufficientlyReplicated();
        boolean placementSatisfied = placementStatus.isPolicySatisfied();
        if (!sufficientlyReplicated || !placementSatisfied) {
          if (!sufficientlyReplicated) {
            report.incrementAndSample(
                HealthState.UNDER_REPLICATED, container.containerID());
            if (replicaSet.isMissing()) {
              report.incrementAndSample(HealthState.MISSING,
                  container.containerID());
            }
          }
          if (!placementSatisfied) {
            report.incrementAndSample(HealthState.MIS_REPLICATED,
                container.containerID());

          }
          handleUnderReplicatedContainer(container,
              replicaSet, placementStatus);
          return;
        }

        /*
         * Check if the container is over replicated and take appropriate
         * action.
         */
        if (replicaSet.isOverReplicated()) {
          report.incrementAndSample(HealthState.OVER_REPLICATED,
              container.containerID());
          handleOverReplicatedContainer(container, replicaSet);
          return;
        }

      /*
       If we get here, the container is not over replicated or under replicated
       but it may be "unhealthy", which means it has one or more replica which
       are not in the same state as the container itself.
       */
        if (!replicaSet.isHealthy()) {
          report.incrementAndSample(HealthState.UNHEALTHY,
              container.containerID());
          handleUnstableContainer(container, replicas);
        }
      }
    } catch (ContainerNotFoundException ex) {
      LOG.warn("Missing container {}.", id);
    } catch (Exception ex) {
      LOG.warn("Process container {} error: ", id, ex);
    }
  }

  /**
   * add a move action for a given container.
   *
   * @param cid Container to move
   * @param src source datanode
   * @param tgt target datanode
   */
  public CompletableFuture<MoveResult> move(
      ContainerID cid, DatanodeDetails src, DatanodeDetails tgt)
      throws ContainerNotFoundException, NodeNotFoundException {
    return move(cid, new MoveDataNodePair(src, tgt));
  }

  /**
   * add a move action for a given container.
   *
   * @param cid Container to move
   * @param mp MoveDataNodePair which contains source and target datanodes
   */
  private CompletableFuture<MoveResult> move(ContainerID cid,
                                                           MoveDataNodePair mp)
      throws ContainerNotFoundException, NodeNotFoundException {
    CompletableFuture<MoveResult> ret = new CompletableFuture<>();

    if (!scmContext.isLeader()) {
      ret.complete(MoveResult.FAIL_NOT_LEADER);
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

    DatanodeDetails srcDn = mp.getSrc();
    DatanodeDetails targetDn = mp.getTgt();
    NodeStatus currentNodeStat = nodeManager.getNodeStatus(srcDn);
    NodeState healthStat = currentNodeStat.getHealth();
    NodeOperationalState operationalState =
        currentNodeStat.getOperationalState();
    if (healthStat != NodeState.HEALTHY) {
      ret.complete(MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY);
      return ret;
    }
    if (operationalState != NodeOperationalState.IN_SERVICE) {
      ret.complete(MoveResult.REPLICATION_FAIL_NODE_NOT_IN_SERVICE);
      return ret;
    }

    currentNodeStat = nodeManager.getNodeStatus(targetDn);
    healthStat = currentNodeStat.getHealth();
    operationalState = currentNodeStat.getOperationalState();
    if (healthStat != NodeState.HEALTHY) {
      ret.complete(MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY);
      return ret;
    }
    if (operationalState != NodeOperationalState.IN_SERVICE) {
      ret.complete(MoveResult.REPLICATION_FAIL_NODE_NOT_IN_SERVICE);
      return ret;
    }

    // we need to synchronize on ContainerInfo, since it is
    // shared by ICR/FCR handler and this.processContainer
    // TODO: use a Read lock after introducing a RW lock into ContainerInfo
    ContainerInfo cif = containerManager.getContainer(cid);
    synchronized (cif) {
      final Set<ContainerReplica> currentReplicas = containerManager
          .getContainerReplicas(cid);
      final Set<DatanodeDetails> replicas = currentReplicas.stream()
            .map(ContainerReplica::getDatanodeDetails)
            .collect(Collectors.toSet());
      if (replicas.contains(targetDn)) {
        ret.complete(MoveResult.REPLICATION_FAIL_EXIST_IN_TARGET);
        return ret;
      }
      if (!replicas.contains(srcDn)) {
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

      if (inflightActionManager.getInflightReplication().containsKey(cid)) {
        ret.complete(MoveResult.REPLICATION_FAIL_INFLIGHT_REPLICATION);
        return ret;
      }
      if (inflightActionManager.getInflightDeletion().containsKey(cid)) {
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

      LifeCycleState currentContainerStat = cif.getState();
      if (currentContainerStat != LifeCycleState.CLOSED) {
        ret.complete(MoveResult.REPLICATION_FAIL_CONTAINER_NOT_CLOSED);
        return ret;
      }

      // check whether {Existing replicas + Target_Dn - Source_Dn}
      // satisfies current placement policy
      if (!isPolicySatisfiedAfterMove(cif, srcDn, targetDn,
          currentReplicas.stream().collect(Collectors.toList()))) {
        ret.complete(MoveResult.PLACEMENT_POLICY_NOT_SATISFIED);
        return ret;
      }

      try {
        moveScheduler.startMove(cid.getProtobuf(),
            mp.getProtobufMessage(ClientVersion.CURRENT_VERSION));
      } catch (IOException e) {
        LOG.warn("Exception while starting move {}", cid);
        ret.complete(MoveResult.FAIL_CAN_NOT_RECORD_TO_DB);
        return ret;
      }

      moveScheduler.addMoveCompleteFuture(cid, ret);
      inflightActionManager.sendReplicateCommand(
          cif, targetDn, Collections.singletonList(srcDn));
    }
    LOG.info("receive a move request about container {} , from {} to {}",
        cid, srcDn.getUuid(), targetDn.getUuid());
    return ret;
  }

  /**
   * Returns whether {Existing replicas + Target_Dn - Source_Dn}
   * satisfies current placement policy.
   * @param cif Container Info of moved container
   * @param srcDn DatanodeDetails of source data node
   * @param targetDn DatanodeDetails of target data node
   * @param replicas container replicas
   * @return whether the placement policy is satisfied after move
   */
  private boolean isPolicySatisfiedAfterMove(ContainerInfo cif,
                    DatanodeDetails srcDn, DatanodeDetails targetDn,
                    final List<ContainerReplica> replicas) {
    Set<ContainerReplica> movedReplicas =
        replicas.stream().collect(Collectors.toSet());
    movedReplicas.removeIf(r -> r.getDatanodeDetails().equals(srcDn));
    movedReplicas.add(ContainerReplica.newBuilder()
        .setDatanodeDetails(targetDn)
        .setContainerID(cif.containerID())
        .setContainerState(State.CLOSED).build());
    ContainerPlacementStatus placementStatus = inflightActionManager
        .getPlacementStatus(movedReplicas,
            cif.getReplicationConfig().getRequiredNodes());
    return placementStatus.isPolicySatisfied();
  }


  /**
   * Returns true if the container is empty and CLOSED.
   * A container is deemed empty if its keyCount (num of blocks) is 0. The
   * usedBytes counter is not checked here because usedBytes is not a
   * accurate representation of the committed blocks. There could be orphaned
   * chunks in the container which contribute to the usedBytes.
   *
   * @param container Container to check
   * @param replicas Set of ContainerReplicas
   * @return true if the container is empty, false otherwise
   */
  private boolean isContainerEmpty(final ContainerInfo container,
      final Set<ContainerReplica> replicas) {
    return container.getState() == LifeCycleState.CLOSED &&
        container.getNumberOfKeys() == 0 && replicas.stream().allMatch(
            r -> r.getState() == State.CLOSED && r.getKeyCount() == 0);
  }


  /**
   * Returns true if more than 50% of the container replicas with unique
   * originNodeId are in QUASI_CLOSED state.
   *
   * @param container Container to check
   * @param replicas Set of ContainerReplicas
   * @return true if we can force close the container, false otherwise
   */
  private boolean canForceCloseContainer(final ContainerInfo container,
      final Set<ContainerReplica> replicas) {
    Preconditions.assertTrue(container.getState() ==
        LifeCycleState.QUASI_CLOSED);
    final int replicationFactor =
        container.getReplicationConfig().getRequiredNodes();
    final long uniqueQuasiClosedReplicaCount = replicas.stream()
        .filter(r -> r.getState() == State.QUASI_CLOSED)
        .map(ContainerReplica::getOriginDatanodeId)
        .distinct()
        .count();
    return uniqueQuasiClosedReplicaCount > (replicationFactor / 2);
  }

  /**
   * Delete the container and its replicas.
   *
   * @param container ContainerInfo
   * @param replicas Set of ContainerReplicas
   */
  private void deleteContainerReplicas(final ContainerInfo container,
      final Set<ContainerReplica> replicas) throws IOException,
      InvalidStateTransitionException {
    Preconditions.assertTrue(container.getState() ==
        LifeCycleState.CLOSED);
    Preconditions.assertTrue(container.getNumberOfKeys() == 0);

    replicas.stream().forEach(rp -> {
      Preconditions.assertTrue(rp.getState() == State.CLOSED);
      Preconditions.assertTrue(rp.getKeyCount() == 0);
      inflightActionManager.sendDeleteCommand(
          container, rp.getDatanodeDetails(), false);
    });
    containerManager.updateContainerState(container.containerID(),
        HddsProtos.LifeCycleEvent.DELETE);
    LOG.debug("Deleting empty container replicas for {},", container);
  }

  /**
   * Handle the container which is under delete.
   *
   * @param container ContainerInfo
   * @param replicas Set of ContainerReplicas
   */
  private void handleContainerUnderDelete(final ContainerInfo container,
      final Set<ContainerReplica> replicas) throws IOException,
      InvalidStateTransitionException {
    if (replicas.size() == 0) {
      containerManager.updateContainerState(container.containerID(),
          HddsProtos.LifeCycleEvent.CLEANUP);
      LOG.debug("Container {} state changes to DELETED", container);
    } else {
      // Check whether to resend the delete replica command
      final List<DatanodeDetails> deletionInFlight =
          inflightActionManager.getInflightDeletion()
          .getOrDefault(container.containerID(), Collections.emptyList())
          .stream()
          .map(action -> action.getDatanode())
          .collect(Collectors.toList());
      Set<ContainerReplica> filteredReplicas = replicas.stream().filter(
          r -> !deletionInFlight.contains(r.getDatanodeDetails()))
          .collect(Collectors.toSet());
      // Resend the delete command
      if (filteredReplicas.size() > 0) {
        filteredReplicas.stream().forEach(rp ->
            inflightActionManager.sendDeleteCommand(
                container, rp.getDatanodeDetails(), false));
        LOG.debug("Resend delete Container command for {}", container);
      }
    }
  }

  /**
   * Force close the container replica(s) with highest sequence Id.
   *
   * <p>
   *   Note: We should force close the container only if >50% (quorum)
   *   of replicas with unique originNodeId are in QUASI_CLOSED state.
   * </p>
   *
   * @param container ContainerInfo
   * @param replicas Set of ContainerReplicas
   */
  private void forceCloseContainer(final ContainerInfo container,
                                   final Set<ContainerReplica> replicas) {
    Preconditions.assertTrue(container.getState() ==
        LifeCycleState.QUASI_CLOSED);

    final List<ContainerReplica> quasiClosedReplicas = replicas.stream()
        .filter(r -> r.getState() == State.QUASI_CLOSED)
        .collect(Collectors.toList());

    final Long sequenceId = quasiClosedReplicas.stream()
        .map(ContainerReplica::getSequenceId)
        .max(Long::compare)
        .orElse(-1L);

    LOG.info("Force closing container {} with BCSID {}," +
        " which is in QUASI_CLOSED state.",
        container.containerID(), sequenceId);

    quasiClosedReplicas.stream()
        .filter(r -> sequenceId != -1L)
        .filter(replica -> replica.getSequenceId().equals(sequenceId))
        .forEach(replica -> sendCloseCommand(
            container, replica.getDatanodeDetails(), true));
  }

  /**
   * If the given container is under replicated, identify a new set of
   * datanode(s) to replicate the container using PlacementPolicy
   * and send replicate container command to the identified datanode(s).
   *
   * @param container ContainerInfo
   * @param replicaSet An instance of ContainerReplicaCount, containing the
   *                   current replica count and inflight adds and deletes
   */
  private void handleUnderReplicatedContainer(final ContainerInfo container,
      final ContainerReplicaCount replicaSet,
      final ContainerPlacementStatus placementStatus) {
    LOG.debug("Handling under-replicated container: {}", container);
    Set<ContainerReplica> replicas = replicaSet.getReplica();
    try {

      if (replicaSet.isSufficientlyReplicated()
          && placementStatus.isPolicySatisfied()) {
        LOG.info("The container {} with replicas {} is sufficiently " +
            "replicated and is not mis-replicated",
            container.getContainerID(), replicaSet);
        return;
      }
      int repDelta = replicaSet.additionalReplicaNeeded();
      final ContainerID id = container.containerID();
      final List<DatanodeDetails> deletionInFlight = inflightActionManager
          .getInflightDeletion().getOrDefault(id, Collections.emptyList())
          .stream().map(action -> action.getDatanode())
          .collect(Collectors.toList());
      final List<DatanodeDetails> replicationInFlight = inflightActionManager
          .getInflightReplication().getOrDefault(id, Collections.emptyList())
          .stream().map(action -> action.getDatanode())
          .collect(Collectors.toList());
      final List<DatanodeDetails> source = replicas.stream()
          .filter(r ->
              r.getState() == State.QUASI_CLOSED ||
              r.getState() == State.CLOSED)
          // Exclude stale and dead nodes. This is particularly important for
          // maintenance nodes, as the replicas will remain present in the
          // container manager, even when they go dead.
          .filter(r ->
              getNodeStatus(r.getDatanodeDetails()).isHealthy())
          .filter(r -> !deletionInFlight.contains(r.getDatanodeDetails()))
          .sorted((r1, r2) -> r2.getSequenceId().compareTo(r1.getSequenceId()))
          .map(ContainerReplica::getDatanodeDetails)
          .collect(Collectors.toList());
      if (source.size() > 0) {
        final int replicationFactor = container
            .getReplicationConfig().getRequiredNodes();
        // Want to check if the container is mis-replicated after considering
        // inflight add and delete.
        // Create a new list from source (healthy replicas minus pending delete)
        List<DatanodeDetails> targetReplicas = new ArrayList<>(source);
        // Then add any pending additions
        targetReplicas.addAll(replicationInFlight);
        final ContainerPlacementStatus inFlightplacementStatus =
            containerPlacement.validateContainerPlacement(
                targetReplicas, replicationFactor);
        final int misRepDelta = inFlightplacementStatus.misReplicationCount();
        final int replicasNeeded
            = repDelta < misRepDelta ? misRepDelta : repDelta;
        if (replicasNeeded <= 0) {
          LOG.debug("Container {} meets replication requirement with " +
              "inflight replicas", id);
          return;
        }

        // We should ensure that the target datanode has enough space
        // for a complete container to be created, but since the container
        // size may be changed smaller than origin, we should be defensive.
        final long dataSizeRequired = Math.max(container.getUsedBytes(),
            currentContainerSize);
        final List<DatanodeDetails> excludeList = replicas.stream()
            .map(ContainerReplica::getDatanodeDetails)
            .collect(Collectors.toList());
        excludeList.addAll(replicationInFlight);
        final List<DatanodeDetails> selectedDatanodes = containerPlacement
            .chooseDatanodes(excludeList, null, replicasNeeded,
                0, dataSizeRequired);
        if (repDelta > 0) {
          LOG.info("Container {} is under replicated. Expected replica count" +
                  " is {}, but found {}.", id, replicationFactor,
              replicationFactor - repDelta);
        }
        int newMisRepDelta = misRepDelta;
        if (misRepDelta > 0) {
          LOG.info("Container: {}. {}",
              id, placementStatus.misReplicatedReason());
          // Check if the new target nodes (original plus newly selected nodes)
          // makes the placement policy valid.
          targetReplicas.addAll(selectedDatanodes);
          newMisRepDelta = containerPlacement.validateContainerPlacement(
              targetReplicas, replicationFactor).misReplicationCount();
        }
        if (repDelta > 0 || newMisRepDelta < misRepDelta) {
          // Only create new replicas if we are missing a replicas or
          // the number of pending mis-replication has improved. No point in
          // creating new replicas for mis-replicated containers unless it
          // improves things.
          for (DatanodeDetails datanode : selectedDatanodes) {
            inflightActionManager.sendReplicateCommand(
                container, datanode, source);
          }
        } else {
          LOG.warn("Container {} is mis-replicated, requiring {} additional " +
              "replicas. After selecting new nodes, mis-replication has not " +
              "improved. No additional replicas will be scheduled",
              id, misRepDelta);
        }
      } else {
        LOG.warn("Cannot replicate container {}, no healthy replica found.",
            container.containerID());
      }
    } catch (IOException | IllegalStateException ex) {
      LOG.warn("Exception while replicating container {}.",
          container.getContainerID(), ex);
    }
  }

  /**
   * If the given container is over replicated, identify the datanode(s)
   * to delete the container and send delete container command to the
   * identified datanode(s).
   *
   * @param container ContainerInfo
   * @param replicaSet An instance of ContainerReplicaCount, containing the
   *                   current replica count and inflight adds and deletes
   */
  private void handleOverReplicatedContainer(final ContainerInfo container,
      final ContainerReplicaCount replicaSet) {

    final Set<ContainerReplica> replicas = replicaSet.getReplica();
    final ContainerID id = container.containerID();
    final int replicationFactor =
        container.getReplicationConfig().getRequiredNodes();
    int excess = replicaSet.additionalReplicaNeeded() * -1;
    if (excess > 0) {

      LOG.info("Container {} is over replicated. Expected replica count" +
              " is {}, but found {}.", id, replicationFactor,
          replicationFactor + excess);

      final List<ContainerReplica> eligibleReplicas = new ArrayList<>(replicas);

      // Iterate replicas in deterministic order to avoid potential data loss.
      // See https://issues.apache.org/jira/browse/HDDS-4589.
      // N.B., sort replicas by (containerID, datanodeDetails).
      eligibleReplicas.sort(
          Comparator.comparingLong(ContainerReplica::hashCode));

      final Map<UUID, ContainerReplica> uniqueReplicas =
          new LinkedHashMap<>();

      if (container.getState() != LifeCycleState.CLOSED) {
        replicas.stream()
            .filter(r -> compareState(container.getState(), r.getState()))
            .forEach(r -> uniqueReplicas
                .putIfAbsent(r.getOriginDatanodeId(), r));

        eligibleReplicas.removeAll(uniqueReplicas.values());
      }
      // Replica which are maintenance or decommissioned are not eligible to
      // be removed, as they do not count toward over-replication and they
      // also many not be available
      eligibleReplicas.removeIf(r ->
          r.getDatanodeDetails().getPersistedOpState() !=
              NodeOperationalState.IN_SERVICE);

      final List<ContainerReplica> unhealthyReplicas = eligibleReplicas
          .stream()
          .filter(r -> !compareState(container.getState(), r.getState()))
          .collect(Collectors.toList());

      // If there are unhealthy replicas, then we should remove them even if it
      // makes the container violate the placement policy, as excess unhealthy
      // containers are not really useful. It will be corrected later as a
      // mis-replicated container will be seen as under-replicated.
      for (ContainerReplica r : unhealthyReplicas) {
        if (excess > 0) {
          inflightActionManager.sendDeleteCommand(
              container, r.getDatanodeDetails(), true);
          excess -= 1;
        } else {
          break;
        }
      }
      eligibleReplicas.removeAll(unhealthyReplicas);
      removeExcessReplicasIfNeeded(excess, container, eligibleReplicas);
    }
  }

  /**
   * remove execess replicas if needed, replicationFactor and placement policy
   * will be take into consideration.
   *
   * @param excess the excess number after subtracting replicationFactor
   * @param container ContainerInfo
   * @param eligibleReplicas An list of replicas, which may have excess replicas
   */
  private void removeExcessReplicasIfNeeded(int excess,
                    final ContainerInfo container,
                    final List<ContainerReplica> eligibleReplicas) {
    // After removing all unhealthy replicas, if the container is still over
    // replicated then we need to check if it is already mis-replicated.
    // If it is, we do no harm by removing excess replicas. However, if it is
    // not mis-replicated, then we can only remove replicas if they don't
    // make the container become mis-replicated.
    if (excess > 0) {
      Set<ContainerReplica> eligibleSet = new HashSet<>(eligibleReplicas);
      final int replicationFactor =
          container.getReplicationConfig().getRequiredNodes();
      ContainerPlacementStatus ps = inflightActionManager
          .getPlacementStatus(eligibleSet, replicationFactor);

      for (ContainerReplica r : eligibleReplicas) {
        if (excess <= 0) {
          break;
        }
        // First remove the replica we are working on from the set, and then
        // check if the set is now mis-replicated.
        eligibleSet.remove(r);
        ContainerPlacementStatus nowPS = inflightActionManager
            .getPlacementStatus(eligibleSet, replicationFactor);
        if (inflightActionManager.isPlacementStatusActuallyEqual(ps, nowPS)) {
          // Remove the replica if the container was already unsatisfied
          // and losing this replica keep actual placement count unchanged.
          // OR if losing this replica still keep satisfied
          inflightActionManager.sendDeleteCommand(
              container, r.getDatanodeDetails(), true);
          excess -= 1;
          continue;
        }
        // If we decided not to remove this replica, put it back into the set
        eligibleSet.add(r);
      }
      if (excess > 0) {
        LOG.info("The container {} is over replicated with {} excess " +
            "replica. The excess replicas cannot be removed without " +
            "violating the placement policy", container, excess);
      }
    }
  }

  /**
   * Handles unstable container.
   * A container is inconsistent if any of the replica state doesn't
   * match the container state. We have to take appropriate action
   * based on state of the replica.
   *
   * @param container ContainerInfo
   * @param replicas Set of ContainerReplicas
   */
  private void handleUnstableContainer(final ContainerInfo container,
      final Set<ContainerReplica> replicas) {
    // Find unhealthy replicas
    List<ContainerReplica> unhealthyReplicas = replicas.stream()
        .filter(r -> !compareState(container.getState(), r.getState()))
        .collect(Collectors.toList());

    Iterator<ContainerReplica> iterator = unhealthyReplicas.iterator();
    while (iterator.hasNext()) {
      final ContainerReplica replica = iterator.next();
      final State state = replica.getState();
      if (state == State.OPEN || state == State.CLOSING) {
        sendCloseCommand(container, replica.getDatanodeDetails(), false);
        iterator.remove();
      }

      if (state == State.QUASI_CLOSED) {
        // Send force close command if the BCSID matches
        if (container.getSequenceId() == replica.getSequenceId()) {
          sendCloseCommand(container, replica.getDatanodeDetails(), true);
          iterator.remove();
        }
      }
    }

    // Now we are left with the replicas which are either unhealthy or
    // the BCSID doesn't match. These replicas should be deleted.

    /*
     * If we have unhealthy replicas we go under replicated and then
     * replicate the healthy copy.
     *
     * We also make sure that we delete only one unhealthy replica at a time.
     *
     * If there are two unhealthy replica:
     *  - Delete first unhealthy replica
     *  - Re-replicate the healthy copy
     *  - Delete second unhealthy replica
     *  - Re-replicate the healthy copy
     *
     * Note: Only one action will be executed in a single ReplicationMonitor
     *       iteration. So to complete all the above actions we need four
     *       ReplicationMonitor iterations.
     */

    unhealthyReplicas.stream().findFirst().ifPresent(replica ->
        inflightActionManager.sendDeleteCommand(
            container, replica.getDatanodeDetails(), true));

  }

  /**
   * Sends close container command for the given container to the given
   * datanode.
   *
   * @param container Container to be closed
   * @param datanode The datanode on which the container
   *                  has to be closed
   * @param force Should be set to true if we want to close a
   *               QUASI_CLOSED container
   */
  private void sendCloseCommand(final ContainerInfo container,
                                final DatanodeDetails datanode,
                                final boolean force) {

    ContainerID containerID = container.containerID();
    LOG.info("Sending close container command for container {}" +
            " to datanode {}.", containerID, datanode);
    CloseContainerCommand closeContainerCommand =
        new CloseContainerCommand(container.getContainerID(),
            container.getPipelineID(), force);
    try {
      closeContainerCommand.setTerm(scmContext.getTermOfLeader());
    } catch (NotLeaderException nle) {
      LOG.warn("Skip sending close container command,"
          + " since current SCM is not leader.", nle);
      return;
    }
    closeContainerCommand.setEncodedToken(getContainerToken(containerID));
    eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND,
        new CommandForDatanode<>(datanode.getUuid(), closeContainerCommand));
  }

  private String getContainerToken(ContainerID containerID) {
    if (scmContext.getScm() instanceof StorageContainerManager) {
      StorageContainerManager scm =
              (StorageContainerManager) scmContext.getScm();
      return scm.getContainerTokenGenerator().generateEncodedToken(containerID);
    }
    return ""; // unit test
  }

  /**
   * Wrap the call to nodeManager.getNodeStatus, catching any
   * NodeNotFoundException and instead throwing an IllegalStateException.
   * @param dn The datanodeDetails to obtain the NodeStatus for
   * @return NodeStatus corresponding to the given Datanode.
   */
  private NodeStatus getNodeStatus(DatanodeDetails dn) {
    try {
      return nodeManager.getNodeStatus(dn);
    } catch (NodeNotFoundException e) {
      throw new IllegalStateException("Unable to find NodeStatus for " + dn, e);
    }
  }

  /**
   * Compares the container state with the replica state.
   *
   * @param containerState ContainerState
   * @param replicaState ReplicaState
   * @return true if the state matches, false otherwise
   */
  public static boolean compareState(final LifeCycleState containerState,
                                      final State replicaState) {
    switch (containerState) {
    case OPEN:
      return replicaState == State.OPEN;
    case CLOSING:
      return replicaState == State.CLOSING;
    case QUASI_CLOSED:
      return replicaState == State.QUASI_CLOSED;
    case CLOSED:
      return replicaState == State.CLOSED;
    case DELETING:
      return false;
    case DELETED:
      return false;
    default:
      return false;
    }
  }

  /**
   * An open container is healthy if all its replicas are in the same state as
   * the container.
   * @param container The container to check
   * @param replicas The replicas belonging to the container
   * @return True if the container is healthy, false otherwise
   */
  private boolean isOpenContainerHealthy(
      ContainerInfo container, Set<ContainerReplica> replicas) {
    LifeCycleState state = container.getState();
    return replicas.stream()
        .allMatch(r -> compareState(state, r.getState()));
  }

  protected void notifyStatusChanged() {
    //now, as the current scm is leader and it`s state is up-to-date,
    //we need to take some action about replicated inflight move options.
    onLeaderReadyAndOutOfSafeMode();
  }

  /**
  * when scm become LeaderReady and out of safe mode, some actions
  * should be taken. for now , it is only used for handle replicated
  * infligtht move.
  */
  private void onLeaderReadyAndOutOfSafeMode() {
    List<HddsProtos.ContainerID> needToRemove = new LinkedList<>();
    moveScheduler.getInflightMove().forEach((k, v) -> {
      Set<ContainerReplica> replicas;
      ContainerInfo cif;
      try {
        replicas = containerManager.getContainerReplicas(k);
        cif = containerManager.getContainer(k);
      } catch (ContainerNotFoundException e) {
        needToRemove.add(k.getProtobuf());
        LOG.error("can not find container {} " +
            "while processing replicated move", k);
        return;
      }
      boolean isSrcExist = replicas.stream()
          .anyMatch(r -> r.getDatanodeDetails().equals(v.getSrc()));
      boolean isTgtExist = replicas.stream()
          .anyMatch(r -> r.getDatanodeDetails().equals(v.getTgt()));

      if (isSrcExist) {
        if (isTgtExist) {
          //the former scm leader may or may not send the deletion command
          //before reelection.here, we just try to send the command again.
          inflightActionManager.deleteSrcDnForMove(cif, replicas);
        } else {
          // resenting replication command is ok , no matter whether there is an
          // on-going replication
          inflightActionManager.sendReplicateCommand(cif, v.getTgt(),
              Collections.singletonList(v.getSrc()));
        }
      } else {
        // if container does not exist in src datanode, no matter it exists
        // in target datanode, we can not take more actions to this option,
        // so just remove it through ratis
        needToRemove.add(k.getProtobuf());
      }
    });

    needToRemove.forEach(moveScheduler::completeMove);
  }
}

