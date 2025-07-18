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

package org.apache.hadoop.hdds.scm.container.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles Ratis containers that are over replicated. It should
 * be used to obtain SCMCommands that can be sent to datanodes to solve
 * over replication.
 */
public class RatisOverReplicationHandler
    extends AbstractOverReplicationHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(RatisOverReplicationHandler.class);

  private final ReplicationManager replicationManager;

  public RatisOverReplicationHandler(PlacementPolicy placementPolicy,
      ReplicationManager replicationManager) {
    super(placementPolicy);
    this.replicationManager = replicationManager;
  }

  /**
   * Identifies datanodes where the specified container's replicas can be
   * deleted. Creates the SCMCommands to be sent to datanodes.
   *
   * @param replicas                 Set of container replicas.
   * @param pendingOps               Pending (in flight) replications or
   *                                 deletions for this
   *                                 container.
   * @param result                   Health check result indicating over
   *                                 replication
   * @param minHealthyForMaintenance Number of healthy replicas that must be
   *                                 available for a DN to enter maintenance
   * @return The number of commands sent.
   */
  @Override
  public int processAndSendCommands(
      Set<ContainerReplica> replicas, List<ContainerReplicaOp> pendingOps,
      ContainerHealthResult result, int minHealthyForMaintenance) throws
      IOException {
    ContainerInfo containerInfo = result.getContainerInfo();
    LOG.debug("Handling container {}.", containerInfo);

    // We are going to check for over replication, so we should filter out any
    // replicas that are not in a HEALTHY state. This is because a replica can
    // be healthy, stale or dead. If it is dead it will be quickly removed from
    // scm. If it is stale, there is a good chance the DN is offline and the
    // replica will go away soon. So, if we have a container that is over
    // replicated with a HEALTHY and STALE replica, and we decide to delete the
    // HEALTHY one, and then the STALE ones goes away, we will lose them both.
    // To avoid this, we will filter out any non-healthy replicas first.
    Set<ContainerReplica> healthyReplicas = replicas.stream()
        .filter(r -> {
          try {
            return replicationManager.getNodeStatus(r.getDatanodeDetails())
                .isHealthy();
          } catch (NodeNotFoundException e) {
            return false;
          }
        })
        .collect(Collectors.toSet());

    RatisContainerReplicaCount replicaCount =
        new RatisContainerReplicaCount(containerInfo, healthyReplicas,
            pendingOps, minHealthyForMaintenance, true);

    // verify that this container is actually over replicated
    if (!verifyOverReplication(replicaCount)) {
      return 0;
    }

    // count pending deletes
    int pendingDelete = 0;
    for (ContainerReplicaOp op : pendingOps) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
        pendingDelete++;
      }
    }
    LOG.info("Container {} is over replicated. Actual replica count is {}, " +
            "with {} pending delete(s). Expected replica count is {}.",
        containerInfo.containerID(),
        replicaCount.getReplicas().size(), pendingDelete,
        replicaCount.getReplicationFactor());

    // get replicas that can be deleted, in sorted order
    List<ContainerReplica> eligibleReplicas =
        getEligibleReplicas(replicaCount, pendingOps);
    if (eligibleReplicas.isEmpty()) {
      LOG.info("Did not find any replicas that are eligible to be deleted for" +
          " container {}.", containerInfo);
      return 0;
    }

    // get number of excess replicas
    int excess = replicaCount.getExcessRedundancy(true);

    return createCommands(containerInfo, eligibleReplicas, excess);
  }

  private boolean verifyOverReplication(
      RatisContainerReplicaCount replicaCount) {
    if (!replicaCount.isOverReplicated()) {
      LOG.info("Container {} is actually not over-replicated any more.",
          replicaCount.getContainer().containerID());
      return false;
    }
    return true;
  }

  /**
   * Finds replicas that are eligible to be deleted, sorted to avoid
   * potential data loss.
   * @see
   * <a href="https://issues.apache.org/jira/browse/HDDS-4589">HDDS-4589</a>
   * @param replicaCount ContainerReplicaCount object for the container
   * @param pendingOps Pending adds and deletes
   * @return List of ContainerReplica sorted using
   * {@link RatisOverReplicationHandler#sortReplicas(Collection, boolean)}
   */
  private List<ContainerReplica> getEligibleReplicas(
      RatisContainerReplicaCount replicaCount,
      List<ContainerReplicaOp> pendingOps) {
    // sort replicas so that they can be selected in a deterministic way
    List<ContainerReplica> eligibleReplicas =
        sortReplicas(replicaCount.getReplicas(),
            replicaCount.getHealthyReplicaCount() == 0);

    Set<DatanodeDetails> pendingDeletion = new HashSet<>();
    // collect the DNs that are going to have their container replica deleted
    for (ContainerReplicaOp op : pendingOps) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
        pendingDeletion.add(op.getTarget());
      }
    }

    // replicas that are not on IN_SERVICE nodes or are already pending
    // delete are not eligible
    eligibleReplicas.removeIf(
        replica -> replica.getDatanodeDetails().getPersistedOpState() !=
            HddsProtos.NodeOperationalState.IN_SERVICE ||
            pendingDeletion.contains(replica.getDatanodeDetails()));

    // retain one replica per unique origin datanode if the container is not
    // closed
    if (replicaCount.getContainer().getState() !=
        HddsProtos.LifeCycleState.CLOSED) {
      saveReplicasWithUniqueOrigins(replicaCount.getContainer(),
          eligibleReplicas);
    }

    return eligibleReplicas;
  }

  /**
   * This method will remove the replicas that need to be saved from the
   * specified list, so the remaining replicas are eligible to be deleted.
   * Removes one replica per unique origin node UUID. Prefers saving healthy
   * replicas over UNHEALTHY ones. Maintains order of the specified replicas
   * list.
   * @param eligibleReplicas List of replicas that are eligible to be deleted
   * and from which replicas with unique origin node ID need to be saved
   */
  private void saveReplicasWithUniqueOrigins(ContainerInfo container,
      List<ContainerReplica> eligibleReplicas) {
    List<ContainerReplica> nonUniqueDeleteCandidates =
        ReplicationManagerUtil.findNonUniqueDeleteCandidates(
            new HashSet<>(eligibleReplicas),
            eligibleReplicas, (dnd) -> {
              try {
                return replicationManager.getNodeStatus(dnd);
              } catch (NodeNotFoundException e) {
                LOG.warn(
                    "Exception while finding excess unhealthy replicas to " +
                        "delete for container {} with eligible replicas {}.",
                    container, eligibleReplicas, e);
                return null;
              }
            });

    // note that this preserves order of the List
    eligibleReplicas.removeIf(
        replica -> !nonUniqueDeleteCandidates.contains(replica));
  }

  /**
   * Sorts replicas using {@link ContainerReplica#hashCode()} (ContainerID and
   * DatanodeDetails). If allUnhealthy is true, sorts using sequence ID.
   * @param replicas replicas to sort
   * @param allUnhealthy should be true if all replicas are UNHEALTHY
   * @return sorted List
   */
  private List<ContainerReplica> sortReplicas(
      Collection<ContainerReplica> replicas, boolean allUnhealthy) {
    if (allUnhealthy) {
      // prefer deleting replicas with lower sequence IDs
      return replicas.stream()
          .sorted(Comparator.comparingLong(ContainerReplica::getSequenceId)
              .thenComparing(ContainerReplica::hashCode))
          .collect(Collectors.toList());
    }

    return replicas.stream()
        .sorted(Comparator.comparingLong(ContainerReplica::hashCode))
        .collect(Collectors.toList());
  }

  private int createCommands(
      ContainerInfo containerInfo, List<ContainerReplica> replicas,
      int excess) throws NotLeaderException, CommandTargetOverloadedException {

    /*
    Being in the over replication queue means we have enough replicas that
    match the container's state, so unhealthy or mismatched replicas can be
    deleted. This might make the container violate placement policy.
     */
    int commandsSent = 0;
    int initialExcess = excess;
    CommandTargetOverloadedException firstOverloadedException = null;
    List<ContainerReplica> replicasRemoved = new ArrayList<>();
    for (ContainerReplica replica : replicas) {
      if (excess == 0) {
        break;
      }
      if (!ReplicationManager.compareState(
          containerInfo.getState(), replica.getState())) {
        // Delete commands are throttled, so they may fail to send. However, the
        // replicas here are not in the same state as the container, so they
        // must be deleted in preference to "healthy" replicas later. Therefore,
        // if they fail to delete, we continue to mark them as deleted by
        // reducing the excess so healthy container are not removed later in
        // this method.
        try {
          replicationManager.sendThrottledDeleteCommand(containerInfo,
              replica.getReplicaIndex(), replica.getDatanodeDetails(), true);
          commandsSent++;
        } catch (CommandTargetOverloadedException e) {
          LOG.debug("Unable to send delete command for a mis-matched state " +
              "container {} to {} as it has too many pending delete commands",
              containerInfo.containerID(), replica.getDatanodeDetails());
          firstOverloadedException = e;
        }
        replicasRemoved.add(replica);
        excess--;
      }
    }
    replicas.removeAll(replicasRemoved);

    /*
    Remove excess replicas if that does not make the container mis replicated.
    If the container was already mis replicated, then remove replicas if that
    does not change the placement count.
     */
    Set<ContainerReplica> replicaSet = new HashSet<>(replicas);
    // iterate through replicas in deterministic order
    ContainerPlacementStatus originalPlacementStatus = getPlacementStatus(replicaSet,
        containerInfo.getReplicationFactor().getNumber());
    for (ContainerReplica replica : replicas) {
      if (excess == 0) {
        break;
      }
      if (super.isPlacementStatusActuallyEqualAfterRemove(originalPlacementStatus, replicaSet, replica,
          containerInfo.getReplicationFactor().getNumber())) {
        try {
          replicationManager.sendThrottledDeleteCommand(containerInfo,
              replica.getReplicaIndex(), replica.getDatanodeDetails(), true);
          commandsSent++;
        } catch (CommandTargetOverloadedException e) {
          LOG.debug("Unable to send delete command for container {} to {} as " +
              "it has too many pending delete commands",
              containerInfo.containerID(), replica.getDatanodeDetails());
          if (firstOverloadedException == null) {
            firstOverloadedException = e;
          }
        }
        // Even if the command fails to send, we still mark the replica as if the command was sent to ensure a
        // deterministic selection order. Then we adjust the replicaSet so it appears as if this replica was deleted
        // to allow subsequent placement checks to be accurate.
        excess--;
        replicaSet.remove(replica);
      }
    }
    // If we encountered an overloaded exception, and then did not send as many
    // delete commands as the original excess number, then it means there must
    // be some replicas we did not delete when we should have. In this case,
    // throw the exception so that container is requeued and processed again
    // later.
    if (firstOverloadedException != null && commandsSent != initialExcess) {
      throw firstOverloadedException;
    }
    return commandsSent;
  }

}
