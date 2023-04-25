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
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.InsufficientDatanodesException;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class handles Ratis containers that are under replicated. It should
 * be used to obtain SCMCommands that can be sent to datanodes to solve
 * under replication.
 */
public class RatisUnderReplicationHandler
    implements UnhealthyReplicationHandler {
  public static final Logger LOG =
      LoggerFactory.getLogger(RatisUnderReplicationHandler.class);
  private final PlacementPolicy placementPolicy;
  private final long currentContainerSize;
  private final ReplicationManager replicationManager;

  public RatisUnderReplicationHandler(final PlacementPolicy placementPolicy,
      final ConfigurationSource conf,
      final ReplicationManager replicationManager) {
    this.placementPolicy = placementPolicy;
    this.currentContainerSize = (long) conf
        .getStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
            ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    this.replicationManager = replicationManager;
  }

  /**
   * Identifies new set of datanodes as targets for container replication.
   * Forms the SCMCommands to be sent to these datanodes.
   *
   * @param replicas Set of container replicas.
   * @param pendingOps Pending ContainerReplicaOp including adds and deletes
   *                   for this container.
   * @param result Health check result indicating under replication.
   * @param minHealthyForMaintenance Number of healthy replicas that must be
   *                                 available for a DN to enter maintenance
   * @return The number of commands sent.
   */
  @Override
  public int processAndSendCommands(
      Set<ContainerReplica> replicas, List<ContainerReplicaOp> pendingOps,
      ContainerHealthResult result, int minHealthyForMaintenance)
      throws IOException {
    ContainerInfo containerInfo = result.getContainerInfo();
    LOG.debug("Handling under replicated Ratis container {}", containerInfo);

    RatisContainerReplicaCount withUnhealthy =
        new RatisContainerReplicaCount(containerInfo, replicas, pendingOps,
            minHealthyForMaintenance, true);

    RatisContainerReplicaCount withoutUnhealthy =
        new RatisContainerReplicaCount(containerInfo, replicas, pendingOps,
            minHealthyForMaintenance, false);

    // verify that this container is still under replicated and we don't have
    // sufficient replication after considering pending adds
    if (!verifyUnderReplication(withUnhealthy, withoutUnhealthy)) {
      return 0;
    }

    // find sources that can provide replicas
    List<DatanodeDetails> sourceDatanodes =
        getSources(withUnhealthy, pendingOps);
    if (sourceDatanodes.isEmpty()) {
      LOG.warn("Cannot replicate container {} because no CLOSED, QUASI_CLOSED" +
          " or UNHEALTHY replicas were found.", containerInfo);
      return 0;
    }

    // find targets to send replicas to
    List<DatanodeDetails> targetDatanodes =
        getTargets(withUnhealthy, pendingOps);

    int commandsSent = sendReplicationCommands(
        containerInfo, sourceDatanodes, targetDatanodes);

    if (targetDatanodes.size() < withUnhealthy.additionalReplicaNeeded()) {
      // The placement policy failed to find enough targets to satisfy fix
      // the under replication. There fore even though some commands were sent,
      // we throw an exception to indicate that the container is still under
      // replicated and should be re-queued for another attempt later.
      LOG.debug("Placement policy failed to find enough targets to satisfy " +
          "under replication for container {}. Targets found: {}, " +
          "additional replicas needed: {}",
          containerInfo, targetDatanodes.size(),
          withUnhealthy.additionalReplicaNeeded());
      throw new InsufficientDatanodesException(
          withUnhealthy.additionalReplicaNeeded(), targetDatanodes.size());
    }
    return commandsSent;
  }

  /**
   * Verify that this container is under replicated, even after considering
   * pending adds. Note that the container might be under replicated but
   * unrecoverable (no replicas), in which case this returns false.
   *
   * @param withUnhealthy RatisContainerReplicaCount object to check with
   * considerHealthy flag true
   * @param withoutUnhealthy RatisContainerReplicaCount object to check with
   * considerHealthy flag false
   * @return true if the container is under replicated, false if the
   * container is sufficiently replicated or unrecoverable.
   */
  private boolean verifyUnderReplication(
      RatisContainerReplicaCount withUnhealthy,
      RatisContainerReplicaCount withoutUnhealthy) {
    if (withoutUnhealthy.isSufficientlyReplicated()) {
      LOG.info("The container {} state changed and it's not under " +
          "replicated any more.", withUnhealthy.getContainer().containerID());
      return false;
    }
    if (withoutUnhealthy.isSufficientlyReplicated(true)) {
      LOG.info("Container {} with replicas {} will be sufficiently " +
              "replicated after pending replicas are created.",
          withoutUnhealthy.getContainer().getContainerID(),
          withoutUnhealthy.getReplicas());
      return false;
    }
    if (withUnhealthy.getReplicas().isEmpty()) {
      LOG.warn("Container {} does not have any replicas and is unrecoverable" +
          ".", withUnhealthy.getContainer());
      return false;
    }
    if (withUnhealthy.isSufficientlyReplicated(true) &&
        withUnhealthy.getHealthyReplicaCount() == 0) {
      LOG.info("Container {} with only UNHEALTHY replicas [{}] will be " +
              "sufficiently replicated after pending adds are created.",
          withUnhealthy.getContainer(), withUnhealthy.getReplicas());
      return false;
    }
    return true;
  }

  /**
   * Returns a list of datanodes that can be used as sources for replication
   * for the container specified in replicaCount.
   *
   * @param replicaCount RatisContainerReplicaCount object for this container
   * @param pendingOps List of pending ContainerReplicaOp
   * @return List of healthy datanodes that have closed/quasi-closed replicas
   * (or UNHEALTHY replicas if they're the only ones available) and are not
   * pending replica deletion. Sorted in descending order of
   * sequence id.
   */
  private List<DatanodeDetails> getSources(
      RatisContainerReplicaCount replicaCount,
      List<ContainerReplicaOp> pendingOps) {
    Set<DatanodeDetails> pendingDeletion = new HashSet<>();
    // collect the DNs that are going to have their container replica deleted
    for (ContainerReplicaOp op : pendingOps) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
        pendingDeletion.add(op.getTarget());
      }
    }

    Predicate<ContainerReplica> predicate;
    if (replicaCount.getHealthyReplicaCount() == 0) {
      predicate = replica -> replica.getState() == State.UNHEALTHY;
    } else {
      predicate = replica -> replica.getState() == State.CLOSED ||
          replica.getState() == State.QUASI_CLOSED;
    }

    /*
     * Return healthy datanodes which have a replica that satisfies the
     * predicate and is not pending replica deletion
     */
    List<ContainerReplica> availableSources = replicaCount.getReplicas()
        .stream()
        .filter(predicate)
        .filter(r -> {
          try {
            return replicationManager.getNodeStatus(r.getDatanodeDetails())
                .isHealthy();
          } catch (NodeNotFoundException e) {
            return false;
          }
        })
        .filter(r -> !pendingDeletion.contains(r.getDatanodeDetails()))
        .collect(Collectors.toList());

    // We should replicate only the max available sequence ID, as replicas with
    // earlier sequence IDs may be stale copies.
    // First we get the max sequence ID, if there is at least one replica with
    // a non-null sequence.
    OptionalLong maxSequenceId = availableSources.stream()
        .filter(r -> r.getSequenceId() != null)
        .mapToLong(ContainerReplica::getSequenceId)
        .max();

    // Filter out all but the max sequence ID, or keep all if there is no
    // max.
    Stream<ContainerReplica> replicaStream = availableSources.stream();
    if (maxSequenceId.isPresent()) {
      replicaStream = replicaStream
          .filter(r -> r.getSequenceId() != null)
          .filter(r -> r.getSequenceId() == maxSequenceId.getAsLong());
    }
    return replicaStream.map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList());
  }

  private List<DatanodeDetails> getTargets(
      RatisContainerReplicaCount replicaCount,
      List<ContainerReplicaOp> pendingOps) throws IOException {
    // DNs that already have replicas cannot be targets and should be excluded
    final List<DatanodeDetails> excludeList =
        replicaCount.getReplicas().stream()
            .map(ContainerReplica::getDatanodeDetails)
            .collect(Collectors.toList());

    // DNs that are already waiting to receive replicas cannot be targets
    final List<DatanodeDetails> pendingReplication =
        pendingOps.stream()
            .filter(containerReplicaOp -> containerReplicaOp.getOpType() ==
                ContainerReplicaOp.PendingOpType.ADD)
            .map(ContainerReplicaOp::getTarget)
            .collect(Collectors.toList());
    excludeList.addAll(pendingReplication);

    return ReplicationManagerUtil.getTargetDatanodes(placementPolicy,
        replicaCount.additionalReplicaNeeded(), null, excludeList,
        currentContainerSize, replicaCount.getContainer());
  }

  private int sendReplicationCommands(
      ContainerInfo containerInfo, List<DatanodeDetails> sources,
      List<DatanodeDetails> targets) throws CommandTargetOverloadedException,
      NotLeaderException {
    final boolean push = replicationManager.getConfig().isPush();
    int commandsSent = 0;

    if (push) {
      for (DatanodeDetails target : targets) {
        replicationManager.sendThrottledReplicationCommand(
            containerInfo, sources, target, 0);
        commandsSent++;
      }
    } else {
      for (DatanodeDetails target : targets) {
        ReplicateContainerCommand command =
            ReplicateContainerCommand.fromSources(
                containerInfo.getContainerID(), sources);
        replicationManager.sendDatanodeCommand(command, containerInfo, target);
        commandsSent++;
      }
    }
    return commandsSent;
  }
}
