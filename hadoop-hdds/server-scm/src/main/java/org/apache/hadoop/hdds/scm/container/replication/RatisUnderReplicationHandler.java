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

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.InsufficientDatanodesException;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles Ratis containers that are under replicated. It should
 * be used to obtain SCMCommands that can be sent to datanodes to solve
 * under replication.
 */
public class RatisUnderReplicationHandler
    implements UnhealthyReplicationHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(RatisUnderReplicationHandler.class);
  private final PlacementPolicy placementPolicy;
  private final long currentContainerSize;
  private final ReplicationManager replicationManager;
  private final ReplicationManagerMetrics metrics;

  public RatisUnderReplicationHandler(final PlacementPolicy placementPolicy,
      final ConfigurationSource conf,
      final ReplicationManager replicationManager) {
    this.placementPolicy = placementPolicy;
    this.currentContainerSize = (long) conf
        .getStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
            ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    this.replicationManager = replicationManager;
    this.metrics = replicationManager.getMetrics();
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

    if (result instanceof ContainerHealthResult.UnderReplicatedHealthResult) {
      ContainerHealthResult.UnderReplicatedHealthResult
          underReplicatedResult = (ContainerHealthResult.UnderReplicatedHealthResult) result;
      if (underReplicatedResult.hasVulnerableUnhealthy()) {
        return handleVulnerableUnhealthyReplicas(withUnhealthy, pendingOps);
      }
    }

    // verify that this container is still under replicated and we don't have
    // sufficient replication after considering pending adds
    RatisContainerReplicaCount replicaCount =
        verifyUnderReplication(withUnhealthy, withoutUnhealthy);
    if (replicaCount == null) {
      return 0;
    }

    // find sources that can provide replicas
    List<DatanodeDetails> sourceDatanodes =
        getSources(replicaCount, pendingOps);
    if (sourceDatanodes.isEmpty()) {
      LOG.warn("Cannot replicate container {} because no CLOSED, QUASI_CLOSED" +
          " or UNHEALTHY replicas were found.", containerInfo);
      return 0;
    }

    List<DatanodeDetails> targetDatanodes;
    try {
      // find targets to send replicas to
      targetDatanodes = getTargets(replicaCount, pendingOps);
    } catch (SCMException e) {
      SCMException.ResultCodes code = e.getResult();
      if (code != SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE) {
        throw e;
      }
      LOG.warn("Cannot replicate container {} because no suitable targets " +
          "were found.", containerInfo);
      removeUnhealthyReplicaIfPossible(containerInfo, replicas, pendingOps);
      // Throw the original exception so the request gets re-queued to try again
      throw e;
    }

    int commandsSent = sendReplicationCommands(
        containerInfo, sourceDatanodes, targetDatanodes);

    if (targetDatanodes.size() < replicaCount.additionalReplicaNeeded()) {
      // The placement policy failed to find enough targets to satisfy fix
      // the under replication. There fore even though some commands were sent,
      // we throw an exception to indicate that the container is still under
      // replicated and should be re-queued for another attempt later.
      LOG.debug("Placement policy failed to find enough targets to satisfy " +
          "under replication for container {}. Targets found: {}, " +
          "additional replicas needed: {}",
          containerInfo, targetDatanodes.size(),
          replicaCount.additionalReplicaNeeded());
      metrics.incrPartialReplicationTotal();
      throw new InsufficientDatanodesException(
          replicaCount.additionalReplicaNeeded(), targetDatanodes.size());
    }
    return commandsSent;
  }

  /**
   * Sends a replicate command for each replica specified in
   * vulnerableUnhealthy.
   * @param replicaCount RatisContainerReplicaCount for this container
   * @param pendingOps List of pending ops
   * @return number of replicate commands sent
   */
  private int handleVulnerableUnhealthyReplicas(RatisContainerReplicaCount replicaCount,
      List<ContainerReplicaOp> pendingOps) throws NotLeaderException, CommandTargetOverloadedException, SCMException {
    ContainerInfo container = replicaCount.getContainer();
    List<ContainerReplica> vulnerableUnhealthy = replicaCount.getVulnerableUnhealthyReplicas(dn -> {
      try {
        return replicationManager.getNodeStatus(dn);
      } catch (NodeNotFoundException e) {
        LOG.warn("Exception for datanode {} while handling vulnerable replicas for container {}, with all replicas" +
            " {}.", dn, container, replicaCount.getReplicas(), e);
        return null;
      }
    });
    LOG.info("Handling vulnerable UNHEALTHY replicas {} for container {}.", vulnerableUnhealthy, container);

    int pendingAdds = 0;
    for (ContainerReplicaOp op : pendingOps) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.ADD) {
        pendingAdds++;
      }
    }
    if (pendingAdds >= vulnerableUnhealthy.size()) {
      LOG.debug("There are {} pending adds for container {}, while the number of UNHEALTHY replicas is {}.",
          pendingAdds, container.containerID(), vulnerableUnhealthy.size());
      return 0;
    }

    /*
    Since we're replicating UNHEALTHY replicas, it's possible that replication keeps on failing. Shuffling gives
    other replicas a chance to be replicated since there's a limit on in-flight adds.
    */
    Collections.shuffle(vulnerableUnhealthy);
    return replicateEachSource(replicaCount, vulnerableUnhealthy, pendingOps);
  }

  /**
   * Replicates each of the ContainerReplica specified in sources to new
   * Datanodes. Will not consider Datanodes hosting existing replicas and
   * Datanodes pending adds as targets. Note that this method simply skips
   * a replica if its datanode is overloaded with commands, throwing an
   * exception once all sources have been looked at.
   * @param replicaCount RatisContainerReplicaCount for this container
   * @param sources List containing replicas, each will be replicated
   */
  private int replicateEachSource(RatisContainerReplicaCount replicaCount, List<ContainerReplica> sources,
      List<ContainerReplicaOp> pendingOps) throws NotLeaderException, SCMException, CommandTargetOverloadedException {
    List<ContainerReplica> allReplicas = replicaCount.getReplicas();
    ContainerInfo container = replicaCount.getContainer();

    /*
    We use the placement policy to get a target Datanode to which a vulnerable replica will be replicated. In
    placement policy terms, a 'used node' is a Datanode which has a legit replica of this container. An 'excluded
    node' is a Datanode that should not be considered to host a replica of this container, but other Datanodes in this
    Datanode's rack are available. So, Datanodes of any vulnerable replicas should be excluded nodes while Datanodes
    of other replicas, including UNHEALTHY replicas that are not pending delete (because they have unique origin),
    should be used nodes.
    */
    ReplicationManagerUtil.ExcludedAndUsedNodes excludedAndUsedNodes =
        ReplicationManagerUtil.getExcludedAndUsedNodes(container, allReplicas, Collections.emptySet(), pendingOps,
            replicationManager);

    CommandTargetOverloadedException firstException = null;
    int numCommandsSent = 0;
    for (ContainerReplica replica : sources) {
      // find a target for each source and send replicate command
      final List<DatanodeDetails> target =
          ReplicationManagerUtil.getTargetDatanodes(placementPolicy, 1, excludedAndUsedNodes.getUsedNodes(),
              excludedAndUsedNodes.getExcludedNodes(), currentContainerSize, container);
      int count = 0;
      try {
        count = sendReplicationCommands(container, ImmutableList.of(replica.getDatanodeDetails()), target);
      } catch (CommandTargetOverloadedException e) {
        LOG.info("Exception while replicating {} to target {} for container {}.", replica, target, container, e);
        if (firstException == null) {
          firstException = e;
        }
      }

      if (count == 1) {
        // a command was sent to target, so it needs to be in the used nodes list because it's pending an add
        excludedAndUsedNodes.getUsedNodes().add(target.get(0));
      }
      numCommandsSent += count;
    }

    if (firstException != null) {
      throw firstException;
    }

    return numCommandsSent;
  }

  private void removeUnhealthyReplicaIfPossible(ContainerInfo containerInfo,
      Set<ContainerReplica> replicas, List<ContainerReplicaOp> pendingOps)
      throws NotLeaderException {
    LOG.info("Finding an unhealthy replica to delete for container {} with " +
        "replicas {} to unblock under replication handling.", containerInfo,
        replicas);
    int pendingDeletes = 0;
    for (ContainerReplicaOp op : pendingOps) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
        pendingDeletes++;
      }
    }
    ContainerReplica deleteCandidate = ReplicationManagerUtil
        .selectUnhealthyReplicaForDelete(containerInfo, replicas,
            pendingDeletes,
            (dnd) -> {
              try {
                return replicationManager.getNodeStatus(dnd);
              } catch (NodeNotFoundException e) {
                LOG.warn("Exception while finding an unhealthy replica to " +
                    "delete for container {} with replicas {}.", containerInfo,
                    replicas, e);
                return null;
              }
            });

    if (deleteCandidate != null) {
      replicationManager.sendDeleteCommand(containerInfo,
          deleteCandidate.getReplicaIndex(),
          deleteCandidate.getDatanodeDetails(), true);
    } else {
      LOG.info("Unable to find a replica to remove for container {} with " +
          "replicas {}", containerInfo, replicas);
    }
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
   * @return null if the container is sufficiently replicated or
   * unrecoverable, otherwise returns the correct RatisContainerReplicaCount
   * object to be used to fix under replication.
   */
  private RatisContainerReplicaCount verifyUnderReplication(
      RatisContainerReplicaCount withUnhealthy,
      RatisContainerReplicaCount withoutUnhealthy) {
    if (withoutUnhealthy.isSufficientlyReplicated()) {
      LOG.info("The container {} state changed and it's not under " +
          "replicated any more.",
          withoutUnhealthy.getContainer().containerID());
      return null;
    }
    if (withoutUnhealthy.isSufficientlyReplicated(true)) {
      LOG.info("Container {} with replicas {} will be sufficiently " +
              "replicated after pending replicas are created.",
          withoutUnhealthy.getContainer().getContainerID(),
          withoutUnhealthy.getReplicas());
      return null;
    }
    if (withUnhealthy.getReplicas().isEmpty()) {
      LOG.warn("Container {} does not have any replicas and is unrecoverable" +
          ".", withUnhealthy.getContainer());
      return null;
    }
    if (withUnhealthy.isSufficientlyReplicated(true) &&
        withUnhealthy.getHealthyReplicaCount() == 0) {
      LOG.info("Container {} with only UNHEALTHY replicas [{}] will be " +
              "sufficiently replicated after pending adds are created.",
          withUnhealthy.getContainer(), withUnhealthy.getReplicas());
      return null;
    }

    /*
    If we reach here, the container is under replicated. If we have any
    healthy replicas, this means we want to solve under replication by
    considering how many more healthy replicas we need, and then replicating
    the healthy replicas. If we have only unhealthy replicas, we need to solve
    under replication by replicating them.
     */
    if (withoutUnhealthy.getHealthyReplicaCount() > 0) {
      return withoutUnhealthy;
    } else {
      return withUnhealthy;
    }
  }

  /**
   * Returns a list of datanodes that can be used as sources for replication
   * for the container specified in replicaCount.
   *
   * @param replicaCount RatisContainerReplicaCount object for this container
   * @param pendingOps List of pending ContainerReplicaOp
   * @return List of healthy datanodes that have closed/quasi-closed replicas
   * (or UNHEALTHY replicas if they're the only ones available) and are not
   * pending replica deletion. If there is a maximum sequence ID, then only
   * replicas with that sequence ID are returned.
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

    Predicate<ContainerReplica> predicate =
        replica -> replica.getState() == State.CLOSED;

    /*
    If no CLOSED replicas are available, or if the container itself is
    QUASI_CLOSED, then QUASI_CLOSED replicas are allowed to be sources.
    */
    boolean hasClosedReplica = false;
    for (ContainerReplica replica : replicaCount.getReplicas()) {
      if (replica.getState() == State.CLOSED) {
        hasClosedReplica = true;
        break;
      }
    }
    if (!hasClosedReplica ||
        replicaCount.getContainer().getState() == LifeCycleState.QUASI_CLOSED) {
      predicate =
          predicate.or(replica -> replica.getState() == State.QUASI_CLOSED);
    }

    if (replicaCount.getHealthyReplicaCount() == 0) {
      predicate = predicate.or(
          replica -> replica.getState() == State.UNHEALTHY);
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
    LOG.debug("Need {} target datanodes for container {}. Current " +
            "replicas: {}.", replicaCount.additionalReplicaNeeded(),
        replicaCount.getContainer().containerID(), replicaCount.getReplicas());

    ReplicationManagerUtil.ExcludedAndUsedNodes excludedAndUsedNodes =
        ReplicationManagerUtil.getExcludedAndUsedNodes(replicaCount.getContainer(),
            replicaCount.getReplicas(), Collections.emptySet(), pendingOps,
            replicationManager);

    List<DatanodeDetails> excluded = excludedAndUsedNodes.getExcludedNodes();
    List<DatanodeDetails> used = excludedAndUsedNodes.getUsedNodes();

    LOG.debug("UsedList: {}, size {}. ExcludeList: {}, size: {}. ",
        used, used.size(), excluded, excluded.size());

    return ReplicationManagerUtil.getTargetDatanodes(placementPolicy,
        replicaCount.additionalReplicaNeeded(), used, excluded,
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
