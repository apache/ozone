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
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
  private final NodeManager nodeManager;
  private final long currentContainerSize;
  private final ReplicationManager replicationManager;

  public RatisUnderReplicationHandler(final PlacementPolicy placementPolicy,
      final ConfigurationSource conf, final NodeManager nodeManager,
      final ReplicationManager replicationManager) {
    this.placementPolicy = placementPolicy;
    this.currentContainerSize = (long) conf
        .getStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
            ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    this.nodeManager = nodeManager;
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
   *
   * @return Returns the key value pair of destination dn where the command gets
   * executed and the command itself. If an empty map is returned, it indicates
   * the container is no longer unhealthy and can be removed from the unhealthy
   * queue. Any exception indicates that the container is still unhealthy and
   * should be retried later.
   */
  @Override
  public Map<DatanodeDetails, SCMCommand<?>> processAndCreateCommands(
      Set<ContainerReplica> replicas, List<ContainerReplicaOp> pendingOps,
      ContainerHealthResult result, int minHealthyForMaintenance)
      throws IOException {
    ContainerInfo containerInfo = result.getContainerInfo();
    LOG.debug("Handling under replicated Ratis container {}", containerInfo);

    // count pending adds and deletes
    int pendingAdd = 0, pendingDelete = 0;
    for (ContainerReplicaOp op : pendingOps) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.ADD) {
        pendingAdd++;
      } else if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
        pendingDelete++;
      }
    }
    RatisContainerReplicaCount replicaCount =
        new RatisContainerReplicaCount(containerInfo, replicas, pendingAdd,
            pendingDelete, containerInfo.getReplicationFactor().getNumber(),
            minHealthyForMaintenance);

    // verify that this container is still under replicated and we don't have
    // sufficient replication after considering pending adds
    if (!verifyUnderReplication(replicaCount)) {
      return Collections.emptyMap();
    }

    // find sources that can provide replicas
    List<DatanodeDetails> sourceDatanodes =
        getSources(replicaCount, pendingOps);
    if (sourceDatanodes.isEmpty()) {
      LOG.warn("Cannot replicate container {} because no healthy replicas " +
          "were found.", containerInfo);
      return Collections.emptyMap();
    }

    // find targets to send replicas to
    List<DatanodeDetails> targetDatanodes =
        getTargets(replicaCount, pendingOps);
    if (targetDatanodes.isEmpty()) {
      LOG.warn("Cannot replicate container {} because no eligible targets " +
          "were found.", containerInfo);
      return Collections.emptyMap();
    }

    return createReplicationCommands(containerInfo.getContainerID(),
        sourceDatanodes, targetDatanodes);
  }

  /**
   * Verify that this container is under replicated, even after considering
   * pending adds. Note that the container might be under replicated but
   * unrecoverable (no replicas), in which case this returns false.
   *
   * @param replicaCount RatisContainerReplicaCount object to check
   * @return true if the container is under replicated, false if the
   * container is sufficiently replicated or unrecoverable.
   */
  private boolean verifyUnderReplication(
      RatisContainerReplicaCount replicaCount) {
    if (replicaCount.isSufficientlyReplicated()) {
      LOG.info("The container {} state changed and it's not under " +
          "replicated any more.", replicaCount.getContainer().containerID());
      return false;
    }
    if (replicaCount.isSufficientlyReplicated(true)) {
      LOG.info("Container {} with replicas {} will be sufficiently " +
              "replicated after pending replicas are created.",
          replicaCount.getContainer().getContainerID(),
          replicaCount.getReplicas());
      return false;
    }
    if (replicaCount.getReplicas().isEmpty()) {
      LOG.warn("Container {} does not have any replicas and is unrecoverable" +
          ".", replicaCount.getContainer());
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
   * and are not pending replica deletion. Sorted in descending order of
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

    /*
     * Return healthy datanodes that have closed/quasi-closed replicas and
     * are not pending replica deletion. Sorted in descending order of
     * sequence id.
     */
    return replicaCount.getReplicas().stream()
        .filter(r -> r.getState() == State.QUASI_CLOSED ||
            r.getState() == State.CLOSED)
        .filter(r -> ReplicationManager.getNodeStatus(r.getDatanodeDetails(),
            nodeManager).isHealthy())
        .filter(r -> !pendingDeletion.contains(r.getDatanodeDetails()))
        .sorted((r1, r2) -> r2.getSequenceId().compareTo(r1.getSequenceId()))
        .map(ContainerReplica::getDatanodeDetails)
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

    /*
    Ensure that target datanodes have enough space to hold a complete
    container.
    */
    final long dataSizeRequired =
        Math.max(replicaCount.getContainer().getUsedBytes(),
            currentContainerSize);
    return placementPolicy.chooseDatanodes(excludeList, null,
        replicaCount.additionalReplicaNeeded(), 0, dataSizeRequired);
  }

  private Map<DatanodeDetails, SCMCommand<?>> createReplicationCommands(
      long containerID, List<DatanodeDetails> sources,
      List<DatanodeDetails> targets) {
    final boolean push = replicationManager.getConfig().isPush()
        && targets.size() <= sources.size();
    // TODO if we need multiple commands per source datanode, we need a small
    // interface change
    Map<DatanodeDetails, SCMCommand<?>> commands = new HashMap<>();

    if (push) {
      Collections.shuffle(sources);
      for (Iterator<DatanodeDetails> srcIter = sources.iterator(),
              targetIter = targets.iterator();
          srcIter.hasNext() && targetIter.hasNext();) {
        DatanodeDetails source = srcIter.next();
        DatanodeDetails target = targetIter.next();
        ReplicateContainerCommand command =
            ReplicateContainerCommand.toTarget(containerID, target);
        commands.put(source, command);
      }
    } else {
      for (DatanodeDetails target : targets) {
        ReplicateContainerCommand command =
            ReplicateContainerCommand.fromSources(containerID, sources);
        commands.put(target, command);
      }
    }

    return commands;
  }
}
