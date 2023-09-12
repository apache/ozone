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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.compareState;

/**
 * Utility class for ReplicationManager.
 */
public final class ReplicationManagerUtil {

  private ReplicationManagerUtil() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(
      ReplicationManagerUtil.class);

  /**
   * Using the passed placement policy attempt to select a list of datanodes to
   * use as new targets. If the placement policy is unable to select enough
   * nodes, the number of nodes requested will be reduced by 1 and the placement
   * policy will be called again. This will continue until the placement policy
   * is able to select enough nodes or the number of nodes requested is reduced
   * to zero when an exception will be thrown.
   * @param policy The placement policy to use to select nodes.
   * @param requiredNodes The number of nodes required
   * @param usedNodes Any nodes already used by the container
   * @param excludedNodes Any Excluded nodes which cannot be selected
   * @param defaultContainerSize The cluster default max container size
   * @param container The container to select new replicas for
   * @return A list of up to requiredNodes datanodes to use as targets for new
   *         replicas. Note the number of nodes returned may be less than the
   *         number of nodes requested if the placement policy is unable to
   *         return enough nodes.
   * @throws SCMException If no nodes can be selected.
   */
  public static List<DatanodeDetails> getTargetDatanodes(PlacementPolicy policy,
      int requiredNodes, List<DatanodeDetails> usedNodes,
      List<DatanodeDetails> excludedNodes, long defaultContainerSize,
      ContainerInfo container) throws SCMException {

    // Ensure that target datanodes have enough space to hold a complete
    // container.
    final long dataSizeRequired =
        Math.max(container.getUsedBytes(), defaultContainerSize);

    int mutableRequiredNodes = requiredNodes;
    while (mutableRequiredNodes > 0) {
      try {
        if (usedNodes == null) {
          return policy.chooseDatanodes(excludedNodes, null,
              mutableRequiredNodes, 0, dataSizeRequired);
        } else {
          return policy.chooseDatanodes(usedNodes, excludedNodes, null,
              mutableRequiredNodes, 0, dataSizeRequired);
        }
      } catch (IOException e) {
        LOG.debug("Placement policy was not able to return {} nodes for " +
            "container {}.",
            mutableRequiredNodes, container.getContainerID(), e);
        mutableRequiredNodes--;
      }
    }
    throw new SCMException(String.format("Placement Policy: %s did not return"
            + " any nodes. Number of required Nodes %d, Datasize Required: %d",
        policy.getClass(), requiredNodes, dataSizeRequired),
        SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
  }

  /**
   * Given a list of replicas and a set of nodes to be removed, returns an
   * object container two lists. One is a list of nodes that should be excluded
   * from being selected as targets for new replicas. The other is a list of
   * nodes that are currently used by the container and the placement policy
   * can consider for rack placement
   * @param replicas List of existing replicas
   * @param toBeRemoved Set of nodes containing replicas that are to be removed
   * @param pendingReplicaOps List of pending replica operations
   * @param replicationManager ReplicationManager instance to get NodeStatus
   * @return ExcludedAndUsedNodes object containing the excluded and used lists
   */
  public static ExcludedAndUsedNodes getExcludedAndUsedNodes(
      List<ContainerReplica> replicas,
      Set<ContainerReplica> toBeRemoved,
      List<ContainerReplicaOp> pendingReplicaOps,
      ReplicationManager replicationManager) {
    List<DatanodeDetails> excludedNodes = new ArrayList<>();
    List<DatanodeDetails> usedNodes = new ArrayList<>();

    for (ContainerReplica r : replicas) {
      if (r.getState() == ContainerReplicaProto.State.UNHEALTHY) {
        // Hosts with an Unhealthy replica cannot receive a new replica, but
        // they are not considered used as they will be removed later.
        excludedNodes.add(r.getDatanodeDetails());
        continue;
      }
      if (toBeRemoved.contains(r)) {
        // This node is currently present, but we plan to remove it so it is not
        // considered used, but must be excluded
        excludedNodes.add(r.getDatanodeDetails());
        continue;
      }
      try {
        NodeStatus nodeStatus =
            replicationManager.getNodeStatus(r.getDatanodeDetails());
        if (nodeStatus.isDecommission()) {
          // Decommission nodes are going to go away and their replicas need to
          // be replaced. Therefore we mark them excluded.
          // Maintenance nodes should return to the cluster, so they would still
          // be considered used (handled in the catch all at the end of the loop
          // ).
          excludedNodes.add(r.getDatanodeDetails());
          continue;
        }
      } catch (NodeNotFoundException e) {
        LOG.warn("Node {} not found in node manager.", r.getDatanodeDetails());
        // This should not happen, but if it does, just add the node to the
        // exclude list
        excludedNodes.add(r.getDatanodeDetails());
        continue;
      }
      // If we get here, this is a used node
      usedNodes.add(r.getDatanodeDetails());
    }
    for (ContainerReplicaOp pending : pendingReplicaOps) {
      if (pending.getOpType() == ContainerReplicaOp.PendingOpType.ADD) {
        // If we are adding a replicas, then its scheduled to become a used node
        usedNodes.add(pending.getTarget());
      }
      if (pending.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
        // If there are any ops pending delete, we cannot use the node, but they
        // are not considered used as they will be removed later.
        excludedNodes.add(pending.getTarget());
      }
    }
    return new ExcludedAndUsedNodes(excludedNodes, usedNodes);
  }


  /**
   * Simple class to hold the excluded and used nodes lists.
   */
  public static class ExcludedAndUsedNodes {
    private final List<DatanodeDetails> excludedNodes;
    private final List<DatanodeDetails> usedNodes;

    public ExcludedAndUsedNodes(List<DatanodeDetails> excludedNodes,
        List<DatanodeDetails> usedNodes) {
      this.excludedNodes = excludedNodes;
      this.usedNodes = usedNodes;
    }

    public List<DatanodeDetails> getExcludedNodes() {
      return excludedNodes;
    }

    public List<DatanodeDetails> getUsedNodes() {
      return usedNodes;
    }
  }

  /**
   * This is intended to be call when a container is under replicated, but there
   * are no spare nodes to create new replicas on, due to having too many
   * unhealthy replicas or quasi-closed replicas which cannot be closed due to
   * having a lagging sequence ID. The logic here will select a replica to
   * delete, or return null if there are none which can be safely deleted.
   * @param containerInfo The container to select a replica to delete from
   * @param replicas The list of replicas for the container
   * @param pendingOps The list of pending replica operations for the container
   * @return A replica to delete, or null if there are none which can be safely
   *         deleted.
   */
  public static ContainerReplica selectUnhealthyReplicaForDelete(
      ContainerInfo containerInfo, Set<ContainerReplica> replicas,
      List<ContainerReplicaOp> pendingOps,
      Function<DatanodeDetails, NodeStatus> nodeStatusFn) {

    for (ContainerReplicaOp op : pendingOps) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
        // There is at least one pending delete which will free up a node.
        // Therefore we do nothing until that delete completes or times out.
        LOG.debug("Container {} has pending deletes which will free nodes",
            pendingOps);
        return null;
      }
    }

    if (replicas.size() <= 2) {
      // We never remove replicas if it will leave us with less than 2 replicas
      LOG.debug("There are only {} replicas for container {} so no more will " +
          "be deleted", replicas.size(), containerInfo);
      return null;
    }

    boolean foundMatchingReplica = false;
    for (ContainerReplica replica : replicas) {
      if (compareState(containerInfo.getState(), replica.getState())) {
        foundMatchingReplica = true;
        break;
      }
    }
    if (!foundMatchingReplica) {
      LOG.debug("No matching replica found for container {} with replicas " +
              "{}. No unhealthy replicas can be safely delete.",
          containerInfo, replicas);
      return null;
    }

    List<ContainerReplica> deleteCandidates = new ArrayList<>();
    for (ContainerReplica r : replicas) {
      NodeStatus nodeStatus = nodeStatusFn.apply(r.getDatanodeDetails());
      if (nodeStatus == null || !nodeStatus.isHealthy()
          || !nodeStatus.isInService()) {
        continue;
      }

      if (containerInfo.getState() != HddsProtos.LifeCycleState.QUASI_CLOSED &&
          r.getState() == ContainerReplicaProto.State.QUASI_CLOSED) {
        // a quasi_closed replica is a candidate only if its seq id is less
        // than the container's
        if (r.getSequenceId() < containerInfo.getSequenceId()) {
          deleteCandidates.add(r);
        }
      } else if (r.getState() == ContainerReplicaProto.State.UNHEALTHY) {
        deleteCandidates.add(r);
      }
    }

    deleteCandidates.sort(
        Comparator.comparingLong(ContainerReplica::getSequenceId));
    if (containerInfo.getState() == HddsProtos.LifeCycleState.CLOSED) {
      return deleteCandidates.size() > 0 ? deleteCandidates.get(0) : null;
    }

    if (containerInfo.getState() == HddsProtos.LifeCycleState.QUASI_CLOSED) {
      List<ContainerReplica> nonUniqueOrigins =
          findNonUniqueDeleteCandidates(replicas, deleteCandidates);
      return nonUniqueOrigins.size() > 0 ? nonUniqueOrigins.get(0) : null;
    }
    return null;
  }

  private static List<ContainerReplica> findNonUniqueDeleteCandidates(
      Set<ContainerReplica> allReplicas,
      List<ContainerReplica> deleteCandidates) {
    // Gather the origin node IDs of replicas which are not candidates for
    // deletion.
    Set<UUID> existingOriginNodeIDs = allReplicas.stream()
        .filter(r -> !deleteCandidates.contains(r))
        .map(ContainerReplica::getOriginDatanodeId)
        .collect(Collectors.toSet());

    List<ContainerReplica> nonUniqueDeleteCandidates = new ArrayList<>();
    for (ContainerReplica replica : deleteCandidates) {
      if (existingOriginNodeIDs.contains(replica.getOriginDatanodeId())) {
        nonUniqueDeleteCandidates.add(replica);
      } else {
        // Spare this replica with this new origin node ID from deletion.
        // delete candidates seen later in the loop with this same origin
        // node ID can be deleted.
        existingOriginNodeIDs.add(replica.getOriginDatanodeId());
      }
    }

    return nonUniqueDeleteCandidates;
  }

}
