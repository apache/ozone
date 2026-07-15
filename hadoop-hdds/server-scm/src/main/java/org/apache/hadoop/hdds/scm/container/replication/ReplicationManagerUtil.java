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

import static org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.compareState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for ReplicationManager.
 */
public final class ReplicationManagerUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationManagerUtil.class);

  private ReplicationManagerUtil() {
  }

  private static String formatDatanodeDetails(List<DatanodeDetails> dns) {
    if (dns == null) {
      return "[]";
    }
    return dns.stream()
        .map(dn -> String.format("%s[%s]", dn, dn.getPersistedOpState()))
        .collect(Collectors.toList()).toString();
  }

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
        HddsServerUtil.requiredReplicationSpace(Math.max(container.getUsedBytes(), defaultContainerSize));

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
            + " any nodes. Number of required Nodes %d, Data size Required: %d. Container: %s, Used Nodes %s, " +
            "Excluded Nodes: %s.", policy.getClass(), requiredNodes, dataSizeRequired, container,
        formatDatanodeDetails(usedNodes), formatDatanodeDetails(excludedNodes)),
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
      ContainerInfo container,
      List<ContainerReplica> replicas,
      Set<ContainerReplica> toBeRemoved,
      List<ContainerReplicaOp> pendingReplicaOps,
      ReplicationManager replicationManager) {
    List<DatanodeDetails> excludedNodes = new ArrayList<>();
    List<DatanodeDetails> usedNodes = new ArrayList<>();

    List<ContainerReplica> nonUniqueUnhealthy = null;
    if (container.getState() == HddsProtos.LifeCycleState.QUASI_CLOSED) {
      /*
      An UNHEALTHY replica with unique origin node id of a QUASI_CLOSED container should be a used node (not excluded
      node) because we preserve it. The following code will find non-unique UNHEALTHY replicas. Later in the method
      this list will be used to determine whether an UNHEALTHY replica's DN should be a used node or excluded node.
      */
      nonUniqueUnhealthy =
          selectUnhealthyReplicasForDelete(container, new HashSet<>(replicas), 0, dn -> {
            try {
              return replicationManager.getNodeStatus(dn);
            } catch (NodeNotFoundException e) {
              LOG.warn("Exception for {} while selecting used and excluded nodes for container {}.", dn, container);
              return null;
            }
          });
    }
    for (ContainerReplica r : replicas) {
      if (r.getState() == ContainerReplicaProto.State.UNHEALTHY) {
        if (container.getState() == HddsProtos.LifeCycleState.QUASI_CLOSED) {
          // any unique UNHEALTHY will get added as used nodes in the catch-all at the end of the loop
          if (nonUniqueUnhealthy != null && nonUniqueUnhealthy.contains(r)) {
            excludedNodes.add(r.getDatanodeDetails());
            continue;
          }
        } else {
          // Hosts with an UNHEALTHY replica (of a non QUASI_CLOSED container) cannot receive a new replica, but
          // they are not considered used as they will be removed later.
          excludedNodes.add(r.getDatanodeDetails());
          continue;
        }
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
        if (nodeStatus.isMaintenance() && nodeStatus.isDead()) {
          // Dead maintenance nodes are removed from the network topology, so the topology logic can't find
          // out their location and hence can't consider them for figuring out rack placement. So, we don't add them
          // to the used nodes list. We also don't add them to excluded nodes, as the placement policy logic won't
          // consider a node that's not in the topology anyway. In fact, adding it to excluded nodes will cause a
          // problem if total nodes (in topology) + required nodes becomes less than excluded + used nodes.

          // TODO: In the future, can the policy logic be changed to use the DatanodeDetails network location to figure
          //  out  the rack?
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
    excludeFullNodes(replicationManager, container, excludedNodes);
    return new ExcludedAndUsedNodes(excludedNodes, usedNodes);
  }

  private static void excludeFullNodes(ReplicationManager replicationManager,
      ContainerInfo container, List<DatanodeDetails> excludedNodes) {
    ContainerReplicaPendingOps pendingOps = replicationManager.getContainerReplicaPendingOps();
    Map<DatanodeID, ContainerReplicaPendingOps.SizeAndTime>
        containerSizeScheduled = pendingOps.getContainerSizeScheduled();
    if (containerSizeScheduled == null || containerSizeScheduled.isEmpty()) {
      return;
    }

    final long requiredSize = HddsServerUtil.requiredReplicationSpace(container.getUsedBytes());
    NodeManager nodeManager = replicationManager.getNodeManager();

    for (Map.Entry<DatanodeID, ContainerReplicaPendingOps.SizeAndTime> entry : containerSizeScheduled.entrySet()) {
      DatanodeDetails dn = nodeManager.getNode(entry.getKey());
      if (dn == null || excludedNodes.contains(dn)) {
        continue;
      }

      SCMNodeMetric nodeMetric = nodeManager.getNodeStat(dn);
      if (nodeMetric == null) {
        continue;
      }

      long scheduledSize = 0;
      ContainerReplicaPendingOps.SizeAndTime sizeAndTime = entry.getValue();
      if (sizeAndTime != null) {
        // Only consider sizes added in the last event timeout window
        if (pendingOps.getClock().millis() - sizeAndTime.getLastUpdatedTime()
            < replicationManager.getConfig().getEventTimeout()) {
          scheduledSize = sizeAndTime.getSize();
        } else {
          LOG.debug("Expired op {} found while computing exclude nodes", entry);
        }
      }

      SCMNodeStat scmNodeStat = nodeMetric.get();
      if (scmNodeStat.getRemaining().get() - scmNodeStat.getFreeSpaceToSpare().get() - scheduledSize
          < requiredSize) {
        LOG.debug("Adding datanode {} to exclude list. Remaining: {}, freeSpaceToSpare: {}, scheduledSize: {}, " +
            "requiredSize: {}. ContainerInfo: {}.", dn, scmNodeStat.getRemaining().get(),
            scmNodeStat.getFreeSpaceToSpare().get(), scheduledSize, requiredSize, container);
        excludedNodes.add(dn);
      }
    }
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

  public static List<ContainerReplica> selectUnhealthyReplicasForDelete(ContainerInfo containerInfo,
      Set<ContainerReplica> replicas, int pendingDeletes, Function<DatanodeDetails, NodeStatus> nodeStatusFn) {
    if (pendingDeletes > 0) {
      LOG.debug("Container {} has {} pending deletes which will free nodes.",
          containerInfo, pendingDeletes);
      return null;
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
      return !deleteCandidates.isEmpty() ? deleteCandidates : null;
    }

    if (containerInfo.getState() == HddsProtos.LifeCycleState.QUASI_CLOSED) {
      List<ContainerReplica> nonUniqueOrigins =
          findNonUniqueDeleteCandidates(replicas, deleteCandidates,
              nodeStatusFn);
      return !nonUniqueOrigins.isEmpty() ? nonUniqueOrigins : null;
    }
    return null;
  }

  /**
   * This is intended to be called when a container is under replicated, but there
   * are no spare nodes to create new replicas on, due to having too many
   * unhealthy replicas or quasi-closed replicas which cannot be closed due to
   * having a lagging sequence ID. The logic here will select a replica to
   * delete, or return null if there are none which can be safely deleted.
   *
   * @param containerInfo The container to select a replica to delete from
   * @param replicas The list of replicas for the container
   * @param pendingDeletes number pending deletes for this container
   * @return A replica to delete, or null if there are none which can be safely
   *         deleted.
   */
  public static ContainerReplica selectUnhealthyReplicaForDelete(
      ContainerInfo containerInfo, Set<ContainerReplica> replicas,
      int pendingDeletes, Function<DatanodeDetails, NodeStatus> nodeStatusFn) {
    List<ContainerReplica> containerReplicas =
        selectUnhealthyReplicasForDelete(containerInfo, replicas, pendingDeletes, nodeStatusFn);
    return containerReplicas != null ? containerReplicas.get(0) : null;
  }

  /**
   * Given a list of all replicas (including deleteCandidates), finds and
   * returns replicas which don't have unique origin node IDs. This method
   * preserves the order of the passed deleteCandidates list. This means that
   * the order of the returned list depends on the order of deleteCandidates;
   * if the same deleteCandidates with the same order is passed into this
   * method on different invocations, the order of the returned list will be
   * the same on each invocation. To protect against different SCMs
   * (old leader and new leader) selecting different replicas to delete,
   * callers of this method are responsible for ensuring consistent ordering.
   * @see
   * <a href="https://issues.apache.org/jira/browse/HDDS-4589">HDDS-4589</a>
   * @param allReplicas all replicas of this container including
   * deleteCandidates
   * @param deleteCandidates replicas that are being considered for deletion
   * @param nodeStatusFn a Function that can be called to check the
   * NodeStatus of each DN
   * @return a List of replicas that can be deleted because they do not have
   * unique origin node IDs
   */
  static List<ContainerReplica> findNonUniqueDeleteCandidates(
      Set<ContainerReplica> allReplicas,
      List<ContainerReplica> deleteCandidates,
      Function<DatanodeDetails, NodeStatus> nodeStatusFn) {
    // Gather the origin node IDs of replicas which are not candidates for
    // deletion.
    final Set<DatanodeID> existingOriginNodeIDs = allReplicas.stream()
        .filter(r -> !deleteCandidates.contains(r))
        .filter(r -> {
          NodeStatus status = nodeStatusFn.apply(r.getDatanodeDetails());
          /*
           Replicas on datanodes that are not in-service, healthy are not
           valid because it's likely they will be gone soon or are already
           lost. See https://issues.apache.org/jira/browse/HDDS-9352. This
           means that these replicas don't count as having unique origin IDs.
           */
          return status != null && status.isHealthy() && status.isInService();
        })
        .map(ContainerReplica::getOriginDatanodeId)
        .collect(Collectors.toSet());

    /*
    In the case of {QUASI_CLOSED, QUASI_CLOSED, QUASI_CLOSED, UNHEALTHY}, if
    both the first and last replicas have the same origin node ID (and no
    other replicas have it), we prefer saving the QUASI_CLOSED replica and
    deleting the UNHEALTHY one. So, we'll first loop through healthy replicas
    and check for uniqueness.
     */
    List<ContainerReplica> nonUniqueDeleteCandidates = new ArrayList<>();
    for (ContainerReplica replica : deleteCandidates) {
      if (replica.getState() == ContainerReplicaProto.State.UNHEALTHY) {
        continue;
      }
      checkUniqueness(existingOriginNodeIDs, nonUniqueDeleteCandidates,
          replica);
    }

    // now, see which UNHEALTHY replicas are not unique and can be deleted
    for (ContainerReplica replica : deleteCandidates) {
      if (replica.getState() != ContainerReplicaProto.State.UNHEALTHY) {
        continue;
      }
      checkUniqueness(existingOriginNodeIDs, nonUniqueDeleteCandidates,
          replica);
    }

    return nonUniqueDeleteCandidates;
  }

  private static void checkUniqueness(Set<DatanodeID> existingOriginNodeIDs,
      List<ContainerReplica> nonUniqueDeleteCandidates,
      ContainerReplica replica) {
    if (existingOriginNodeIDs.contains(replica.getOriginDatanodeId())) {
      nonUniqueDeleteCandidates.add(replica);
    } else {
      // Spare this replica with this new origin node ID from deletion.
      // delete candidates seen later with this same origin node ID can be
      // deleted.
      existingOriginNodeIDs.add(replica.getOriginDatanodeId());
    }
  }

}
