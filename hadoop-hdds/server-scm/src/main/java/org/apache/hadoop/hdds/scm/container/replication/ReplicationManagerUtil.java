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
import java.util.List;
import java.util.Set;

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
    private List<DatanodeDetails> excludedNodes;
    private List<DatanodeDetails> usedNodes;

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

}
