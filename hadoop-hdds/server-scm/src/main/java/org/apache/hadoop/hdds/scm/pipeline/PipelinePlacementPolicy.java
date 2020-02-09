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

package org.apache.hadoop.hdds.scm.pipeline;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.SCMCommonPlacementPolicy;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Pipeline placement policy that choose datanodes based on load balancing
 * and network topology to supply pipeline creation.
 * <p>
 * 1. get a list of healthy nodes
 * 2. filter out nodes that are not too heavily engaged in other pipelines
 * 3. Choose an anchor node among the viable nodes.
 * 4. Choose other nodes around the anchor node based on network topology
 */
public final class PipelinePlacementPolicy extends SCMCommonPlacementPolicy {
  @VisibleForTesting
  static final Logger LOG =
      LoggerFactory.getLogger(PipelinePlacementPolicy.class);
  private final NodeManager nodeManager;
  private final PipelineStateManager stateManager;
  private final Configuration conf;
  private final int heavyNodeCriteria;

  /**
   * Constructs a pipeline placement with considering network topology,
   * load balancing and rack awareness.
   *
   * @param nodeManager NodeManager
   * @param stateManager PipelineStateManager
   * @param conf        Configuration
   */
  public PipelinePlacementPolicy(final NodeManager nodeManager,
      final PipelineStateManager stateManager, final Configuration conf) {
    super(nodeManager, conf);
    this.nodeManager = nodeManager;
    this.conf = conf;
    this.stateManager = stateManager;
    this.heavyNodeCriteria = conf.getInt(
        ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT,
        ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT_DEFAULT);
  }

  /**
   * Returns true if this node meets the criteria.
   *
   * @param datanodeDetails DatanodeDetails
   * @param nodesRequired nodes required count
   * @return true if we have enough space.
   */
  @VisibleForTesting
  boolean meetCriteria(DatanodeDetails datanodeDetails, int nodesRequired) {
    if (heavyNodeCriteria == 0) {
      // no limit applied.
      return true;
    }
    // Datanodes from pipeline in some states can also be considered available
    // for pipeline allocation. Thus the number of these pipeline shall be
    // deducted from total heaviness calculation.
    int pipelineNumDeductable = 0;
    Set<PipelineID> pipelines = nodeManager.getPipelines(datanodeDetails);
    for (PipelineID pid : pipelines) {
      Pipeline pipeline;
      try {
        pipeline = stateManager.getPipeline(pid);
      } catch (PipelineNotFoundException e) {
        LOG.error("Pipeline not found in pipeline state manager during" +
            " pipeline creation. PipelineID: " + pid +
            " exception: " + e.getMessage());
        continue;
      }
      if (pipeline != null &&
          pipeline.getFactor().getNumber() == nodesRequired &&
          pipeline.getType() == HddsProtos.ReplicationType.RATIS &&
          pipeline.getPipelineState() == Pipeline.PipelineState.CLOSED) {
        pipelineNumDeductable++;
      }
    }
    boolean meet = (nodeManager.getPipelinesCount(datanodeDetails)
        - pipelineNumDeductable) < heavyNodeCriteria;
    if (!meet && LOG.isDebugEnabled()) {
      LOG.debug("Pipeline Placement: can't place more pipeline on heavy " +
          "datanode： " + datanodeDetails.getUuid().toString() +
          " Heaviness: " + nodeManager.getPipelinesCount(datanodeDetails) +
          " limit: " + heavyNodeCriteria);
    }
    return meet;
  }

  /**
   * Filter out viable nodes based on
   * 1. nodes that are healthy
   * 2. nodes that are not too heavily engaged in other pipelines
   *
   * @param excludedNodes - excluded nodes
   * @param nodesRequired - number of datanodes required.
   * @return a list of viable nodes
   * @throws SCMException when viable nodes are not enough in numbers
   */
  List<DatanodeDetails> filterViableNodes(
      List<DatanodeDetails> excludedNodes, int nodesRequired)
      throws SCMException {
    // get nodes in HEALTHY state
    List<DatanodeDetails> healthyNodes =
        nodeManager.getNodes(HddsProtos.NodeState.HEALTHY);
    if (excludedNodes != null) {
      healthyNodes.removeAll(excludedNodes);
    }
    int initialHealthyNodesCount = healthyNodes.size();
    String msg;

    if (initialHealthyNodesCount < nodesRequired) {
      msg = String.format("Pipeline creation failed due to no sufficient" +
              " healthy datanodes. Required %d. Found %d.",
          nodesRequired, initialHealthyNodesCount);
      LOG.warn(msg);
      throw new SCMException(msg,
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }

    // filter nodes that meet the size and pipeline engagement criteria.
    // Pipeline placement doesn't take node space left into account.
    List<DatanodeDetails> healthyList = healthyNodes.stream()
        .filter(d -> meetCriteria(d, nodesRequired))
        .collect(Collectors.toList());

    if (healthyList.size() < nodesRequired) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Unable to find enough nodes that meet the criteria that" +
            " cannot engage in more than" + heavyNodeCriteria +
            " pipelines. Nodes required: " + nodesRequired + " Found:" +
            healthyList.size() + " healthy nodes count in NodeManager: " +
            initialHealthyNodesCount);
      }
      msg = String.format("Pipeline creation failed because nodes are engaged" +
              " in other pipelines and every node can only be engaged in" +
              " max %d pipelines. Required %d. Found %d",
          heavyNodeCriteria, nodesRequired, healthyList.size());
      throw new SCMException(msg,
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }
    return healthyList;
  }

  /**
   * Pipeline placement choose datanodes to join the pipeline.
   *
   * @param excludedNodes - excluded nodes
   * @param favoredNodes  - list of nodes preferred.
   * @param nodesRequired - number of datanodes required.
   * @param sizeRequired  - size required for the container or block.
   * @return a list of chosen datanodeDetails
   * @throws SCMException when chosen nodes are not enough in numbers
   */
  @Override
  public List<DatanodeDetails> chooseDatanodes(
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes,
      int nodesRequired, final long sizeRequired) throws SCMException {
    // Get a list of viable nodes based on criteria
    // and make sure excludedNodes are excluded from list.
    List<DatanodeDetails> healthyNodes =
        filterViableNodes(excludedNodes, nodesRequired);

    // Randomly picks nodes when all nodes are equal or factor is ONE.
    // This happens when network topology is absent or
    // all nodes are on the same rack.
    if (checkAllNodesAreEqual(nodeManager.getClusterNetworkTopologyMap())) {
      return super.getResultSet(nodesRequired, healthyNodes);
    } else {
      // Since topology and rack awareness are available, picks nodes
      // based on them.
      return this.getResultSet(nodesRequired, healthyNodes);
    }
  }

  // Fall back logic for node pick up.
  DatanodeDetails fallBackPickNodes(
      List<DatanodeDetails> nodeSet, List<DatanodeDetails> excludedNodes)
      throws SCMException{
    DatanodeDetails node;
    if (excludedNodes == null || excludedNodes.isEmpty()) {
      node = chooseNode(nodeSet);
    } else {
      List<DatanodeDetails> inputNodes = nodeSet.stream()
          .filter(p -> !excludedNodes.contains(p)).collect(Collectors.toList());
      node = chooseNode(inputNodes);
    }

    if (node == null) {
      String msg = String.format("Unable to find fall back node in" +
          " pipeline allocation. nodeSet size: {}", nodeSet.size());
      LOG.warn(msg);
      throw new SCMException(msg,
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }
    return node;
  }

  /**
   * Get result set based on the pipeline placement algorithm which considers
   * network topology and rack awareness.
   * @param nodesRequired - Nodes Required
   * @param healthyNodes - List of Nodes in the result set.
   * @return a list of datanodes
   * @throws SCMException SCMException
   */
  @Override
  public List<DatanodeDetails> getResultSet(
      int nodesRequired, List<DatanodeDetails> healthyNodes)
      throws SCMException {
    if (nodesRequired != HddsProtos.ReplicationFactor.THREE.getNumber()) {
      throw new SCMException("Nodes required number is not supported: " +
          nodesRequired, SCMException.ResultCodes.INVALID_CAPACITY);
    }

    // Assume rack awareness is not enabled.
    boolean rackAwareness = false;
    List <DatanodeDetails> results = new ArrayList<>(nodesRequired);
    // Since nodes are widely distributed, the results should be selected
    // base on distance in topology, rack awareness and load balancing.
    List<DatanodeDetails> exclude = new ArrayList<>();
    // First choose an anchor nodes randomly
    DatanodeDetails anchor = chooseNode(healthyNodes);
    if (anchor != null) {
      results.add(anchor);
      exclude.add(anchor);
    } else {
      LOG.warn("Unable to find healthy node for anchor(first) node.");
      throw new SCMException("Unable to find anchor node.",
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("First node chosen: {}", anchor);
    }


    // Choose the second node on different racks from anchor.
    DatanodeDetails nextNode = chooseNodeBasedOnRackAwareness(
        healthyNodes, exclude,
        nodeManager.getClusterNetworkTopologyMap(), anchor);
    if (nextNode != null) {
      // Rack awareness is detected.
      rackAwareness = true;
      results.add(nextNode);
      exclude.add(nextNode);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Second node chosen: {}", nextNode);
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Pipeline Placement: Unable to find 2nd node on different " +
            "rack based on rack awareness.");
      }
    }

    // Then choose nodes close to anchor based on network topology
    int nodesToFind = nodesRequired - results.size();
    for (int x = 0; x < nodesToFind; x++) {
      // Pick remaining nodes based on the existence of rack awareness.
      DatanodeDetails pick = rackAwareness
          ? chooseNodeFromNetworkTopology(
              nodeManager.getClusterNetworkTopologyMap(), anchor, exclude)
          : fallBackPickNodes(healthyNodes, exclude);
      if (pick != null) {
        results.add(pick);
        exclude.add(pick);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Remaining node chosen: {}", pick);
        }
      }
    }

    if (results.size() < nodesRequired) {
      LOG.warn("Unable to find the required number of " +
              "healthy nodes that  meet the criteria. Required nodes: {}, " +
              "Found nodes: {}", nodesRequired, results.size());
      throw new SCMException("Unable to find required number of nodes.",
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }
    return results;
  }

  /**
   * Find a node from the healthy list and return it after removing it from the
   * list that we are operating on.
   *
   * @param healthyNodes - Set of healthy nodes we can choose from.
   * @return chosen datanodDetails
   */
  @Override
  public DatanodeDetails chooseNode(
      List<DatanodeDetails> healthyNodes) {
    if (healthyNodes == null || healthyNodes.isEmpty()) {
      return null;
    }
    int firstNodeNdx = getRand().nextInt(healthyNodes.size());
    int secondNodeNdx = getRand().nextInt(healthyNodes.size());

    DatanodeDetails datanodeDetails;
    // There is a possibility that both numbers will be same.
    // if that is so, we just return the node.
    if (firstNodeNdx == secondNodeNdx) {
      datanodeDetails = healthyNodes.get(firstNodeNdx);
    } else {
      DatanodeDetails firstNodeDetails = healthyNodes.get(firstNodeNdx);
      DatanodeDetails secondNodeDetails = healthyNodes.get(secondNodeNdx);
      SCMNodeMetric firstNodeMetric =
          nodeManager.getNodeStat(firstNodeDetails);
      SCMNodeMetric secondNodeMetric =
          nodeManager.getNodeStat(secondNodeDetails);
      datanodeDetails = firstNodeMetric.isGreater(secondNodeMetric.get())
          ? firstNodeDetails : secondNodeDetails;
    }
    healthyNodes.remove(datanodeDetails);
    return datanodeDetails;
  }

  /**
   * Choose node on different racks as anchor is on based on rack awareness.
   * If a node on different racks cannot be found, then return a random node.
   * @param healthyNodes healthy nodes
   * @param excludedNodes excluded nodes
   * @param networkTopology network topology
   * @param anchor anchor node
   * @return a node on different rack
   */
  @VisibleForTesting
  protected DatanodeDetails chooseNodeBasedOnRackAwareness(
      List<DatanodeDetails> healthyNodes,  List<DatanodeDetails> excludedNodes,
      NetworkTopology networkTopology, DatanodeDetails anchor) {
    Preconditions.checkArgument(networkTopology != null);
    if (checkAllNodesAreEqual(networkTopology)) {
      return null;
    }

    for (DatanodeDetails node : healthyNodes) {
      if (excludedNodes.contains(node) ||
          anchor.getNetworkLocation().equals(node.getNetworkLocation())) {
        continue;
      } else {
        return node;
      }
    }
    return null;
  }

  /**
   * Check if all nodes are equal in topology.
   * They are equal when network topology is absent or there are on
   * the same rack.
   * @param topology network topology
   * @return true when all nodes are equal
   */
  private boolean checkAllNodesAreEqual(NetworkTopology topology) {
    if (topology == null) {
      return true;
    }
    return (topology.getNumOfNodes(topology.getMaxLevel() - 1) == 1);
  }

  /**
   * Choose node based on network topology.
   * @param networkTopology network topology
   * @param anchor anchor datanode to start with
   * @param excludedNodes excluded datanodes
   * @return chosen datanode
   */
  @VisibleForTesting
  protected DatanodeDetails chooseNodeFromNetworkTopology(
      NetworkTopology networkTopology, DatanodeDetails anchor,
      List<DatanodeDetails> excludedNodes) {
    Preconditions.checkArgument(networkTopology != null);

    Collection<Node> excluded = new ArrayList<>();
    if (excludedNodes != null && excludedNodes.size() != 0) {
      excluded.addAll(excludedNodes);
    }

    Node pick = networkTopology.chooseRandom(
        anchor.getNetworkLocation(), excluded);
    DatanodeDetails pickedNode = (DatanodeDetails) pick;
    return pickedNode;
  }
}
