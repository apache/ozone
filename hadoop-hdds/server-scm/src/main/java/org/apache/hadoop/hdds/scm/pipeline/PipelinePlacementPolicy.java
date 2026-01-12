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

package org.apache.hadoop.hdds.scm.pipeline;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.SCMCommonPlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline placement policy that choose datanodes based on load balancing
 * and network topology to supply pipeline creation.
 * <p>
 * 1. get a list of healthy nodes
 * 2. filter out nodes that have space less than container size.
 * 3. filter out nodes that are not too heavily engaged in other pipelines
 * 4. Choose an anchor node among the viable nodes.
 * 5. Choose other nodes around the anchor node based on network topology
 */
public final class PipelinePlacementPolicy extends SCMCommonPlacementPolicy {
  @VisibleForTesting
  static final Logger LOG =
      LoggerFactory.getLogger(PipelinePlacementPolicy.class);
  private final NodeManager nodeManager;
  private final PipelineStateManager stateManager;
  private final int datanodePipelineLimit;
  private static final int REQUIRED_RACKS = 2;

  public static final String MULTIPLE_RACK_PIPELINE_MSG =
      "The cluster has multiple racks, but all nodes with available " +
      "pipeline capacity are on a single rack. There are insufficient " +
      "cross rack nodes available to create a pipeline";

  /**
   * Constructs a pipeline placement with considering network topology,
   * load balancing and rack awareness.
   *
   * @param nodeManager NodeManager
   * @param stateManager PipelineStateManagerImpl
   * @param conf        Configuration
   */
  public PipelinePlacementPolicy(final NodeManager nodeManager,
                                 final PipelineStateManager stateManager,
                                 final ConfigurationSource conf) {
    super(nodeManager, conf);
    this.nodeManager = nodeManager;
    this.stateManager = stateManager;
    this.datanodePipelineLimit = conf.getInt(
        ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT,
        ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT_DEFAULT);
  }

  public static int currentRatisThreePipelineCount(
      NodeManager nodeManager,
      PipelineStateManager stateManager,
      DatanodeDetails datanodeDetails) {
    // Safe to cast collection's size to int
    return (int) nodeManager.getPipelines(datanodeDetails).stream()
        .map(id -> {
          try {
            return stateManager.getPipeline(id);
          } catch (PipelineNotFoundException e) {
            LOG.debug("Pipeline not found in pipeline state manager during" +
                " pipeline creation. PipelineID: {}", id, e);
            return null;
          }
        })
        .filter(PipelinePlacementPolicy::isNonClosedRatisThreePipeline)
        .count();
  }

  private static boolean isNonClosedRatisThreePipeline(Pipeline p) {
    return p != null && p.getReplicationConfig()
        .equals(RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        && !p.isClosed();
  }

  @Override
  protected int getMaxReplicasPerRack(int numReplicas, int numberOfRacks) {
    if (numberOfRacks == 1) {
      return numReplicas;
    }
    return Math.max(numReplicas - 1, 1);
  }

  /**
   * Filter out viable nodes based on
   * 1. nodes that are healthy
   * 2. nodes that have enough space
   * 3. nodes that are not too heavily engaged in other pipelines
   * The results are sorted based on pipeline count of each node.
   *
   * @param excludedNodes - excluded nodes
   * @param usedNodes - used nodes
   * @param nodesRequired - number of datanodes required.
   * @return a list of viable nodes
   * @throws SCMException when viable nodes are not enough in numbers
   */
  List<DatanodeDetails> filterViableNodes(
      List<DatanodeDetails> excludedNodes,
      List<DatanodeDetails> usedNodes, int nodesRequired,
      long metadataSizeRequired, long dataSizeRequired)
      throws SCMException {
    // get nodes in HEALTHY state
    List<DatanodeDetails> healthyNodes =
        nodeManager.getNodes(NodeStatus.inServiceHealthy());
    String msg;
    if (healthyNodes.isEmpty()) {
      msg = "No healthy node found to allocate container.";
      LOG.error(msg);
      throw new SCMException(msg, SCMException.ResultCodes
              .FAILED_TO_FIND_HEALTHY_NODES);
    }

    healthyNodes = filterNodesWithSpace(healthyNodes, nodesRequired,
        metadataSizeRequired, dataSizeRequired);
    boolean multipleRacks = multipleRacksAvailable(healthyNodes);
    int excludedNodesSize = 0;
    if (excludedNodes != null) {
      excludedNodesSize = excludedNodes.size();
      healthyNodes.removeAll(excludedNodes);
    }
    if (usedNodes != null) {
      excludedNodesSize += usedNodes.size();
      healthyNodes.removeAll(usedNodes);
    }
    int initialHealthyNodesCount = healthyNodes.size();

    if (initialHealthyNodesCount < nodesRequired) {
      msg = String.format("Pipeline creation failed due to no sufficient" +
              " healthy datanodes. Required %d. Found %d. Excluded %d.",
          nodesRequired, initialHealthyNodesCount, excludedNodesSize);
      LOG.debug(msg);
      throw new SCMException(msg,
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }

    // filter nodes that meet the size and pipeline engagement criteria.
    // Pipeline placement doesn't take node space left into account.
    // Sort the DNs by pipeline load.
    // TODO check if sorting could cause performance issue: HDDS-3466.
    List<DatanodeDetails> healthyList = healthyNodes.stream()
        .map(d ->
            new DnWithPipelines(d, currentRatisThreePipelineCount(nodeManager,
                stateManager, d)))
        .filter(d ->
            (d.getPipelines() < nodeManager.pipelineLimit(d.getDn())))
        .sorted(Comparator.comparingInt(DnWithPipelines::getPipelines))
        .map(d -> d.getDn())
        .collect(Collectors.toList());

    if (healthyList.size() < nodesRequired) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Unable to find enough nodes that meet the criteria that" +
            " cannot engage in more than" + datanodePipelineLimit +
            " pipelines. Nodes required: " + nodesRequired + " Excluded: " +
            excludedNodesSize + " Found:" +
            healthyList.size() + " healthy nodes count in NodeManager: " +
            initialHealthyNodesCount);
      }
      msg = String.format("Pipeline creation failed because nodes are engaged" +
              " in other pipelines and every node can only be engaged in" +
              " max %d pipelines. Required %d. Found %d. Excluded: %d.",
          datanodePipelineLimit, nodesRequired, healthyList.size(),
          excludedNodesSize);
      throw new SCMException(msg,
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }

    if (!checkAllNodesAreEqual(nodeManager.getClusterNetworkTopologyMap())) {
      boolean multipleRacksAfterFilter = multipleRacksAvailable(healthyList);
      if (multipleRacks && !multipleRacksAfterFilter) {
        LOG.debug(MULTIPLE_RACK_PIPELINE_MSG);
        throw new SCMException(MULTIPLE_RACK_PIPELINE_MSG,
            SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
      }
    }
    return healthyList;
  }

  /**
   * Given a list of Datanodes, return false if the entire list is only on a
   * single rack, or the list is empty. If there is more than 1 rack, return
   * true.
   * @param dns List of datanodes to check
   * @return True if there are multiple racks, false otherwise
   */
  private boolean multipleRacksAvailable(List<DatanodeDetails> dns) {
    if (dns.size() <= 1) {
      return false;
    }
    String initialRack = dns.get(0).getNetworkLocation();
    for (DatanodeDetails dn : dns) {
      if (!dn.getNetworkLocation().equals(initialRack)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Pipeline placement choose datanodes to join the pipeline.
   * TODO: HDDS-7227: Update Implementation to accomodate for already used
   * nodes in pipeline to conform to existing placement policy.
   * @param usedNodes - list of the datanodes to already chosen in the
   *                      pipeline.
   * @param excludedNodes - excluded nodes
   * @param favoredNodes  - list of nodes preferred.
   * @param nodesRequired - number of datanodes required.
   * @param dataSizeRequired - size required for the container.
   * @param metadataSizeRequired - size required for Ratis metadata.
   * @return a list of chosen datanodeDetails
   * @throws SCMException when chosen nodes are not enough in numbers
   */
  @Override
  protected List<DatanodeDetails> chooseDatanodesInternal(
          List<DatanodeDetails> usedNodes, List<DatanodeDetails> excludedNodes,
          List<DatanodeDetails> favoredNodes,
          int nodesRequired, long metadataSizeRequired, long dataSizeRequired)
      throws SCMException {
    // Get a list of viable nodes based on criteria
    // and make sure excludedNodes are excluded from list.
    List<DatanodeDetails> healthyNodes = filterViableNodes(excludedNodes,
        usedNodes, nodesRequired, metadataSizeRequired, dataSizeRequired);

    // Randomly picks nodes when all nodes are equal or factor is ONE.
    // This happens when network topology is absent or
    // all nodes are on the same rack.
    if (checkAllNodesAreEqual(nodeManager.getClusterNetworkTopologyMap())) {
      return super.getResultSet(nodesRequired, healthyNodes);
    } else {
      // Since topology and rack awareness are available, picks nodes
      // based on them.
      return this.getResultSetWithTopology(nodesRequired, healthyNodes,
          usedNodes);
    }
  }

  // Fall back logic for node pick up.
  DatanodeDetails fallBackPickNodes(
      List<DatanodeDetails> nodeSet, List<DatanodeDetails> excludedNodes) {
    DatanodeDetails node;
    if (excludedNodes == null || excludedNodes.isEmpty()) {
      node = chooseNode(nodeSet);
    } else {
      List<DatanodeDetails> inputNodes = nodeSet.stream()
          .filter(p -> !excludedNodes.contains(p)).collect(Collectors.toList());
      node = chooseNode(inputNodes);
    }
    return node;
  }

  /**
   * Get result set based on the pipeline placement algorithm which considers
   * network topology and rack awareness.
   * @param nodesRequired - Nodes Required
   * @param healthyNodes - List of Nodes in the result set.
   * @param usedNodes - List of used Nodes.
   * @return a list of datanodes
   * @throws SCMException SCMException
   */
  private List<DatanodeDetails> getResultSetWithTopology(
      int nodesRequired, List<DatanodeDetails> healthyNodes,
      List<DatanodeDetails> usedNodes)
      throws SCMException {
    Objects.requireNonNull(usedNodes, "usedNodes == null");
    Objects.requireNonNull(healthyNodes, "healthyNodes == null");
    Preconditions.checkState(nodesRequired >= 1);

    if (nodesRequired + usedNodes.size() !=
        HddsProtos.ReplicationFactor.THREE.getNumber()) {
      throw new SCMException("Nodes required number is not supported: " +
          nodesRequired, SCMException.ResultCodes.INVALID_CAPACITY);
    }

    List <DatanodeDetails> results = new ArrayList<>(nodesRequired);
    List <DatanodeDetails> mutableLstNodes = new ArrayList<>();
    List<DatanodeDetails> mutableExclude = new ArrayList<>();
    boolean rackAwareness = getAnchorAndNextNode(healthyNodes,
        usedNodes, results, mutableLstNodes, mutableExclude);
    if (mutableLstNodes.isEmpty()) {
      LOG.warn("Unable to find healthy node for anchor(first) node.");
      throw new SCMException("Unable to find anchor node.",
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }
    // First node is anchor node
    DatanodeDetails anchor = mutableLstNodes.get(0);
    DatanodeDetails nextNode = null;
    if (mutableLstNodes.size() == 2) {
      // Second node is next node
      nextNode = mutableLstNodes.get(1);
    }

    // Then choose nodes close to anchor based on network topology
    int nodesToFind = nodesRequired - results.size();
    boolean bCheckNodeInAnchorRack = true;
    for (int x = 0; x < nodesToFind; x++) {
      // Pick remaining nodes based on the existence of rack awareness.
      DatanodeDetails pick = null;
      if (rackAwareness && bCheckNodeInAnchorRack) {
        pick = chooseNodeBasedOnSameRack(
            healthyNodes, mutableExclude,
            nodeManager.getClusterNetworkTopologyMap(), anchor);
        if (pick == null) {
          // No available node to pick from first anchor node
          // Make nextNode as anchor node and pick remaining node
          anchor = nextNode;
          pick = chooseNodeBasedOnSameRack(
              healthyNodes, mutableExclude,
              nodeManager.getClusterNetworkTopologyMap(), anchor);
        }
      }
      // fall back protection
      if (pick == null) {
        // Make bNodeFoundInAnchorRack to false so that from next node search
        // it will just search in fallback nodes and avoid searching in
        // anchor node rack.
        bCheckNodeInAnchorRack = false;
        pick = fallBackPickNodes(healthyNodes, mutableExclude);
        if (rackAwareness) {
          LOG.debug("Failed to choose node based on topology. Fallback " +
              "picks node as: {}", pick);
        }
      }

      if (pick != null) {
        results.add(pick);
        removePeers(pick, healthyNodes);
        mutableExclude.add(pick);
        LOG.debug("Remaining node chosen: {}", pick);
      } else {
        String msg = String.format("Unable to find suitable node in " +
            "pipeline allocation. healthyNodes size: %d, " +
            "excludeNodes size: %d", healthyNodes.size(),
            mutableExclude.size());
        LOG.debug(msg);
        throw new SCMException(msg,
            SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
      }
    }

    if (results.size() < nodesRequired) {
      LOG.debug("Unable to find the required number of " +
              "healthy nodes that  meet the criteria. Required nodes: {}, " +
              "Found nodes: {}", nodesRequired, results.size());
      throw new SCMException("Unable to find required number of nodes.",
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }
    return results;
  }

  /**
   * Get Anchor and Next node based on healthyNodes and usedNodes.
   * @param healthyNodes - List of healthy nodes.
   * @param usedNodes - List of used nodes.
   * @param results - List of Nodes in the result set.
   * @param mutableLstNodes - Anchor and Next node.
   * @param mutableExclude - List of excluded nodes.
   * @return whether rack aware is satisfied
   * @throws SCMException SCMException
   */
  private boolean getAnchorAndNextNode(List<DatanodeDetails> healthyNodes,
                                       List<DatanodeDetails> usedNodes,
                                       List<DatanodeDetails> results,
                                       List<DatanodeDetails> mutableLstNodes,
                                       List<DatanodeDetails> mutableExclude)
      throws SCMException {
    // Assume rack awareness is not enabled.
    boolean rackAwareness = false;
    // Since nodes are widely distributed, the results should be selected
    // base on distance in topology, rack awareness and load balancing.
    DatanodeDetails anchor;
    DatanodeDetails nextNode = null;
    // First choose an anchor node.
    if (usedNodes.isEmpty()) {
      // No usedNode, choose anchor based on healthyNodes
      anchor = chooseFirstNode(healthyNodes);
      if (anchor != null) {
        results.add(anchor);
        removePeers(anchor, healthyNodes);
        mutableExclude.add(anchor);
      } else {
        LOG.debug("Unable to find healthy node for anchor(first) node.");
        throw new SCMException("Unable to find anchor node.",
            SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
      }
      LOG.debug("First node chosen: {}", anchor);
    } else if (usedNodes.size() == 1) {
      // Only 1 usedNode, consider it as anchor node.
      anchor = usedNodes.get(0);
      removePeers(anchor, healthyNodes);
      mutableExclude.add(anchor);
    } else if (usedNodes.size() == 2) {
      // 2 usedNodes, consider 1st as anchor node
      anchor = usedNodes.get(0);
      removePeers(anchor, healthyNodes);
      mutableExclude.add(anchor);
      if (usedNodes.get(0).getParent() != usedNodes.get(1).getParent()) {
        // Rack awareness is detected.
        // If 2 usedNodes are in different rack then
        // consider second node as next node
        nextNode = usedNodes.get(1);
        rackAwareness = true;
      }
      mutableExclude.add(usedNodes.get(1));
      removePeers(nextNode, healthyNodes);
    } else {
      // Maximum 2 used nodes can exist.
      LOG.warn("More than 2 used nodes, unable to choose anchor node.");
      throw new SCMException("Used Nodes required number is not supported: " +
          usedNodes.size(), SCMException.ResultCodes.INVALID_CAPACITY);
    }

    if (nextNode == null) {
      // Choose the second node on different racks from anchor.
      nextNode = chooseNodeBasedOnRackAwareness(
          healthyNodes, mutableExclude,
          nodeManager.getClusterNetworkTopologyMap(), anchor);
      if (nextNode != null) {
        // Rack awareness is detected.
        rackAwareness = true;
        results.add(nextNode);
        removePeers(nextNode, healthyNodes);
        mutableExclude.add(nextNode);
        LOG.debug("Second node chosen: {}", nextNode);
      } else {
        LOG.debug("Pipeline Placement: Unable to find 2nd node on different " +
            "rack based on rack awareness. anchor: {}", anchor);
      }
    }
    mutableLstNodes.add(anchor);
    if (nextNode != null) {
      mutableLstNodes.add(nextNode);
    }
    return rackAwareness;
  }

  /**
   * Find a node from the healthy list and return it after removing it from the
   * list that we are operating on.
   * Return random node in the list.
   *
   * @param healthyNodes - Set of healthy nodes we can choose from.
   * @return chosen datanodeDetails
   */
  @Override
  public DatanodeDetails chooseNode(final List<DatanodeDetails> healthyNodes) {
    if (healthyNodes == null || healthyNodes.isEmpty()) {
      return null;
    }
    DatanodeDetails selectedNode =
            healthyNodes.get(getRand().nextInt(healthyNodes.size()));
    healthyNodes.remove(selectedNode);
    if (selectedNode != null) {
      removePeers(selectedNode, healthyNodes);
    }
    return selectedNode;
  }

  /**
   * Find a node from the healthy list and return it after removing it from the
   * list that we are operating on.
   * Return the first node in the list.
   *
   * @param healthyNodes - Set of healthy nodes we can choose from.
   * @return chosen datanodeDetails
   */
  private DatanodeDetails chooseFirstNode(
      final List<DatanodeDetails> healthyNodes) {
    if (healthyNodes == null || healthyNodes.isEmpty()) {
      return null;
    }
    DatanodeDetails selectedNode = healthyNodes.get(0);
    healthyNodes.remove(selectedNode);
    if (selectedNode != null) {
      removePeers(selectedNode, healthyNodes);
    }
    return selectedNode;
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

    List<DatanodeDetails> nodesOnOtherRack = healthyNodes.stream().filter(
        p -> !excludedNodes.contains(p)
            && !anchor.getNetworkLocation().equals(p.getNetworkLocation()))
        .collect(Collectors.toList());
    if (!nodesOnOtherRack.isEmpty()) {
      return nodesOnOtherRack.get(0);
    }
    return null;
  }

  @VisibleForTesting
  protected DatanodeDetails chooseNodeBasedOnSameRack(
      List<DatanodeDetails> healthyNodes,  List<DatanodeDetails> excludedNodes,
      NetworkTopology networkTopology, DatanodeDetails anchor) {
    Preconditions.checkArgument(networkTopology != null);
    if (checkAllNodesAreEqual(networkTopology)) {
      return null;
    }

    List<DatanodeDetails> nodesOnSameRack = healthyNodes.stream().filter(
        p -> !excludedNodes.contains(p)
            && anchor.getNetworkLocation().equals(p.getNetworkLocation()))
        .collect(Collectors.toList());
    if (!nodesOnSameRack.isEmpty()) {
      return nodesOnSameRack.get(0);
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

  @Override
  protected int getRequiredRackCount(int numReplicas, int excludedRackCount) {
    return REQUIRED_RACKS;
  }

  /**
   * static inner utility class for datanodes with pipeline, used for
   * pipeline engagement checking.
   */
  public static class DnWithPipelines {
    private DatanodeDetails dn;
    private int pipelines;

    DnWithPipelines(DatanodeDetails dn, int pipelines) {
      this.dn = dn;
      this.pipelines = pipelines;
    }

    public int getPipelines() {
      return pipelines;
    }

    public DatanodeDetails getDn() {
      return dn;
    }
  }

}
