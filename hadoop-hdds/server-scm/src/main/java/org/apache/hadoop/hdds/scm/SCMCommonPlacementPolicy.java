/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This policy implements a set of invariants which are common
 * for all basic placement policies, acts as the repository of helper
 * functions which are common to placement policies.
 */
public abstract class SCMCommonPlacementPolicy implements PlacementPolicy {
  @VisibleForTesting
  static final Logger LOG =
      LoggerFactory.getLogger(SCMCommonPlacementPolicy.class);
  private final NodeManager nodeManager;
  private final Random rand;
  private final ConfigurationSource conf;

  /**
   * Return for replication factor 1 containers where the placement policy
   * is always met, or not met (zero replicas available) rather than creating a
   * new object each time. There are only used when there is no network topology
   * or the replication factor is 1 or the required racks is 1.
   */
  private ContainerPlacementStatus validPlacement
      = new ContainerPlacementStatusDefault(1, 1, 1);
  private ContainerPlacementStatus invalidPlacement
      = new ContainerPlacementStatusDefault(0, 1, 1);

  /**
   * Constructor.
   *
   * @param nodeManager NodeManager
   * @param conf        Configuration class.
   */
  public SCMCommonPlacementPolicy(NodeManager nodeManager,
      ConfigurationSource conf) {
    this.nodeManager = nodeManager;
    this.rand = new Random();
    this.conf = conf;
  }

  /**
   * Return node manager.
   *
   * @return node manager
   */
  public NodeManager getNodeManager() {
    return nodeManager;
  }

  /**
   * Returns the Random Object.
   *
   * @return rand
   */
  public Random getRand() {
    return rand;
  }

  /**
   * Get Config.
   *
   * @return Configuration
   */
  public ConfigurationSource getConf() {
    return conf;
  }

  /**
   * Given size required, return set of datanodes
   * that satisfy the nodes and size requirement.
   * <p>
   * Here are some invariants of container placement.
   * <p>
   * 1. We place containers only on healthy nodes.
   * 2. We place containers on nodes with enough space for that container.
   * 3. if a set of containers are requested, we either meet the required
   * number of nodes or we fail that request.
   *
   * @param excludedNodes - datanodes with existing replicas
   * @param favoredNodes  - list of nodes preferred.
   * @param nodesRequired - number of datanodes required.
   * @param sizeRequired  - size required for the container or block.
   * @return list of datanodes chosen.
   * @throws SCMException SCM exception.
   */
  @Override
  public List<DatanodeDetails> chooseDatanodes(
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes,
      int nodesRequired, final long sizeRequired) throws SCMException {
    List<DatanodeDetails> healthyNodes =
        nodeManager.getNodes(NodeStatus.inServiceHealthy());
    if (excludedNodes != null) {
      healthyNodes.removeAll(excludedNodes);
    }
    String msg;
    if (healthyNodes.size() == 0) {
      msg = "No healthy node found to allocate container.";
      LOG.error(msg);
      throw new SCMException(msg, SCMException.ResultCodes
          .FAILED_TO_FIND_HEALTHY_NODES);
    }

    if (healthyNodes.size() < nodesRequired) {
      msg = String.format("Not enough healthy nodes to allocate container. %d "
              + " datanodes required. Found %d",
          nodesRequired, healthyNodes.size());
      LOG.error(msg);
      throw new SCMException(msg,
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }
    List<DatanodeDetails> healthyList = healthyNodes.stream().filter(d ->
        hasEnoughSpace(d, sizeRequired)).collect(Collectors.toList());

    if (healthyList.size() < nodesRequired) {
      msg = String.format("Unable to find enough nodes that meet the space " +
              "requirement of %d bytes in healthy node set." +
              " Nodes required: %d Found: %d",
          sizeRequired, nodesRequired, healthyList.size());
      LOG.error(msg);
      throw new SCMException(msg,
          SCMException.ResultCodes.FAILED_TO_FIND_NODES_WITH_SPACE);
    }
    return healthyList;
  }

  /**
   * Returns true if this node has enough space to meet our requirement.
   *
   * @param datanodeDetails DatanodeDetails
   * @return true if we have enough space.
   */
  public boolean hasEnoughSpace(DatanodeDetails datanodeDetails,
      long sizeRequired) {
    SCMNodeMetric nodeMetric = nodeManager.getNodeStat(datanodeDetails);
    return (nodeMetric != null) && (nodeMetric.get() != null)
        && nodeMetric.get().getRemaining().hasResources(sizeRequired);
  }

  /**
   * This function invokes the derived classes chooseNode Function to build a
   * list of nodes. Then it verifies that invoked policy was able to return
   * expected number of nodes.
   *
   * @param nodesRequired - Nodes Required
   * @param healthyNodes  - List of Nodes in the result set.
   * @return List of Datanodes that can be used for placement.
   * @throws SCMException SCMException
   */
  public List<DatanodeDetails> getResultSet(
      int nodesRequired, List<DatanodeDetails> healthyNodes)
      throws SCMException {
    List<DatanodeDetails> results = new ArrayList<>();
    for (int x = 0; x < nodesRequired; x++) {
      // invoke the choose function defined in the derived classes.
      DatanodeDetails nodeId = chooseNode(healthyNodes);
      if (nodeId != null) {
        results.add(nodeId);
      }
    }

    if (results.size() < nodesRequired) {
      LOG.error("Unable to find the required number of healthy nodes that " +
              "meet the criteria. Required nodes: {}, Found nodes: {}",
          nodesRequired, results.size());
      throw new SCMException("Unable to find required number of nodes.",
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }
    return results;
  }

  /**
   * Choose a datanode according to the policy, this function is implemented
   * by the actual policy class.
   *
   * @param healthyNodes - Set of healthy nodes we can choose from.
   * @return DatanodeDetails
   */
  public abstract DatanodeDetails chooseNode(
      List<DatanodeDetails> healthyNodes);

  /**
   * Default implementation to return the number of racks containers should span
   * to meet the placement policy. For simple policies that are not rack aware
   * we return 1, from this default implementation.
   * should have
   * @return The number of racks containers should span to meet the policy
   */
  protected int getRequiredRackCount() {
    return 1;
  }

  /**
   * This default implementation handles rack aware policies and non rack
   * aware policies. If a future placement policy needs to check more than racks
   * to validate the policy (eg node groups, HDFS like upgrade domain) this
   * method should be overridden in the sub class.
   * This method requires that subclasses which implement rack aware policies
   * override the default method getRequiredRackCount and getNetworkTopology.
   * @param dns List of datanodes holding a replica of the container
   * @param replicas The expected number of replicas
   * @return ContainerPlacementStatus indicating if the placement policy is
   *         met or not. Not this only considers the rack count and not the
   *         number of replicas.
   */
  @Override
  public ContainerPlacementStatus validateContainerPlacement(
      List<DatanodeDetails> dns, int replicas) {
    NetworkTopology topology = nodeManager.getClusterNetworkTopologyMap();
    int requiredRacks = getRequiredRackCount();
    if (topology == null || replicas == 1 || requiredRacks == 1) {
      if (dns.size() > 0) {
        // placement is always satisfied if there is at least one DN.
        return validPlacement;
      } else {
        return invalidPlacement;
      }
    }
    // We have a network topology so calculate if it is satisfied or not.
    int numRacks = 1;
    final int maxLevel = topology.getMaxLevel();
    // The leaf nodes are all at max level, so the number of nodes at
    // leafLevel - 1 is the rack count
    numRacks = topology.getNumOfNodes(maxLevel - 1);
    final long currentRackCount = dns.stream()
        .map(d -> topology.getAncestor(d, 1))
        .distinct()
        .count();

    if (replicas < requiredRacks) {
      requiredRacks = replicas;
    }
    return new ContainerPlacementStatusDefault(
        (int)currentRackCount, requiredRacks, numRacks);
  }
}
