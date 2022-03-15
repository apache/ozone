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

package org.apache.hadoop.hdds.scm.container.placement.algorithms;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.SCMCommonPlacementPolicy;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Container placement policy that scatter datanodes on different racks
 * , together with the space to satisfy the size constraints.
 * <p>
 * This placement policy will try to distribute datanodes on as many racks as
 * possible.
 * <p>
 * This implementation applies to network topology like "/rack/node". Don't
 * recommend to use this if the network topology has more layers.
 * <p>
 */
public final class SCMContainerPlacementRackScatter
    extends SCMCommonPlacementPolicy {
  @VisibleForTesting
  public static final Logger LOG =
      LoggerFactory.getLogger(SCMContainerPlacementRackScatter.class);
  private final NetworkTopology networkTopology;
  private static final int RACK_LEVEL = 1;
  // OUTER_LOOP is to avoid infinity choose on all racks
  private static final int OUTER_LOOP_MAX_RETRY = 3;
  // INNER_LOOP is to choose node in each rack
  private static final int INNER_LOOP_MAX_RETRY = 5;
  private final SCMContainerPlacementMetrics metrics;

  /**
   * Constructs a Container Placement with rack awareness.
   *
   * @param nodeManager Node Manager
   * @param conf Configuration
   */
  public SCMContainerPlacementRackScatter(final NodeManager nodeManager,
      final ConfigurationSource conf, final NetworkTopology networkTopology,
      boolean fallback, final SCMContainerPlacementMetrics metrics) {
    super(nodeManager, conf);
    this.networkTopology = networkTopology;
    this.metrics = metrics;
  }

  /**
   * Called by SCM to choose datanodes.
   *
   *
   * @param excludedNodes - list of the datanodes to exclude.
   * @param favoredNodes - list of nodes preferred. This is a hint to the
   *                     allocator, whether the favored nodes will be used
   *                     depends on whether the nodes meets the allocator's
   *                     requirement.
   * @param nodesRequired - number of datanodes required.
   * @param dataSizeRequired - size required for the container.
   * @param metadataSizeRequired - size required for Ratis metadata.
   * @return List of datanodes.
   * @throws SCMException  SCMException
   */
  @Override
  public List<DatanodeDetails> chooseDatanodes(
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes,
      int nodesRequired, long metadataSizeRequired, long dataSizeRequired)
      throws SCMException {
    Preconditions.checkArgument(nodesRequired > 0);
    metrics.incrDatanodeRequestCount(nodesRequired);
    int excludedNodesCount = excludedNodes == null ? 0 : excludedNodes.size();
    List<Node> totalNodes = networkTopology.getNodes(
        networkTopology.getMaxLevel());
    if (excludedNodes != null) {
      totalNodes.removeAll(excludedNodes);
    }
    if (totalNodes.size() < nodesRequired) {
      throw new SCMException("No enough datanodes to choose. " +
          "TotalNode = " + totalNodes.size() +
          " RequiredNode = " + nodesRequired +
          " ExcludedNode = " + excludedNodesCount, null);
    }

    List<DatanodeDetails> mutableFavoredNodes = new ArrayList<>();
    if (favoredNodes != null) {
      // Generate mutableFavoredNodes, only stores valid favoredNodes
      for (DatanodeDetails datanodeDetails : favoredNodes) {
        if (isValidNode(datanodeDetails, metadataSizeRequired,
            dataSizeRequired)) {
          mutableFavoredNodes.add(datanodeDetails);
        }
      }
      Collections.shuffle(mutableFavoredNodes);
    }
    if (excludedNodes != null) {
      mutableFavoredNodes.removeAll(excludedNodes);
    }

    // For excluded nodes, we sort their racks at rear
    List<Node> racks = getAllRacks();
    if (excludedNodes != null && excludedNodes.size() > 0) {
      racks = sortRackWithExcludedNodes(racks, excludedNodes);
    }

    List<Node> toChooseRacks = new LinkedList<>(racks);
    List<DatanodeDetails> chosenNodes = new ArrayList<>();
    List<Node> unavailableNodes = new ArrayList<>();
    Set<Node> skippedRacks = new HashSet<>();
    if (excludedNodes != null) {
      unavailableNodes.addAll(excludedNodes);
    }

    // If the result doesn't change after retryCount, we return with exception
    int retryCount = 0;
    while (nodesRequired > 0) {
      if (retryCount > OUTER_LOOP_MAX_RETRY) {
        throw new SCMException("No satisfied datanode to meet the" +
            " excludedNodes and affinityNode constrains.", null);
      }
      int chosenListSize = chosenNodes.size();

      // Refill toChooseRacks, we put skippedRacks in front of toChooseRacks
      // for a even distribution
      toChooseRacks.addAll(racks);
      if (!skippedRacks.isEmpty()) {
        toChooseRacks.removeAll(skippedRacks);
        toChooseRacks.addAll(0, skippedRacks);
        skippedRacks.clear();
      }

      if (mutableFavoredNodes.size() > 0) {
        List<Node> chosenFavoredNodesInForLoop = new ArrayList<>();
        for (DatanodeDetails favoredNode : mutableFavoredNodes) {
          Node curRack = getRackOfDatanodeDetails(favoredNode);
          if (toChooseRacks.contains(curRack)) {
            chosenNodes.add(favoredNode);
            toChooseRacks.remove(curRack);
            chosenFavoredNodesInForLoop.add(favoredNode);
            unavailableNodes.add(favoredNode);
            nodesRequired--;
            if (nodesRequired == 0) {
              break;
            }
          }
        }
        mutableFavoredNodes.removeAll(chosenFavoredNodesInForLoop);
      }

      // If satisfied by favored nodes, return then.
      if (nodesRequired == 0) {
        break;
      }

      for (Node rack : toChooseRacks) {
        if (rack == null) {
          // TODO: need to recheck why null coming here.
          continue;
        }
        Node node = chooseNode(rack.getNetworkFullPath(), unavailableNodes,
            metadataSizeRequired, dataSizeRequired);
        if (node != null) {
          chosenNodes.add((DatanodeDetails) node);
          mutableFavoredNodes.remove(node);
          unavailableNodes.add(node);
          nodesRequired--;
          if (nodesRequired == 0) {
            break;
          }
        } else {
          // Store the skipped racks to check them first in next outer loop
          skippedRacks.add(rack);
        }
      }
      // Clear toChooseRacks for this loop
      toChooseRacks.clear();

      // If chosenNodes not changed, increase the retryCount
      if (chosenListSize == chosenNodes.size()) {
        retryCount++;
      } else {
        // If chosenNodes changed, reset the retryCount
        retryCount = 0;
      }
    }
    ContainerPlacementStatus placementStatus =
        validateContainerPlacement(chosenNodes, nodesRequired);
    if (!placementStatus.isPolicySatisfied()) {
      LOG.warn("ContainerPlacementPolicy not met, currentRacks is {}," +
          " desired racks is {}.", placementStatus.actualPlacementCount(),
          placementStatus.expectedPlacementCount());
    }
    return chosenNodes;
  }

  @Override
  public DatanodeDetails chooseNode(List<DatanodeDetails> healthyNodes) {
    return null;
  }

  /**
   * Choose a datanode which meets the requirements. If there is no node which
   * meets all the requirements, there is fallback chosen process depending on
   * whether fallback is allowed when this class is instantiated.
   *
   *
   * @param scope - the rack we are searching nodes under
   * @param excludedNodes - list of the datanodes to excluded. Can be null.
   * @param dataSizeRequired - size required for the container.
   * @param metadataSizeRequired - size required for Ratis metadata.
   * @return the chosen datanode.
   */
  private Node chooseNode(String scope, List<Node> excludedNodes,
      long metadataSizeRequired, long dataSizeRequired) {
    int maxRetry = INNER_LOOP_MAX_RETRY;
    while (true) {
      metrics.incrDatanodeChooseAttemptCount();
      Node node = networkTopology.chooseRandom(scope, excludedNodes);
      if (node != null) {
        DatanodeDetails datanodeDetails = (DatanodeDetails) node;
        if (isValidNode(datanodeDetails, metadataSizeRequired,
            dataSizeRequired)) {
          metrics.incrDatanodeChooseSuccessCount();
          return node;
        }
        // exclude the unavailable node for the following retries.
        excludedNodes.add(node);
      } else {
        LOG.debug("Failed to find the datanode for container. excludedNodes: " +
            "{}, rack {}", excludedNodes, scope);
      }
      maxRetry--;
      if (maxRetry == 0) {
        // avoid the infinite loop
        LOG.info("No satisfied datanode to meet the constraints. "
            + "Metadatadata size required: {} Data size required: {}, scope "
            + "{}, excluded nodes {}",
            metadataSizeRequired, dataSizeRequired, scope, excludedNodes);
        return null;
      }
    }
  }

  /**
   * For EC placement policy, desired rack count would be equal to the num of
   * Replicas.
   * @param numReplicas - num of Replicas.
   * @return required rack count.
   */
  @Override
  protected int getRequiredRackCount(int numReplicas) {
    if (networkTopology == null) {
      return 1;
    }
    int maxLevel = networkTopology.getMaxLevel();
    int numRacks = networkTopology.getNumOfNodes(maxLevel - 1);
    // Return the num of Rack if numRack less than numReplicas
    return Math.min(numRacks, numReplicas);
  }

  private Node getRackOfDatanodeDetails(DatanodeDetails datanodeDetails) {
    String location = datanodeDetails.getNetworkLocation();
    return networkTopology.getNode(location);
  }

  /**
   * For the rack holding excludedNodes, we prefer not to choose from these
   * racks, so we sort these racks at rear.
   * @param racks
   * @param excludedNodes
   * @return
   */
  private List<Node> sortRackWithExcludedNodes(List<Node> racks,
      List<DatanodeDetails> excludedNodes) {
    Set<Node> lessPreferredRacks = excludedNodes.stream()
        .map(node -> networkTopology.getAncestor(node, RACK_LEVEL))
        // Dead Nodes have been removed from the topology and so have a
        // null rack. We need to exclude those from the rack list.
        .filter(node -> node != null)
        .collect(Collectors.toSet());
    List <Node> result = new ArrayList<>();
    for (Node rack : racks) {
      if (!lessPreferredRacks.contains(rack)) {
        result.add(rack);
      }
    }
    result.addAll(lessPreferredRacks);
    return result;
  }

  private List<Node> getAllRacks() {
    int rackLevel = networkTopology.getMaxLevel() - 1;
    List<Node> racks = networkTopology.getNodes(rackLevel);
    Collections.shuffle(racks);
    return racks;
  }

}
