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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.SCMCommonPlacementPolicy;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineStateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
   * Constructor for Pipeline Provider Pipeline Placement with rack awareness.
   * @param nodeManager Node Manager
   * @param stateManager State Manager
   * @param conf Configuration
   */
  public SCMContainerPlacementRackScatter(NodeManager nodeManager,
      PipelineStateManager stateManager, ConfigurationSource conf) {
    super(nodeManager, conf);
    this.networkTopology = nodeManager.getClusterNetworkTopologyMap();
    this.metrics = null;
  }

  public Set<DatanodeDetails> chooseNodesFromRacks(List<Node> racks,
       List<Node> unavailableNodes,
       List<DatanodeDetails> mutableFavoredNodes,
       int nodesRequired, final Pair<Long, Long> metadatasizeDatasizePair,
       int maxOuterLoopIterations, final Pair<Map<Node, Integer>, Integer>
       rackCntMapMaxReplicaPerRackPair) {
    if (nodesRequired <= 0) {
      return Collections.emptySet();
    }
    final long metadataSizeRequired = metadatasizeDatasizePair.getKey();
    final long dataSizeRequired = metadatasizeDatasizePair.getValue();
    final Map<Node, Integer> rackCntMap =
            rackCntMapMaxReplicaPerRackPair.getKey();
    final int maxReplicasPerRack = rackCntMapMaxReplicaPerRackPair.getValue();
    List<Node> toChooseRacks = new LinkedList<>();
    Set<DatanodeDetails> chosenNodes = new LinkedHashSet<>();
    Set<Node> skippedRacks = new HashSet<>();
    // If the result doesn't change after retryCount, we return with exception
    int retryCount = 0;
    while (nodesRequired > 0 && maxOuterLoopIterations > 0) {
      if (retryCount > OUTER_LOOP_MAX_RETRY) {
        return chosenNodes;
      }
      int chosenListSize = chosenNodes.size();

      // Refill toChooseRacks, we put skippedRacks in front of toChooseRacks
      // for an even distribution
      toChooseRacks.addAll(racks.stream()
              .filter(rack -> rackCntMap.getOrDefault(rack, 0)
                      < maxReplicasPerRack).collect(Collectors.toList()));
      if (!skippedRacks.isEmpty()) {
        toChooseRacks.removeAll(skippedRacks);
        toChooseRacks.addAll(0, skippedRacks);
        skippedRacks.clear();
      }

      if (mutableFavoredNodes.size() > 0) {
        List<DatanodeDetails> chosenFavoredNodesInForLoop = new ArrayList<>();
        for (DatanodeDetails favoredNode : mutableFavoredNodes) {
          Node curRack = getRackOfDatanodeDetails(favoredNode);
          if (toChooseRacks.contains(curRack)) {
            chosenNodes.add(favoredNode);
            rackCntMap.merge(curRack, 1, Math::addExact);
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
          rackCntMap.merge(rack, 1, Math::addExact);
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
      maxOuterLoopIterations--;
    }
    return chosenNodes;
  }

  /**
   * Called by SCM to choose datanodes.
   *
   * @param usedNodes - list of the datanodes to already chosen in the
   *                      pipeline.
   * @param excludedNodes - list of the datanodes to exclude.
   * @param favoredNodes - list of nodes preferred. This is a hint to the
   *                     allocator, whether the favored nodes will be used
   *                     depends on whether the nodes meets the allocator's
   *                     requirement.
   * @param nodesRequiredToChoose - number of datanodes required.
   * @param dataSizeRequired - size required for the container.
   * @param metadataSizeRequired - size required for Ratis metadata.
   * @return List of datanodes.
   * @throws SCMException  SCMException
   */
  @Override
  protected List<DatanodeDetails> chooseDatanodesInternal(
          List<DatanodeDetails> usedNodes,
          final List<DatanodeDetails> excludedNodes,
          final List<DatanodeDetails> favoredNodes,
          final int nodesRequiredToChoose, final long metadataSizeRequired,
          final long dataSizeRequired) throws SCMException {
    if (nodesRequiredToChoose <= 0) {
      String errorMsg = "num of nodes required to choose should bigger" +
          "than 0, but the given num is " + nodesRequiredToChoose;
      throw new SCMException(errorMsg, null);
    }
    if (metrics != null) {
      metrics.incrDatanodeRequestCount(nodesRequiredToChoose);
    }
    int nodesRequired = nodesRequiredToChoose;
    int excludedNodesCount = excludedNodes == null ? 0 : excludedNodes.size();
    List<Node> availableNodes = networkTopology.getNodes(
        networkTopology.getMaxLevel());
    int totalNodesCount = availableNodes.size();
    if (excludedNodes != null) {
      availableNodes.removeAll(excludedNodes);
    }
    if (availableNodes.size() < nodesRequired) {
      throw new SCMException("No enough datanodes to choose. " +
          "TotalNode = " + totalNodesCount +
          " AvailableNode = " + availableNodes.size() +
          " RequiredNode = " + nodesRequired +
          " ExcludedNode = " + excludedNodesCount,
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
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

    if (usedNodes == null) {
      usedNodes = Collections.emptyList();
    }
    List<Node> racks = getAllRacks();
    Map<Node, Integer> usedRacksCntMap = usedNodes.stream()
            .map(node -> networkTopology.getAncestor(node, RACK_LEVEL))
            .filter(node -> node != null)
            .collect(Collectors.toMap(Function.identity(), e -> 1,
                    Math::addExact));
    int requiredReplicationFactor = usedNodes.size() + nodesRequired;
    int numberOfRacksRequired = getRequiredRackCount(requiredReplicationFactor);
    int additionalRacksRequired =
            numberOfRacksRequired - usedRacksCntMap.size();
    if (nodesRequired < additionalRacksRequired) {
      String reason = "Required nodes size: " + nodesRequired
              + " is less than required number of racks to choose: "
              + additionalRacksRequired + ".";
      LOG.warn("Placement policy cannot choose the enough racks. {}"
                      + "Total number of Required Racks: {} Used Racks Count:" +
                      " {}, Required Nodes count: {}",
              reason, numberOfRacksRequired, usedRacksCntMap.size(),
              nodesRequired);
      throw new SCMException(reason,
              SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }
    int maxReplicasPerRack = getMaxReplicasPerRack(requiredReplicationFactor,
            numberOfRacksRequired);
    // For excluded nodes, we sort their racks at rear
    racks = sortRackWithExcludedNodes(racks, excludedNodes, usedRacksCntMap);

    List<Node> unavailableNodes = new ArrayList<>();
    if (excludedNodes != null) {
      unavailableNodes.addAll(excludedNodes);
    }
    unavailableNodes.addAll(usedNodes);

    Set<DatanodeDetails> chosenNodes = new LinkedHashSet<>();

    if (racks.size() < additionalRacksRequired) {
      String reason = "Number of existing racks: " + racks.size()
              + "is less than additional required number of racks to choose: "
              + additionalRacksRequired + " do not match.";
      LOG.warn("Placement policy cannot choose the enough racks. {}"
                      + "Total number of Required Racks: {} Used Racks Count:" +
                      " {}, Required Nodes count: {}",
              reason, numberOfRacksRequired, usedRacksCntMap.size(),
              nodesRequired);
      throw new SCMException(reason,
              SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }

    chosenNodes.addAll(chooseNodesFromRacks(racks, unavailableNodes,
            mutableFavoredNodes, additionalRacksRequired,
            Pair.of(metadataSizeRequired, dataSizeRequired), 1,
            Pair.of(usedRacksCntMap, maxReplicasPerRack)));

    if (chosenNodes.size() < additionalRacksRequired) {
      String reason = "Chosen nodes size from Unique Racks: " + chosenNodes
              .size() + ", but required nodes to choose from Unique Racks: "
              + additionalRacksRequired + " do not match.";
      LOG.warn("Placement policy could not choose the enough nodes from " +
                      "available racks."
                      + " {} Available racks count: {}," +
                      " Excluded nodes count: {}",
              reason, racks.size(), excludedNodesCount);
      throw new SCMException(reason,
              SCMException.ResultCodes.FAILED_TO_FIND_HEALTHY_NODES);
    }

    if (chosenNodes.size() < nodesRequired) {
      racks.addAll(usedRacksCntMap.keySet());
      racks = sortRackWithExcludedNodes(racks, excludedNodes, usedRacksCntMap);
      racks.addAll(usedRacksCntMap.keySet());
      chosenNodes.addAll(chooseNodesFromRacks(racks, unavailableNodes,
              mutableFavoredNodes, nodesRequired - chosenNodes.size(),
              Pair.of(metadataSizeRequired, dataSizeRequired),
              Integer.MAX_VALUE, Pair.of(usedRacksCntMap, maxReplicasPerRack)));
    }

    List<DatanodeDetails> result = new ArrayList<>(chosenNodes);

    if (nodesRequiredToChoose != chosenNodes.size()) {
      String reason = "Chosen nodes size: " + chosenNodes
              .size() + ", but required nodes to choose: "
              + nodesRequiredToChoose + " do not match.";
      LOG.warn("Placement policy could not choose the enough nodes."
               + " {} Available nodes count: {}, Excluded nodes count: {}",
              reason, totalNodesCount, excludedNodesCount);
      throw new SCMException(reason,
              SCMException.ResultCodes.FAILED_TO_FIND_HEALTHY_NODES);
    }

    ContainerPlacementStatus placementStatus =
        validateContainerPlacement(
                Stream.of(usedNodes, result)
                      .flatMap(List::stream).collect(Collectors.toList()),
                requiredReplicationFactor);
    if (!placementStatus.isPolicySatisfied()) {
      String errorMsg = "ContainerPlacementPolicy not met. Misreplication" +
              " Reason: " + placementStatus.misReplicatedReason();
      throw new SCMException(errorMsg, null);
    }
    return result;
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
      if (metrics != null) {
        metrics.incrDatanodeChooseAttemptCount();
      }
      Node node = null;
      try {
        node = networkTopology.chooseRandom(scope, excludedNodes);
      } catch (Exception e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Error while choosing Node: Scope: {}, Excluded Nodes: " +
                          "{}, MetaDataSizeRequired: {}, DataSizeRequired: " +
                  "{}", scope, excludedNodes, metadataSizeRequired,
                  dataSizeRequired, e);
        }
      }
      if (node != null) {
        DatanodeDetails datanodeDetails = (DatanodeDetails) node;
        if (isValidNode(datanodeDetails, metadataSizeRequired,
            dataSizeRequired)) {
          if (metrics != null) {
            metrics.incrDatanodeChooseSuccessCount();
          }
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
   * @param usedRacks
   * @return
   */
  private List<Node> sortRackWithExcludedNodes(List<Node> racks,
          List<DatanodeDetails> excludedNodes, Map<Node, Integer> usedRacks) {
    if ((excludedNodes == null || excludedNodes.isEmpty())
            && usedRacks.isEmpty()) {
      return racks;
    }
    Set<Node> lessPreferredRacks = excludedNodes.stream()
        .map(node -> networkTopology.getAncestor(node, RACK_LEVEL))
        // Dead Nodes have been removed from the topology and so have a
        // null rack. We need to exclude those from the rack list.
        .filter(Objects::nonNull)
        .filter(node -> !usedRacks.containsKey(node))
        .collect(Collectors.toSet());
    List <Node> result = new ArrayList<>();
    for (Node rack : racks) {
      if (!usedRacks.containsKey(rack) && !lessPreferredRacks.contains(rack)) {
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
