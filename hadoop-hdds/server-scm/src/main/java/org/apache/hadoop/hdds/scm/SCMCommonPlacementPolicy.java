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


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This policy implements a set of invariants which are common
 * for all basic placement policies, acts as the repository of helper
 * functions which are common to placement policies.
 */
public abstract class SCMCommonPlacementPolicy implements
        PlacementPolicy<ContainerReplica> {
  @VisibleForTesting
  static final Logger LOG =
      LoggerFactory.getLogger(SCMCommonPlacementPolicy.class);
  private final NodeManager nodeManager;

  @SuppressWarnings("java:S2245") // no need for secure random
  private final Random rand = new Random();
  private final ConfigurationSource conf;
  private final boolean shouldRemovePeers;

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
    this.conf = conf;
    this.shouldRemovePeers = ScmUtils.shouldRemovePeers(conf);
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

  @Override
  public final List<DatanodeDetails> chooseDatanodes(
          List<DatanodeDetails> excludedNodes,
          List<DatanodeDetails> favoredNodes, int nodesRequired,
          long metadataSizeRequired,
          long dataSizeRequired) throws SCMException {
    return this.chooseDatanodes(Collections.emptyList(), excludedNodes,
            favoredNodes, nodesRequired, metadataSizeRequired,
            dataSizeRequired);
  }

  /**
   * Null Check for List and returns empty list.
   * @param dns
   * @return Non null List
   */
  private List<DatanodeDetails> validateDatanodes(List<DatanodeDetails> dns) {
    return Objects.isNull(dns) ? Collections.emptyList() :
            dns.stream().map(node -> {
              DatanodeDetails datanodeDetails =
                      nodeManager.getNodeByUuid(node.getUuidString());
              return datanodeDetails != null ? datanodeDetails : node;
            }).collect(Collectors.toList());
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
   * @param usedNodes - datanodes with existing replicas
   * @param excludedNodes - datanodes with failures
   * @param favoredNodes  - list of nodes preferred.
   * @param nodesRequired - number of datanodes required.
   * @param dataSizeRequired - size required for the container.
   * @param metadataSizeRequired - size required for Ratis metadata.
   * @return list of datanodes chosen.
   * @throws SCMException SCM exception.
   */
  @Override
  public final List<DatanodeDetails> chooseDatanodes(
          List<DatanodeDetails> usedNodes,
          List<DatanodeDetails> excludedNodes,
          List<DatanodeDetails> favoredNodes,
          int nodesRequired, long metadataSizeRequired, long dataSizeRequired)
          throws SCMException {
/*
  This method calls the chooseDatanodeInternal after fixing
  the excludeList to get the DatanodeDetails from the node manager.
  When the object of the Class DataNodeDetails is built from protobuf
  only UUID of the datanode is added which is used for the hashcode.
  Thus not passing any information about the topology. While excluding
  datanodes the object is built from protobuf @Link {ExcludeList.java}.
  NetworkTopology removes all nodes from the list which does not fall under
  the scope while selecting a random node. Default scope value is
  "/default-rack/" which won't match the required scope. Thus passing the proper
  object of DatanodeDetails(with Topology Information) while trying to get the
  random node from NetworkTopology should fix this. Check HDDS-7015
 */
    return chooseDatanodesInternal(validateDatanodes(usedNodes),
            validateDatanodes(excludedNodes), favoredNodes, nodesRequired,
            metadataSizeRequired, dataSizeRequired);
  }

  /**
   * Pipeline placement choose datanodes to join the pipeline.
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
  protected List<DatanodeDetails> chooseDatanodesInternal(
      List<DatanodeDetails> usedNodes, List<DatanodeDetails> excludedNodes,
      List<DatanodeDetails> favoredNodes,
      int nodesRequired, long metadataSizeRequired, long dataSizeRequired)
      throws SCMException {
    List<DatanodeDetails> healthyNodes =
        nodeManager.getNodes(NodeStatus.inServiceHealthy());
    if (excludedNodes != null) {
      healthyNodes.removeAll(excludedNodes);
    }
    if (usedNodes != null) {
      healthyNodes.removeAll(usedNodes);
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

    return filterNodesWithSpace(healthyNodes, nodesRequired,
        metadataSizeRequired, dataSizeRequired);
  }

  public List<DatanodeDetails> filterNodesWithSpace(List<DatanodeDetails> nodes,
      int nodesRequired, long metadataSizeRequired, long dataSizeRequired)
      throws SCMException {
    List<DatanodeDetails> nodesWithSpace = nodes.stream().filter(d ->
        hasEnoughSpace(d, metadataSizeRequired, dataSizeRequired))
        .collect(Collectors.toList());

    if (nodesWithSpace.size() < nodesRequired) {
      String msg = String.format("Unable to find enough nodes that meet the " +
              "space requirement of %d bytes for metadata and %d bytes for " +
              "data in healthy node set. Required %d. Found %d.",
          metadataSizeRequired, dataSizeRequired, nodesRequired,
          nodesWithSpace.size());
      LOG.error(msg);
      throw new SCMException(msg,
          SCMException.ResultCodes.FAILED_TO_FIND_NODES_WITH_SPACE);
    }

    return nodesWithSpace;
  }

  /**
   * Returns true if this node has enough space to meet our requirement.
   *
   * @param datanodeDetails DatanodeDetails
   * @return true if we have enough space.
   */
  public static boolean hasEnoughSpace(DatanodeDetails datanodeDetails,
      long metadataSizeRequired, long dataSizeRequired) {
    Preconditions.checkArgument(datanodeDetails instanceof DatanodeInfo);

    boolean enoughForData = false;
    boolean enoughForMeta = false;

    DatanodeInfo datanodeInfo = (DatanodeInfo) datanodeDetails;

    if (dataSizeRequired > 0) {
      for (StorageReportProto reportProto : datanodeInfo.getStorageReports()) {
        if (reportProto.getRemaining() > dataSizeRequired) {
          enoughForData = true;
          break;
        }
      }
    } else {
      enoughForData = true;
    }

    if (!enoughForData) {
      return false;
    }

    if (metadataSizeRequired > 0) {
      for (MetadataStorageReportProto reportProto
          : datanodeInfo.getMetadataStorageReports()) {
        if (reportProto.getRemaining() > metadataSizeRequired) {
          enoughForMeta = true;
          break;
        }
      }
    } else {
      enoughForMeta = true;
    }

    return enoughForData && enoughForMeta;
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
        removePeers(nodeId, healthyNodes);
        results.add(nodeId);
        healthyNodes.remove(nodeId);
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
   *
   * @param numReplicas - The desired replica counts
   * @return The number of racks containers should span to meet the policy
   */
  protected int getRequiredRackCount(int numReplicas) {
    return 1;
  }

  /**
   * Default implementation to return the max number of replicas per rack.
   * For simple policies that are not rack aware
   * we return numReplicas, from this default implementation.
   *
   * @param numReplicas - The desired replica counts
   * @param numberOfRacks - The desired number of racks
   * @return The max number of replicas per rack
   */
  protected int getMaxReplicasPerRack(int numReplicas, int numberOfRacks) {
    return numReplicas / numberOfRacks
            + Math.min(numReplicas % numberOfRacks, 1);
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
    // We have a network topology so calculate if it is satisfied or not.
    int requiredRacks = getRequiredRackCount(replicas);
    if (topology == null || replicas == 1 || requiredRacks == 1) {
      if (dns.size() > 0) {
        // placement is always satisfied if there is at least one DN.
        return validPlacement;
      } else {
        return invalidPlacement;
      }
    }
    Map<Node, Long> currentRackCount = dns.stream()
            .collect(Collectors.groupingBy(this::getPlacementGroup,
                    Collectors.counting()));
    final int maxLevel = topology.getMaxLevel();
    // The leaf nodes are all at max level, so the number of nodes at
    // leafLevel - 1 is the rack count
    int numRacks = topology.getNumOfNodes(maxLevel - 1);
    if (replicas < requiredRacks) {
      requiredRacks = replicas;
    }
    int maxReplicasPerRack = getMaxReplicasPerRack(replicas,
            Math.min(requiredRacks, numRacks));
    return new ContainerPlacementStatusDefault(
        currentRackCount.size(), requiredRacks, numRacks, maxReplicasPerRack,
            currentRackCount.values().stream().map(Long::intValue)
                    .collect(Collectors.toList()));
  }

  /**
   * Removes the datanode peers from all the existing pipelines for this dn.
   */
  public void removePeers(DatanodeDetails dn,
      List<DatanodeDetails> healthyList) {
    if (shouldRemovePeers) {
      healthyList.removeAll(nodeManager.getPeerList(dn));
    }
  }

  /**
   * Check If a datanode is an available node.
   * @param datanodeDetails - the datanode to check.
   * @param metadataSizeRequired - the required size for metadata.
   * @param dataSizeRequired - the required size for data.
   * @return true if the datanode is available.
   */
  public boolean isValidNode(DatanodeDetails datanodeDetails,
      long metadataSizeRequired, long dataSizeRequired) {
    DatanodeInfo datanodeInfo = (DatanodeInfo)getNodeManager()
        .getNodeByUuid(datanodeDetails.getUuidString());
    if (datanodeInfo == null) {
      LOG.error("Failed to find the DatanodeInfo for datanode {}",
          datanodeDetails);
    } else {
      if (datanodeInfo.getNodeStatus().isNodeWritable() &&
          (hasEnoughSpace(datanodeInfo, metadataSizeRequired,
              dataSizeRequired))) {
        LOG.debug("Datanode {} is chosen. Required metadata size is {} and " +
                "required data size is {}",
            datanodeDetails, metadataSizeRequired, dataSizeRequired);
        return true;
      }
    }
    return false;
  }

  /**
   * Given a set of replicas of a container which are
   * neither over underreplicated nor overreplicated,
   * return a set of replicas to copy to another node to fix misreplication.
   * @param replicas
   */
  @Override
  public Set<ContainerReplica> replicasToCopyToFixMisreplication(
         Set<ContainerReplica> replicas) {
    Map<Node, List<ContainerReplica>> placementGroupReplicaIdMap
            = replicas.stream().collect(Collectors.groupingBy(replica ->
            this.getPlacementGroup(replica.getDatanodeDetails())));

    int totalNumberOfReplicas = replicas.size();
    int requiredNumberOfPlacementGroups =
            getRequiredRackCount(totalNumberOfReplicas);
    Set<ContainerReplica> copyReplicaSet = Sets.newHashSet();
    List<List<ContainerReplica>> replicaSet = placementGroupReplicaIdMap
            .values().stream()
            .sorted((o1, o2) -> Integer.compare(o2.size(), o1.size()))
            .limit(requiredNumberOfPlacementGroups)
            .collect(Collectors.toList());
    for (List<ContainerReplica> replicaList: replicaSet) {
      int maxReplicasPerPlacementGroup = getMaxReplicasPerRack(
              totalNumberOfReplicas, requiredNumberOfPlacementGroups);
      int numberOfReplicasToBeCopied = Math.max(0,
              replicaList.size() - maxReplicasPerPlacementGroup);
      totalNumberOfReplicas -= maxReplicasPerPlacementGroup;
      requiredNumberOfPlacementGroups -= 1;
      if (numberOfReplicasToBeCopied > 0) {
        List<ContainerReplica> replicasToBeCopied = replicaList.stream()
                .limit(numberOfReplicasToBeCopied)
                .collect(Collectors.toList());
        copyReplicaSet.addAll(replicasToBeCopied);
      }
    }
    return copyReplicaSet;
  }

  protected Node getPlacementGroup(DatanodeDetails dn) {
    return nodeManager.getClusterNetworkTopologyMap().getAncestor(dn, 1);
  }
}
