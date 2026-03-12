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

package org.apache.hadoop.hdds.scm;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
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
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.ozone.container.common.volume.VolumeUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This policy implements a set of invariants which are common
 * for all basic placement policies, acts as the repository of helper
 * functions which are common to placement policies.
 */
public abstract class SCMCommonPlacementPolicy implements
        PlacementPolicy {
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
   * This is an empty list which is passed to the placement policy when the
   * old interface is called that does not pass usedNodes. The sub-classes
   * can then call usedNodesPassed to determine if the usedNodes list was passed
   * or intentionally set to an empty list. There is logic in at least the
   * RackAwarePlacementPolicy that needs to know if the old or new interface is
   * used to be able to use the node lists correctly.
   */
  private static final List<DatanodeDetails> UNSET_USED_NODES
      = Collections.unmodifiableList(new ArrayList<>());

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
    return this.chooseDatanodes(UNSET_USED_NODES, excludedNodes,
          favoredNodes, nodesRequired, metadataSizeRequired,
          dataSizeRequired);
  }

  /**
   * For each node in the list, lookup the in memory object in node manager and
   * if it exists, swap the passed node with the in memory node. Then return
   * the list.
   * When the object of the Class DataNodeDetails is built from protobuf
   * only UUID of the datanode is added which is used for the hashcode.
   * Thus, not passing any information about the topology.
   * @param dns
   * @return Non null List
   */
  private List<DatanodeDetails> validateDatanodes(List<DatanodeDetails> dns) {
    if (Objects.isNull(dns)) {
      return Collections.emptyList();
    }
    for (int i = 0; i < dns.size(); i++) {
      DatanodeDetails node = dns.get(i);
      final DatanodeDetails datanodeDetails = nodeManager.getNode(node.getID());
      if (datanodeDetails != null) {
        dns.set(i, datanodeDetails);
      }
    }
    return dns;
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
    if (healthyNodes.isEmpty()) {
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

  /**
   * Give a List of DatanodeDetails representing the usedNodes, check if the
   * list matches the UNSET_USED_NODES list. If it does, then the usedNodes are
   * not passed by the caller, otherwise they were passed.
   * @param list List of datanodeDetails to check
   * @return true if the passed list is not the UNSET_USED_NODES list
   */
  protected boolean usedNodesPassed(List<DatanodeDetails> list) {
    if (list == null) {
      return true;
    }
    return list != UNSET_USED_NODES;
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
      LOG.warn(msg);
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
                                       long metadataSizeRequired,
                                       long dataSizeRequired) {
    Preconditions.checkArgument(datanodeDetails instanceof DatanodeInfo);

    boolean enoughForData = false;
    boolean enoughForMeta = false;

    DatanodeInfo datanodeInfo = (DatanodeInfo) datanodeDetails;

    if (dataSizeRequired > 0) {
      for (StorageReportProto reportProto : datanodeInfo.getStorageReports()) {
        if (VolumeUsage.getUsableSpace(reportProto) > dataSizeRequired) {
          enoughForData = true;
          break;
        }
      }
    } else {
      enoughForData = true;
    }

    if (!enoughForData) {
      LOG.debug("Datanode {} has no volumes with enough space to allocate {} " +
              "bytes for data.", datanodeDetails, dataSizeRequired);
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
    if (!enoughForMeta) {
      LOG.debug("Datanode {} has no volumes with enough space to allocate {} " +
              "bytes for metadata.", datanodeDetails, metadataSizeRequired);
    }
    return enoughForMeta;
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
   * @param excludedRackCount - The number of racks excluded due to containing
   *                          only excluded nodes. The total racks on the
   *                          cluster will be reduced by this number.
   * @return The number of racks containers should span to meet the policy
   */
  protected int getRequiredRackCount(int numReplicas, int excludedRackCount) {
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
    int requiredRacks = getRequiredRackCount(replicas, 0);
    if (topology == null || replicas == 1 || requiredRacks == 1) {
      if (!dns.isEmpty()) {
        // placement is always satisfied if there is at least one DN.
        return validPlacement;
      } else {
        return invalidPlacement;
      }
    }
    List<Integer> currentRackCount = new ArrayList<>(dns.stream()
        .map(dn -> {
          Node rack = getPlacementGroup(dn);
          if (rack == null) {
            try {
              NodeStatus nodeStatus = nodeManager.getNodeStatus(dn);
              if (nodeStatus.isDead() && nodeStatus.isMaintenance()) {
                LOG.debug("Using rack [{}] for dead and in-maintenance dn {}.", dn.getNetworkLocation(), dn);
                return dn.getNetworkLocation();
              }
              return null;
            } catch (NodeNotFoundException e) {
              LOG.debug("Could not get NodeStatus for dn {}.", dn, e);
              return null;
            }
          }
          /*
          data-centre/rack1/dn1. Here, data-centre/rack1 is the network location of dn1 and data-centre/rack1 is also
          the network full path of rack1.
          */
          return rack.getNetworkFullPath();
        })
        .filter(Objects::nonNull)
        .collect(Collectors.groupingBy(
            Function.identity(),
            Collectors.reducing(0, e -> 1, Integer::sum)))
        .values());
    final int maxLevel = topology.getMaxLevel();
    // The leaf nodes are all at max level, so the number of nodes at
    // leafLevel - 1 is the rack count
    int numRacks = topology.getNumOfNodes(maxLevel - 1);
    if (replicas < requiredRacks) {
      requiredRacks = replicas;
    }
    int maxReplicasPerRack = getMaxReplicasPerRack(replicas,
            Math.min(requiredRacks, numRacks));

    // There are scenarios where there could be excessive replicas on a rack due to nodes
    // in decommission or maintenance or over-replication in general.
    // In these cases, ReplicationManager shouldn't report mis-replication.
    // The original intention of mis-replication was to indicate that there isn't enough rack tolerance.
    // In case of over-replication, this isn't an issue.
    // Mis-replication should be an issue once over-replication is fixed, not before.
    // Adjust the maximum number of replicas per rack to allow containers
    // with excessive replicas to not be reported as mis-replicated.
    maxReplicasPerRack += Math.max(0, dns.size() - replicas);
    return new ContainerPlacementStatusDefault(
        currentRackCount.size(), requiredRacks, numRacks, maxReplicasPerRack,
            currentRackCount);
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
        .getNode(datanodeDetails.getID());
    if (datanodeInfo == null) {
      LOG.error("Failed to find the DatanodeInfo for datanode {}",
          datanodeDetails);
      return false;
    }
    NodeStatus nodeStatus = datanodeInfo.getNodeStatus();
    if (nodeStatus.isNodeWritable() && (hasEnoughSpace(datanodeInfo, metadataSizeRequired, dataSizeRequired))) {
      LOG.debug("Datanode {} is chosen. Required metadata size is {} and " +
              "required data size is {} and NodeStatus is {}",
          datanodeDetails, metadataSizeRequired, dataSizeRequired, nodeStatus);
      return true;
    }
    LOG.info("Datanode {} is not chosen. Required metadata size is {} and " +
            "required data size is {} and NodeStatus is {}",
        datanodeDetails, metadataSizeRequired, dataSizeRequired, nodeStatus);
    return false;
  }

  /**
   * Given a set of replicas of a container which are
   * neither over underreplicated nor overreplicated,
   * return a set of replicas to copy to another node to fix misreplication.
   * @param replicas Map of replicas with value signifying if
   *                  replica can be copied
   */
  @Override
  public Set<ContainerReplica> replicasToCopyToFixMisreplication(
         Map<ContainerReplica, Boolean> replicas) {
    Map<Node, List<ContainerReplica>> placementGroupReplicaIdMap
            = replicas.keySet().stream()
            .collect(Collectors.groupingBy(replica ->
                    getPlacementGroup(replica.getDatanodeDetails())));

    int totalNumberOfReplicas = replicas.size();
    int requiredNumberOfPlacementGroups =
            getRequiredRackCount(totalNumberOfReplicas, 0);
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
                .filter(replicas::get)
                .limit(numberOfReplicasToBeCopied)
                .collect(Collectors.toList());
        if (numberOfReplicasToBeCopied > replicasToBeCopied.size()) {
          Node rack = !replicaList.isEmpty() ? this.getPlacementGroup(
                  replicaList.get(0).getDatanodeDetails()) : null;
          LOG.warn("Not enough copyable replicas available in rack {}. " +
                  "Required number of Replicas to be copied: {}." +
                  " Available Replicas to be copied: {}",
                  rack, numberOfReplicasToBeCopied,
                  replicasToBeCopied.size());
        }
        copyReplicaSet.addAll(replicasToBeCopied);
      }
    }
    return copyReplicaSet;
  }

  protected Node getPlacementGroup(DatanodeDetails dn) {
    return nodeManager.getClusterNetworkTopologyMap().getAncestor(dn, 1);
  }

  /**
   * Given a set of replicas, expectedCount for Each replica,
   * number of unique replica indexes. Replicas to be deleted for fixing over
   * replication is computed.
   * The algorithm starts with creating a replicaIdMap which contains the
   * replicas grouped by replica Index. A placementGroup Map is created which
   * groups replicas based on their rack and the replicas within the rack
   * are further grouped based on the replica Index.
   * A placement Group Count Map is created which keeps
   * track of the count of replicas in each rack.
   * We iterate through overreplicated replica indexes sorted in descending
   * order based on their current replication factor in a descending factor.
   * For each replica Index the replica is removed from the rack which contains
   * the most replicas, in order to achieve this the racks are put
   * into priority queue and are based on the number of replicas they have.
   * The replica is removed from the rack with maximum replicas and the replica
   * to be removed is also removed from the maps created above and
   * the count for rack is reduced.
   * The set of replicas computed are then returned by the function.
   * @param replicas Set of existing replicas of the container
   * @param expectedCountPerUniqueReplica Replication factor of each
   *    *                                     unique replica
   * @return Set of replicas to be removed are computed.
   */
  @Override
  public Set<ContainerReplica> replicasToRemoveToFixOverreplication(
          Set<ContainerReplica> replicas, int expectedCountPerUniqueReplica) {
    Map<Integer, Set<ContainerReplica>> replicaIdMap = new HashMap<>();
    Map<Node, Map<Integer, Set<ContainerReplica>>> placementGroupReplicaIdMap
            = new HashMap<>();
    Map<Node, Integer> placementGroupCntMap = new HashMap<>();
    for (ContainerReplica replica:replicas) {
      Integer replicaId = replica.getReplicaIndex();
      Node placementGroup = getPlacementGroup(replica.getDatanodeDetails());
      replicaIdMap.computeIfAbsent(replicaId, (rid) -> Sets.newHashSet())
              .add(replica);
      placementGroupCntMap.compute(placementGroup,
              (group, cnt) -> (cnt == null ? 0 : cnt) + 1);
      placementGroupReplicaIdMap.computeIfAbsent(placementGroup,
              (pg) -> Maps.newHashMap()).computeIfAbsent(replicaId, (rid) ->
                      Sets.newHashSet()).add(replica);
    }

    Set<ContainerReplica> replicasToRemove = new HashSet<>();
    List<Integer> sortedRIDList = replicaIdMap.keySet().stream()
            .filter(rid -> replicaIdMap.get(rid).size() >
                    expectedCountPerUniqueReplica)
            .sorted((o1, o2) -> Integer.compare(replicaIdMap.get(o2).size(),
                    replicaIdMap.get(o1).size()))
            .collect(Collectors.toList());
    for (Integer rid : sortedRIDList) {
      if (replicaIdMap.get(rid).size() <= expectedCountPerUniqueReplica) {
        break;
      }
      Queue<Node> pq = new PriorityQueue<>((o1, o2) ->
              Integer.compare(placementGroupCntMap.get(o2),
                      placementGroupCntMap.get(o1)));
      pq.addAll(placementGroupReplicaIdMap.entrySet()
              .stream()
              .filter(nodeMapEntry -> nodeMapEntry.getValue().containsKey(rid))
              .map(Map.Entry::getKey)
              .collect(Collectors.toList()));

      while (replicaIdMap.get(rid).size() > expectedCountPerUniqueReplica) {
        Node rack = pq.poll();
        Set<ContainerReplica> replicaSet =
                placementGroupReplicaIdMap.get(rack).get(rid);
        if (!replicaSet.isEmpty()) {
          ContainerReplica r = replicaSet.stream().findFirst().get();
          replicasToRemove.add(r);
          replicaSet.remove(r);
          replicaIdMap.get(rid).remove(r);
          placementGroupCntMap.compute(rack,
                  (group, cnt) -> (cnt == null ? 0 : cnt) - 1);
          if (replicaSet.isEmpty()) {
            placementGroupReplicaIdMap.get(rack).remove(rid);
          } else {
            pq.add(rack);
          }
        }
      }
    }
    return replicasToRemove;
  }
}
