/*
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

package org.apache.hadoop.hdds.scm.container.balancer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicyValidateProxy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Find a target for a source datanode with greedy strategy.
 */
public abstract class AbstractFindTargetGreedy implements FindTargetStrategy {
  private Logger logger;
  private ContainerManager containerManager;
  private PlacementPolicyValidateProxy placementPolicyValidateProxy;
  private Map<DatanodeDetails, Long> sizeEnteringNode;
  private NodeManager nodeManager;
  private ContainerBalancerConfiguration config;
  private Double upperLimit;
  private Collection<DatanodeUsageInfo> potentialTargets;

  protected AbstractFindTargetGreedy(
      ContainerManager containerManager,
      PlacementPolicyValidateProxy placementPolicyValidateProxy,
      NodeManager nodeManager) {
    sizeEnteringNode = new HashMap<>();
    this.containerManager = containerManager;
    this.placementPolicyValidateProxy = placementPolicyValidateProxy;
    this.nodeManager = nodeManager;
  }

  protected void setLogger(Logger log) {
    logger = log;
  }

  protected void setPotentialTargets(Collection<DatanodeUsageInfo> pt) {
    potentialTargets = pt;
  }

  private void setUpperLimit(Double upperLimit) {
    this.upperLimit = upperLimit;
  }

  protected int compareByUsage(DatanodeUsageInfo a, DatanodeUsageInfo b) {
    double currentUsageOfA = a.calculateUtilization(
        sizeEnteringNode.get(a.getDatanodeDetails()));
    double currentUsageOfB = b.calculateUtilization(
        sizeEnteringNode.get(b.getDatanodeDetails()));
    int ret = Double.compare(currentUsageOfA, currentUsageOfB);
    if (ret != 0) {
      return ret;
    }
    UUID uuidA = a.getDatanodeDetails().getUuid();
    UUID uuidB = b.getDatanodeDetails().getUuid();
    return uuidA.compareTo(uuidB);
  }

  private void setConfiguration(ContainerBalancerConfiguration conf) {
    config = conf;
  }

  /**
   * Find a {@link ContainerMoveSelection} consisting of a target and
   * container to move for a source datanode. Favours more under-utilized nodes.
   * @param source Datanode to find a target for
   * @param candidateContainers Set of candidate containers satisfying
   *                            selection criteria
   *                            {@link ContainerBalancerSelectionCriteria}
   * (DatanodeDetails, Long) method returns true if the size specified in the
   * second argument can enter the specified DatanodeDetails node
   * @return Found target and container
   */
  @Override
  public ContainerMoveSelection findTargetForContainerMove(
      DatanodeDetails source, Set<ContainerID> candidateContainers) {
    sortTargetForSource(source);
    for (DatanodeUsageInfo targetInfo : potentialTargets) {
      DatanodeDetails target = targetInfo.getDatanodeDetails();
      for (ContainerID container : candidateContainers) {
        Set<ContainerReplica> replicas;
        ContainerInfo containerInfo;
        try {
          replicas = containerManager.getContainerReplicas(container);
          containerInfo = containerManager.getContainer(container);
        } catch (ContainerNotFoundException e) {
          logger.warn("Could not get Container {} from Container Manager for " +
              "obtaining replicas in Container Balancer.", container, e);
          continue;
        }

        if (replicas.stream().noneMatch(
            replica -> replica.getDatanodeDetails().equals(target)) &&
            containerMoveSatisfiesPlacementPolicy(container, replicas, source,
            target) &&
            canSizeEnterTarget(target, containerInfo.getUsedBytes())) {
          return new ContainerMoveSelection(target, container);
        }
      }
    }
    logger.info("Container Balancer could not find a target for " +
        "source datanode {}", source.getUuidString());
    return null;
  }

  /**
   * Checks if container being present in target instead of source satisfies
   * the placement policy.
   * @param containerID Container to be moved from source to target
   * @param replicas Set of replicas of the given container
   * @param source Source datanode for container move
   * @param target Target datanode for container move
   * @return true if placement policy is satisfied, otherwise false
   */
  private boolean containerMoveSatisfiesPlacementPolicy(
      ContainerID containerID, Set<ContainerReplica> replicas,
      DatanodeDetails source, DatanodeDetails target) {
    ContainerInfo containerInfo;
    try {
      containerInfo = containerManager.getContainer(containerID);
    } catch (ContainerNotFoundException e) {
      logger.warn("Could not get Container {} from Container Manager while " +
          "checking if container move satisfies placement policy in " +
          "Container Balancer.", containerID.toString(), e);
      return false;
    }
    List<DatanodeDetails> replicaList =
        replicas.stream()
            .map(ContainerReplica::getDatanodeDetails)
            .filter(datanodeDetails -> !datanodeDetails.equals(source))
            .collect(Collectors.toList());
    replicaList.add(target);
    ContainerPlacementStatus placementStatus = placementPolicyValidateProxy
        .validateContainerPlacement(replicaList, containerInfo);
    return placementStatus.isPolicySatisfied();
  }

  /**
   * Checks if specified size can enter specified target datanode
   * according to {@link ContainerBalancerConfiguration}
   * "size.entering.target.max".
   *
   * @param target target datanode in which size is entering
   * @param size   size in bytes
   * @return true if size can enter target, else false
   */
  private boolean canSizeEnterTarget(DatanodeDetails target, long size) {
    if (sizeEnteringNode.containsKey(target)) {
      long sizeEnteringAfterMove = sizeEnteringNode.get(target) + size;
      //size can be moved into target datanode only when the following
      //two condition are met.
      //1 sizeEnteringAfterMove does not succeed the configured
      // MaxSizeEnteringTarget
      //2 current usage of target datanode plus sizeEnteringAfterMove
      // is smaller than or equal to upperLimit
      return sizeEnteringAfterMove <= config.getMaxSizeEnteringTarget() &&
          Double.compare(nodeManager.getUsageInfo(target)
              .calculateUtilization(sizeEnteringAfterMove), upperLimit) <= 0;
    }
    return false;
  }

  /**
   * increase the Entering size of a candidate target data node.
   */
  @Override
  public void increaseSizeEntering(DatanodeDetails target, long size) {
    if (sizeEnteringNode.containsKey(target)) {
      long totalEnteringSize = sizeEnteringNode.get(target) + size;
      sizeEnteringNode.put(target, totalEnteringSize);
      potentialTargets.removeIf(
          c -> c.getDatanodeDetails().equals(target));
      if (totalEnteringSize < config.getMaxSizeEnteringTarget()) {
        //reorder
        potentialTargets.add(nodeManager.getUsageInfo(target));
      }
      return;
    }
    logger.warn("Cannot find {} in the candidates target nodes",
        target.getUuid());
  }

  /**
   * reInitialize FindTargetStrategy with the given new parameters.
   */
  @Override
  public void reInitialize(List<DatanodeUsageInfo> potentialDataNodes,
                           ContainerBalancerConfiguration conf,
                           Double upLimit) {
    setConfiguration(conf);
    setUpperLimit(upLimit);
    sizeEnteringNode.clear();
    resetTargets(potentialDataNodes);
  }

  @VisibleForTesting
  public Collection<DatanodeUsageInfo> getPotentialTargets() {
    return potentialTargets;
  }

  /**
   * sort potentialTargets for specified source datanode according to
   * network topology if enabled.
   * @param source the specified source datanode
   */
  @VisibleForTesting
  public abstract void sortTargetForSource(DatanodeDetails source);

  /**
   * Resets the collection of potential target datanodes that are considered
   * to identify a target for a source.
   * @param targets potential targets
   */
  void resetTargets(Collection<DatanodeUsageInfo> targets) {
    potentialTargets.clear();
    targets.forEach(datanodeUsageInfo -> {
      sizeEnteringNode.putIfAbsent(datanodeUsageInfo.getDatanodeDetails(), 0L);
      potentialTargets.add(datanodeUsageInfo);
    });
  }

  NodeManager getNodeManager() {
    return nodeManager;
  }

}
