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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Find a target giving preference to more under-utilized nodes.
 */
public class FindTargetGreedy implements FindTargetStrategy {
  public static final Logger LOG =
      LoggerFactory.getLogger(FindTargetGreedy.class);

  private ContainerManager containerManager;
  private PlacementPolicy placementPolicy;

  public FindTargetGreedy(
      ContainerManager containerManager,
      PlacementPolicy placementPolicy) {
    this.containerManager = containerManager;
    this.placementPolicy = placementPolicy;
  }

  /**
   * Find a {@link ContainerMoveSelection} consisting of a target and
   * container to move for a source datanode. Favours more under-utilized nodes.
   * @param source Datanode to find a target for
   * @param potentialTargets Collection of potential target datanodes
   * @param candidateContainers Set of candidate containers satisfying
   *                            selection criteria
   *                            {@link ContainerBalancerSelectionCriteria}
   * @param canSizeEnterTarget A functional interface whose apply
   * (DatanodeDetails, Long) method returns true if the size specified in the
   * second argument can enter the specified DatanodeDetails node
   * @return Found target and container
   */
  @Override
  public ContainerMoveSelection findTargetForContainerMove(
      DatanodeDetails source, Collection<DatanodeDetails> potentialTargets,
      Set<ContainerID> candidateContainers,
      BiFunction<DatanodeDetails, Long, Boolean> canSizeEnterTarget) {
    for (DatanodeDetails target : potentialTargets) {
      for (ContainerID container : candidateContainers) {
        Set<ContainerReplica> replicas;
        ContainerInfo containerInfo;

        try {
          replicas = containerManager.getContainerReplicas(container);
          containerInfo = containerManager.getContainer(container);
        } catch (ContainerNotFoundException e) {
          LOG.warn("Could not get Container {} from Container Manager for " +
              "obtaining replicas in Container Balancer.", container, e);
          continue;
        }

        if (replicas.stream().noneMatch(
            replica -> replica.getDatanodeDetails().equals(target)) &&
            containerMoveSatisfiesPlacementPolicy(container, replicas, source,
            target) &&
            canSizeEnterTarget.apply(target, containerInfo.getUsedBytes())) {
          return new ContainerMoveSelection(target, container);
        }
      }
    }
    LOG.debug("Container Balancer could not find a target for source" +
          " datanode {}", source.getUuidString());
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
  @Override
  public boolean containerMoveSatisfiesPlacementPolicy(
      ContainerID containerID, Set<ContainerReplica> replicas,
      DatanodeDetails source, DatanodeDetails target) {
    ContainerInfo containerInfo;
    try {
      containerInfo = containerManager.getContainer(containerID);
    } catch (ContainerNotFoundException e) {
      LOG.warn("Could not get Container {} from Container Manager while " +
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
    ContainerPlacementStatus placementStatus =
        placementPolicy.validateContainerPlacement(replicaList,
        containerInfo.getReplicationConfig().getRequiredNodes());

    if (placementStatus.isPolicySatisfied()) {
      return true;
    } else {
      LOG.debug("Moving container {} from source datanode {} to target " +
              "datanode {} does not satisfy placement policy", containerID,
          source.getUuidString(), target.getUuidString());
      return false;
    }
  }
}
