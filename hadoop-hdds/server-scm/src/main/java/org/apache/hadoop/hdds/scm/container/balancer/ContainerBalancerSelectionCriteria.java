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

package org.apache.hadoop.hdds.scm.container.balancer;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The selection criteria for selecting containers that will be moved and
 * selecting datanodes that containers will move to.
 */
public class ContainerBalancerSelectionCriteria {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerBalancerSelectionCriteria.class);

  private ContainerBalancerConfiguration balancerConfiguration;
  private NodeManager nodeManager;
  private ReplicationManager replicationManager;
  private ContainerManager containerManager;
  private Map<ContainerID, DatanodeDetails> containerToSourceMap;
  private Set<ContainerID> excludeContainers;
  private Set<ContainerID> includeContainers;
  private Set<ContainerID> excludeContainersDueToFailure;
  private FindSourceStrategy findSourceStrategy;
  private Map<DatanodeDetails, NavigableSet<ContainerID>> setMap;

  public ContainerBalancerSelectionCriteria(
      ContainerBalancerConfiguration balancerConfiguration,
      NodeManager nodeManager,
      ReplicationManager replicationManager,
      ContainerManager containerManager,
      FindSourceStrategy findSourceStrategy,
      Map<ContainerID, DatanodeDetails> containerToSourceMap) {
    this.balancerConfiguration = balancerConfiguration;
    this.nodeManager = nodeManager;
    this.replicationManager = replicationManager;
    this.containerManager = containerManager;
    this.containerToSourceMap = containerToSourceMap;
    excludeContainersDueToFailure = new HashSet<>();
    excludeContainers = balancerConfiguration.getExcludeContainers();
    includeContainers = balancerConfiguration.getIncludeContainers();
    this.findSourceStrategy = findSourceStrategy;
    this.setMap = new HashMap<>();
  }

  /**
   * Checks whether container is currently undergoing replication or deletion by checking if there's an add or delete
   * scheduled for it.
   *
   * @param containerID Container to check.
   * @return true if container is replicating or deleting, otherwise false.
   */
  private boolean isContainerReplicatingOrDeleting(ContainerID containerID) {
    return replicationManager.isContainerReplicatingOrDeleting(containerID);
  }

  /**
   * Get ContainerID Set for the Datanode, it will be returned as NavigableSet
   * Since sorting will be time-consuming, the Set will be cached.
   *
   * @param node source datanode
   * @return cached Navigable ContainerID Set
   */
  public Set<ContainerID> getContainerIDSet(DatanodeDetails node) {
    // Check if the node is registered at the beginning
    if (!nodeManager.isNodeRegistered(node)) {
      return Collections.emptySet();
    }
    Set<ContainerID> containers = setMap.computeIfAbsent(node,
        this::getCandidateContainers);
    return containers != null ? containers : Collections.emptySet();
  }

  /**
   * Checks if the first container has more used space than second.
   * @param first first container to compare
   * @param second second container to compare
   * @return An integer greater than 0 if first is more used, 0 if they're
   * the same containers or a container is not found, and a value less than 0
   * if first is less used than second. If both containers have equal used
   * space, they're compared using {@link ContainerID#compareTo(ContainerID)}.
   */
  private int isContainerMoreUsed(ContainerID first,
                                  ContainerID second) {
    if (first.equals(second)) {
      return 0;
    }
    try {
      ContainerInfo firstInfo = containerManager.getContainer(first);
      ContainerInfo secondInfo = containerManager.getContainer(second);
      if (firstInfo.getUsedBytes() > secondInfo.getUsedBytes()) {
        return 1;
      } else if (firstInfo.getUsedBytes() < secondInfo.getUsedBytes()) {
        return -1;
      } else {
        return first.compareTo(second);
      }
    } catch (ContainerNotFoundException e) {
      LOG.warn("Could not retrieve ContainerInfo from container manager for " +
          "comparison.", e);
      return 0;
    }
  }

  /**
   * Compares containers on the basis of used space.
   * @return First container is more used if it has used space greater than
   * second container.
   */
  private Comparator<ContainerID> orderContainersByUsedBytes() {
    return this::isContainerMoreUsed;
  }

  /**
   * Gets containers that are suitable for moving based on the following
   * required criteria:
   * 1. Container must not be undergoing replication.
   * 2. Container must not already be selected for balancing.
   * 3. Container size should be closer to 5GB.
   * 4. Container must not be in the configured exclude containers list.
   * 5. Container should be closed.
   * @param node DatanodeDetails for which to find candidate containers.
   * @return true if the container should be excluded, else false
   */
  public boolean shouldBeExcluded(ContainerID containerID,
      DatanodeDetails node, long sizeMovedAlready) {
    ContainerInfo container;
    //if any container is excluded only those should be used
    if (!includeContainers.isEmpty() && !includeContainers.contains(containerID)) {
      return true; // Exclude if not in include list
    }
    try {
      container = containerManager.getContainer(containerID);
    } catch (ContainerNotFoundException e) {
      LOG.warn("Could not find Container {} to check if it should be a " +
          "candidate container. Excluding it.", containerID);
      return true;
    }

    if (excludeContainers.contains(containerID) ||
        excludeContainersDueToFailure.contains(containerID) ||
        containerToSourceMap.containsKey(containerID) ||
        !findSourceStrategy.canSizeLeaveSource(node, container.getUsedBytes())
        || breaksMaxSizeToMoveLimit(container.containerID(),
        container.getUsedBytes(), sizeMovedAlready)) {
      return true;
    }

    Set<ContainerReplica> replicas;
    try {
      replicas = containerManager.getContainerReplicas(container.containerID());
    } catch (ContainerNotFoundException e) {
      LOG.warn("Container {} does not exist in ContainerManager. Skipping " +
          "this container.", container.getContainerID(), e);
      return true;
    }

    return !isContainerClosed(container, node, replicas) ||
        !isContainerHealthyForMove(container, replicas) ||
        isContainerReplicatingOrDeleting(containerID);
  }

  /**
   * Checks whether specified container is closed. Also checks if the replica
   * on the specified datanode is CLOSED. Assumes that there will only be one
   * replica of a container on a particular Datanode.
   * @param container container to check
   * @param datanodeDetails datanode on which a replica of the container is
   * present
   * @return true if container LifeCycleState is
   * {@link HddsProtos.LifeCycleState#CLOSED} and its replica on the
   * specified datanode is CLOSED, else false
   */
  private boolean isContainerClosed(ContainerInfo container,
                                    DatanodeDetails datanodeDetails,
                                    Set<ContainerReplica> replicas) {
    if (!container.getState().equals(HddsProtos.LifeCycleState.CLOSED)) {
      return false;
    }

    for (ContainerReplica replica : replicas) {
      if (replica.getDatanodeDetails().equals(datanodeDetails)) {
        // don't consider replica if it's not closed
        // assumption: there's only one replica of this container on this DN
        return replica.getState().equals(ContainerReplicaProto.State.CLOSED);
      }
    }

    return false;
  }

  /**
   * This asks replication manager whether a container is under/over/mis replicated. The intention is the same as
   * isContainerReplicatingOrDeleting but the check is done in a different way to be doubly sure.
   * @param container container to check
   * @param replicas the container's replicas
   * @return false if it should not be moved, true otherwise
   */
  private boolean isContainerHealthyForMove(ContainerInfo container, Set<ContainerReplica> replicas) {
    ContainerHealthResult.HealthState state =
        replicationManager.getContainerReplicationHealth(container, replicas).getHealthState();
    if (state != ContainerHealthResult.HealthState.HEALTHY) {
      LOG.debug("Excluding container {} with replicas {} as its health is {}.", container, replicas, state);
      return false;
    }

    return true;
  }

  private boolean breaksMaxSizeToMoveLimit(ContainerID containerID,
                                           long usedBytes,
                                           long sizeMovedAlready) {
    // check max size to move per iteration limit
    if (sizeMovedAlready + usedBytes >
        balancerConfiguration.getMaxSizeToMovePerIteration()) {
      LOG.debug("Removing container {} because it fails max size " +
          "to move per iteration check.", containerID);
      return true;
    }
    return false;
  }

  public void setExcludeContainers(
      Set<ContainerID> excludeContainers) {
    this.excludeContainers = excludeContainers;
  }

  public void setIncludeContainers(
      Set<ContainerID> includeContainers) {
    this.includeContainers = includeContainers;
  }

  public void addToExcludeDueToFailContainers(ContainerID container) {
    this.excludeContainersDueToFailure.add(container);
  }

  private NavigableSet<ContainerID> getCandidateContainers(DatanodeDetails node) {
    NavigableSet<ContainerID> newSet =
        new TreeSet<>(orderContainersByUsedBytes().reversed());
    try {
      Set<ContainerID> idSet = nodeManager.getContainers(node);
      if (includeContainers != null && !includeContainers.isEmpty()) {
        idSet.retainAll(includeContainers);
      }
      if (excludeContainers != null) {
        idSet.removeAll(excludeContainers);
      }
      if (excludeContainersDueToFailure != null) {
        idSet.removeAll(excludeContainersDueToFailure);
      }
      idSet.removeAll(containerToSourceMap.keySet());
      newSet.addAll(idSet);
      return newSet;
    } catch (NodeNotFoundException e) {
      LOG.warn("Could not find Datanode {} while selecting candidate " +
              "containers for Container Balancer.", node, e);
      return null;
    }
  }
}
