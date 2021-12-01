/*
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
package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

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
  private Set<ContainerID> selectedContainers;
  private Set<ContainerID> excludeContainers;
  private FindSourceStrategy findSourceStrategy;

  public ContainerBalancerSelectionCriteria(
      ContainerBalancerConfiguration balancerConfiguration,
      NodeManager nodeManager,
      ReplicationManager replicationManager,
      ContainerManager containerManager,
      FindSourceStrategy findSourceStrategy) {
    this.balancerConfiguration = balancerConfiguration;
    this.nodeManager = nodeManager;
    this.replicationManager = replicationManager;
    this.containerManager = containerManager;
    selectedContainers = new HashSet<>();
    excludeContainers = balancerConfiguration.getExcludeContainers();
    this.findSourceStrategy = findSourceStrategy;
  }

  /**
   * Checks whether container is currently undergoing replication or deletion.
   *
   * @param containerID Container to check.
   * @return true if container is replicating or deleting, otherwise false.
   */
  private boolean isContainerReplicatingOrDeleting(ContainerID containerID) {
    return replicationManager.isContainerReplicatingOrDeleting(containerID);
  }

  /**
   * Gets containers that are suitable for moving based on the following
   * required criteria:
   * 1. Container must not be undergoing replication.
   * 2. Container must not already be selected for balancing.
   * 3. Container size should be closer to 5GB.
   * 4. Container must not be in the configured exclude containers list.
   * 5. Container should be closed.
   *
   * @param node DatanodeDetails for which to find candidate containers.
   * @return NavigableSet of candidate containers that satisfy the criteria.
   */
  public NavigableSet<ContainerID> getCandidateContainers(
      DatanodeDetails node) {
    NavigableSet<ContainerID> containerIDSet =
        new TreeSet<>(orderContainersByUsedBytes().reversed());
    try {
      containerIDSet.addAll(nodeManager.getContainers(node));
    } catch (NodeNotFoundException e) {
      LOG.warn("Could not find Datanode {} while selecting candidate " +
          "containers for Container Balancer.", node.toString(), e);
      return containerIDSet;
    }
    if (excludeContainers != null) {
      containerIDSet.removeAll(excludeContainers);
    }
    if (selectedContainers != null) {
      containerIDSet.removeAll(selectedContainers);
    }

    // remove not closed containers
    containerIDSet.removeIf(containerID -> {
      try {
        return containerManager.getContainer(containerID).getState() !=
            HddsProtos.LifeCycleState.CLOSED;
      } catch (ContainerNotFoundException e) {
        LOG.warn("Could not retrieve ContainerInfo for container {} for " +
            "checking LifecycleState in ContainerBalancer. Excluding this " +
            "container.", containerID.toString(), e);
        return true;
      }
    });

    //if the utilization of the source data node becomes lower than lowerLimit
    //after the container is moved out , then the container can not be
    // a candidate one, and we should remove it from the candidateContainers.
    containerIDSet.removeIf(c -> {
      ContainerInfo cInfo;
      try {
        cInfo = containerManager.getContainer(c);
      } catch (ContainerNotFoundException e) {
        LOG.warn("Could not find container {} when " +
            "be matched with a move target", c);
        //remove this not found container
        return true;
      }
      return !findSourceStrategy.canSizeLeaveSource(
          node, cInfo.getUsedBytes());
    });

    containerIDSet.removeIf(this::isContainerReplicatingOrDeleting);
    return containerIDSet;
  }

  /**
   * Checks if the first container has more used space than second.
   * @param first first container to compare
   * @param second second container to compare
   * @return An integer greater than 0 if first is more used, 0 if they're
   * the same containers or a container is not found, and a value less than 0
   * if first is not more used than second.
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
      } else {
        return -1;
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

  public void setExcludeContainers(
      Set<ContainerID> excludeContainers) {
    this.excludeContainers = excludeContainers;
  }

  public void setSelectedContainers(
      Set<ContainerID> selectedContainers) {
    this.selectedContainers = selectedContainers;
  }

}
