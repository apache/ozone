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
package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
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
  private ContainerManagerV2 containerManagerV2;
  private Set<ContainerID> selectedContainers;
  private Set<ContainerID> includeContainers;
  private Set<ContainerID> excludeContainers;

  public ContainerBalancerSelectionCriteria(
      ContainerBalancerConfiguration balancerConfiguration,
      NodeManager nodeManager,
      ReplicationManager replicationManager,
      ContainerManagerV2 containerManagerV2) {
    this.balancerConfiguration = balancerConfiguration;
    this.nodeManager = nodeManager;
    this.replicationManager = replicationManager;
    this.containerManagerV2 = containerManagerV2;
    selectedContainers = new HashSet<>();
  }

  /**
   * Checks whether container is currently undergoing replication.
   *
   * @param containerID Container to check.
   * @return true if container is replicating, otherwise false.
   */
  public boolean isContainerReplicating(ContainerID containerID) {
    return false;
  }

  /**
   * Gets containers that are suitable for moving based on the following
   * required criteria:
   * 1. Container must not be undergoing replication.
   * 2. Container must not already be selected for balancing.
   * 3. Container size should be above the preferred lower limit (try to move
   * containers with size closer to 5GB).
   * 4. Containers must not be in the configured exclude containers list.
   *
   * @param node DatanodeDetails for which to find candidate containers.
   * @return NavigableSet of candidate containers that satisfy the criteria.
   */
  public NavigableSet<ContainerID> getCandidateContainers(
      DatanodeDetails node) {
    NavigableSet<ContainerID> containerIDSet =
        new TreeSet<>(orderContainersByUsedBytes());
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
    containerIDSet.removeIf(this::isContainerReplicating);
    return containerIDSet;
  }

  private int isContainerMoreUsed(ContainerID first,
                                  ContainerID second) {
    if (first.equals(second)) {
      return 0;
    }
    try {
      ContainerInfo firstInfo = containerManagerV2.getContainer(first);
      ContainerInfo secondInfo = containerManagerV2.getContainer(second);
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

  private Comparator<ContainerID> orderContainersByUsedBytes() {
    return this::isContainerMoreUsed;
  }

  public void setIncludeContainers(
      Set<ContainerID> includeContainers) {
    this.includeContainers = includeContainers;
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
