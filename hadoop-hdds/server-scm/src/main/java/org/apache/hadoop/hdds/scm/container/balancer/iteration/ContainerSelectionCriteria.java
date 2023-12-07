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

package org.apache.hadoop.hdds.scm.container.balancer.iteration;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerConfiguration;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * The selection criteria for selecting containers that will be moved and
 * selecting datanodes that containers will move to.
 */
public class ContainerSelectionCriteria {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerSelectionCriteria.class);

  private final ContainerBalancerConfiguration config;
  private final NodeManager nodeManager;
  private final ReplicationManager replicationManager;
  private final ContainerManager containerManager;
  private final Set<ContainerID> selectedContainers = new HashSet<>();
  private final Set<ContainerID> excludeContainers;

  public ContainerSelectionCriteria(
      @Nonnull ContainerBalancerConfiguration balancerConfig,
      @Nonnull StorageContainerManager scm
  ) {
    config = balancerConfig;
    nodeManager = scm.getScmNodeManager();
    replicationManager = scm.getReplicationManager();
    containerManager = scm.getContainerManager();
    excludeContainers = balancerConfig.getExcludeContainers();
  }

  /**
   * Checks whether container is currently undergoing replication or deletion.
   *
   * @param containerID Container to check.
   * @return true if container is replicating or deleting, otherwise false.
   */
  private boolean isContainerReplicatingOrDeleting(
      @Nonnull ContainerID containerID
  ) {
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
   * 6. Container should not be an EC container
   * //TODO Temporarily not considering EC containers as candidates
   *
   * @param node       datanodeDetails for which to find candidate containers
   * @param lowerLimit the value of lower limit for node utilization:
   *                   clusterAvgUtilisation - threshold
   * @return NavigableSet of candidate containers that satisfy the criteria.
   * @see
   * <a href="https://issues.apache.org/jira/browse/HDDS-6940">HDDS-6940</a>
   */
  public @Nonnull Set<ContainerID> getCandidateContainers(
      @Nonnull DatanodeDetails node,
      @Nonnull FindSourceStrategy sourceStrategy,
      long sizeMovedAlready,
      double lowerLimit
  ) {
    Set<ContainerID> containerIDSet =
        new TreeSet<>(orderContainersByUsedBytes().reversed());
    try {
      containerIDSet.addAll(nodeManager.getContainers(node));
    } catch (NodeNotFoundException e) {
      LOG.warn("Could not find Datanode {} while selecting candidate " +
          "containers for Container Balancer.", node, e);
      return containerIDSet;
    }
    containerIDSet.removeAll(excludeContainers);
    containerIDSet.removeAll(selectedContainers);

    containerIDSet.removeIf(
        containerID -> shouldBeExcluded(containerID, node, sourceStrategy,
            sizeMovedAlready, lowerLimit)
    );
    return containerIDSet;
  }

  /**
   * Checks if the first container has more used space than second.
   *
   * @param first  first container to compare
   * @param second second container to compare
   * @return An integer greater than 0 if first is more used, 0 if they're
   * the same containers or a container is not found, and a value less than 0
   * if first is less used than second. If both containers have equal used
   * space, they're compared using {@link ContainerID#compareTo(ContainerID)}.
   */
  private int isContainerMoreUsed(
      @Nonnull ContainerID first,
      @Nonnull ContainerID second
  ) {
    if (first.equals(second)) {
      return 0;
    }
    try {
      long firstUsedBytes = containerManager.getContainer(first).getUsedBytes();
      long secondUsedBytes =
          containerManager.getContainer(second).getUsedBytes();
      if (firstUsedBytes > secondUsedBytes) {
        return 1;
      } else if (firstUsedBytes < secondUsedBytes) {
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
   *
   * @return First container is more used if it has used space greater than
   * second container.
   */
  private @Nonnull Comparator<ContainerID> orderContainersByUsedBytes() {
    return this::isContainerMoreUsed;
  }

  /**
   * Checks whether a Container has the ReplicationType
   * {@link HddsProtos.ReplicationType#EC}.
   *
   * @param container container to check
   * @return true if the ReplicationType is EC and "hdds.scm.replication
   * .enable.legacy" is true, else false
   */
  private boolean isECContainer(@Nonnull ContainerInfo container) {
    return container.getReplicationType().equals(HddsProtos.ReplicationType.EC)
        && replicationManager.getConfig().isLegacyEnabled();
  }

  private boolean shouldBeExcluded(
      @Nonnull ContainerID containerID,
      @Nonnull DatanodeDetails node,
      @Nonnull FindSourceStrategy sourceStrategy,
      long sizeMovedAlready,
      double lowerLimit
  ) {
    ContainerInfo container;
    try {
      container = containerManager.getContainer(containerID);
    } catch (ContainerNotFoundException e) {
      LOG.warn("Could not find Container {} to check if it should be a " +
          "candidate container. Excluding it.", containerID);
      return true;
    }
    long usedBytes = container.getUsedBytes();
    return !isContainerClosed(container, node) ||
        isECContainer(container) ||
        isContainerReplicatingOrDeleting(containerID) ||
        !sourceStrategy.canSizeLeaveSource(node, usedBytes,
            config.getMaxSizeLeavingSource(), lowerLimit) ||
        breaksMaxSizeToMoveLimit(container.containerID(), usedBytes,
            sizeMovedAlready);
  }

  /**
   * Checks whether specified container is closed. Also checks if the replica
   * on the specified datanode is CLOSED. Assumes that there will only be one
   * replica of a container on a particular Datanode.
   *
   * @param container       container to check
   * @param datanodeDetails datanode on which a replica of the container is
   *                        present
   * @return true if container LifeCycleState is
   * {@link HddsProtos.LifeCycleState#CLOSED} and its replica on the
   * specified datanode is CLOSED, else false
   */
  private boolean isContainerClosed(
      @Nonnull ContainerInfo container,
      @Nonnull DatanodeDetails datanodeDetails
  ) {
    if (!container.getState().equals(HddsProtos.LifeCycleState.CLOSED)) {
      return false;
    }

    // also check that the replica on the specified DN is closed
    Set<ContainerReplica> replicas;
    try {
      replicas = containerManager.getContainerReplicas(container.containerID());
    } catch (ContainerNotFoundException e) {
      LOG.warn("Container {} does not exist in ContainerManager. Skipping " +
          "this container.", container.getContainerID(), e);
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

  private boolean breaksMaxSizeToMoveLimit(
      @Nonnull ContainerID containerID,
      long usedBytes,
      long sizeMovedAlready
  ) {
    // check max size to move per iteration limit
    if (sizeMovedAlready + usedBytes > config.getMaxSizeToMovePerIteration()) {
      LOG.debug("Removing container {} because it fails max size " +
          "to move per iteration check.", containerID);
      return true;
    }
    return false;
  }

  public void updateSelectedContainer(
      @Nonnull Set<ContainerID> newSelectedContainers
  ) {
    selectedContainers.clear();
    selectedContainers.addAll(newSelectedContainers);
  }

}
