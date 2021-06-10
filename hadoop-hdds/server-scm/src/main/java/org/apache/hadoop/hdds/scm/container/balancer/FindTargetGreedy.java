package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Find a target giving preference to more under-utilized nodes.
 */
public class FindTargetGreedy implements FindTargetStrategy {
  private static final Logger LOG =
      LoggerFactory.getLogger(FindTargetGreedy.class);

  private ContainerManagerV2 containerManager;
  private PlacementPolicy placementPolicy;

  public FindTargetGreedy(
      ContainerManagerV2 containerManager,
      PlacementPolicy placementPolicy) {
    this.containerManager = containerManager;
    this.placementPolicy = placementPolicy;
  }

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
    return null;
  }

  @Override
  public boolean containerMoveSatisfiesPlacementPolicy(
      ContainerID containerID, Set<ContainerReplica> replicas,
      DatanodeDetails source, DatanodeDetails target) {
    return true;
  }
}
