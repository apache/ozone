package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
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
            replica -> replica.getDatanodeDetails().equals(target))) {
//            containerMoveSatisfiesPlacementPolicy(container, replicas, source,
//                target) &&

          if (canSizeEnterTarget.apply(target, containerInfo.getUsedBytes())) {
            LOG.info("Container Balancer found target {} and container {} for" +
                " source {}", target.getUuidString(),
                containerInfo.containerID(), source.getUuidString());
            return new ContainerMoveSelection(target, container);
          } else {
            LOG.info("ContainerBalancer can't move size {} to target {}.",
                containerInfo.getUsedBytes(), target.getUuidString());
          }
        }
//        LOG.info("For source {} and potential target {} found container " +
//                "replicas {}", source.getUuidString(),
//            target.getUuidString(), replicas);
      }
    }
    LOG.info("Container Balancer could not find a target for source datanode " +
        "{}", source.getUuidString());
    return null;
  }

  @Override
  public boolean containerMoveSatisfiesPlacementPolicy(
      ContainerID containerID, Set<ContainerReplica> replicas,
      DatanodeDetails source, DatanodeDetails target) {
    ContainerInfo containerInfo;
    try {
      containerInfo = containerManager.getContainer(containerID);
    } catch (ContainerNotFoundException e) {
      LOG.warn("Could not get Container {} from Container Manager while " +
          "checking if container move satisfies placemenet policy in " +
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
        containerInfo.getReplicationFactor().getNumber());

//    return placementStatus.isPolicySatisfied();
    return true;
  }
}
