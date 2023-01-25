/**
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

package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * This class handles Ratis containers that are over replicated. It should
 * be used to obtain SCMCommands that can be sent to datanodes to solve
 * over replication.
 */
public class RatisOverReplicationHandler
    extends AbstractOverReplicationHandler {
  public static final Logger LOG =
      LoggerFactory.getLogger(RatisOverReplicationHandler.class);

  private final NodeManager nodeManager;

  public RatisOverReplicationHandler(PlacementPolicy placementPolicy,
      NodeManager nodeManager) {
    super(placementPolicy);
    this.nodeManager = nodeManager;
  }

  /**
   * Identifies datanodes where the specified container's replicas can be
   * deleted. Creates the SCMCommands to be sent to datanodes.
   *
   * @param replicas                 Set of container replicas.
   * @param pendingOps               Pending (in flight) replications or
   *                                 deletions for this
   *                                 container.
   * @param result                   Health check result indicating over
   *                                 replication
   * @param minHealthyForMaintenance Number of healthy replicas that must be
   *                                 available for a DN to enter maintenance
   * @return Returns a map of Datanodes and SCMCommands that can be sent to
   * delete replicas on those datanodes.
   */
  @Override
  public Set<Pair<DatanodeDetails, SCMCommand<?>>> processAndCreateCommands(
      Set<ContainerReplica> replicas, List<ContainerReplicaOp> pendingOps,
      ContainerHealthResult result, int minHealthyForMaintenance) throws
      IOException {
    ContainerInfo containerInfo = result.getContainerInfo();
    LOG.debug("Handling container {}.", containerInfo);

    // We are going to check for over replication, so we should filter out any
    // replicas that are not in a HEALTHY state. This is because a replica can
    // be healthy, stale or dead. If it is dead is will be quickly removed from
    // scm. If it is state, there is a good chance the DN is offline and the
    // replica will go away soon. So, if we have a container that is over
    // replicated with a HEALTHY and STALE replica, and we decide to delete the
    // HEALTHY one, and then the STALE ones goes away, we will lose them both.
    // To avoid this, we will filter out any non-healthy replicas first.
    Set<ContainerReplica> healthyReplicas = replicas.stream()
        .filter(r -> ReplicationManager.getNodeStatus(
            r.getDatanodeDetails(), nodeManager).isHealthy()
        )
        .collect(Collectors.toSet());

    // count pending adds and deletes
    int pendingAdd = 0, pendingDelete = 0;
    for (ContainerReplicaOp op : pendingOps) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.ADD) {
        pendingAdd++;
      } else if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
        pendingDelete++;
      }
    }
    RatisContainerReplicaCount replicaCount =
        new RatisContainerReplicaCount(containerInfo, healthyReplicas,
            pendingAdd, pendingDelete,
            containerInfo.getReplicationFactor().getNumber(),
            minHealthyForMaintenance);

    // verify that this container is actually over replicated
    if (!verifyOverReplication(replicaCount)) {
      return Collections.emptySet();
    }

    // get number of excess replicas
    int excess = replicaCount.getExcessRedundancy(true);
    LOG.info("Container {} is over replicated. Actual replica count is {}, " +
            "with {} pending delete(s). Expected replica count is {}.",
        containerInfo.containerID(),
        replicaCount.getReplicas().size(), pendingDelete,
        replicaCount.getReplicationFactor());

    // get replicas that can be deleted, in sorted order
    List<ContainerReplica> eligibleReplicas =
        getEligibleReplicas(replicaCount, pendingOps);
    if (eligibleReplicas.size() == 0) {
      LOG.info("Did not find any replicas that are eligible to be deleted for" +
          " container {}.", containerInfo);
      return Collections.emptySet();
    }

    return createCommands(containerInfo, eligibleReplicas, excess);
  }

  private boolean verifyOverReplication(
      RatisContainerReplicaCount replicaCount) {
    if (!replicaCount.isOverReplicated()) {
      LOG.info("Container {} is actually not over-replicated any more.",
          replicaCount.getContainer().containerID());
      return false;
    }
    return true;
  }

  /**
   * Finds replicas that are eligible to be deleted, sorted to avoid
   * potential data loss.
   * @see
   * <a href="https://issues.apache.org/jira/browse/HDDS-4589">HDDS-4589</a>
   * @param replicaCount ContainerReplicaCount object for the container
   * @param pendingOps Pending adds and deletes
   * @return List of ContainerReplica sorted using
   * {@link RatisOverReplicationHandler#sortReplicas(Collection)}
   */
  private List<ContainerReplica> getEligibleReplicas(
      ContainerReplicaCount replicaCount, List<ContainerReplicaOp> pendingOps) {
    // sort replicas so that they can be selected in a deterministic way
    List<ContainerReplica> eligibleReplicas =
        sortReplicas(replicaCount.getReplicas());

    // retain one replica per unique origin datanode if the container is not
    // closed
    final Map<UUID, ContainerReplica> uniqueReplicas =
        new LinkedHashMap<>();
    if (replicaCount.getContainer().getState() !=
        HddsProtos.LifeCycleState.CLOSED) {
      eligibleReplicas.stream()
          // get replicas with state that matches container state
          .filter(r -> ReplicationManager.compareState(
              replicaCount.getContainer().getState(),
              r.getState()))
          .forEach(r -> uniqueReplicas
              .putIfAbsent(r.getOriginDatanodeId(), r));

      // note that this preserves order of the List
      eligibleReplicas.removeAll(uniqueReplicas.values());
    }

    Set<DatanodeDetails> pendingDeletion = new HashSet<>();
    // collect the DNs that are going to have their container replica deleted
    for (ContainerReplicaOp op : pendingOps) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
        pendingDeletion.add(op.getTarget());
      }
    }

    // replicas that are not on IN_SERVICE nodes or are already pending
    // delete are not eligible
    // TODO what about nodes that are not healthy?
    eligibleReplicas.removeIf(
        replica -> replica.getDatanodeDetails().getPersistedOpState() !=
            HddsProtos.NodeOperationalState.IN_SERVICE ||
            pendingDeletion.contains(replica.getDatanodeDetails()));

    return eligibleReplicas;
  }

  /**
   * Sorts replicas using {@link ContainerReplica#hashCode()} (ContainerID and
   * DatanodeDetails).
   * @param replicas replicas to sort
   * @return sorted List
   */
  private List<ContainerReplica> sortReplicas(
      Collection<ContainerReplica> replicas) {
    return replicas.stream()
        .sorted(Comparator.comparingLong(ContainerReplica::hashCode))
        .collect(Collectors.toList());
  }

  private Set<Pair<DatanodeDetails, SCMCommand<?>>> createCommands(
      ContainerInfo containerInfo, List<ContainerReplica> replicas,
      int excess) {
    Set<Pair<DatanodeDetails, SCMCommand<?>>> commands = new HashSet<>();

    /*
    Over replication means we have enough healthy replicas, so unhealthy
    replicas can be deleted. This might make the container violate placement
    policy.
     */
    List<ContainerReplica> unhealthyReplicas = new ArrayList<>();
    for (ContainerReplica replica : replicas) {
      if (excess == 0) {
        return commands;
      }
      if (!ReplicationManager.compareState(
          containerInfo.getState(), replica.getState())) {
        commands.add(Pair.of(replica.getDatanodeDetails(),
            createDeleteCommand(containerInfo)));
        unhealthyReplicas.add(replica);
        excess--;
      }
    }
    replicas.removeAll(unhealthyReplicas);

    /*
    Remove excess replicas if that does not make the container mis replicated.
    If the container was already mis replicated, then remove replicas if that
    does not change the placement count.
     */
    Set<ContainerReplica> replicaSet = new HashSet<>(replicas);
    // iterate through replicas in deterministic order
    for (ContainerReplica replica : replicas) {
      if (excess == 0) {
        return commands;
      }

      if (super.isPlacementStatusActuallyEqualAfterRemove(replicaSet, replica,
          containerInfo.getReplicationFactor().getNumber())) {
        commands.add(Pair.of(replica.getDatanodeDetails(),
            createDeleteCommand(containerInfo)));
        excess--;
      }
    }
    return commands;
  }

  private DeleteContainerCommand createDeleteCommand(ContainerInfo container) {
    return new DeleteContainerCommand(container.containerID(), true);
  }
}
