/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

/**
 * Handles the EC Over replication processing and forming the respective SCM
 * commands.
 */
public class ECOverReplicationHandler extends AbstractOverReplicationHandler {
  public static final Logger LOG =
      LoggerFactory.getLogger(ECOverReplicationHandler.class);

  private final NodeManager nodeManager;

  public ECOverReplicationHandler(PlacementPolicy placementPolicy,
      NodeManager nodeManager) {
    super(placementPolicy);
    this.nodeManager = nodeManager;

  }

  /**
   * Identify a new set of datanode(s) to delete the container
   * and form the SCM commands to send it to DN.
   *
   * @param replicas - Set of available container replicas.
   * @param pendingOps - Inflight replications and deletion ops.
   * @param result - Health check result.
   * @param remainingMaintenanceRedundancy - represents that how many nodes go
   *                                      into maintenance.
   * @return Returns the key value pair of destination dn where the command gets
   * executed and the command itself.
   */
  @Override
  public Map<DatanodeDetails, SCMCommand<?>> processAndCreateCommands(
      Set<ContainerReplica> replicas, List<ContainerReplicaOp> pendingOps,
      ContainerHealthResult result, int remainingMaintenanceRedundancy) {
    ContainerInfo container = result.getContainerInfo();

    // We are going to check for over replication, so we should filter out any
    // replicas that are not in a HEALTHY state. This is because a replica can
    // be healthy, stale or dead. If it is dead is will be quickly removed from
    // scm. If it is state, there is a good chance the DN is offline and the
    // replica will go away soon. So, if we have a container that is over
    // replicated with a HEALTHY and STALE replica, and we decide to delete the
    // HEALTHY one, and then the STALE ones goes away, we will lose them both.
    // To avoid this, we will filter out any non-healthy replicas first.
    // EcContainerReplicaCount will ignore nodes which are not IN_SERVICE for
    // over replication checks, but we need to filter these out later in this
    // method anyway, so it makes sense to filter them here too, to avoid a
    // second lookup of the NodeStatus
    Set<ContainerReplica> healthyReplicas = replicas.stream()
        .filter(r -> {
          NodeStatus ns = ReplicationManager.getNodeStatus(
              r.getDatanodeDetails(), nodeManager);
          return ns.isHealthy() && ns.getOperationalState() ==
              HddsProtos.NodeOperationalState.IN_SERVICE;
        })
        .collect(Collectors.toSet());

    final ECContainerReplicaCount replicaCount =
        new ECContainerReplicaCount(container, healthyReplicas, pendingOps,
            remainingMaintenanceRedundancy);
    if (!replicaCount.isOverReplicated()) {
      LOG.info("The container {} state changed and it is no longer over"
              + " replication. Replica count: {}, healthy replica count: {}",
          container.getContainerID(), replicas.size(), healthyReplicas.size());
      return emptyMap();
    }

    if (!replicaCount.isOverReplicated(true)) {
      LOG.info("The container {} with replicas {} will be corrected " +
          "by the pending delete", container.getContainerID(), replicas);
      return emptyMap();
    }

    List<Integer> overReplicatedIndexes =
        replicaCount.overReplicatedIndexes(true);
    //sanity check
    if (overReplicatedIndexes.size() == 0) {
      LOG.warn("The container {} with replicas {} was found over replicated "
          + "by EcContainerReplicaCount, but there are no over replicated "
          + "indexes returned", container.getContainerID(), replicas);
      return emptyMap();
    }

    final List<DatanodeDetails> deletionInFlight = new ArrayList<>();
    for (ContainerReplicaOp op : pendingOps) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
        deletionInFlight.add(op.getTarget());
      }
    }

    Set<ContainerReplica> candidates = healthyReplicas.stream()
        .filter(r -> !deletionInFlight.contains(r.getDatanodeDetails()))
        .filter(r -> r.getState() == StorageContainerDatanodeProtocolProtos
            .ContainerReplicaProto.State.CLOSED)
        .collect(Collectors.toSet());

    Set<ContainerReplica> replicasToRemove =
        selectReplicasToRemove(candidates, 1);

    if (replicasToRemove.size() == 0) {
      LOG.warn("The container {} is over replicated, but no replicas were "
          + "selected to remove by the placement policy. Replicas: {}",
          container, replicas);
      return emptyMap();
    }

    final Map<DatanodeDetails, SCMCommand<?>> commands = new HashMap<>();
    // As a sanity check, sum up the current counts of each replica index. When
    // processing replicasToRemove, ensure that removing the replica would not
    // drop the count of that index to zero.
    Map<Integer, Integer> replicaIndexCounts = new HashMap<>();
    for (ContainerReplica r : candidates) {
      replicaIndexCounts.put(r.getReplicaIndex(),
          replicaIndexCounts.getOrDefault(r.getReplicaIndex(), 0) + 1);
    }
    for (ContainerReplica r : replicasToRemove) {
      int currentCount = replicaIndexCounts.getOrDefault(
          r.getReplicaIndex(), 0);
      if (currentCount < 2) {
        LOG.warn("The replica {} selected to remove would reduce the count " +
            "for that index to zero. Candidate Replicas: {}", r, candidates);
        continue;
      }
      replicaIndexCounts.put(r.getReplicaIndex(), currentCount - 1);
      DeleteContainerCommand deleteCommand =
          new DeleteContainerCommand(container.getContainerID(), true);
      deleteCommand.setReplicaIndex(r.getReplicaIndex());
      commands.put(r.getDatanodeDetails(), deleteCommand);
    }

    if (commands.size() == 0) {
      LOG.warn("With the current state of available replicas {}, no" +
          " commands were created to remove excess replicas.", replicas);
    }
    return commands;
  }
}
