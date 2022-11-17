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
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.health.ECReplicationCheckHandler;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;

/**
 * Handles the EC Over replication processing and forming the respective SCM
 * commands.
 */
public class ECOverReplicationHandler extends AbstractOverReplicationHandler {
  public static final Logger LOG =
      LoggerFactory.getLogger(ECOverReplicationHandler.class);

  private final ECReplicationCheckHandler ecReplicationCheck;
  private final NodeManager nodeManager;

  public ECOverReplicationHandler(ECReplicationCheckHandler ecReplicationCheck,
      PlacementPolicy placementPolicy, NodeManager nodeManager) {
    super(placementPolicy);
    this.ecReplicationCheck = ecReplicationCheck;
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

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setContainerInfo(container)
        .setContainerReplicas(replicas)
        .setPendingOps(pendingOps)
        .setMaintenanceRedundancy(remainingMaintenanceRedundancy)
        .build();
    ContainerHealthResult currentUnderRepRes = ecReplicationCheck
        .checkHealth(request);
    LOG.debug("Handling over-replicated EC container: {}", container);

    //sanity check
    if (currentUnderRepRes.getHealthState() !=
        ContainerHealthResult.HealthState.OVER_REPLICATED) {
      LOG.info("The container {} state changed and it's not in over"
              + " replication any more. Current state is: {}",
          container.getContainerID(), currentUnderRepRes);
      return emptyMap();
    }

    ContainerHealthResult.OverReplicatedHealthResult containerHealthResult =
        ((ContainerHealthResult.OverReplicatedHealthResult)
            currentUnderRepRes);
    if (containerHealthResult.isReplicatedOkAfterPending()) {
      LOG.info("The container {} with replicas {} will be corrected " +
              "by the pending delete", container.getContainerID(), replicas);
      return emptyMap();
    }

    // we don`t support hybrid state(both under and over replicated) for
    // EC container and we always handle under-replicated first now. it
    // means when reaching here, we have all the replica indexes and some
    // of them are more than 1.
    // TODO: support hybrid state if needed.
    final ECContainerReplicaCount replicaCount =
        new ECContainerReplicaCount(container, replicas, pendingOps,
            remainingMaintenanceRedundancy);

    List<Integer> overReplicatedIndexes =
        replicaCount.overReplicatedIndexes(true);
    //sanity check
    if (overReplicatedIndexes.size() == 0) {
      LOG.warn("The container {} with replicas {} is found over replicated " +
              "by ContainerHealthCheck, but found not over replicated by " +
              "ECContainerReplicaCount",
          container.getContainerID(), replicas);
      return emptyMap();
    }

    final List<DatanodeDetails> deletionInFlight = new ArrayList<>();
    for (ContainerReplicaOp op : pendingOps) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
        deletionInFlight.add(op.getTarget());
      }
    }
    Map<Integer, List<ContainerReplica>> index2replicas = new HashMap<>();
    replicas.stream()
        .filter(r -> overReplicatedIndexes.contains(r.getReplicaIndex()))
        .filter(r -> r
            .getState() == StorageContainerDatanodeProtocolProtos
            .ContainerReplicaProto.State.CLOSED)
        .filter(r -> ReplicationManager
            .getNodeStatus(r.getDatanodeDetails(), nodeManager).isHealthy())
        .filter(r -> !deletionInFlight.contains(r.getDatanodeDetails()))
        .forEach(r -> {
          int index = r.getReplicaIndex();
          index2replicas.computeIfAbsent(index, k -> new LinkedList<>());
          index2replicas.get(index).add(r);
        });

    if (index2replicas.size() > 0) {
      final Map<DatanodeDetails, SCMCommand<?>> commands = new HashMap<>();
      final int replicationFactor =
          container.getReplicationConfig().getRequiredNodes();
      index2replicas.values().forEach(l -> {
        Iterator<ContainerReplica> it = l.iterator();
        Set<ContainerReplica> tempReplicaSet = new HashSet<>(replicas);
        while (it.hasNext() && l.size() > 1) {
          ContainerReplica r = it.next();
          if (isPlacementStatusActuallyEqualAfterRemove(
              tempReplicaSet, r, replicationFactor)) {
            DeleteContainerCommand deleteCommand =
                new DeleteContainerCommand(container.getContainerID(), true);
            deleteCommand.setReplicaIndex(r.getReplicaIndex());
            commands.put(r.getDatanodeDetails(), deleteCommand);
            it.remove();
            tempReplicaSet.remove(r);
          }
        }
      });
      if (commands.size() == 0) {
        LOG.info("With the current state of avilable replicas {}, no" +
            " commands to process due to over replication.", replicas);
      }
      return commands;
    }

    return emptyMap();
  }
}
