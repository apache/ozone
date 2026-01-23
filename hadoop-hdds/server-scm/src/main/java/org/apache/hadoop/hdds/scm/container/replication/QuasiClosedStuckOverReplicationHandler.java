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

package org.apache.hadoop.hdds.scm.container.replication;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;

/**
 * Class to correct over replicated QuasiClosed Stuck Ratis containers.
 */
public class QuasiClosedStuckOverReplicationHandler implements UnhealthyReplicationHandler {

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(QuasiClosedStuckOverReplicationHandler.class);
  private final ReplicationManager replicationManager;
  private final ReplicationManagerMetrics metrics;

  public QuasiClosedStuckOverReplicationHandler(final ReplicationManager replicationManager) {
    this.replicationManager = replicationManager;
    this.metrics = replicationManager.getMetrics();
  }

  @Override
  public int processAndSendCommands(Set<ContainerReplica> replicas, List<ContainerReplicaOp> pendingOps,
      ContainerHealthResult result, int remainingMaintenanceRedundancy)
      throws IOException {

    ContainerInfo containerInfo = result.getContainerInfo();
    LOG.debug("Handling over replicated QuasiClosed Stuck Ratis container {}", containerInfo);

    int pendingDelete = 0;
    for (ContainerReplicaOp op : pendingOps) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
        pendingDelete++;
      }
    }

    if (pendingDelete > 0) {
      LOG.debug("Container {} has pending delete operations. No more over replication will be scheduled until they " +
          "complete", containerInfo);
      return 0;
    }

    // Filter out any STALE replicas, as they may go dead soon. If so, we don't want to remove other healthy replicas
    // instead of them, as they could result in under replication.
    Set<ContainerReplica> healthyReplicas = replicas.stream()
        .filter(replica -> {
          try {
            return replicationManager.getNodeStatus(
                replica.getDatanodeDetails()).getHealth() == HddsProtos.NodeState.HEALTHY;
          } catch (NodeNotFoundException e) {
            return false;
          }
        })
        .collect(Collectors.toSet());

    QuasiClosedStuckReplicaCount replicaCount =
        new QuasiClosedStuckReplicaCount(healthyReplicas, remainingMaintenanceRedundancy);

    List<QuasiClosedStuckReplicaCount.MisReplicatedOrigin> misReplicatedOrigins
        = replicaCount.getOverReplicatedOrigins();

    if (misReplicatedOrigins.isEmpty()) {
      LOG.debug("Container {} is not over replicated", containerInfo);
      return 0;
    }

    int totalCommandsSent = 0;
    IOException firstException = null;
    for (QuasiClosedStuckReplicaCount.MisReplicatedOrigin origin : misReplicatedOrigins) {
      List<ContainerReplica> sortedReplicas = getSortedReplicas(origin.getSources());
      for (int i = 0; i < origin.getReplicaDelta(); i++) {
        try {
          replicationManager.sendThrottledDeleteCommand(
              containerInfo, 0, sortedReplicas.get(i).getDatanodeDetails(), true);
          totalCommandsSent++;
        } catch (CommandTargetOverloadedException e) {
          LOG.debug("Unable to send delete command for container {} to {} as it has too many pending delete commands",
              containerInfo, sortedReplicas.get(i).getDatanodeDetails());
          firstException = e;
        }
      }
    }

    if (firstException != null) {
      // Some nodes were overloaded when attempting to send commands.
      if (totalCommandsSent > 0) {
        metrics.incrPartialReplicationTotal();
      }
      throw firstException;
    }
    return totalCommandsSent;
  }

  private List<ContainerReplica> getSortedReplicas(
      Set<ContainerReplica> replicas) {
    // sort replicas so that they can be selected in a deterministic way
    return replicas.stream()
        .sorted(Comparator.comparingLong(ContainerReplica::hashCode))
        .collect(Collectors.toList());
  }
}
