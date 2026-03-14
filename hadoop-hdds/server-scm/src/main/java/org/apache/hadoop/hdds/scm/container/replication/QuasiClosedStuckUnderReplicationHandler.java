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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.pipeline.InsufficientDatanodesException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to correct under replicated QuasiClosed Stuck Ratis containers.
 */
public class QuasiClosedStuckUnderReplicationHandler implements UnhealthyReplicationHandler {
  private static final Logger LOG = LoggerFactory.getLogger(QuasiClosedStuckUnderReplicationHandler.class);

  private final PlacementPolicy placementPolicy;
  private final ReplicationManager replicationManager;
  private final long currentContainerSize;
  private final ReplicationManagerMetrics metrics;

  public QuasiClosedStuckUnderReplicationHandler(final PlacementPolicy placementPolicy,
      final ConfigurationSource conf, final ReplicationManager replicationManager) {
    this.placementPolicy = placementPolicy;
    this.currentContainerSize = (long) conf.getStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    this.replicationManager = replicationManager;
    this.metrics = replicationManager.getMetrics();
  }

  @Override
  public int processAndSendCommands(Set<ContainerReplica> replicas, List<ContainerReplicaOp> pendingOps,
      ContainerHealthResult result, int remainingMaintenanceRedundancy) throws IOException {
    ContainerInfo containerInfo = result.getContainerInfo();
    LOG.debug("Handling under replicated QuasiClosed Stuck Ratis container {}", containerInfo);

    int pendingAdd = 0;
    for (ContainerReplicaOp op : pendingOps) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.ADD) {
        pendingAdd++;
      }
    }

    if (pendingAdd > 0) {
      LOG.debug("Container {} has pending add operations. No more replication will be scheduled until they complete",
          containerInfo);
      return 0;
    }

    ReplicationManager.ReplicationManagerConfiguration rmConf = replicationManager.getConfig();
    QuasiClosedStuckReplicaCount replicaCount =
        new QuasiClosedStuckReplicaCount(replicas, remainingMaintenanceRedundancy,
            rmConf.getQuasiClosedStuckBestOriginCopies(), rmConf.getQuasiClosedStuckOtherOriginCopies());

    List<QuasiClosedStuckReplicaCount.MisReplicatedOrigin> misReplicatedOrigins
        = replicaCount.getUnderReplicatedReplicas();

    if (misReplicatedOrigins.isEmpty()) {
      LOG.debug("Container {} is not under replicated", containerInfo);
      return 0;
    }

    // Schedule Replicas for the under replicated origins.
    int totalRequiredReplicas = 0;
    int totalCommandsSent = 0;
    IOException firstException = null;
    List<ContainerReplicaOp> mutablePendingOps = new ArrayList<>(pendingOps);
    for (QuasiClosedStuckReplicaCount.MisReplicatedOrigin origin : misReplicatedOrigins) {
      totalRequiredReplicas += origin.getReplicaDelta();
      List<DatanodeDetails> targets;
      try {
        targets = getTargets(containerInfo, replicas, origin.getReplicaDelta(), mutablePendingOps);
      } catch (SCMException e) {
        if (firstException == null) {
          firstException = e;
        }
        LOG.warn("Cannot replicate container {} because no suitable targets were found.", containerInfo, e);
        continue;
      }

      List<DatanodeDetails> sourceDatanodes = origin.getSources().stream()
          .map(ContainerReplica::getDatanodeDetails)
          .collect(Collectors.toList());
      for (DatanodeDetails target : targets) {
        try {
          replicationManager.sendThrottledReplicationCommand(
              containerInfo, sourceDatanodes, target, 0);
          // Add the pending op, so we exclude the node for subsequent origins
          mutablePendingOps.add(new ContainerReplicaOp(
              ContainerReplicaOp.PendingOpType.ADD, target, 0,
              null, System.currentTimeMillis(), 0));
          totalCommandsSent++;
        } catch (CommandTargetOverloadedException e) {
          LOG.warn("Cannot replicate container {} because all sources are overloaded.", containerInfo);
          if (firstException == null) {
            firstException = e;
          }
        }
      }
    }

    if (firstException != null || totalCommandsSent < totalRequiredReplicas) {
      // Some commands were not sent as expected (not enough nodes found or overloaded nodes), so we just rethrow
      // the first exception we encountered.
      LOG.info("A command was not sent for all required new replicas for container {}. Total sent {}, required {} ",
          containerInfo, totalCommandsSent, totalRequiredReplicas);
      metrics.incrPartialReplicationTotal();
      if (firstException != null) {
        throw firstException;
      } else {
        throw new InsufficientDatanodesException(totalRequiredReplicas, totalCommandsSent);
      }
    }
    return totalCommandsSent;
  }

  private List<DatanodeDetails> getTargets(ContainerInfo containerInfo,
      Set<ContainerReplica> replicas, int additionalRequired, List<ContainerReplicaOp> pendingOps) throws IOException {
    LOG.debug("Need {} target datanodes for container {}. Current replicas: {}.",
        additionalRequired, containerInfo, replicas);

    ReplicationManagerUtil.ExcludedAndUsedNodes excludedAndUsedNodes =
        ReplicationManagerUtil.getExcludedAndUsedNodes(containerInfo, new ArrayList<>(replicas), Collections.emptySet(),
            pendingOps, replicationManager);

    List<DatanodeDetails> excluded = excludedAndUsedNodes.getExcludedNodes();
    List<DatanodeDetails> used = excludedAndUsedNodes.getUsedNodes();

    LOG.debug("UsedList: {}, size {}. ExcludeList: {}, size: {}. ",
        used, used.size(), excluded, excluded.size());

    return ReplicationManagerUtil.getTargetDatanodes(placementPolicy,
        additionalRequired, used, excluded, currentContainerSize, containerInfo);
  }

}
