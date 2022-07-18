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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

/**
 * Handles the EC Under replication processing and forming the respective SCM
 * commands.
 */
public class ECUnderReplicationHandler implements UnhealthyReplicationHandler {

  public static final Logger LOG =
      LoggerFactory.getLogger(ECUnderReplicationHandler.class);
  private final ECContainerHealthCheck ecContainerHealthCheck =
      new ECContainerHealthCheck();
  private final PlacementPolicy containerPlacement;
  private final long currentContainerSize;
  private final NodeManager nodeManager;

  public ECUnderReplicationHandler(
      final PlacementPolicy containerPlacement, final ConfigurationSource conf,
      NodeManager nodeManager) {
    this.containerPlacement = containerPlacement;
    this.currentContainerSize = (long) conf
        .getStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
            ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    this.nodeManager = nodeManager;
  }

  /**
   * Identify a new set of datanode(s) to reconstruct the container and form the
   * SCM command to send it to DN. In the case of decommission, it will just
   * generate the replicate commands instead of reconstruction commands.
   *
   * @param replicas - Set of available container replicas.
   * @param pendingOps - Inflight replications and deletion ops.
   * @param result - Health check result.
   * @param remainingMaintenanceRedundancy - represents that how many nodes go
   *                                      into maintenance.
   * @return Returns the key value pair of destination dn where the command gets
   * executed and the command itself. If an empty list is returned, it indicates
   * the container is no longer unhealthy and can be removed from the unhealthy
   * queue. Any exception indicates that the container is still unhealthy and
   * should be retried later.
   */
  @Override
  public Map<DatanodeDetails, SCMCommand<?>> processAndCreateCommands(
      final Set<ContainerReplica> replicas,
      final List<ContainerReplicaOp> pendingOps,
      final ContainerHealthResult result,
      final int remainingMaintenanceRedundancy) throws IOException {
    ContainerInfo container = result.getContainerInfo();
    ECReplicationConfig repConfig =
        (ECReplicationConfig) container.getReplicationConfig();
    final ECContainerReplicaCount replicaCount =
        new ECContainerReplicaCount(container, replicas, pendingOps,
            remainingMaintenanceRedundancy);

    ContainerHealthResult currentUnderRepRes = ecContainerHealthCheck
        .checkHealth(container, replicas, pendingOps,
            remainingMaintenanceRedundancy);

    LOG.debug("Handling under-replicated EC container: {}", container);
    if (currentUnderRepRes
        .getHealthState() != ContainerHealthResult.HealthState
        .UNDER_REPLICATED) {
      LOG.info("The container {} state changed and it's not in under"
              + " replication any more. Current state is: {}",
          container.getContainerID(), currentUnderRepRes);
      return emptyMap();
    }
    ContainerHealthResult.UnderReplicatedHealthResult containerHealthResult =
        ((ContainerHealthResult.UnderReplicatedHealthResult)
            currentUnderRepRes);
    if (containerHealthResult.isSufficientlyReplicatedAfterPending()) {
      LOG.info("The container {} with replicas {} is sufficiently replicated",
          container.getContainerID(), replicaCount.getReplicas());
      return emptyMap();
    }
    if (replicaCount.isUnrecoverable()) {
      LOG.warn("The container {} is unrecoverable. The available replicas" +
          " are: {}.", container.containerID(), replicaCount.getReplicas());
      return emptyMap();
    }
    final ContainerID id = container.containerID();
    final Map<DatanodeDetails, SCMCommand<?>> commands = new HashMap<>();
    try {
     // State is UNDER_REPLICATED
      final List<DatanodeDetails> deletionInFlight = new ArrayList<>();
      for (ContainerReplicaOp op : pendingOps) {
        if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
          deletionInFlight.add(op.getTarget());
        }
      }
      List<Integer> missingIndexes = replicaCount.unavailableIndexes(true);
      // We got the missing indexes, this is excluded any decommissioning
      // indexes. Find the good source nodes.
      if (missingIndexes.size() > 0) {
        Map<Integer, ContainerReplica> sources = replicas.stream().filter(r -> r
            .getState() == StorageContainerDatanodeProtocolProtos
            .ContainerReplicaProto.State.CLOSED)
            // Exclude stale and dead nodes. This is particularly important for
            // maintenance nodes, as the replicas will remain present in the
            // container manager, even when they go dead.
            .filter(r -> ReplicationManager
                .getNodeStatus(r.getDatanodeDetails(), nodeManager).isHealthy())
            .filter(r -> !deletionInFlight.contains(r.getDatanodeDetails()))
            .collect(
                Collectors.toMap(ContainerReplica::getReplicaIndex, x -> x));
        LOG.debug("Missing indexes detected for the container {}." +
                " The missing indexes are {}", id, missingIndexes);
        // We have source nodes.
        if (sources.size() >= repConfig.getData()) {
          final List<DatanodeDetails> selectedDatanodes =
              getTargetDatanodes(replicas, container, missingIndexes.size());

          List<ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex>
              sourceDatanodesWithIndex = new ArrayList<>();
          for (ContainerReplica srcReplica : sources.values()) {
            sourceDatanodesWithIndex.add(
                new ReconstructECContainersCommand
                    .DatanodeDetailsAndReplicaIndex(
                    srcReplica.getDatanodeDetails(),
                    srcReplica.getReplicaIndex()));
          }

          final ReconstructECContainersCommand reconstructionCommand =
              new ReconstructECContainersCommand(id.getProtobuf().getId(),
                  sourceDatanodesWithIndex, selectedDatanodes,
                  int2byte(missingIndexes),
                  repConfig);
          // Keeping the first target node as coordinator.
          commands.put(selectedDatanodes.get(0), reconstructionCommand);
        } else {
          LOG.warn("Cannot proceed for EC container reconstruction for {}, due"
              + "to insufficient source replicas found. Number of source "
              + "replicas needed: {}. Number of available source replicas are:"
              + " {}. Available sources are: {}", container.containerID(),
              repConfig.getData(), sources.size(), sources);
        }
      }

      Set<Integer> decomIndexes = replicaCount.decommissioningOnlyIndexes(true);
      if (decomIndexes.size() > 0) {
        final List<DatanodeDetails> selectedDatanodes =
            getTargetDatanodes(replicas, container, decomIndexes.size());
        Iterator<DatanodeDetails> iterator = selectedDatanodes.iterator();
        // In this case we need to do one to one copy.
        for (ContainerReplica replica : replicas) {
          if (decomIndexes.contains(replica.getReplicaIndex())) {
            if (!iterator.hasNext()) {
              LOG.warn("Couldn't find the enough targets. Available source"
                  + " nodes: {}, the target nodes: {} and the decommission"
                  + " indexes: {}", replicas, selectedDatanodes, decomIndexes);
              break;
            }
            DatanodeDetails decommissioningSrcNode
                = replica.getDatanodeDetails();
            final ReplicateContainerCommand replicateCommand =
                new ReplicateContainerCommand(id.getProtobuf().getId(),
                    ImmutableList.of(decommissioningSrcNode));
            // For EC containers, we need to track the replica index which is
            // to be replicated, so add it to the command.
            replicateCommand.setReplicaIndex(replica.getReplicaIndex());
            DatanodeDetails target = iterator.next();
            commands.put(target, replicateCommand);
          }
        }
      }
    } catch (IOException | IllegalStateException ex) {
      LOG.warn("Exception while processing for creating the EC reconstruction" +
              " container commands for {}.",
          id, ex);
      throw ex;
    }
    return commands;
  }

  private List<DatanodeDetails> getTargetDatanodes(
      Set<ContainerReplica> replicas, ContainerInfo container,
      int requiredNodes) throws IOException {
    // We should ensure that the target datanode has enough space
    // for a complete container to be created, but since the container
    // size may be changed smaller than origin, we should be defensive.
    final long dataSizeRequired =
        Math.max(container.getUsedBytes(), currentContainerSize);
    final List<DatanodeDetails> excludeList =
        replicas.stream().map(ContainerReplica::getDatanodeDetails)
            .collect(Collectors.toList());

    return containerPlacement
        .chooseDatanodes(excludeList, null, requiredNodes, 0, dataSizeRequired);
  }

  private static byte[] int2byte(List<Integer> src) {
    byte[] dst = new byte[src.size()];

    for (int i = 0; i < src.size(); i++) {
      dst[i] = src.get(i).byteValue();
    }
    return dst;
  }
}
