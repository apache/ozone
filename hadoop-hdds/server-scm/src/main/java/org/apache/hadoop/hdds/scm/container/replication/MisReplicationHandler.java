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
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.InsufficientDatanodesException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the Mis replication processing and forming the respective SCM
 * commands.
 * Mis-replication: State of replicas where containers are neither
 * under-replicated or over-replicated, but the existing placement
 * of containers do not conform to the configured placement policy
 * of the container.
 */
public abstract class MisReplicationHandler implements
        UnhealthyReplicationHandler {

  protected static final Logger LOG =
          LoggerFactory.getLogger(MisReplicationHandler.class);
  private final PlacementPolicy containerPlacement;
  private final long currentContainerSize;
  private final ReplicationManager replicationManager;
  private final ReplicationManagerMetrics metrics;

  public MisReplicationHandler(
      final PlacementPolicy containerPlacement,
      final ConfigurationSource conf, ReplicationManager replicationManager) {
    this.containerPlacement = containerPlacement;
    this.currentContainerSize = (long) conf.getStorageSize(
            ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
            ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    this.replicationManager = replicationManager;
    this.metrics = replicationManager.getMetrics();
  }

  protected ReplicationManager getReplicationManager() {
    return replicationManager;
  }

  protected abstract ContainerReplicaCount getContainerReplicaCount(
      ContainerInfo containerInfo, Set<ContainerReplica> replicas,
      List<ContainerReplicaOp> pendingOps, int remainingMaintenanceRedundancy)
      throws IOException;

  private Set<ContainerReplica> filterSources(Set<ContainerReplica> replicas) {
    return replicas.stream()
        .filter(r -> r.getState() == StorageContainerDatanodeProtocolProtos
            .ContainerReplicaProto.State.CLOSED || r.getState() ==
                StorageContainerDatanodeProtocolProtos
                    .ContainerReplicaProto.State.QUASI_CLOSED
        )
        .filter(r -> {
          try {
            return replicationManager.getNodeStatus(r.getDatanodeDetails())
                .isHealthy();
          } catch (NodeNotFoundException e) {
            return false;
          }
        })
        .filter(r -> r.getDatanodeDetails().getPersistedOpState()
            == HddsProtos.NodeOperationalState.IN_SERVICE)
        .collect(Collectors.toSet());
  }

  protected abstract int sendReplicateCommands(
      ContainerInfo containerInfo,
      Set<ContainerReplica> replicasToBeReplicated,
      List<DatanodeDetails> sources, List<DatanodeDetails> targetDns)
      throws CommandTargetOverloadedException, NotLeaderException;

  @Override
  public int processAndSendCommands(
      Set<ContainerReplica> replicas, List<ContainerReplicaOp> pendingOps,
      ContainerHealthResult result, int remainingMaintenanceRedundancy)
      throws IOException {
    ContainerInfo container = result.getContainerInfo();
    if (!pendingOps.isEmpty()) {
      LOG.info("Skipping Mis-Replication for Container {}, " +
               "as there are still some pending ops for the container: {}",
              container, pendingOps);
      return 0;
    }
    ContainerReplicaCount replicaCount = getContainerReplicaCount(container,
            replicas, Collections.emptyList(), remainingMaintenanceRedundancy);

    if (!replicaCount.isSufficientlyReplicated() ||
            replicaCount.isOverReplicated()) {
      LOG.info("Container {} state should be neither under replicated " +
              "nor over replicated before resolving misreplication." +
              "Container UnderReplication status: {}," +
              "Container OverReplication status: {}",
              container.getContainerID(),
              !replicaCount.isSufficientlyReplicated(),
              replicaCount.isOverReplicated());
      return 0;
    }

    List<DatanodeDetails> usedDns = replicas.stream()
            .map(ContainerReplica::getDatanodeDetails)
            .collect(Collectors.toList());
    if (containerPlacement.validateContainerPlacement(usedDns,
            usedDns.size()).isPolicySatisfied()) {
      LOG.info("Container {} is currently not misreplicated",
              container.getContainerID());
      return 0;
    }

    LOG.debug("Handling mis replicated container {}.", container);
    Set<ContainerReplica> sources = filterSources(replicas);
    Set<ContainerReplica> replicasToBeReplicated = containerPlacement
            .replicasToCopyToFixMisreplication(replicas.stream()
            .collect(Collectors.toMap(Function.identity(), sources::contains)));

    ReplicationManagerUtil.ExcludedAndUsedNodes excludedAndUsedNodes
        = ReplicationManagerUtil.getExcludedAndUsedNodes(container,
            new ArrayList(replicas), replicasToBeReplicated,
            Collections.emptyList(), replicationManager);

    int requiredNodes = replicasToBeReplicated.size();

    List<DatanodeDetails> targetDatanodes = ReplicationManagerUtil
        .getTargetDatanodes(containerPlacement, requiredNodes,
            excludedAndUsedNodes.getUsedNodes(),
            excludedAndUsedNodes.getExcludedNodes(), currentContainerSize,
            container);
    List<DatanodeDetails> availableSources = sources.stream()
        .map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList());

    int count = sendReplicateCommands(container, replicasToBeReplicated,
        availableSources, targetDatanodes);

    int found = targetDatanodes.size();
    if (found < requiredNodes) {
      if (container.getReplicationType() == HddsProtos.ReplicationType.EC) {
        metrics.incrEcPartialReplicationForMisReplicationTotal();
      } else {
        metrics.incrPartialReplicationForMisReplicationTotal();
      }
      LOG.warn("Placement Policy {} found only {} nodes for Container: {}," +
          " number of required nodes: {}, usedNodes : {}",
          containerPlacement.getClass(), found,
          container.getContainerID(), requiredNodes,
          usedDns);
      throw new InsufficientDatanodesException(requiredNodes, found);
    }

    return count;
  }
}
