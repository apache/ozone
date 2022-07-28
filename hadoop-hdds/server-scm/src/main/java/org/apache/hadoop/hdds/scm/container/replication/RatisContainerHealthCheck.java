/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class to determine the health state of a Ratis Container. Given the container
 * and current replica details, along with replicas pending add and delete,
 * this class will return a ContainerHealthResult indicating if the container
 * is healthy, or under / over replicated etc.
 */
public class RatisContainerHealthCheck implements ContainerHealthCheck {
  public static final Logger LOG =
      LoggerFactory.getLogger(RatisContainerHealthCheck.class);

  /**
   * PlacementPolicy which is used to identify where a container
   * should be replicated.
   */
  private final PlacementPolicy ratisContainerPlacement;

  public RatisContainerHealthCheck(final PlacementPolicy containerPlacement) {
    this.ratisContainerPlacement = containerPlacement;
  }

  @Override
  public ContainerHealthResult checkHealth(ContainerInfo container,
      Set<ContainerReplica> replicas,
      List<ContainerReplicaOp> replicaPendingOps,
      int remainingRedundancyForMaintenance) {
    int pendingAdd = 0;
    int pendingDelete = 0;
    for (ContainerReplicaOp op : replicaPendingOps) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.ADD) {
        pendingAdd++;
      } else if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
        pendingDelete++;
      } else {
        LOG.warn("unknown opType of op : {}", op);
        //TODO: return an appropriate state
        return new ContainerHealthResult.UnHealthyResult(container);
      }
    }
    int requiredNodes = container.getReplicationConfig().getRequiredNodes();

    RatisContainerReplicaCount replicaCount =
        new RatisContainerReplicaCount(container, replicas, pendingAdd,
            pendingDelete, requiredNodes, remainingRedundancyForMaintenance);

    ContainerPlacementStatus placementStatus =
        getPlacementStatus(replicas, requiredNodes);

    boolean sufficientlyReplicated = replicaCount.isSufficientlyReplicated();
    boolean isPolicySatisfied = placementStatus.isPolicySatisfied();
    if (!sufficientlyReplicated || !isPolicySatisfied) {
      //TODO: HDDS-6892 return a Mis-Replicated health result
      // if due to placementStatus.
      return new ContainerHealthResult.UnderReplicatedHealthResult(
        container, replicaCount.getRemainingRedundancy(),
        isPolicySatisfied && replicas.size() >= requiredNodes,
        replicaCount.isSufficientlyReplicatedAfterPending(),
        replicaCount.isUnrecoverable());
    }

    boolean isOverReplicated = replicaCount.isOverReplicated();
    if (isOverReplicated) {
      return new ContainerHealthResult.OverReplicatedHealthResult(
          container, replicaCount.getExcessRedundancy(), true);
    }

    // No issues detected, just return healthy.
    return new ContainerHealthResult.HealthyResult(container);
  }

  /**
   * Given a set of ContainerReplica, transform it to a list of DatanodeDetails
   * and then check if the list meets the container placement policy.
   * @param replicas List of containerReplica
   * @param replicationFactor Expected Replication Factor of the containe
   * @return ContainerPlacementStatus indicating if the policy is met or not
   */
  private ContainerPlacementStatus getPlacementStatus(
      Set<ContainerReplica> replicas, int replicationFactor) {
    List<DatanodeDetails> replicaDns = replicas.stream()
        .map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList());
    return ratisContainerPlacement.validateContainerPlacement(
        replicaDns, replicationFactor);
  }
}
