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
package org.apache.hadoop.hdds.scm.container.replication.health;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.RatisContainerReplicaCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;

/**
 * Class to determine the health state of a Ratis Container. Given the container
 * and current replica details, along with replicas pending add and delete,
 * this class will return a ContainerHealthResult indicating if the container
 * is healthy, or under / over replicated etc.
 */
public class RatisReplicationCheckHandler extends AbstractCheck {
  public static final Logger LOG =
      LoggerFactory.getLogger(RatisReplicationCheckHandler.class);

  /**
   * PlacementPolicy which is used to identify where a container
   * should be replicated.
   */
  private final PlacementPolicy ratisContainerPlacement;

  public RatisReplicationCheckHandler(PlacementPolicy containerPlacement) {
    this.ratisContainerPlacement = containerPlacement;
  }

  @Override
  public boolean handle(ContainerCheckRequest request) {
    if (request.getContainerInfo().getReplicationType() != RATIS) {
      // This handler is only for Ratis containers.
      return false;
    }
    ReplicationManagerReport report = request.getReport();
    ContainerInfo container = request.getContainerInfo();
    ContainerHealthResult health = checkHealth(request);
    if (health.getHealthState() == ContainerHealthResult.HealthState.HEALTHY) {
      // If the container is healthy, there is nothing else to do in this
      // handler so return as unhandled so any further handlers will be tried.
      return false;
    }
    if (health.getHealthState()
        == ContainerHealthResult.HealthState.UNDER_REPLICATED) {
      report.incrementAndSample(
          ReplicationManagerReport.HealthState.UNDER_REPLICATED,
          container.containerID());
      ContainerHealthResult.UnderReplicatedHealthResult underHealth
          = ((ContainerHealthResult.UnderReplicatedHealthResult) health);
      if (underHealth.isUnrecoverable()) {
        report.incrementAndSample(ReplicationManagerReport.HealthState.MISSING,
            container.containerID());
      }
      // TODO - if it is unrecoverable, should we return false to other
      //        handlers can be tried?
      if (!underHealth.isUnrecoverable() &&
          !underHealth.isReplicatedOkAfterPending()) {
        request.getReplicationQueue().enqueue(underHealth);
      }
      return true;
    }

    if (health.getHealthState()
        == ContainerHealthResult.HealthState.OVER_REPLICATED) {
      report.incrementAndSample(
          ReplicationManagerReport.HealthState.OVER_REPLICATED,
          container.containerID());
      ContainerHealthResult.OverReplicatedHealthResult overHealth
          = ((ContainerHealthResult.OverReplicatedHealthResult) health);
      if (!overHealth.isReplicatedOkAfterPending()) {
        request.getReplicationQueue().enqueue(overHealth);
      }
      return true;
    }
    if (health.getHealthState() ==
        ContainerHealthResult.HealthState.MIS_REPLICATED) {
      report.incrementAndSample(
          ReplicationManagerReport.HealthState.MIS_REPLICATED,
          container.containerID());
      ContainerHealthResult.MisReplicatedHealthResult misRepHealth
          = ((ContainerHealthResult.MisReplicatedHealthResult) health);
      if (!misRepHealth.isReplicatedOkAfterPending()) {
        request.getReplicationQueue().enqueue(misRepHealth);
      }
      return true;
    }
    return false;
  }

  public ContainerHealthResult checkHealth(ContainerCheckRequest request) {
    ContainerInfo container = request.getContainerInfo();
    Set<ContainerReplica> replicas = request.getContainerReplicas();
    List<ContainerReplicaOp> replicaPendingOps = request.getPendingOps();
    // Note that this setting is minReplicasForMaintenance. For EC the variable
    // is defined as remainingRedundancy which is subtly different.
    int minReplicasForMaintenance = request.getMaintenanceRedundancy();
    int pendingAdd = 0;
    int pendingDelete = 0;
    for (ContainerReplicaOp op : replicaPendingOps) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.ADD) {
        pendingAdd++;
      } else if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
        pendingDelete++;
      }
    }
    int requiredNodes = container.getReplicationConfig().getRequiredNodes();

    // RatisContainerReplicaCount uses the minReplicasForMaintenance rather
    // than remainingRedundancy which ECContainerReplicaCount uses.
    RatisContainerReplicaCount replicaCount =
        new RatisContainerReplicaCount(container, replicas, pendingAdd,
            pendingDelete, requiredNodes, minReplicasForMaintenance);

    boolean sufficientlyReplicated
        = replicaCount.isSufficientlyReplicated(false);
    if (!sufficientlyReplicated) {
      ContainerHealthResult.UnderReplicatedHealthResult result =
          new ContainerHealthResult.UnderReplicatedHealthResult(
          container, replicaCount.getRemainingRedundancy(),
          replicas.size() - pendingDelete >= requiredNodes,
          replicaCount.isSufficientlyReplicated(true),
          replicaCount.isUnrecoverable());
      return result;
    }

    boolean isOverReplicated = replicaCount.isOverReplicated(false);
    if (isOverReplicated) {
      boolean repOkWithPending = !replicaCount.isOverReplicated(true);
      return new ContainerHealthResult.OverReplicatedHealthResult(
          container, replicaCount.getExcessRedundancy(false), repOkWithPending);
    }

    ContainerPlacementStatus placementStatus =
        getPlacementStatus(replicas, requiredNodes, Collections.emptyList());
    ContainerPlacementStatus placementStatusWithPending = placementStatus;
    if (!placementStatus.isPolicySatisfied()) {
      if (replicaPendingOps.size() > 0) {
        placementStatusWithPending =
            getPlacementStatus(replicas, requiredNodes, replicaPendingOps);
      }
      return new ContainerHealthResult.MisReplicatedHealthResult(
          container, placementStatusWithPending.isPolicySatisfied());
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
      Set<ContainerReplica> replicas, int replicationFactor,
      List<ContainerReplicaOp> pendingOps) {

    Set<DatanodeDetails> replicaDns = replicas.stream()
        .map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toSet());
    for (ContainerReplicaOp op : pendingOps) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.ADD) {
        replicaDns.add(op.getTarget());
      }
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
        replicaDns.remove(op.getTarget());
      }
    }
    return ratisContainerPlacement.validateContainerPlacement(
        new ArrayList<>(replicaDns), replicationFactor);
  }
}
