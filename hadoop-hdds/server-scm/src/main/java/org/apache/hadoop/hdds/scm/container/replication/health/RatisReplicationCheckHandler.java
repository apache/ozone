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

package org.apache.hadoop.hdds.scm.container.replication.health;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.RatisContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManagerUtil;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to determine the health state of a Ratis Container. Given the container
 * and current replica details, along with replicas pending add and delete,
 * this class will return a ContainerHealthResult indicating if the container
 * is healthy, or under / over replicated etc.
 * <p>
 * For example, this class handles:
 * <ul>
 *   <li>A CLOSED container with 4 CLOSED replicas.</li>
 *   <li>A CLOSED container with 1 CLOSED replica.</li>
 *   <li>A CLOSED container with 3 CLOSED and 1 UNHEALTHY replica. Or 3
 *   CLOSED and 1 QUASI_CLOSED replica with incorrect sequence ID.</li>
 *   <li>etc.</li>
 * </ul>
 */
public class RatisReplicationCheckHandler extends AbstractCheck {
  private static final Logger LOG =
      LoggerFactory.getLogger(RatisReplicationCheckHandler.class);

  /**
   * PlacementPolicy which is used to identify where a container
   * should be replicated.
   */
  private final PlacementPolicy ratisContainerPlacement;
  private final ReplicationManager replicationManager;

  public RatisReplicationCheckHandler(PlacementPolicy containerPlacement,
      ReplicationManager replicationManager) {
    this.ratisContainerPlacement = containerPlacement;
    this.replicationManager = replicationManager;
  }

  @Override
  public boolean handle(ContainerCheckRequest request) {
    if (request.getContainerInfo().getReplicationType() != RATIS) {
      // This handler is only for Ratis containers.
      return false;
    }
    if (QuasiClosedStuckReplicationCheck
        .shouldHandleAsQuasiClosedStuck(request.getContainerInfo(), request.getContainerReplicas())) {
      return false;
    }
    ReplicationManagerReport report = request.getReport();
    ContainerInfo container = request.getContainerInfo();
    ContainerHealthResult health = checkHealth(request);
    LOG.debug("Checking container {} in RatisReplicationCheckHandler",
        container);
    if (health.getHealthState() == ContainerHealthResult.HealthState.HEALTHY ||
        health.getHealthState() ==
            ContainerHealthResult.HealthState.UNHEALTHY) {
      // If the container is healthy, there is nothing else to do in this
      // handler so return as unhandled so any further handlers will be tried.
      return false;
    }

    if (health.getHealthState()
        == ContainerHealthResult.HealthState.UNDER_REPLICATED) {
      ContainerHealthResult.UnderReplicatedHealthResult underHealth
          = ((ContainerHealthResult.UnderReplicatedHealthResult) health);
      if (!underHealth.isUnrecoverable() && !underHealth.hasHealthyReplicas()) {
        /*
        If the container is recoverable but does not have healthy replicas,
        return false. Unhealthy replication can be checked in a handler
        further down the chain.
         */
        return false;
      }

      LOG.debug("Container {} is Under Replicated. isReplicatedOkAfterPending" +
              " is [{}]. isUnrecoverable is [{}]. hasHealthyReplicas is [{}].",
          container,
          underHealth.isReplicatedOkAfterPending(),
          underHealth.isUnrecoverable(), underHealth.hasHealthyReplicas());

      if (underHealth.isUnrecoverable()) {
        report.incrementAndSample(ReplicationManagerReport.HealthState.MISSING,
            container.containerID());
        return true;
      }
      report.incrementAndSample(
          ReplicationManagerReport.HealthState.UNDER_REPLICATED,
          container.containerID());

      if (!underHealth.isReplicatedOkAfterPending() &&
          underHealth.hasHealthyReplicas()) {
        request.getReplicationQueue().enqueue(underHealth);
      }
      return true;
    }

    /*
    If we reach here, it's assumed that under replication without considering
    UNHEALTHY replicas has been checked first. This means that a CLOSED
    container with 2 CLOSED and 2 UNHEALTHY replicas is called under
    replicated and not over replicated because only the 2 CLOSED replicas
    are 'healthy' and have the expected data.
    */
    if (health.getHealthState()
        == ContainerHealthResult.HealthState.OVER_REPLICATED) {
      report.incrementAndSample(
          ReplicationManagerReport.HealthState.OVER_REPLICATED,
          container.containerID());
      ContainerHealthResult.OverReplicatedHealthResult overHealth
          = ((ContainerHealthResult.OverReplicatedHealthResult) health);
      if (!overHealth.isReplicatedOkAfterPending() &&
          !overHealth.hasMismatchedReplicas() &&
          overHealth.isSafelyOverReplicated()) {
        /*
        A mis matched replica is one whose state does not match the
        container's state and the state is not UNHEALTHY.
        For example, a CLOSED container with 1 CLOSED, 2 CLOSING, and 1
        UNHEALTHY replica has 2 mis matched replicas (the 2 CLOSING ones).
        We want to CLOSE the mis matched replicas first before queuing the
        container for over replication.
         */
        request.getReplicationQueue().enqueue(overHealth);
      }
      LOG.debug("Container {} is Over Replicated. isReplicatedOkAfterPending" +
              " is [{}]. hasMismatchedReplicas is [{}]. " +
              "isSafelyOverReplicated is [{}].",
          container,
          overHealth.isReplicatedOkAfterPending(),
          overHealth.hasMismatchedReplicas(),
          overHealth.isSafelyOverReplicated());
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
      LOG.debug("Container {} is Mis Replicated. isReplicatedOkAfterPending" +
              " is [{}]. Reason for mis replication is [{}].", container,
          misRepHealth.isReplicatedOkAfterPending(),
          misRepHealth.getMisReplicatedReason());
      return true;
    }
    // Should not get here, but in case it does the container is not healthy,
    // but is also not under, over or mis replicated.
    LOG.warn("Container {} is not healthy but is not under, over or "
        + " mis-replicated. Should not happen.", container);
    return false;
  }

  public ContainerHealthResult checkHealth(ContainerCheckRequest request) {
    ContainerInfo container = request.getContainerInfo();
    Set<ContainerReplica> replicas = request.getContainerReplicas();
    List<ContainerReplicaOp> replicaPendingOps = request.getPendingOps();
    // Note that this setting is minReplicasForMaintenance. For EC the variable
    // is defined as remainingRedundancy which is subtly different.
    int minReplicasForMaintenance = request.getMaintenanceRedundancy();

    /*
    When checking for under replication, don't consider UNHEALTHY replicas.
    This means that a CLOSED container with 1 CLOSED and 2 UNHEALTHY replicas
    is under replicated and needs 2 more healthy replicas because we're not
    counting the UNHEALTHY ones.
     */
    RatisContainerReplicaCount replicaCount =
        new RatisContainerReplicaCount(container, replicas, replicaPendingOps,
            minReplicasForMaintenance, false);
    boolean sufficientlyReplicated
        = replicaCount.isSufficientlyReplicated(false);
    if (!sufficientlyReplicated) {
      return replicaCount.toUnderHealthResult();
    }


    if (replicaCount.isOverReplicated(false)) {
      // If the container is over replicated without considering UNHEALTHY
      // then we know for sure it is over replicated, so mark as such.
      return replicaCount.toOverHealthResult();
    }
    /*
    When checking for over replication, consider UNHEALTHY replicas. This means
    that other than checking over replication of healthy replicas (such as 4
    CLOSED replicas of a CLOSED container), we're also checking for an excess
    of unhealthy replicas (such as 3 CLOSED and 1 UNHEALTHY replicas of a
    CLOSED container, or 3 CLOSED and 1 QUASI_CLOSED with incorrect sequence
    ID for a CLOSED container).
     */
    RatisContainerReplicaCount consideringUnhealthy =
        new RatisContainerReplicaCount(container, replicas, replicaPendingOps,
            minReplicasForMaintenance, true);

    if (consideringUnhealthy.isOverReplicated(false)) {
      if (container.getState() == HddsProtos.LifeCycleState.CLOSED) {
        return consideringUnhealthy.toOverHealthResult();
      } else if (container.getState()
          == HddsProtos.LifeCycleState.QUASI_CLOSED) {
        // If the container is quasi-closed and over replicated, we may have a
        // case where the excess replica is an unhealthy one, but it has a
        // unique origin and therefore should not be deleted. In this case,
        // we should not mark the container as over replicated.
        // We ignore pending deletes, as a container is still over replicated
        // until the pending delete completes.
        ContainerReplica toDelete = ReplicationManagerUtil
            .selectUnhealthyReplicaForDelete(container, replicas, 0,
                (dnd) -> {
                  try {
                    return replicationManager.getNodeStatus(dnd);
                  } catch (NodeNotFoundException e) {
                    return null;
                  }
                });
        if (toDelete != null) {
          // There is at least one unhealthy replica that can be deleted, so
          // return as over replicated.
          return consideringUnhealthy.toOverHealthResult();
        } else {
          // Even though we have at least 4 replicas with some unhealthy, we
          // can't delete any of them, so the container is not over replicated.
          return new ContainerHealthResult.HealthyResult(container);
        }
      }
    }

    int requiredNodes = container.getReplicationConfig().getRequiredNodes();
    ContainerPlacementStatus placementStatus =
        getPlacementStatus(replicas, requiredNodes, Collections.emptyList());
    ContainerPlacementStatus placementStatusWithPending = placementStatus;
    if (!placementStatus.isPolicySatisfied()) {
      if (!replicaPendingOps.isEmpty()) {
        placementStatusWithPending =
            getPlacementStatus(replicas, requiredNodes, replicaPendingOps);
      }
      return new ContainerHealthResult.MisReplicatedHealthResult(
          container, placementStatusWithPending.isPolicySatisfied(),
          placementStatusWithPending.misReplicatedReason());
    }

    if (replicaCount.getUnhealthyReplicaCount() != 0) {
      return new ContainerHealthResult.UnHealthyResult(container);
    }
    // No issues detected, just return healthy.
    return new ContainerHealthResult.HealthyResult(container);
  }

  /**
   * Given a set of ContainerReplica, transform it to a list of DatanodeDetails
   * and then check if the list meets the container placement policy.
   * @param replicas List of containerReplica
   * @param replicationFactor Expected Replication Factor of the container
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
