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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class checks if an EC container is mis replicated. The container should
 * not be over or under replicated, and should not have an excess of UNHEALTHY
 * replicas.
 */
public class ECMisReplicationCheckHandler extends AbstractCheck {
  static final Logger LOG =
      LoggerFactory.getLogger(ECMisReplicationCheckHandler.class);

  private final PlacementPolicy placementPolicy;

  public ECMisReplicationCheckHandler(PlacementPolicy placementPolicy) {
    this.placementPolicy = placementPolicy;
  }

  @Override
  public boolean handle(ContainerCheckRequest request) {
    if (request.getContainerInfo().getReplicationType() != EC) {
      // This handler is only for EC containers.
      return false;
    }

    ReplicationManagerReport report = request.getReport();
    ContainerInfo container = request.getContainerInfo();
    ContainerID containerID = container.containerID();
    LOG.debug("Checking container {} for mis replication.", container);

    ContainerHealthResult health = checkMisReplication(request);
    if (health.getHealthState() ==
        ContainerHealthResult.HealthState.MIS_REPLICATED) {
      report.incrementAndSample(
          ReplicationManagerReport.HealthState.MIS_REPLICATED, containerID);
      ContainerHealthResult.MisReplicatedHealthResult misRepHealth
          = ((ContainerHealthResult.MisReplicatedHealthResult) health);
      if (!misRepHealth.isReplicatedOkAfterPending()) {
        request.getReplicationQueue().enqueue(misRepHealth);
      }
      LOG.debug("Container {} is Mis Replicated. isReplicatedOkAfterPending "
              + "is [{}]. Reason for mis replication is [{}].", container,
          misRepHealth.isReplicatedOkAfterPending(),
          misRepHealth.getMisReplicatedReason());
      return true;
    }

    return false;
  }

  ContainerHealthResult checkMisReplication(ContainerCheckRequest request) {
    ContainerInfo container = request.getContainerInfo();
    Set<ContainerReplica> replicas = request.getContainerReplicas();

    ContainerPlacementStatus placement = getPlacementStatus(replicas,
        container.getReplicationConfig().getRequiredNodes(),
        Collections.emptyList());
    if (!placement.isPolicySatisfied()) {
      /*
      Check if policy is satisfied by considering pending ops. For example if
      there are some pending adds that can fix mis replication then we don't
      want to queue this container.
       */
      ContainerPlacementStatus placementAfterPending = getPlacementStatus(
          replicas, container.getReplicationConfig().getRequiredNodes(),
          request.getPendingOps());
      return new ContainerHealthResult.MisReplicatedHealthResult(
          container, placementAfterPending.isPolicySatisfied(),
          placementAfterPending.misReplicatedReason());
    }

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

    Set<DatanodeDetails> replicaDns = replicas
        .stream()
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

    return placementPolicy.validateContainerPlacement(
        new ArrayList<>(replicaDns), replicationFactor);
  }

}
