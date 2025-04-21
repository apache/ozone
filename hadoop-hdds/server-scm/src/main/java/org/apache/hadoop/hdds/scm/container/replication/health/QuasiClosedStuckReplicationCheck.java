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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.QUASI_CLOSED;

import java.util.Set;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.QuasiClosedStuckReplicaCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to check for the replication of the replicas in quasi-closed stuck containers. As we want to maintain
 * as much data and information as possible, the rule for QC stuck container is to maintain 2 copies of each origin
 * if there is more than 1 origin. If there is only 1 origin, then we need to maintain 3 copies.
 */
public class QuasiClosedStuckReplicationCheck  extends AbstractCheck {
  private static final Logger LOG = LoggerFactory.getLogger(QuasiClosedStuckReplicationCheck.class);

  public static boolean shouldHandleAsQuasiClosedStuck(ContainerInfo containerInfo, Set<ContainerReplica> replicas) {
    if (containerInfo.getState() != QUASI_CLOSED) {
      return false;
    }
    if (!QuasiClosedContainerHandler.isQuasiClosedStuck(containerInfo, replicas)) {
      return false;
    }
    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(replicas, 0);
    if (replicaCount.availableOrigins() == 1) {
      // This is the 3 copies of a single origin case, so allow it to be handled via the normal under-replicated
      // handler.
      return false;
    }
    // If we have all origins with open replicas, and not unhealthy then the container should close after the close
    // goes through, so this handler should not run.
    return !hasEnoughOriginsWithOpen(containerInfo, replicas);
  }

  @Override
  public boolean handle(ContainerCheckRequest request) {
    if (!shouldHandleAsQuasiClosedStuck(request.getContainerInfo(), request.getContainerReplicas())) {
      return false;
    }

    if (request.getContainerReplicas().isEmpty()) {
      // If there are no replicas, then mark as missing and return.
      request.getReport().incrementAndSample(
          ReplicationManagerReport.HealthState.MISSING, request.getContainerInfo().containerID());
      return true;
    }

    QuasiClosedStuckReplicaCount replicaCount = new QuasiClosedStuckReplicaCount(
        request.getContainerReplicas(), request.getMaintenanceRedundancy());

    if (!replicaCount.hasHealthyReplicas()) {
      // All unhealthy are handled by a different handler
      return false;
    }

    int pendingAdd = 0;
    int pendingDelete = 0;
    for (ContainerReplicaOp op : request.getPendingOps()) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.ADD) {
        pendingAdd++;
      } else if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
        pendingDelete++;
      }
    }

    if (replicaCount.isUnderReplicated()) {
      LOG.debug("Container {} is quasi-closed-stuck under-replicated", request.getContainerInfo());
      request.getReport().incrementAndSample(ReplicationManagerReport.HealthState.UNDER_REPLICATED,
          request.getContainerInfo().containerID());
      if (pendingAdd == 0) {
        // Only queue if there are no pending adds, as that could correct the under replication.
        LOG.debug("Queueing under-replicated health result for container {}", request.getContainerInfo());
        ContainerHealthResult.UnderReplicatedHealthResult underReplicatedHealthResult =
            new ContainerHealthResult.UnderReplicatedHealthResult(request.getContainerInfo(), 1,
                replicaCount.hasOutOfServiceReplicas(), false, false);
        request.getReplicationQueue().enqueue(underReplicatedHealthResult);
      }
      return true;
    }

    if (replicaCount.isOverReplicated()) {
      LOG.debug("Container {} is quasi-closed-stuck over-replicated", request.getContainerInfo());
      request.getReport().incrementAndSample(ReplicationManagerReport.HealthState.OVER_REPLICATED,
          request.getContainerInfo().containerID());
      if (pendingDelete == 0) {
        // Only queue if there are no pending deletes which could correct the over replication
        LOG.debug("Queueing over-replicated health result for container {}", request.getContainerInfo());
        ContainerHealthResult.OverReplicatedHealthResult overReplicatedHealthResult =
            new ContainerHealthResult.OverReplicatedHealthResult(request.getContainerInfo(), 1, false);
        request.getReplicationQueue().enqueue(overReplicatedHealthResult);
      }
      return true;
    }
    return false;
  }

  private static boolean hasEnoughOriginsWithOpen(ContainerInfo containerInfo, Set<ContainerReplica> replicas) {
    final long uniqueOpenReplicaCount = replicas.stream()
        .filter(r -> r.getState() == State.QUASI_CLOSED || r.getState() == State.OPEN)
        .map(ContainerReplica::getOriginDatanodeId)
        .distinct()
        .count();
    final int replicationFactor = containerInfo.getReplicationConfig().getRequiredNodes();
    return uniqueOpenReplicaCount >= replicationFactor;
  }

}
