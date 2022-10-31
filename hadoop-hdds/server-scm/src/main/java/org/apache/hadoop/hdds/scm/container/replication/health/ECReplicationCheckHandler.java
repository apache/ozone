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

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.ECContainerReplicaCount;

import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;

/**
 * Container Check handler to check the under / over replication state for
 * EC containers. If any containers are found to be over or under replicated
 * they are added to the queue passed within the request object.
 */
public class ECReplicationCheckHandler extends AbstractCheck {

  public ECReplicationCheckHandler() {
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
    ContainerHealthResult health = checkHealth(request);
    if (health.getHealthState() == ContainerHealthResult.HealthState.HEALTHY) {
      // If the container is healthy, there is nothing else to do in this
      // handler so return as unhandled so any further handlers will be tried.
      return false;
    }
    // TODO - should the report have a HEALTHY state, rather than just bad
    //        states? It would need to be added to legacy RM too.
    if (health.getHealthState()
        == ContainerHealthResult.HealthState.UNDER_REPLICATED) {
      report.incrementAndSample(
          ReplicationManagerReport.HealthState.UNDER_REPLICATED, containerID);
      ContainerHealthResult.UnderReplicatedHealthResult underHealth
          = ((ContainerHealthResult.UnderReplicatedHealthResult) health);
      if (underHealth.isUnrecoverable()) {
        // TODO - do we need a new health state for unrecoverable EC?
        report.incrementAndSample(
            ReplicationManagerReport.HealthState.MISSING, containerID);
      }
      // TODO - if it is unrecoverable, should we return false to other
      //        handlers can be tried?
      if (!underHealth.isSufficientlyReplicatedAfterPending() &&
          !underHealth.isUnrecoverable()) {
        request.getReplicationQueue().enqueue(underHealth);
      }
      return true;
    } else if (health.getHealthState()
        == ContainerHealthResult.HealthState.OVER_REPLICATED) {
      report.incrementAndSample(
          ReplicationManagerReport.HealthState.OVER_REPLICATED, containerID);
      ContainerHealthResult.OverReplicatedHealthResult overHealth
          = ((ContainerHealthResult.OverReplicatedHealthResult) health);
      if (!overHealth.isSufficientlyReplicatedAfterPending()) {
        request.getReplicationQueue().enqueue(overHealth);
      }
      return true;
    }
    // Should not get here, but incase it does the container is not healthy,
    // but is also not under or over replicated.
    return false;
  }

  public ContainerHealthResult checkHealth(ContainerCheckRequest request) {
    ContainerInfo container = request.getContainerInfo();
    Set<ContainerReplica> replicas = request.getContainerReplicas();
    List<ContainerReplicaOp> replicaPendingOps = request.getPendingOps();
    ECContainerReplicaCount replicaCount =
        new ECContainerReplicaCount(container, replicas, replicaPendingOps,
            request.getMaintenanceRedundancy());

    ECReplicationConfig repConfig =
        (ECReplicationConfig) container.getReplicationConfig();

    if (!replicaCount.isSufficientlyReplicated(false)) {
      List<Integer> missingIndexes = replicaCount.unavailableIndexes(false);
      int remainingRedundancy = repConfig.getParity();
      boolean dueToDecommission = true;
      if (missingIndexes.size() > 0) {
        // The container has reduced redundancy and will need reconstructed
        // via an EC reconstruction command. Note that it may also have some
        // replicas in decommission / maintenance states, but as the under
        // replication is not caused only by decommission, we say it is not
        // due to decommission/
        dueToDecommission = false;
        remainingRedundancy = repConfig.getParity() - missingIndexes.size();
      }
      return new ContainerHealthResult.UnderReplicatedHealthResult(
          container, remainingRedundancy, dueToDecommission,
          replicaCount.isSufficientlyReplicated(true),
          replicaCount.isUnrecoverable());
    }

    if (replicaCount.isOverReplicated(false)) {
      List<Integer> overRepIndexes = replicaCount.overReplicatedIndexes(false);
      return new ContainerHealthResult
          .OverReplicatedHealthResult(container, overRepIndexes.size(),
          !replicaCount.isOverReplicated(true));
    }
    // No issues detected, so return healthy.
    return new ContainerHealthResult.HealthyResult(container);
  }
}
