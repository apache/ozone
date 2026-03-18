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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.RatisContainerReplicaCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles RATIS containers which only have replicas in UNHEALTHY
 * state, or CLOSED containers with replicas in QUASI_CLOSED state having
 * less sequence ID. There are no other replicas. This class ensures that
 * such containers have replication factor number of UNHEALTHY/QUASI_CLOSED
 * replicas.
 * <p>
 * For example, if a CLOSED container with replication factor 3 has 4 UNHEALTHY
 * replicas, then it's called over replicated and 1 UNHEALTHY replica must be
 * deleted. On the other hand, if it has only 2 UNHEALTHY replicas, it's
 * under replicated and 1 more replica should be created.
 * </p>
 */
public class RatisUnhealthyReplicationCheckHandler extends AbstractCheck {
  private static final Logger LOG = LoggerFactory.getLogger(
      RatisUnhealthyReplicationCheckHandler.class);

  @Override
  public boolean handle(ContainerCheckRequest request) {
    if (request.getContainerInfo().getReplicationType() != RATIS) {
      // This handler is only for Ratis containers.
      return false;
    }
    ReplicationManagerReport report = request.getReport();
    ContainerInfo container = request.getContainerInfo();

    RatisContainerReplicaCount replicaCount = getReplicaCount(request);
    if (replicaCount.getHealthyReplicaCount() > 0 ||
        replicaCount.getUnhealthyReplicaCount() == 0) {
      LOG.debug("Not handling container {} with replicas [{}].", container,
          request.getContainerReplicas());
      return false;
    } else {
      LOG.info("Container {} has unhealthy replicas [{}]. Checking its " +
          "replication status.", container, replicaCount.getReplicas());
    }

    // At this point, we know there are only unhealthy replicas, so the
    // container in UNHEALTHY, but it can also be over or under replicated with
    // unhealthy replicas.
    ContainerHealthResult health = checkReplication(replicaCount);
    if (health.getHealthState()
        == ContainerHealthResult.HealthState.UNDER_REPLICATED) {
      ContainerHealthResult.UnderReplicatedHealthResult underHealth
          = ((ContainerHealthResult.UnderReplicatedHealthResult) health);
      // Container is UNHEALTHY + UNDER_REPLICATED
      report.incrementAndSample(ContainerHealthState.UNHEALTHY_UNDER_REPLICATED, container);
      LOG.debug("Container {} is Under Replicated. isReplicatedOkAfterPending" +
              " is [{}]. isUnrecoverable is [{}]. hasHealthyReplicas is [{}].",
          container,
          underHealth.isReplicatedOkAfterPending(),
          underHealth.isUnrecoverable(), underHealth.hasHealthyReplicas());

      if (!underHealth.isReplicatedOkAfterPending()) {
        request.getReplicationQueue().enqueue(underHealth);
      }
      return true;
    }

    if (health.getHealthState()
        == ContainerHealthResult.HealthState.OVER_REPLICATED) {
      // Container is UNHEALTHY + OVER_REPLICATED
      report.incrementAndSample(ContainerHealthState.UNHEALTHY_OVER_REPLICATED, container);
      ContainerHealthResult.OverReplicatedHealthResult overHealth
          = ((ContainerHealthResult.OverReplicatedHealthResult) health);
      LOG.debug("Container {} is Over Replicated. isReplicatedOkAfterPending" +
              " is [{}]. hasMismatchedReplicas is [{}]", container,
          overHealth.isReplicatedOkAfterPending(),
          overHealth.hasMismatchedReplicas());

      if (!overHealth.isReplicatedOkAfterPending()) {
        request.getReplicationQueue().enqueue(overHealth);
      }
      return true;
    }

    return false;
  }

  /**
   * Checks if the container is over or under replicated.
   */
  @VisibleForTesting
  ContainerHealthResult checkReplication(ContainerCheckRequest request) {
    return checkReplication(getReplicaCount(request));
  }

  private static RatisContainerReplicaCount getReplicaCount(
      ContainerCheckRequest request) {
    return new RatisContainerReplicaCount(request.getContainerInfo(),
        request.getContainerReplicas(), request.getPendingOps(),
        request.getMaintenanceRedundancy(), true);
  }

  private ContainerHealthResult checkReplication(
      RatisContainerReplicaCount replicaCount) {

    boolean sufficientlyReplicated
        = replicaCount.isSufficientlyReplicated(false);
    if (!sufficientlyReplicated) {
      return replicaCount.toUnderHealthResult();
    }

    boolean isOverReplicated = replicaCount.isOverReplicated(false);
    if (isOverReplicated) {
      return replicaCount.toOverHealthResult();
    }

    return new ContainerHealthResult.UnHealthyResult(
        replicaCount.getContainer());
  }
}
