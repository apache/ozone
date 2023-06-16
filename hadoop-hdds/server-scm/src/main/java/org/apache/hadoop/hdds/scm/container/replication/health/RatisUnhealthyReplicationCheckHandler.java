/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.replication.health;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.RatisContainerReplicaCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;

/**
 * This class handles UNHEALTHY RATIS replicas. Its responsibilities include:
 * <ul>
 *   <li>If only unhealthy replicas exist, check if there are replication
 *   factor number of replicas and queue them for under/over replication if
 *   needed.
 *   </li>
 *   <li>If there is perfect replication of healthy replicas, remove any
 *   excess unhealthy replicas.
 *   </li>
 * </ul>
 */
public class RatisUnhealthyReplicationCheckHandler extends AbstractCheck {
  public static final Logger LOG = LoggerFactory.getLogger(
      RatisUnhealthyReplicationCheckHandler.class);

  @Override
  public boolean handle(ContainerCheckRequest request) {
    if (request.getContainerInfo().getReplicationType() != RATIS) {
      // This handler is only for Ratis containers.
      return false;
    }
    ReplicationManagerReport report = request.getReport();
    ContainerInfo container = request.getContainerInfo();

    /*
    First, verify there's perfect replication without considering UNHEALTHY
    replicas. If not, we return false. Replication issues without UNHEALTHY
    replicas should be solved first.
     */
    if (!verifyPerfectReplication(request)) {
      return false;
    }

    // Now, consider UNHEALTHY replicas when calculating replication status
    RatisContainerReplicaCount replicaCount = getReplicaCount(request);
    if (replicaCount.getUnhealthyReplicaCount() == 0) {
      LOG.debug("No UNHEALTHY replicas are present for container {} with " +
          "replicas [{}].", container, request.getContainerReplicas());
      return false;
    } else {
      LOG.debug("Container {} has UNHEALTHY replicas. Checking its " +
          "replication status.", container);
      report.incrementAndSample(ReplicationManagerReport.HealthState.UNHEALTHY,
          container.containerID());
    }

    ContainerHealthResult health = checkReplication(replicaCount);
    if (health.getHealthState()
        == ContainerHealthResult.HealthState.UNDER_REPLICATED) {
      ContainerHealthResult.UnderReplicatedHealthResult underHealth
          = ((ContainerHealthResult.UnderReplicatedHealthResult) health);
      report.incrementAndSample(
          ReplicationManagerReport.HealthState.UNDER_REPLICATED,
          container.containerID());
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
      report.incrementAndSample(
          ReplicationManagerReport.HealthState.OVER_REPLICATED,
          container.containerID());
      ContainerHealthResult.OverReplicatedHealthResult overHealth
          = ((ContainerHealthResult.OverReplicatedHealthResult) health);
      LOG.debug("Container {} is Over Replicated. isReplicatedOkAfterPending" +
              " is [{}]. hasMismatchedReplicas is [{}]", container,
          overHealth.isReplicatedOkAfterPending(),
          overHealth.hasMismatchedReplicas());

      if (!overHealth.isReplicatedOkAfterPending() &&
          overHealth.isSafelyOverReplicated()) {
        request.getReplicationQueue().enqueue(overHealth);
      }
      return true;
    }

    return false;
  }

  /**
   * Verify there's no under or over replication if only healthy replicas are
   * considered, or that there are no healthy replicas.
   * @return true if there's no under/over replication considering healthy
   * replicas
   */
  private boolean verifyPerfectReplication(ContainerCheckRequest request) {
    RatisContainerReplicaCount replicaCountWithoutUnhealthy =
        new RatisContainerReplicaCount(request.getContainerInfo(),
            request.getContainerReplicas(), request.getPendingOps(),
            request.getMaintenanceRedundancy(), false);

    if (replicaCountWithoutUnhealthy.getHealthyReplicaCount() == 0) {
      return true;
    }
    if (replicaCountWithoutUnhealthy.isUnderReplicated() ||
        replicaCountWithoutUnhealthy.isOverReplicated()) {
      LOG.debug("Checking replication for container {} without considering " +
              "UNHEALTHY replicas. isUnderReplicated is [{}]. " +
              "isOverReplicated is [{}]. Returning false because there should" +
              " be perfect replication for this handler to work.",
          request.getContainerInfo(),
          replicaCountWithoutUnhealthy.isUnderReplicated(),
          replicaCountWithoutUnhealthy.isOverReplicated());
      return false;
    }
    return true;
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
