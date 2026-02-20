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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used in Replication Manager to close replicas of CLOSING containers.
 */
public class ClosingContainerHandler extends AbstractCheck {
  private static final Logger LOG =
      LoggerFactory.getLogger(ClosingContainerHandler.class);

  private final ReplicationManager replicationManager;
  private final Clock clock;

  public ClosingContainerHandler(ReplicationManager rm, Clock clock) {
    replicationManager = rm;
    this.clock = clock;
  }

  /**
   * If the container is in CLOSING state, send close commands to replicas
   * that are not UNHEALTHY.
   * @param request ContainerCheckRequest object representing the container
   * @return false if the specified container is not CLOSING, otherwise true
   * @see
   * <a href="https://issues.apache.org/jira/browse/HDDS-5708">HDDS-5708</a>
   */
  @Override
  public boolean handle(ContainerCheckRequest request) {
    ContainerInfo containerInfo = request.getContainerInfo();

    if (containerInfo.getState() != LifeCycleState.CLOSING) {
      return false;
    }
    LOG.debug("Checking container {} in ClosingContainerHandler",
        containerInfo);

    boolean forceClose = containerInfo.getReplicationConfig()
        .getReplicationType() != ReplicationType.RATIS;

    // Report MISSING only for containers with no replicas and keys > 0
    if (request.getContainerReplicas().isEmpty() && containerInfo.getNumberOfKeys() > 0) {
      request.getReport().incrementAndSample(ContainerHealthState.MISSING, containerInfo);
    }

    if (request.isReadOnly()) {
      // The reset of this method modifies container state, so we just return
      // here if the request is read only.
      return true;
    }

    boolean allUnhealthy = true;
    for (ContainerReplica replica : request.getContainerReplicas()) {
      if (replica.getState() != ContainerReplicaProto.State.UNHEALTHY) {
        allUnhealthy = false;
        replicationManager.sendCloseContainerReplicaCommand(
            containerInfo, replica.getDatanodeDetails(), forceClose);
      }
    }

    // Moving a RATIS container that has only unhealthy replicas to QUASI_CLOSED
    // so the unhealthy replicas will be replicated if needed.
    if (allUnhealthy && !request.getContainerReplicas().isEmpty()) {
      LifeCycleEvent event = LifeCycleEvent.QUASI_CLOSE;
      if (containerInfo.getReplicationConfig().getReplicationType()
          == ReplicationType.EC) {
        event = LifeCycleEvent.CLOSE;
      }

      LOG.debug("Container {} has only unhealthy replicas and is closing, so "
          + "executing the {} event.", containerInfo, event);
      replicationManager.updateContainerState(
          containerInfo.containerID(), event);
    }

    /*
     * Empty containers in CLOSING state should be CLOSED.
     *
     * These are containers that are allocated in SCM but never got created
     * on Datanodes. Since these containers don't have any replica associated
     * with them, they are stuck in CLOSING state forever as there is no
     * replicas to CLOSE.
     *
     * We should wait for sometime before moving the container to CLOSED state.
     * This will give enough time for Datanodes to report the container,
     * in cases where the container creation was successful on Datanodes.
     *
     * Should we have a separate configuration for this wait time?
     * For now, we are using ReplicationManagerThread Interval * 5 as the wait
     * time.
     */
    if (request.getContainerReplicas().isEmpty() &&
        containerInfo.getNumberOfKeys() == 0 &&
        hasWaitTimeElapsed(containerInfo)) {

      LOG.debug("Container appears to be empty, has no replicas, and has been "
          + "closing, so moving to closed state: {}", containerInfo);

      replicationManager.updateContainerState(
          containerInfo.containerID(), LifeCycleEvent.CLOSE);
    }

    return true;
  }

  private boolean hasWaitTimeElapsed(ContainerInfo containerInfo) {
    Duration waitTime = replicationManager.getConfig().getInterval()
        .multipliedBy(5);
    Instant closingTime = containerInfo.getStateEnterTime();
    return clock.instant().isAfter(closingTime.plus(waitTime));
  }
}
