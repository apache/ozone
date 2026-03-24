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

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;

import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler to process containers EC which are closed but have some replicas that
 * are unhealthy.
 */
public class ClosedWithUnhealthyReplicasHandler extends AbstractCheck {

  private static final Logger LOG =
      LoggerFactory.getLogger(ClosedWithUnhealthyReplicasHandler.class);
  private final ReplicationManager replicationManager;

  public ClosedWithUnhealthyReplicasHandler(
      ReplicationManager replicationManager) {
    this.replicationManager = replicationManager;
  }

  /**
   * Handles a closed EC container with unhealthy replicas. Note that if we
   * reach here, there is no over or under replication. This handler
   * will just send commands to delete the unhealthy replicas.
   *
   * <p>
   * Consider the following set of replicas for a closed EC 3-2 container:
   * Replica Index 1: Closed
   * Replica Index 2: Closed
   * Replica Index 3: Closed replica, Unhealthy replica (2 replicas)
   * Replica Index 4: Closed
   * Replica Index 5: Closed
   *
   * In this case, the unhealthy replica of index 3 should be deleted. The
   * container will be marked over replicated as the unhealthy replicas need
   * to be removed.
   * </p>
   * @param request ContainerCheckRequest object representing the container
   * @return true if this is a closed EC container with unhealthy replicas,
   * else false
   */
  @Override
  public boolean handle(ContainerCheckRequest request) {
    ContainerInfo containerInfo = request.getContainerInfo();
    if (containerInfo.getReplicationType() != HddsProtos.ReplicationType.EC) {
      return false;
    }
    if (containerInfo.getState() != HddsProtos.LifeCycleState.CLOSED) {
      return false;
    }
    LOG.debug("Checking container {} in ClosedWithUnhealthyReplicasHandler",
        containerInfo);
    Set<ContainerReplica> replicas = request.getContainerReplicas();
    // create a set of indexes that are closed
    Set<Integer> closedIndexes = replicas.stream()
        .filter(replica -> replica.getState() == State.CLOSED)
        .map(ContainerReplica::getReplicaIndex)
        .collect(Collectors.toSet());

    boolean foundUnhealthy = false;
    // send delete commands for unhealthy replicas
    for (ContainerReplica replica : replicas) {
      if (replica.getState() == State.UNHEALTHY) {
        /* Sanity check to ensure this index is not under replicated: verify
        that a closed replica with this index is also present.
         */
        if (!closedIndexes.contains(replica.getReplicaIndex())) {
          LOG.warn("Not handling container {} because replica index {} is " +
                  "under replicated. Not deleting UNHEALTHY replica [{}].",
              containerInfo.containerID(), replica.getReplicaIndex(), replica);
          return false;
        }

        foundUnhealthy = true;
        if (!request.isReadOnly()) {
          sendDeleteCommand(containerInfo, replica);
        }
      }
    }

    // some unhealthy replicas were found so the container must be
    // over replicated due to unhealthy replicas.
    if (foundUnhealthy) {
      request.getReport().incrementAndSample(ContainerHealthState.UNHEALTHY_OVER_REPLICATED, containerInfo);
    }
    LOG.debug("Returning {} for container {}", foundUnhealthy, containerInfo);
    return foundUnhealthy;
  }

  private void sendDeleteCommand(ContainerInfo containerInfo,
      ContainerReplica replica) {
    LOG.debug("Trying to delete UNHEALTHY replica [{}]", replica);
    try {
      replicationManager.sendDeleteCommand(containerInfo,
          replica.getReplicaIndex(), replica.getDatanodeDetails(), true);
    } catch (NotLeaderException e) {
      LOG.warn("Failed to delete UNHEALTHY replica [{}]", replica, e);
    }
  }
}
