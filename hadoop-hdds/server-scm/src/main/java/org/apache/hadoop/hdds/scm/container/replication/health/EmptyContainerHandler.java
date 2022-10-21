/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.replication.health;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * This handler deletes a container if it's closed and empty (0 key count)
 * and all its replicas are empty.
 */
public class EmptyContainerHandler extends AbstractCheck {
  public static final Logger LOG =
      LoggerFactory.getLogger(EmptyContainerHandler.class);

  private final ReplicationManager replicationManager;
  private final ContainerManager containerManager;

  public EmptyContainerHandler(ReplicationManager replicationManager,
      ContainerManager containerManager) {
    this.replicationManager = replicationManager;
    this.containerManager = containerManager;
  }

  /**
   * Deletes a container if it's closed and empty (0 key count) and all its
   * replicas are closed and empty.
   * @param request ContainerCheckRequest object representing the container
   * @return true if the specified container is empty, otherwise false
   */
  @Override
  public boolean handle(ContainerCheckRequest request) {
    ContainerInfo containerInfo = request.getContainerInfo();
    Set<ContainerReplica> replicas = request.getContainerReplicas();

    if (isContainerEmptyAndClosed(containerInfo, replicas)) {
      request.getReport()
          .incrementAndSample(ReplicationManagerReport.HealthState.EMPTY,
              containerInfo.containerID());

      // delete replicas if they are closed and empty
      deleteContainerReplicas(containerInfo, replicas);

      // Update the container's state
      try {
        containerManager.updateContainerState(containerInfo.containerID(),
            HddsProtos.LifeCycleEvent.DELETE);
      } catch (IOException | InvalidStateTransitionException |
          TimeoutException e) {
        LOG.error("Failed to delete empty container {}",
            request.getContainerInfo(), e);
      }
      return true;
    }

    return false;
  }

  /**
   * Returns true if the container is empty and CLOSED.
   * A container is empty if its key count is 0. The usedBytes counter is not
   * checked here because usedBytes is not an accurate representation of the
   * committed blocks. There could be orphaned chunks in the container which
   * contribute to usedBytes.
   *
   * @param container Container to check
   * @param replicas Set of ContainerReplica
   * @return true if the container is considered empty, false otherwise
   */
  private boolean isContainerEmptyAndClosed(final ContainerInfo container,
      final Set<ContainerReplica> replicas) {
    return container.getState() == HddsProtos.LifeCycleState.CLOSED &&
        container.getNumberOfKeys() == 0 && replicas.stream()
        .allMatch(r -> r.getState() == ContainerReplicaProto.State.CLOSED &&
            r.getKeyCount() == 0);
  }

  /**
   * Deletes the specified container's replicas if they are closed and empty.
   *
   * @param containerInfo ContainerInfo to delete
   * @param replicas Set of ContainerReplica
   */
  private void deleteContainerReplicas(final ContainerInfo containerInfo,
      final Set<ContainerReplica> replicas) {
    Preconditions.assertTrue(containerInfo.getState() ==
        HddsProtos.LifeCycleState.CLOSED);
    Preconditions.assertTrue(containerInfo.getNumberOfKeys() == 0);

    for (ContainerReplica rp : replicas) {
      Preconditions.assertTrue(
          rp.getState() == ContainerReplicaProto.State.CLOSED);
      Preconditions.assertTrue(rp.getKeyCount() == 0);

      LOG.debug("Trying to delete empty replica with index {} for container " +
              "{} on datanode {}", rp.getReplicaIndex(),
          containerInfo.containerID(), rp.getDatanodeDetails().getUuidString());
      try {
        replicationManager.sendDeleteCommand(containerInfo,
            rp.getReplicaIndex(), rp.getDatanodeDetails());
      } catch (NotLeaderException e) {
        LOG.warn("Failed to delete empty replica with index {} for container" +
                " {} on datanode {}",
            rp.getReplicaIndex(),
            containerInfo.containerID(),
            rp.getDatanodeDetails().getUuidString(), e);
      }
    }
  }

}
