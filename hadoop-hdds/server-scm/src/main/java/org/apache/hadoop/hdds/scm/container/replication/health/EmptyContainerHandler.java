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

import java.util.Set;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This handler deletes a container if it's closed and empty (0 key count)
 * and all its replicas are empty.
 */
public class EmptyContainerHandler extends AbstractCheck {
  private static final Logger LOG =
      LoggerFactory.getLogger(EmptyContainerHandler.class);

  private final ReplicationManager replicationManager;

  public EmptyContainerHandler(ReplicationManager replicationManager) {
    this.replicationManager = replicationManager;
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
      request.getReport().incrementAndSample(ContainerHealthState.EMPTY, containerInfo);
      if (!request.isReadOnly()) {
        LOG.debug("Container {} is empty and closed, marking as DELETING",
            containerInfo);
        // delete replicas if they are closed and empty
        deleteContainerReplicas(containerInfo, replicas);

        if (containerInfo.getReplicationType() == HddsProtos.ReplicationType.RATIS) {
          if (replicas.stream().noneMatch(r -> r.getSequenceId() == containerInfo.getSequenceId())) {
            // don't update container state if replica seqid don't match with container seq id
            return true;
          }
        }
        // Update the container's state
        replicationManager.updateContainerState(
            containerInfo.containerID(), HddsProtos.LifeCycleEvent.DELETE);
      }
      return true;
    } else if (containerInfo.getState() == HddsProtos.LifeCycleState.CLOSED
        && containerInfo.getNumberOfKeys() == 0 && replicas.isEmpty()) {
      // If the container is empty and has no replicas, it is possible it was
      // a container which stuck in the closing state which never got any
      // replicas created on the datanodes. In this case, we don't have enough
      // information to delete the container, so we just log it as EMPTY,
      // leaving it as CLOSED and return true, otherwise, it will end up marked
      // as missing in the replication check handlers.
      request.getReport().incrementAndSample(ContainerHealthState.EMPTY, containerInfo);
      LOG.debug("Container {} appears empty and is closed, but cannot be " +
              "deleted because it has no replicas. Marking as EMPTY.",
          containerInfo);
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
        !replicas.isEmpty() &&
        replicas.stream().allMatch(
            r -> r.getState() == ContainerReplicaProto.State.CLOSED &&
                r.isEmpty());
  }

  /**
   * Deletes the specified container's replicas if they are closed and empty.
   *
   * @param containerInfo ContainerInfo to delete
   * @param replicas Set of ContainerReplica
   */
  private void deleteContainerReplicas(final ContainerInfo containerInfo,
      final Set<ContainerReplica> replicas) {
    Preconditions.assertSame(HddsProtos.LifeCycleState.CLOSED,
        containerInfo.getState(), "container state");

    for (ContainerReplica rp : replicas) {
      Preconditions.assertSame(ContainerReplicaProto.State.CLOSED,
          rp.getState(), "replica state");
      Preconditions.assertSame(true, rp.isEmpty(), "replica empty");

      try {
        replicationManager.sendDeleteCommand(containerInfo,
            rp.getReplicaIndex(), rp.getDatanodeDetails(), false);
      } catch (NotLeaderException e) {
        LOG.warn("Failed to delete empty replica with index {} for container" +
                " {} on datanode {}",
            rp.getReplicaIndex(),
            containerInfo.containerID(),
            rp.getDatanodeDetails(), e);
      }
    }
  }

}
