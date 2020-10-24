/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Base class for all the container report handlers.
 */
public class AbstractContainerReportHandler {

  private final ContainerManager containerManager;
  private final Logger logger;

  /**
   * Constructs AbstractContainerReportHandler instance with the
   * given ContainerManager instance.
   *
   * @param containerManager ContainerManager
   * @param logger Logger to be used for logging
   */
  AbstractContainerReportHandler(final ContainerManager containerManager,
                                 final Logger logger) {
    Preconditions.checkNotNull(containerManager);
    Preconditions.checkNotNull(logger);
    this.containerManager = containerManager;
    this.logger = logger;
  }

  /**
   * Process the given ContainerReplica received from specified datanode.
   *
   * @param datanodeDetails DatanodeDetails of the node which reported
   *                        this replica
   * @param replicaProto ContainerReplica
   *
   * @throws IOException In case of any Exception while processing the report
   */
  protected void processContainerReplica(final DatanodeDetails datanodeDetails,
      final ContainerReplicaProto replicaProto, final EventPublisher publisher)
      throws IOException {
    final ContainerID containerId = ContainerID
        .valueof(replicaProto.getContainerID());

    if (logger.isDebugEnabled()) {
      logger.debug("Processing replica of container {} from datanode {}",
          containerId, datanodeDetails);
    }
    // Synchronized block should be replaced by container lock,
    // once we have introduced lock inside ContainerInfo.
    synchronized (containerManager.getContainer(containerId)) {
      updateContainerStats(datanodeDetails, containerId, replicaProto);
      if (!updateContainerState(datanodeDetails, containerId, replicaProto,
          publisher)) {
        updateContainerReplica(datanodeDetails, containerId, replicaProto);
      }
    }
  }

  /**
   * Update the container stats if it's lagging behind the stats in reported
   * replica.
   *
   * @param containerId ID of the container
   * @param replicaProto Container Replica information
   * @throws ContainerNotFoundException If the container is not present
   */
  private void updateContainerStats(final DatanodeDetails datanodeDetails,
                                    final ContainerID containerId,
                                    final ContainerReplicaProto replicaProto)
      throws ContainerNotFoundException {
    final ContainerInfo containerInfo = containerManager
        .getContainer(containerId);

    if (isHealthy(replicaProto::getState)) {
      if (containerInfo.getSequenceId() <
          replicaProto.getBlockCommitSequenceId()) {
        containerInfo.updateSequenceId(
            replicaProto.getBlockCommitSequenceId());
      }
      List<ContainerReplica> otherReplicas =
          getOtherReplicas(containerId, datanodeDetails);
      long usedBytes = replicaProto.getUsed();
      long keyCount = replicaProto.getKeyCount();
      for (ContainerReplica r : otherReplicas) {
        // Open containers are generally growing in key count and size, the
        // overall size should be the min of all reported replicas.
        if (containerInfo.getState().equals(HddsProtos.LifeCycleState.OPEN)) {
          usedBytes = Math.min(usedBytes, r.getBytesUsed());
          keyCount = Math.min(keyCount, r.getKeyCount());
        } else {
          // Containers which are not open can only shrink in size, so use the
          // largest values reported.
          usedBytes = Math.max(usedBytes, r.getBytesUsed());
          keyCount = Math.max(keyCount, r.getKeyCount());
        }
      }

      if (containerInfo.getUsedBytes() != usedBytes) {
        containerInfo.setUsedBytes(usedBytes);
      }
      if (containerInfo.getNumberOfKeys() != keyCount) {
        containerInfo.setNumberOfKeys(keyCount);
      }
    }
  }

  private List<ContainerReplica> getOtherReplicas(ContainerID containerId,
      DatanodeDetails exclude) throws ContainerNotFoundException {
    List<ContainerReplica> filteredReplicas = new ArrayList<>();
    Set<ContainerReplica> replicas
        = containerManager.getContainerReplicas(containerId);
    for (ContainerReplica r : replicas) {
      if (!r.getDatanodeDetails().equals(exclude)) {
        filteredReplicas.add(r);
      }
    }
    return filteredReplicas;
  }

  /**
   * Updates the container state based on the given replica state.
   *
   * @param datanode Datanode from which the report is received
   * @param containerId ID of the container
   * @param replica ContainerReplica
   * @boolean true - replica should be ignored in the next process
   * @throws IOException In case of Exception
   */
  private boolean updateContainerState(final DatanodeDetails datanode,
                                    final ContainerID containerId,
                                    final ContainerReplicaProto replica,
                                    final EventPublisher publisher)
      throws IOException {

    final ContainerInfo container = containerManager
        .getContainer(containerId);
    boolean ignored = false;

    switch (container.getState()) {
    case OPEN:
      /*
       * If the state of a container is OPEN, datanodes cannot report
       * any other state.
       */
      if (replica.getState() != State.OPEN) {
        logger.warn("Container {} is in OPEN state, but the datanode {} " +
            "reports an {} replica.", containerId,
            datanode, replica.getState());
        // Should we take some action?
      }
      break;
    case CLOSING:
      /*
       * When the container is in CLOSING state the replicas can be in any
       * of the following states:
       *
       * - OPEN
       * - CLOSING
       * - QUASI_CLOSED
       * - CLOSED
       *
       * If all the replica are either in OPEN or CLOSING state, do nothing.
       *
       * If the replica is in QUASI_CLOSED state, move the container to
       * QUASI_CLOSED state.
       *
       * If the replica is in CLOSED state, mark the container as CLOSED.
       *
       */

      if (replica.getState() == State.QUASI_CLOSED) {
        logger.info("Moving container {} to QUASI_CLOSED state, datanode {} " +
                "reported QUASI_CLOSED replica.", containerId, datanode);
        containerManager.updateContainerState(containerId,
            LifeCycleEvent.QUASI_CLOSE);
      }

      if (replica.getState() == State.CLOSED) {
        logger.info("Moving container {} to CLOSED state, datanode {} " +
            "reported CLOSED replica.", containerId, datanode);
        Preconditions.checkArgument(replica.getBlockCommitSequenceId()
            == container.getSequenceId());
        containerManager.updateContainerState(containerId,
            LifeCycleEvent.CLOSE);
      }

      break;
    case QUASI_CLOSED:
      /*
       * The container is in QUASI_CLOSED state, this means that at least
       * one of the replica was QUASI_CLOSED.
       *
       * Now replicas can be in any of the following state.
       *
       * 1. OPEN
       * 2. CLOSING
       * 3. QUASI_CLOSED
       * 4. CLOSED
       *
       * If at least one of the replica is in CLOSED state, mark the
       * container as CLOSED.
       *
       */
      if (replica.getState() == State.CLOSED) {
        logger.info("Moving container {} to CLOSED state, datanode {} " +
            "reported CLOSED replica.", containerId, datanode);
        Preconditions.checkArgument(replica.getBlockCommitSequenceId()
            == container.getSequenceId());
        containerManager.updateContainerState(containerId,
            LifeCycleEvent.FORCE_CLOSE);
      }
      break;
    case CLOSED:
      /*
       * The container is already in closed state. do nothing.
       */
      break;
    case DELETING:
      /*
       * The container is under deleting. do nothing.
       */
      break;
    case DELETED:
      /*
       * The container is deleted. delete the replica.
       */
      deleteReplica(containerId, datanode, publisher, "DELETED");
      ignored = true;
      break;
    default:
      break;
    }

    return ignored;
  }

  private void updateContainerReplica(final DatanodeDetails datanodeDetails,
                                      final ContainerID containerId,
                                      final ContainerReplicaProto replicaProto)
      throws ContainerNotFoundException, ContainerReplicaNotFoundException {

    final ContainerReplica replica = ContainerReplica.newBuilder()
        .setContainerID(containerId)
        .setContainerState(replicaProto.getState())
        .setDatanodeDetails(datanodeDetails)
        .setOriginNodeId(UUID.fromString(replicaProto.getOriginNodeId()))
        .setSequenceId(replicaProto.getBlockCommitSequenceId())
        .setKeyCount(replicaProto.getKeyCount())
        .setBytesUsed(replicaProto.getUsed())
        .build();

    if (replica.getState().equals(State.DELETED)) {
      containerManager.removeContainerReplica(containerId, replica);
    } else {
      containerManager.updateContainerReplica(containerId, replica);
    }
  }

  /**
   * Returns true if the container replica is HEALTHY. <br>
   * A replica is considered healthy if it's not in UNHEALTHY,
   * INVALID or DELETED state.
   *
   * @param replicaState State of the container replica.
   * @return true if healthy, false otherwise
   */
  private boolean isHealthy(final Supplier<State> replicaState) {
    return replicaState.get() != State.UNHEALTHY
        && replicaState.get() != State.INVALID
        && replicaState.get() != State.DELETED;
  }

  /**
   * Return ContainerManager.
   * @return {@link ContainerManager}
   */
  protected ContainerManager getContainerManager() {
    return containerManager;
  }

  protected void deleteReplica(ContainerID containerID, DatanodeDetails dn,
      EventPublisher publisher, String reason) {
    final DeleteContainerCommand deleteCommand =
        new DeleteContainerCommand(containerID.getId(), true);
    final CommandForDatanode datanodeCommand = new CommandForDatanode<>(
        dn.getUuid(), deleteCommand);
    publisher.fireEvent(SCMEvents.DATANODE_COMMAND, datanodeCommand);
    logger.info("Sending delete container command for " + reason +
        " container {} to datanode {}", containerID.getId(), dn);
  }
}