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
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Base class for all the container report handlers.
 */
public class AbstractContainerReportHandler {

  private final ContainerManager containerManager;
  private final SCMContext scmContext;
  private final Logger logger;

  /**
   * Constructs AbstractContainerReportHandler instance with the
   * given ContainerManager instance.
   *
   * @param containerManager ContainerManager
   * @param logger Logger to be used for logging
   */
  AbstractContainerReportHandler(final ContainerManager containerManager,
                                 final SCMContext scmContext,
                                 final Logger logger) {
    Preconditions.checkNotNull(containerManager);
    Preconditions.checkNotNull(scmContext);
    Preconditions.checkNotNull(logger);
    this.containerManager = containerManager;
    this.scmContext = scmContext;
    this.logger = logger;
  }

  /**
   * Process the given ContainerReplica received from specified datanode.
   *
   * @param datanodeDetails DatanodeDetails for the DN
   * @param replicaProto Protobuf representing the replicas
   * @param publisher EventPublisher instance
   * @throws IOException
   * @throws InvalidStateTransitionException
   * @throws TimeoutException
   */
  protected void processContainerReplica(final DatanodeDetails datanodeDetails,
      final ContainerReplicaProto replicaProto, final EventPublisher publisher)
      throws IOException, InvalidStateTransitionException, TimeoutException {
    ContainerInfo container = getContainerManager().getContainer(
        ContainerID.valueOf(replicaProto.getContainerID()));
    processContainerReplica(
        datanodeDetails, container, replicaProto, publisher);
  }

  /**
   * Process the given ContainerReplica received from specified datanode.
   *
   * @param datanodeDetails DatanodeDetails of the node which reported
   *                        this replica
   * @param containerInfo ContainerInfo represending the container
   * @param replicaProto ContainerReplica
   * @param publisher EventPublisher instance
   *
   * @throws IOException In case of any Exception while processing the report
   * @throws TimeoutException In case of timeout while updating container state
   */
  protected void processContainerReplica(final DatanodeDetails datanodeDetails,
      final ContainerInfo containerInfo,
      final ContainerReplicaProto replicaProto, final EventPublisher publisher)
      throws IOException, InvalidStateTransitionException, TimeoutException {
    final ContainerID containerId = containerInfo.containerID();

    if (logger.isDebugEnabled()) {
      logger.debug("Processing replica of container {} from datanode {}",
          containerId, datanodeDetails);
    }
    // Synchronized block should be replaced by container lock,
    // once we have introduced lock inside ContainerInfo.
    synchronized (containerInfo) {
      updateContainerStats(datanodeDetails, containerInfo, replicaProto);
      if (!updateContainerState(datanodeDetails, containerInfo, replicaProto,
          publisher)) {
        updateContainerReplica(datanodeDetails, containerId, replicaProto);
      }
    }
  }

  /**
   * Update the container stats if it's lagging behind the stats in reported
   * replica.
   *
   * @param containerInfo ContainerInfo representing the container
   * @param replicaProto Container Replica information
   * @throws ContainerNotFoundException If the container is not present
   */
  private void updateContainerStats(final DatanodeDetails datanodeDetails,
                                    final ContainerInfo containerInfo,
                                    final ContainerReplicaProto replicaProto)
      throws ContainerNotFoundException {

    if (isHealthy(replicaProto::getState)) {
      if (containerInfo.getSequenceId() <
          replicaProto.getBlockCommitSequenceId()) {
        containerInfo.updateSequenceId(
            replicaProto.getBlockCommitSequenceId());
      }
      if (containerInfo.getReplicationConfig().getReplicationType()
          == HddsProtos.ReplicationType.EC) {
        updateECContainerStats(containerInfo, replicaProto, datanodeDetails);
      } else {
        updateRatisContainerStats(containerInfo, replicaProto, datanodeDetails);
      }
    }
  }

  private void updateRatisContainerStats(ContainerInfo containerInfo,
      ContainerReplicaProto newReplica, DatanodeDetails newSource)
      throws ContainerNotFoundException {
    List<ContainerReplica> otherReplicas =
        getOtherReplicas(containerInfo.containerID(), newSource);
    long usedBytes = newReplica.getUsed();
    long keyCount = newReplica.getKeyCount();

    for (ContainerReplica r : otherReplicas) {
      usedBytes = calculateUsage(containerInfo, usedBytes, r.getBytesUsed());
      keyCount = calculateUsage(containerInfo, keyCount, r.getKeyCount());
    }
    updateContainerUsedAndKeys(containerInfo, usedBytes, keyCount);
  }

  private void updateECContainerStats(ContainerInfo containerInfo,
      ContainerReplicaProto newReplica, DatanodeDetails newSource)
      throws ContainerNotFoundException {
    int dataNum =
        ((ECReplicationConfig)containerInfo.getReplicationConfig()).getData();
    // The first EC index and the parity indexes must all be the same size
    // while the other data indexes may be smaller due to partial stripes.
    // When calculating the stats, we only use the first data and parity and
    // ignore the others. We only need to run the check if we are processing
    // the first data or parity replicas.
    if (newReplica.getReplicaIndex() == 1
        || newReplica.getReplicaIndex() > dataNum) {
      List<ContainerReplica> otherReplicas =
          getOtherReplicas(containerInfo.containerID(), newSource);
      long usedBytes = newReplica.getUsed();
      long keyCount = newReplica.getKeyCount();
      for (ContainerReplica r : otherReplicas) {
        if (r.getReplicaIndex() > 1 && r.getReplicaIndex() <= dataNum) {
          // Ignore all data replicas except the first for stats
          continue;
        }
        usedBytes = calculateUsage(containerInfo, usedBytes, r.getBytesUsed());
        keyCount = calculateUsage(containerInfo, keyCount, r.getKeyCount());
      }
      updateContainerUsedAndKeys(containerInfo, usedBytes, keyCount);
    }
  }

  private long calculateUsage(ContainerInfo containerInfo, long lastValue,
      long thisValue) {
    if (containerInfo.getState().equals(HddsProtos.LifeCycleState.OPEN)) {
      // Open containers are generally growing in key count and size, the
      // overall size should be the min of all reported replicas.
      return Math.min(lastValue, thisValue);
    } else {
      // Containers which are not open can only shrink in size, so use the
      // largest values reported.
      return Math.max(lastValue, thisValue);
    }
  }

  private void updateContainerUsedAndKeys(ContainerInfo containerInfo,
      long usedBytes, long keyCount) {
    if (containerInfo.getUsedBytes() != usedBytes) {
      containerInfo.setUsedBytes(usedBytes);
    }
    if (containerInfo.getNumberOfKeys() != keyCount) {
      containerInfo.setNumberOfKeys(keyCount);
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
   * @param container ContainerInfo representing the the container
   * @param replica ContainerReplica
   * @boolean true - replica should be ignored in the next process
   * @throws IOException In case of Exception
   * @throws TimeoutException In case of timeout while updating container state
   */
  private boolean updateContainerState(final DatanodeDetails datanode,
                                    final ContainerInfo container,
                                    final ContainerReplicaProto replica,
                                    final EventPublisher publisher)
      throws IOException, InvalidStateTransitionException, TimeoutException {

    final ContainerID containerId = container.containerID();
    boolean ignored = false;

    switch (container.getState()) {
    case OPEN:
      /*
       * If the state of a container is OPEN, datanodes cannot report
       * any other state.
       */
      if (replica.getState() != State.OPEN) {
        logger.info("Moving OPEN container {} to CLOSING state, datanode {} " +
                "reported {} replica with index {}.", containerId, datanode,
            replica.getState(), replica.getReplicaIndex());
        containerManager.updateContainerState(containerId,
            LifeCycleEvent.FINALIZE);
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
        Preconditions.checkArgument(replica.getBlockCommitSequenceId()
            == container.getSequenceId());

        /*
        For an EC container, only the first index and the parity indexes are
        guaranteed to have block data. So, update the container's state in SCM
        only if replica index is one of these indexes.
         */
        if (container.getReplicationType()
            .equals(HddsProtos.ReplicationType.EC)) {
          int replicaIndex = replica.getReplicaIndex();
          int dataNum =
              ((ECReplicationConfig)container.getReplicationConfig()).getData();
          if (replicaIndex != 1 && replicaIndex <= dataNum) {
            break;
          }
        }

        logger.info("Moving container {} to CLOSED state, datanode {} " +
            "reported CLOSED replica with index {}.", containerId, datanode,
            replica.getReplicaIndex());
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
      HDDS-11136: If a DELETING container has a non-empty CLOSED replica, the container should be moved back to CLOSED
      state.
       */
      boolean replicaStateAllowed = replica.getState() == State.CLOSED;
      boolean replicaNotEmpty = replica.hasIsEmpty() && !replica.getIsEmpty();
      if (replicaStateAllowed && replicaNotEmpty) {
        logger.info("Moving DELETING container {} to CLOSED state, datanode {} reported replica with state={}, " +
            "isEmpty={} and keyCount={}.", containerId, datanode.getHostName(), replica.getState(), false,
            replica.getKeyCount());
        containerManager.transitionDeletingToClosedState(containerId);
      }

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
        .setReplicaIndex(replicaProto.getReplicaIndex())
        .setBytesUsed(replicaProto.getUsed())
        .setEmpty(replicaProto.getIsEmpty())
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
    SCMCommand<?> command = new DeleteContainerCommand(
        containerID.getId(), true);
    try {
      command.setTerm(scmContext.getTermOfLeader());
    } catch (NotLeaderException nle) {
      logger.warn("Skip sending delete container command," +
          " since not leader SCM", nle);
      return;
    }
    publisher.fireEvent(SCMEvents.DATANODE_COMMAND,
        new CommandForDatanode<>(dn.getUuid(), command));
    logger.info("Sending delete container command for " + reason +
        " container {} to datanode {}", containerID.getId(), dn);
  }
}
