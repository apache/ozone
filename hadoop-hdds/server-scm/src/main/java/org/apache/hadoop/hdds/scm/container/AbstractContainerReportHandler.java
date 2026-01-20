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

package org.apache.hadoop.hdds.scm.container;

import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;

/**
 * Base class for all the container report handlers.
 */
abstract class AbstractContainerReportHandler {
  private final NodeManager nodeManager;
  private final ContainerManager containerManager;
  private final SCMContext scmContext;

  AbstractContainerReportHandler(NodeManager nodeManager, ContainerManager containerManager, SCMContext scmContext) {
    this.nodeManager = Objects.requireNonNull(nodeManager, "nodeManager == null");
    this.containerManager = Objects.requireNonNull(containerManager, "containerManager == null");
    this.scmContext = Objects.requireNonNull(scmContext, "scmContext == null");
  }

  protected abstract Logger getLogger();

  /** @return the container in SCM and the replica from a datanode details for logging. */
  protected static Object getDetailsForLogging(ContainerInfo container, ContainerReplicaProto replica,
      DatanodeDetails datanode) {
    Objects.requireNonNull(replica, "replica == null");
    Objects.requireNonNull(datanode, "datanode == null");
    if (container != null) {
      Preconditions.assertSame(container.getContainerID(), replica.getContainerID(), "Container ID");
    }

    final Supplier<String> details = MemoizedSupplier.valueOf(() -> {
      final StringBuilder b = new StringBuilder();
      if (container == null) {
        b.append("Container #").append(replica.getContainerID()).append(" (NOT_FOUND");
      } else {
        b.append("Container ").append(container.containerID())
            .append(" (").append(container.getState()).append(", sid=").append(container.getSequenceId());
      }
      return b.append(") r").append(replica.getReplicaIndex())
          .append(" (").append(replica.getState())
          .append(", bcsid=").append(replica.getBlockCommitSequenceId())
          .append(", origin=").append(replica.getOriginNodeId())
          .append(", ").append(replica.hasIsEmpty() && replica.getIsEmpty() ? "empty" : "non-empty")
          .append(") from dn ").append(datanode)
          .toString();
    });

    return new Object() {
      @Override
      public String toString() {
        return details.get();
      }
    };
  }

  /**
   * Process the given ContainerReplica received from specified datanode.
   *
   * @param datanodeDetails DatanodeDetails of the node which reported
   *                        this replica
   * @param containerInfo ContainerInfo represending the container
   * @param replicaProto ContainerReplica
   * @param publisher EventPublisher instance
   */
  protected void processContainerReplica(final DatanodeDetails datanodeDetails,
      final ContainerInfo containerInfo,
      final ContainerReplicaProto replicaProto, final EventPublisher publisher, Object detailsForLogging)
      throws IOException, InvalidStateTransitionException {
    getLogger().debug("Processing replica {}", detailsForLogging);
    // Synchronized block should be replaced by container lock,
    // once we have introduced lock inside ContainerInfo.
    synchronized (containerInfo) {
      updateContainerStats(datanodeDetails, containerInfo, replicaProto, detailsForLogging);
      if (!updateContainerState(datanodeDetails, containerInfo, replicaProto, publisher, detailsForLogging)) {
        updateContainerReplica(datanodeDetails, containerInfo.containerID(), replicaProto);
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
                                    final ContainerReplicaProto replicaProto,
      Object detailsForLogging) throws ContainerNotFoundException {
    if (containerInfo.getState() == HddsProtos.LifeCycleState.CLOSED && containerInfo.getSequenceId() <
        replicaProto.getBlockCommitSequenceId()) {
      getLogger().error("Container CLOSED with sequence ID lower than a replica: {}, proto={}",
          detailsForLogging, TextFormat.shortDebugString(replicaProto));
    }

    if (isHealthy(replicaProto.getState())) {
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

  private static long calculateUsage(ContainerInfo containerInfo, long lastValue, long thisValue) {
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

  private static void updateContainerUsedAndKeys(ContainerInfo containerInfo, long usedBytes, long keyCount) {
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
   * @param container ContainerInfo representing the container
   * @param replica ContainerReplica
   * @return true iff replica must be ignored in the next process
   * @throws IOException In case of Exception
   */
  private boolean updateContainerState(final DatanodeDetails datanode,
                                    final ContainerInfo container,
                                    final ContainerReplicaProto replica,
                                    final EventPublisher publisher,
      Object detailsForLogging) throws IOException, InvalidStateTransitionException {

    final ContainerID containerId = container.containerID();
    boolean replicaIsEmpty = replica.hasIsEmpty() && replica.getIsEmpty();

    switch (container.getState()) {
    case OPEN:
      // If the state of a container is OPEN and a replica is in different state, finalize the container.
      if (replica.getState() != State.OPEN) {
        getLogger().info("FINALIZE (i.e. CLOSING) {}", detailsForLogging);
        containerManager.updateContainerState(containerId, LifeCycleEvent.FINALIZE);
      }
      return false;
    case CLOSING:
      // When the container is in CLOSING state, a replica can be either OPEN, CLOSING, QUASI_CLOSED or CLOSED

      // If the replica are either in OPEN or CLOSING state, do nothing.

      // If the replica is in QUASI_CLOSED state, move the container to QUASI_CLOSED state.
      if (replica.getState() == State.QUASI_CLOSED) {
        getLogger().info("QUASI_CLOSE {}", detailsForLogging);
        containerManager.updateContainerState(containerId, LifeCycleEvent.QUASI_CLOSE);
        return false;
      }

      // If the replica is in CLOSED state, mark the container as CLOSED.
      if (replica.getState() == State.CLOSED) {
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
            return false;
          }
        }

        if (bcsidMismatched(container, replica, detailsForLogging)) {
          return true;
        }
        getLogger().info("CLOSE {}", detailsForLogging);
        containerManager.updateContainerState(containerId, LifeCycleEvent.CLOSE);
      }
      return false;
    case QUASI_CLOSED:
      // The container is QUASI_CLOSED, this means that at least one of the replicas was QUASI_CLOSED.
      // Now replicas can be in either OPEN, CLOSING, QUASI_CLOSED or CLOSED

      // If one of the replica is in CLOSED state, mark the container as CLOSED.
      if (replica.getState() == State.CLOSED) {
        if (bcsidMismatched(container, replica, detailsForLogging)) {
          return true;
        }
        getLogger().info("FORCE_CLOSE for {}", detailsForLogging);
        containerManager.updateContainerState(containerId, LifeCycleEvent.FORCE_CLOSE);
      }
      return false;
    case CLOSED:
      // The container is already in closed state. do nothing.
      return false;
    case DELETED:
      // If container is in DELETED state and the reported replica is empty, delete the empty replica.
      // We should also do this for DELETING containers and currently DeletingContainerHandler does that
      if (replicaIsEmpty) {
        deleteReplica(containerId, datanode, publisher, "DELETED", false, detailsForLogging);
        return false;
      }
      // HDDS-12421: fall-through to case DELETING
    case DELETING:
      // HDDS-11136: If a DELETING container has a non-empty CLOSED replica, transition the container to CLOSED
      // HDDS-12421: If a DELETING or DELETED container has a non-empty replica, transition the container to CLOSED
      //
      if (replica.getState() == State.CLOSED && replica.getBlockCommitSequenceId() <= container.getSequenceId()
          && container.getReplicationType().equals(HddsProtos.ReplicationType.RATIS)) {
        deleteReplica(containerId, datanode, publisher, "DELETED", true, detailsForLogging);
        // We should not move back to CLOSED state if replica bcsid <= container bcsid
        return false;
      }
      boolean replicaStateAllowed = (replica.getState() != State.INVALID && replica.getState() != State.DELETED);
      if (!replicaIsEmpty && replicaStateAllowed) {
        getLogger().info("transitionDeletingToClosed due to non-empty CLOSED replica (keyCount={}) for {}",
            replica.getKeyCount(), detailsForLogging);
        containerManager.transitionDeletingOrDeletedToClosedState(containerId);
      }
      return false;
    default:
      getLogger().error("Replica not processed due to container state {}: {}",
          container.getState(), detailsForLogging);
      return false;
    }
  }

  /**
   * Helper method to verify that the replica's bcsId matches the container's in SCM.
   *
   * @param replica Replica reported from a datanode
   * @param container Container in SCM
   * @param detailsForLogging The detail information of the container in SCM and the replica from a datanode
   * @return true iff the BCSIDs are mismatched
   */
  private boolean bcsidMismatched(ContainerInfo container, ContainerReplicaProto replica, Object detailsForLogging) {
    if (replica.getBlockCommitSequenceId() == container.getSequenceId()) {
      return false;
    }
    getLogger().warn("Replica BCSID mismatched for {} ", detailsForLogging);
    return true;
  }

  private void updateContainerReplica(final DatanodeDetails datanodeDetails,
                                      final ContainerID containerId,
                                      final ContainerReplicaProto replicaProto)
      throws ContainerNotFoundException, ContainerReplicaNotFoundException {

    final ContainerReplica replica = ContainerReplica.newBuilder()
        .setContainerID(containerId)
        .setContainerState(replicaProto.getState())
        .setDatanodeDetails(datanodeDetails)
        .setOriginNodeId(DatanodeID.fromUuidString(replicaProto.getOriginNodeId()))
        .setSequenceId(replicaProto.getBlockCommitSequenceId())
        .setKeyCount(replicaProto.getKeyCount())
        .setReplicaIndex(replicaProto.getReplicaIndex())
        .setBytesUsed(replicaProto.getUsed())
        .setEmpty(replicaProto.getIsEmpty())
        .setChecksums(ContainerChecksums.of(replicaProto.getDataChecksum()))
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
  private boolean isHealthy(final State replicaState) {
    return replicaState != State.UNHEALTHY
        && replicaState != State.INVALID
        && replicaState != State.DELETED;
  }

  protected NodeManager getNodeManager() {
    return nodeManager;
  }

  protected ContainerManager getContainerManager() {
    return containerManager;
  }

  protected void deleteReplica(ContainerID containerID, DatanodeDetails dn,
      EventPublisher publisher, String reason, boolean force, Object detailsForLogging) {
    final long term;
    try {
      term = scmContext.getTermOfLeader();
    } catch (NotLeaderException nle) {
      final String message = "Skip sending DeleteContainerCommand for " + detailsForLogging + ": " + nle;
      getLogger().warn(message);
      return;
    }

    final SCMCommand<?> command = new DeleteContainerCommand(containerID, force);
    command.setTerm(term);
    publisher.fireEvent(SCMEvents.DATANODE_COMMAND, new CommandForDatanode<>(dn, command));
    getLogger().info("Sending {}DeleteContainerCommand due to {} for {}",
        force ? "force" : "", reason, detailsForLogging);
  }
}
