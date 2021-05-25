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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.block.PendingDeleteStatusList;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .ContainerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handles container reports from datanode.
 */
public class ContainerReportHandler extends AbstractContainerReportHandler
    implements EventHandler<ContainerReportFromDatanode> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerReportHandler.class);

  private final NodeManager nodeManager;
  private final ContainerManagerV2 containerManager;
  private final String unknownContainerHandleAction;

  /**
   * The action taken by ContainerReportHandler to handle
   * unknown containers.
   */
  static final String UNKNOWN_CONTAINER_ACTION_WARN = "WARN";
  static final String UNKNOWN_CONTAINER_ACTION_DELETE = "DELETE";

  /**
   * Constructs ContainerReportHandler instance with the
   * given NodeManager and ContainerManager instance.
   *
   * @param nodeManager NodeManager instance
   * @param containerManager ContainerManager instance
   * @param conf OzoneConfiguration instance
   */
  public ContainerReportHandler(final NodeManager nodeManager,
                                final ContainerManagerV2 containerManager,
                                final SCMContext scmContext,
                                OzoneConfiguration conf) {
    super(containerManager, scmContext, LOG);
    this.nodeManager = nodeManager;
    this.containerManager = containerManager;

    if (conf != null) {
      ScmConfig scmConfig = conf.getObject(ScmConfig.class);
      unknownContainerHandleAction = scmConfig.getUnknownContainerAction();
    } else {
      unknownContainerHandleAction = UNKNOWN_CONTAINER_ACTION_WARN;
    }
  }

  public ContainerReportHandler(final NodeManager nodeManager,
      final ContainerManagerV2 containerManager) {
    this(nodeManager, containerManager, SCMContext.emptyContext(), null);
  }

  /**
   * Process the container reports from datanodes.
   *
   * @param reportFromDatanode Container Report
   * @param publisher EventPublisher reference
   */
  @Override
  public void onMessage(final ContainerReportFromDatanode reportFromDatanode,
                        final EventPublisher publisher) {

    final DatanodeDetails dnFromReport =
        reportFromDatanode.getDatanodeDetails();
    DatanodeDetails datanodeDetails =
        nodeManager.getNodeByUuid(dnFromReport.getUuidString());
    if (datanodeDetails == null) {
      LOG.warn("Received container report from unknown datanode {}",
          dnFromReport);
      return;
    }
    final ContainerReportsProto containerReport =
        reportFromDatanode.getReport();
    try {
      // HDDS-5249 - we must ensure that an ICR and FCR for the same datanode
      // do not run at the same time or it can result in a data consistency
      // issue between the container list in NodeManager and the replicas in
      // ContainerManager.
      synchronized (datanodeDetails) {
        final List<ContainerReplicaProto> replicas =
            containerReport.getReportsList();
        final Set<ContainerID> containersInSCM =
            nodeManager.getContainers(datanodeDetails);

        final Set<ContainerID> containersInDn = replicas.parallelStream()
            .map(ContainerReplicaProto::getContainerID)
            .map(ContainerID::valueOf).collect(Collectors.toSet());

        final Set<ContainerID> missingReplicas = new HashSet<>(containersInSCM);
        missingReplicas.removeAll(containersInDn);

        processContainerReplicas(datanodeDetails, replicas, publisher);
        processMissingReplicas(datanodeDetails, missingReplicas);
        updateDeleteTransaction(datanodeDetails, replicas, publisher);

        /*
         * Update the latest set of containers for this datanode in
         * NodeManager
         */
        nodeManager.setContainers(datanodeDetails, containersInDn);

        containerManager.notifyContainerReportProcessing(true, true);
      }
    } catch (NodeNotFoundException ex) {
      containerManager.notifyContainerReportProcessing(true, false);
      LOG.error("Received container report from unknown datanode {}.",
          datanodeDetails, ex);
    }

  }

  /**
   * Processes the ContainerReport, unknown container reported
   * that will be deleted by SCM.
   *
   * @param datanodeDetails Datanode from which this report was received
   * @param replicas list of ContainerReplicaProto
   * @param publisher EventPublisher reference
   */
  private void processContainerReplicas(final DatanodeDetails datanodeDetails,
      final List<ContainerReplicaProto> replicas,
      final EventPublisher publisher) {
    for (ContainerReplicaProto replicaProto : replicas) {
      try {
        processContainerReplica(datanodeDetails, replicaProto, publisher);
      } catch (ContainerNotFoundException e) {
        if(unknownContainerHandleAction.equals(
            UNKNOWN_CONTAINER_ACTION_WARN)) {
          LOG.error("Received container report for an unknown container" +
              " {} from datanode {}.", replicaProto.getContainerID(),
              datanodeDetails, e);
        } else if (unknownContainerHandleAction.equals(
            UNKNOWN_CONTAINER_ACTION_DELETE)) {
          final ContainerID containerId = ContainerID
              .valueOf(replicaProto.getContainerID());
          deleteReplica(containerId, datanodeDetails, publisher, "unknown");
        }
      } catch (IOException | InvalidStateTransitionException e) {
        LOG.error("Exception while processing container report for container" +
                " {} from datanode {}.", replicaProto.getContainerID(),
            datanodeDetails, e);
      }
    }
  }

  /**
   * Process the missing replica on the given datanode.
   *
   * @param datanodeDetails DatanodeDetails
   * @param missingReplicas ContainerID which are missing on the given datanode
   */
  private void processMissingReplicas(final DatanodeDetails datanodeDetails,
                                      final Set<ContainerID> missingReplicas) {
    for (ContainerID id : missingReplicas) {
      try {
        containerManager.getContainerReplicas(id).stream()
            .filter(replica -> replica.getDatanodeDetails()
                .equals(datanodeDetails)).findFirst()
            .ifPresent(replica -> {
              try {
                containerManager.removeContainerReplica(id, replica);
              } catch (ContainerNotFoundException |
                  ContainerReplicaNotFoundException ignored) {
                // This should not happen, but even if it happens, not an issue
              }
            });
      } catch (ContainerNotFoundException e) {
        LOG.warn("Cannot remove container replica, container {} not found.",
            id, e);
      }
    }
  }

  /**
   * Updates the Delete Transaction Id for the given datanode.
   *
   * @param datanodeDetails DatanodeDetails
   * @param replicas List of ContainerReplicaProto
   * @param publisher EventPublisher reference
   */
  private void updateDeleteTransaction(final DatanodeDetails datanodeDetails,
      final List<ContainerReplicaProto> replicas,
      final EventPublisher publisher) {
    final PendingDeleteStatusList pendingDeleteStatusList =
        new PendingDeleteStatusList(datanodeDetails);
    for (ContainerReplicaProto replica : replicas) {
      try {
        final ContainerInfo containerInfo = containerManager.getContainer(
            ContainerID.valueOf(replica.getContainerID()));
        if (containerInfo.getDeleteTransactionId() >
            replica.getDeleteTransactionId()) {
          pendingDeleteStatusList.addPendingDeleteStatus(
              replica.getDeleteTransactionId(),
              containerInfo.getDeleteTransactionId(),
              containerInfo.getContainerID());
        }
      } catch (ContainerNotFoundException cnfe) {
        LOG.warn("Cannot update pending delete transaction for " +
            "container #{}. Reason: container missing.",
            replica.getContainerID());
      }
    }
    if (pendingDeleteStatusList.getNumPendingDeletes() > 0) {
      publisher.fireEvent(SCMEvents.PENDING_DELETE_STATUS,
          pendingDeleteStatusList);
    }
  }
}
