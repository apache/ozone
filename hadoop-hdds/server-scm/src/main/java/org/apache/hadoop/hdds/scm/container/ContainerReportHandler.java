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
import java.util.List;
import java.util.Set;

/**
 * Handles container reports from datanode.
 */
public class ContainerReportHandler extends AbstractContainerReportHandler
    implements EventHandler<ContainerReportFromDatanode> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerReportHandler.class);

  private final NodeManager nodeManager;
  private final ContainerManager containerManager;
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
                                final ContainerManager containerManager,
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
      final ContainerManager containerManager) {
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

        for (ContainerReplicaProto replica : replicas) {
          ContainerID cid = ContainerID.valueOf(replica.getContainerID());
          ContainerInfo container = null;
          try {
            // We get the container using the ContainerID object we obtained
            // from protobuf. However we don't want to store that object if
            // there is already an instance for the same ContainerID we can
            // reuse.
            container = containerManager.getContainer(cid);
            cid = container.containerID();
          } catch (ContainerNotFoundException e) {
            // Ignore this for now. It will be handle later with a null check.
          }

          boolean wasInSCM = containersInSCM.remove(cid);
          if (!wasInSCM) {
            // This is a new Container not in the nodeManager -> dn map yet
            nodeManager.addContainer(datanodeDetails, cid);
          }
          processSingleReplica(datanodeDetails, container, replica, publisher);
        }
        // Anything left in containersInSCM was not in the full report, so it is
        // now missing on the DN
        processMissingReplicas(datanodeDetails, containersInSCM);
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
   * @param container ContainerInfo representing the container
   * @param replicaProto Proto message for the replica
   * @param publisher EventPublisher reference
   */
  private void processSingleReplica(final DatanodeDetails datanodeDetails,
      final ContainerInfo container, final ContainerReplicaProto replicaProto,
      final EventPublisher publisher) {
    if (container == null) {
      if (unknownContainerHandleAction.equals(
          UNKNOWN_CONTAINER_ACTION_WARN)) {
        LOG.error("Received container report for an unknown container" +
                " {} from datanode {}.", replicaProto.getContainerID(),
            datanodeDetails);
      } else if (unknownContainerHandleAction.equals(
          UNKNOWN_CONTAINER_ACTION_DELETE)) {
        final ContainerID containerId = ContainerID
            .valueOf(replicaProto.getContainerID());
        deleteReplica(containerId, datanodeDetails, publisher, "unknown");
      }
      return;
    }
    try {
      processContainerReplica(
          datanodeDetails, container, replicaProto, publisher);
    } catch (IOException | InvalidStateTransitionException e) {
      LOG.error("Exception while processing container report for container" +
              " {} from datanode {}.", replicaProto.getContainerID(),
          datanodeDetails, e);
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
    // TODO - must also remove from NodeManager DN -> Cid list.
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
}
