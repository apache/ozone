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

package org.apache.hadoop.ozone.recon.scm;

import java.io.IOException;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.IncrementalContainerReportHandler;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon ICR handler.
 */
public class ReconIncrementalContainerReportHandler
    extends IncrementalContainerReportHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      ReconIncrementalContainerReportHandler.class);

  public ReconIncrementalContainerReportHandler(NodeManager nodeManager,
             ContainerManager containerManager, SCMContext scmContext) {
    super(nodeManager, containerManager, scmContext);
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void onMessage(final IncrementalContainerReportFromDatanode report,
                        final EventPublisher publisher) {
    final DatanodeDetails dnFromReport = report.getDatanodeDetails();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing incremental container report from data node {}",
          dnFromReport);
    }

    final DatanodeDetails dd = getNodeManager().getNode(dnFromReport.getID());
    if (dd == null) {
      LOG.warn("Received container report from unknown datanode {}",
          dnFromReport);
      return;
    }

    ReconContainerManager containerManager =
        (ReconContainerManager) getContainerManager();
    try {
      containerManager.checkAndAddNewContainerBatch(
          report.getReport().getReportList());
    } catch (Exception ioEx) {
      LOG.error("Exception while checking and adding new container.", ioEx);
      return;
    }
    boolean success = true;
    for (ContainerReplicaProto replicaProto :
        report.getReport().getReportList()) {
      ContainerID id = ContainerID.valueOf(replicaProto.getContainerID());
      ContainerInfo container = null;
      try {
        try {
          container = getContainerManager().getContainer(id);
          // Ensure we reuse the same ContainerID instance in containerInfo
          id = container.containerID();
        } finally {
          if (replicaProto.getState().equals(
              ContainerReplicaProto.State.DELETED)) {
            getNodeManager().removeContainer(dd, id);
          } else {
            getNodeManager().addContainer(dd, id);
          }
        }
        processContainerReplica(dd, replicaProto, publisher);
        success = true;
      } catch (NodeNotFoundException ex) {
        success = false;
        LOG.error("Received ICR from unknown datanode {}.",
            report.getDatanodeDetails(), ex);
      } catch (IOException | InvalidStateTransitionException e) {
        success = false;
        LOG.error("Exception while processing ICR for container {}",
            replicaProto.getContainerID());
      }
    }
    containerManager.notifyContainerReportProcessing(false, success);
  }
}
