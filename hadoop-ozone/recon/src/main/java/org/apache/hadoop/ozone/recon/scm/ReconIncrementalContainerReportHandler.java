/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.IncrementalContainerReportHandler;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon ICR handler.
 */
public class ReconIncrementalContainerReportHandler
    extends IncrementalContainerReportHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      ReconIncrementalContainerReportHandler.class);

  private StorageContainerServiceProvider scmClient;

  public ReconIncrementalContainerReportHandler(NodeManager nodeManager,
      ContainerManager containerManager,
      StorageContainerServiceProvider scmClient) {
    super(nodeManager, containerManager);
    this.scmClient = scmClient;
  }

  @Override
  public void onMessage(final IncrementalContainerReportFromDatanode report,
                        final EventPublisher publisher) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing incremental container report from data node {}",
          report.getDatanodeDetails());
    }

    ReconContainerManager containerManager =
        (ReconContainerManager) getContainerManager();
    boolean success = true;
    for (ContainerReplicaProto replicaProto :
        report.getReport().getReportList()) {
      try {
        final DatanodeDetails dd = report.getDatanodeDetails();
        final ContainerID id = ContainerID.valueof(
            replicaProto.getContainerID());
        if (!getContainerManager().exists(id)) {
          LOG.info("New container {} got from {}.", id,
              report.getDatanodeDetails());
          try {
            ContainerWithPipeline containerWithPipeline =
                scmClient.getContainerWithPipeline(id.getId());
            LOG.info("Verified new container from SCM {} ",
                containerWithPipeline.getContainerInfo().containerID());
            containerManager.addNewContainer(id.getId(), containerWithPipeline);
          } catch (IOException ioEx) {
            LOG.error("Exception while getting new container info from SCM",
                ioEx);
            return;
          }
        }
        getNodeManager().addContainer(dd, id);
        processContainerReplica(dd, replicaProto);
      } catch (ContainerNotFoundException e) {
        success = false;
        LOG.warn("Container {} not found!", replicaProto.getContainerID());
      } catch (NodeNotFoundException ex) {
        success = false;
        LOG.error("Received ICR from unknown datanode {} {}",
            report.getDatanodeDetails(), ex);
      } catch (IOException e) {
        success = false;
        LOG.error("Exception while processing ICR for container {}",
            replicaProto.getContainerID());
      }
    }
    getContainerManager().notifyContainerReportProcessing(false, success);
  }
}
