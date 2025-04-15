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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.IncrementalContainerReportHandler;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon ICR handler.
 */
public class ReconIncrementalContainerReportHandler
    extends IncrementalContainerReportHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      ReconIncrementalContainerReportHandler.class);

  ReconIncrementalContainerReportHandler(NodeManager nodeManager,
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
    final DatanodeDetails datanode = getDatanodeDetails(report);
    if (datanode == null) {
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
    processICR(report, publisher, datanode);
  }
}
