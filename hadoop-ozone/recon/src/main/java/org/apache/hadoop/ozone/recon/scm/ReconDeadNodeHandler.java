/*
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

import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.Node;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
import org.apache.hadoop.hdds.scm.node.DeadNodeHandler;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon's handling of Dead node.
 */
public class ReconDeadNodeHandler extends DeadNodeHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconDeadNodeHandler.class);


  private StorageContainerServiceProvider scmClient;

  public ReconDeadNodeHandler(NodeManager nodeManager,
                              PipelineManager pipelineManager,
                              ContainerManagerV2 containerManager,
                              StorageContainerServiceProvider scmClient) {
    super(nodeManager, pipelineManager, containerManager);
    this.scmClient = scmClient;
  }

  @Override
  public void onMessage(final DatanodeDetails datanodeDetails,
                        final EventPublisher publisher) {

    super.onMessage(datanodeDetails, publisher);
    ReconNodeManager nodeManager = (ReconNodeManager) getNodeManager();
    try {
      List<Node> nodes = scmClient.getNodes();
      Optional<Node> matchedDn = nodes.stream()
              .filter(n -> n.getNodeID().getUuid()
                  .equals(datanodeDetails.getUuidString()))
              .findAny();

      if (matchedDn.isPresent()) {
        nodeManager.updateNodeOperationalStateFromScm(matchedDn.get(),
            datanodeDetails);
      } else {
        LOG.warn("Node {} has reached DEAD state, but SCM does not have " +
            "information about it.", datanodeDetails);
      }
    } catch (Exception ioEx) {
      LOG.error("Error trying to verify Node operational state from SCM.",
          ioEx);
    }
  }
}
