/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Handles datanode ip or hostname change event.
 */
public class NodeIpOrHostnameUpdateHandler
        implements EventHandler<DatanodeDetails> {
  private static final Logger LOG =
          LoggerFactory.getLogger(NodeIpOrHostnameUpdateHandler.class);

  private final PipelineManager pipelineManager;
  private final NodeDecommissionManager decommissionManager;
  private final SCMServiceManager serviceManager;

  public NodeIpOrHostnameUpdateHandler(PipelineManager pipelineManager,
                                   NodeDecommissionManager decommissionManager,
                                   SCMServiceManager serviceManager) {
    this.pipelineManager = pipelineManager;
    this.decommissionManager = decommissionManager;
    this.serviceManager = serviceManager;
  }

  @Override
  public void onMessage(DatanodeDetails datanodeDetails,
                        EventPublisher publisher) {
    try {
      closeStalePipelines(datanodeDetails);
      serviceManager.notifyEventTriggered(SCMService.Event
              .NODE_IP_OR_HOSTNAME_UPDATE_HANDLER_TRIGGERED);

      if (datanodeDetails.getPersistedOpState()
              != HddsProtos.NodeOperationalState.IN_SERVICE) {
        decommissionManager.continueAdminForNode(datanodeDetails);
      }
    } catch (NodeNotFoundException e) {
      // Should not happen, as the node has just registered to call this event
      // handler.
      LOG.warn(
              "NodeNotFound when updating the node Ip or host name to the " +
                      "decommissionManager",
              e);
    }
  }

  /**
   * close the pipelines whose nodes' IPs are stale.
   *
   * @param datanodeDetails new datanodeDetails
   */
  private void closeStalePipelines(DatanodeDetails datanodeDetails) {
    List<Pipeline> pipelines = pipelineManager.getPipelines();
    List<Pipeline> pipelinesWithStaleIpOrHostname =
            pipelines.stream()
                    .filter(p -> p.getNodes().stream()
                            .anyMatch(n -> n.getUuid()
                                    .equals(datanodeDetails.getUuid())
                                    && (!n.getIpAddress()
                                    .equals(datanodeDetails.getIpAddress())
                                    || !n.getHostName()
                                    .equals(datanodeDetails.getHostName()))))
                    .collect(Collectors.toList());
    LOG.info("Pipelines with stale IP or Host name: {}",
            pipelinesWithStaleIpOrHostname);
    pipelinesWithStaleIpOrHostname.forEach(p -> {
      try {
        LOG.info("Closing pipeline: {}", p.getId());
        pipelineManager.closePipeline(p, false);
        LOG.info("Closed pipeline: {}", p.getId());
      } catch (IOException e) {
        LOG.error("Close pipeline failed: {}", p, e);
      }
    });
  }
}
