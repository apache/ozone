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

package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ha.SCMService.Event;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles New Node event.
 */
public class NewNodeHandler implements EventHandler<DatanodeDetails> {
  private static final Logger LOG =
      LoggerFactory.getLogger(NewNodeHandler.class);

  private final PipelineManager pipelineManager;
  private final NodeDecommissionManager decommissionManager;
  private final SCMServiceManager serviceManager;

  public NewNodeHandler(PipelineManager pipelineManager,
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
      pipelineManager.closeStalePipelines(datanodeDetails);
      serviceManager.notifyEventTriggered(Event.NEW_NODE_HANDLER_TRIGGERED);

      if (datanodeDetails.getPersistedOpState()
          != HddsProtos.NodeOperationalState.IN_SERVICE) {
        decommissionManager.continueAdminForNode(datanodeDetails);
      }
    } catch (NodeNotFoundException e) {
      // Should not happen, as the node has just registered to call this event
      // handler.
      LOG.warn("NodeNotFound when adding the node to the decommissionManager",
          e);
    }
  }
}
