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

import java.util.Objects;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles non healthy to healthy node event.
 */
public class UnhealthyToHealthyNodeHandler implements EventHandler<DatanodeDetails> {

  private static final Logger LOG = LoggerFactory.getLogger(UnhealthyToHealthyNodeHandler.class);
  private final SCMServiceManager serviceManager;
  private final NodeManager nodeManager;

  public UnhealthyToHealthyNodeHandler(NodeManager nodeManager, SCMServiceManager serviceManager) {
    this.serviceManager = serviceManager;
    this.nodeManager = nodeManager;
  }

  @Override
  public void onMessage(DatanodeDetails datanodeDetails, EventPublisher publisher) {
    LOG.info("Datanode {} moved to HEALTHY state.", datanodeDetails);

    //add node back if it is not present in networkTopology
    NetworkTopology nt = nodeManager.getClusterNetworkTopologyMap();
    if (!nt.contains(datanodeDetails)) {
      nt.add(datanodeDetails);
      // make sure after DN is added back into topology, DatanodeDetails
      // instance returned from nodeStateManager has parent correctly set.
      Objects.requireNonNull(nodeManager.getNode(datanodeDetails.getID()).getParent(), "Parent == null");
    }
    serviceManager.notifyEventTriggered(SCMService.Event.UNHEALTHY_TO_HEALTHY_NODE_HANDLER_TRIGGERED);
  }
}
