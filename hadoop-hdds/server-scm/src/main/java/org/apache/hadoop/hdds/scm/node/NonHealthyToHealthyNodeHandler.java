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
package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ha.SCMService.Event;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles Stale node event.
 */
public class NonHealthyToHealthyNodeHandler
    implements EventHandler<DatanodeDetails> {
  private static final Logger LOG =
      LoggerFactory.getLogger(NonHealthyToHealthyNodeHandler.class);

  private final ConfigurationSource conf;
  private final SCMServiceManager serviceManager;

  public NonHealthyToHealthyNodeHandler(
      OzoneConfiguration conf,  SCMServiceManager serviceManager) {
    this.conf = conf;
    this.serviceManager = serviceManager;
  }

  @Override
  public void onMessage(DatanodeDetails datanodeDetails,
      EventPublisher publisher) {
    serviceManager.notifyEventTriggered(
        Event.UNHEALTHY_TO_HEALTHY_NODE_HANDLER_TRIGGERED);
  }
}
