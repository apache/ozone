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
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Updates the DiskBalancer status of a datanode to UNKNOWN when the datanode is marked dead.
 *
 * This handler listens for DEAD_NODE events and ensures that DiskBalancer
 * does not retain outdated state for unreachable datanodes.
 */


public class DiskBalancerDeadNodeHandler implements EventHandler<DatanodeDetails> {

  private static final Logger LOG = LoggerFactory.getLogger(DiskBalancerDeadNodeHandler.class);
  private final DiskBalancerManager diskBalancerManager;

  public DiskBalancerDeadNodeHandler(DiskBalancerManager diskBalancerManager) {
    this.diskBalancerManager = diskBalancerManager;
  }

  @Override
  public void onMessage(DatanodeDetails datanodeDetails, EventPublisher publisher) {
    LOG.info("Marking DiskBalancer status UNKNOWN for dead DN {}", datanodeDetails.getUuidString());
    diskBalancerManager.markStatusUnknown(datanodeDetails);
  }
}
