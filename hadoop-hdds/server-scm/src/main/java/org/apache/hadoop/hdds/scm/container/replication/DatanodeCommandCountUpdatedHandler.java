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

package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Event handler for the DATANODE_COMMAND_COUNT_UPDATED event.
 */
public class DatanodeCommandCountUpdatedHandler implements
    EventHandler<DatanodeDetails> {

  private final ReplicationManager replicationManager;
  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeCommandCountUpdatedHandler.class);

  public DatanodeCommandCountUpdatedHandler(
      ReplicationManager replicationManager) {
    this.replicationManager = replicationManager;
  }

  @Override
  public void onMessage(DatanodeDetails datanodeDetails,
      EventPublisher publisher) {
    LOG.trace("DatanodeCommandCountUpdatedHandler called with datanode {}",
        datanodeDetails);
    replicationManager.datanodeCommandCountUpdated(datanodeDetails);
  }
}
