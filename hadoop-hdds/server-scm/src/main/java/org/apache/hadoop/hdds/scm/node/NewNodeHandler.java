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

package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles New Node event.
 */
public class NewNodeHandler implements EventHandler<DatanodeDetails> {
  private static final Logger LOG =
      LoggerFactory.getLogger(NewNodeHandler.class);

  private final PipelineManager pipelineManager;
  private final ConfigurationSource conf;

  public NewNodeHandler(PipelineManager pipelineManager,
      ConfigurationSource conf) {
    this.pipelineManager = pipelineManager;
    this.conf = conf;
  }

  @Override
  public void onMessage(DatanodeDetails datanodeDetails,
      EventPublisher publisher) {
    try {
      pipelineManager.triggerPipelineCreation();
    } catch (NotLeaderException ex) {
      LOG.debug("Not the current leader SCM and cannot start pipeline" +
          " creation.");
    }
  }
}
