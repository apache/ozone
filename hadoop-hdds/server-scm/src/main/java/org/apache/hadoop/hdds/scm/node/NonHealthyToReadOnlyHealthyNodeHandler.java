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

import java.util.Set;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles non healthy to healthy(ReadOnly) node event.
 */
public class NonHealthyToReadOnlyHealthyNodeHandler
    implements EventHandler<DatanodeDetails> {

  private static final Logger LOG =
      LoggerFactory.getLogger(NonHealthyToReadOnlyHealthyNodeHandler.class);
  private final PipelineManager pipelineManager;
  private final NodeManager nodeManager;
  private final ConfigurationSource conf;

  public NonHealthyToReadOnlyHealthyNodeHandler(
      NodeManager nodeManager, PipelineManager pipelineManager,
      OzoneConfiguration conf) {
    this.pipelineManager = pipelineManager;
    this.nodeManager = nodeManager;
    this.conf = conf;
  }

  @Override
  public void onMessage(DatanodeDetails datanodeDetails,
      EventPublisher publisher) {
    Set<PipelineID> pipelineIds =
        nodeManager.getPipelines(datanodeDetails);
    LOG.info("Datanode {} moved to HEALTH READ ONLY state.",
        datanodeDetails);
    if (!pipelineIds.isEmpty()) {
      LOG.error("Datanode {} is part of pipelines {} in HEALTH READ ONLY " +
              "state.",
          datanodeDetails, pipelineIds);
    }
  }
}
