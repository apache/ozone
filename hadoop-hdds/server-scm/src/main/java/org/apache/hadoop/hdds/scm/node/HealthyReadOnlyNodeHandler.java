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

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;

/**
 * Handles non healthy to healthy(ReadOnly) node event.
 */
public class HealthyReadOnlyNodeHandler
    implements EventHandler<DatanodeDetails> {

  private static final Logger LOG =
      LoggerFactory.getLogger(HealthyReadOnlyNodeHandler.class);
  private final PipelineManager pipelineManager;
  private final NodeManager nodeManager;
  private final ConfigurationSource conf;

  public HealthyReadOnlyNodeHandler(
      NodeManager nodeManager, PipelineManager pipelineManager,
      OzoneConfiguration conf) {
    this.pipelineManager = pipelineManager;
    this.nodeManager = nodeManager;
    this.conf = conf;
  }

  @Override
  public void onMessage(DatanodeDetails datanodeDetails,
      EventPublisher publisher) {
    LOG.info("Datanode {} moved to HEALTHY READONLY state.", datanodeDetails);

    Set<PipelineID> pipelineIDs = nodeManager.getPipelines(datanodeDetails);
    for (PipelineID id: pipelineIDs) {
      LOG.info("Closing pipeline {} which uses HEALTHY READONLY datanode {} ",
              id,  datanodeDetails);
      try {
        pipelineManager.closePipeline(pipelineManager.getPipeline(id), false);
      } catch (IOException ex) {
        LOG.error("Failed to close pipeline {} which uses HEALTHY READONLY " +
            "datanode {}: ", id, datanodeDetails, ex);
      }
    }
    //add node back if it is not present in networkTopology
    NetworkTopology nt = nodeManager.getClusterNetworkTopologyMap();
    if (!nt.contains(datanodeDetails)) {
      nt.add(datanodeDetails);
      // make sure after DN is added back into topology, DatanodeDetails
      // instance returned from nodeStateManager has parent correctly set.
      Preconditions.checkNotNull(
          nodeManager.getNodeByUuid(datanodeDetails.getUuidString())
              .getParent());
    }
  }
}
