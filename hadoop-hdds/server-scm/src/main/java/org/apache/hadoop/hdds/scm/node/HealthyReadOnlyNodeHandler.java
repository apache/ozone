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

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles non healthy to healthy(ReadOnly) node event.
 */
public class HealthyReadOnlyNodeHandler
    implements EventHandler<DatanodeDetails> {

  private static final Logger LOG =
      LoggerFactory.getLogger(HealthyReadOnlyNodeHandler.class);
  private final PipelineManager pipelineManager;
  private final NodeManager nodeManager;

  public HealthyReadOnlyNodeHandler(
      NodeManager nodeManager, PipelineManager pipelineManager) {
    this.pipelineManager = pipelineManager;
    this.nodeManager = nodeManager;
  }

  @Override
  public void onMessage(DatanodeDetails datanodeDetails,
      EventPublisher publisher) {
    LOG.info("Datanode {} moved to HEALTHY READONLY state.", datanodeDetails);

    /*
     * Order of finalization operations should be:
     * 1. SCM closes all pipelines.
     *   - This queues close commands for all containers to be sent to the
     *     datanodes.
     *   - Pipelines will remain in the DB until the scrubber removes them
     *     since we did not force close the pipelines.
     * 2. SCM finalizes.
     * 3. SCM moves all datanodes healthy readonly state.
     *   - Before this, no datanode should have been moved to healthy
     *     readonly, even if it heartbeated while SCM was finalizing.
     *
     * During the initial pipeline close phase, some containers may end up
     * in the CLOSING state if they were in the process of leader election
     * for their pipeline when the close command was received. A datanode
     * cannot finalize with CLOSING containers, so we want to move those
     * containers to CLOSE soon without waiting for the replication manager
     * to do it.
     *
     * To do this, we will resend close commands for each pipeline. Since the
     * pipelines should already be closed and we are not force closing them, no
     * pipeline action is queued for the datanode. However, close container
     * commands are still queued for all containers currently in the pipeline.
     * The datanode will ignore these commands for CLOSED containers, but it
     * allows CLOSING containers to move to CLOSED so finalization can progress.
     */
    Set<PipelineID> pipelineIDs = nodeManager.getPipelines(datanodeDetails);
    for (PipelineID pipelineID : pipelineIDs) {
      try {
        Pipeline pipeline = pipelineManager.getPipeline(pipelineID);
        LOG.info("Sending close command for pipeline {} in state {} which " +
                "uses {} datanode {}. This will send close commands for its " +
                "containers.",
            pipelineID, pipeline.getPipelineState(),
            HddsProtos.NodeState.HEALTHY_READONLY,
            datanodeDetails);
        pipelineManager.closePipeline(pipelineID);
      } catch (IOException ex) {
        LOG.error("Failed to close pipeline {} which uses HEALTHY READONLY " +
            "datanode {}: ", pipelineID, datanodeDetails, ex);
      }
    }

    //add node back if it is not present in networkTopology
    NetworkTopology nt = nodeManager.getClusterNetworkTopologyMap();
    if (!nt.contains(datanodeDetails)) {
      nt.add(datanodeDetails);
      // make sure after DN is added back into topology, DatanodeDetails
      // instance returned from nodeStateManager has parent correctly set.
      Objects.requireNonNull(
          nodeManager.getNode(datanodeDetails.getID())
              .getParent(), "Parent == null");
    }
  }
}
