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

package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.api.types.DatanodeMetadata;
import org.apache.hadoop.ozone.recon.api.types.DatanodeStorageReport;
import org.apache.hadoop.ozone.recon.api.types.DatanodesResponse;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Endpoint to fetch details about datanodes.
 */
@Path("/datanodes")
@Produces(MediaType.APPLICATION_JSON)
public class NodeEndpoint {

  private static final Logger LOG =
      LoggerFactory.getLogger(NodeEndpoint.class);

  private ReconNodeManager nodeManager;

  @Inject
  NodeEndpoint(OzoneStorageContainerManager reconSCM) {
    this.nodeManager =
        (ReconNodeManager) reconSCM.getScmNodeManager();
  }

  /**
   * Return the list of datanodes with detailed information about each datanode.
   * @return {@link Response}
   */
  @GET
  public Response getDatanodes() {
    List<DatanodeMetadata> datanodes = new ArrayList<>();
    List<DatanodeDetails> datanodeDetails = nodeManager.getAllNodes();

    for (DatanodeDetails datanode : datanodeDetails) {
      DatanodeStorageReport storageReport = getStorageReport(datanode);
      NodeState nodeState = nodeManager.getNodeState(datanode);
      String hostname = datanode.getHostName();
      Set<PipelineID> pipelineIDS = nodeManager.getPipelines(datanode);
      List<UUID> pipelines = new ArrayList<>();
      for (PipelineID pipelineID: pipelineIDS) {
        pipelines.add(pipelineID.getId());
      }
      int containers;
      try {
        containers = nodeManager.getContainers(datanode).size();
      } catch (NodeNotFoundException e) {
        containers = 0;
        LOG.warn("Cannot get containers, datanode {} not found.",
            datanode.getUuid());
      }
      long heartbeat = nodeManager.getLastHeartbeat(datanode);
      datanodes.add(new DatanodeMetadata(hostname, nodeState, heartbeat,
          storageReport, pipelines, containers));
    }

    DatanodesResponse datanodesResponse =
        new DatanodesResponse(datanodes.size(), datanodes);
    return Response.ok(datanodesResponse).build();
  }

  /**
   * Returns DatanodeStorageReport for the given Datanode.
   * @param datanode DatanodeDetails
   * @return DatanodeStorageReport
   */
  private DatanodeStorageReport getStorageReport(DatanodeDetails datanode) {
    SCMNodeStat nodeStat =
        nodeManager.getNodeStat(datanode).get();
    long capacity = nodeStat.getCapacity().get();
    long used = nodeStat.getScmUsed().get();
    long remaining = nodeStat.getRemaining().get();
    return new DatanodeStorageReport(capacity, used, remaining);
  }
}
