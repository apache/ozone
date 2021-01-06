/*
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
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.api.types.DatanodeMetadata;
import org.apache.hadoop.ozone.recon.api.types.DatanodePipeline;
import org.apache.hadoop.ozone.recon.api.types.DatanodeStorageReport;
import org.apache.hadoop.ozone.recon.api.types.DatanodesResponse;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.ozone.recon.scm.ReconPipelineManager;
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
  private ReconPipelineManager pipelineManager;

  @Inject
  NodeEndpoint(OzoneStorageContainerManager reconSCM) {
    this.nodeManager =
        (ReconNodeManager) reconSCM.getScmNodeManager();
    this.pipelineManager = (ReconPipelineManager) reconSCM.getPipelineManager();
  }

  /**
   * Return the list of datanodes with detailed information about each datanode.
   * @return {@link Response}
   */
  @GET
  public Response getDatanodes() {
    List<DatanodeMetadata> datanodes = new ArrayList<>();
    List<DatanodeDetails> datanodeDetails = nodeManager.getAllNodes();

    datanodeDetails.forEach(datanode -> {
      DatanodeStorageReport storageReport = getStorageReport(datanode);
      NodeState nodeState = null;
      try {
        nodeState = nodeManager.getNodeStatus(datanode).getHealth();
      } catch (NodeNotFoundException e) {
        LOG.warn("Cannot get nodeState for datanode {}", datanode, e);
      }
      String hostname = datanode.getHostName();
      Set<PipelineID> pipelineIDs = nodeManager.getPipelines(datanode);
      List<DatanodePipeline> pipelines = new ArrayList<>();
      AtomicInteger leaderCount = new AtomicInteger();
      DatanodeMetadata.Builder builder = DatanodeMetadata.newBuilder();
      pipelineIDs.forEach(pipelineID -> {
        try {
          Pipeline pipeline = pipelineManager.getPipeline(pipelineID);
          String leaderNode = pipeline.getLeaderNode().getHostName();
          DatanodePipeline datanodePipeline = new DatanodePipeline(
              pipelineID.getId(),
              pipeline.getType().toString(),
              pipeline.getFactor().getNumber(),
              leaderNode
          );
          pipelines.add(datanodePipeline);
          if (datanode.getUuid().equals(pipeline.getLeaderId())) {
            leaderCount.getAndIncrement();
          }
        } catch (PipelineNotFoundException ex) {
          LOG.warn("Cannot get pipeline {} for datanode {}, pipeline not found",
              pipelineID.getId(), hostname, ex);
        } catch (IOException ioEx) {
          LOG.warn("Cannot get leader node of pipeline with id {}.",
              pipelineID.getId(), ioEx);
        }
      });
      try {
        int containers = nodeManager.getContainers(datanode).size();
        builder.withContainers(containers);
      } catch (NodeNotFoundException ex) {
        LOG.warn("Cannot get containers, datanode {} not found.",
            datanode.getUuid(), ex);
      }
      datanodes.add(builder.withHostname(hostname)
          .withDatanodeStorageReport(storageReport)
          .withLastHeartbeat(nodeManager.getLastHeartbeat(datanode))
          .withState(nodeState)
          .withPipelines(pipelines)
          .withLeaderCount(leaderCount.get())
          .withUUid(datanode.getUuidString())
          .withVersion(datanode.getVersion())
          .withSetupTime(datanode.getSetupTime())
          .withRevision(datanode.getRevision())
          .withBuildDate(datanode.getBuildDate())
          .build());
    });

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
