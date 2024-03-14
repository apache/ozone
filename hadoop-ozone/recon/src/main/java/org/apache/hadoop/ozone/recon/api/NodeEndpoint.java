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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.DatanodeMetadata;
import org.apache.hadoop.ozone.recon.api.types.DatanodePipeline;
import org.apache.hadoop.ozone.recon.api.types.DatanodeStorageReport;
import org.apache.hadoop.ozone.recon.api.types.DatanodesResponse;
import org.apache.hadoop.ozone.recon.api.types.DecommissionStatusInfoResponse;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
  private ReconContainerManager reconContainerManager;

  @Inject
  NodeEndpoint(OzoneStorageContainerManager reconSCM) {
    this.nodeManager =
        (ReconNodeManager) reconSCM.getScmNodeManager();
    this.reconContainerManager = 
        (ReconContainerManager) reconSCM.getContainerManager();
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
      final NodeOperationalState nodeOpState = datanode.getPersistedOpState();
      String hostname = datanode.getHostName();
      Set<PipelineID> pipelineIDs = nodeManager.getPipelines(datanode);
      List<DatanodePipeline> pipelines = new ArrayList<>();
      AtomicInteger leaderCount = new AtomicInteger();
      AtomicInteger openContainers = new AtomicInteger();
      DatanodeMetadata.Builder builder = DatanodeMetadata.newBuilder();

      pipelineIDs.forEach(pipelineID -> {
        try {
          Pipeline pipeline = pipelineManager.getPipeline(pipelineID);
          String leaderNode = pipeline.getLeaderNode().getHostName();
          DatanodePipeline datanodePipeline =
              new DatanodePipeline(pipelineID.getId(),
                  pipeline.getReplicationConfig(), leaderNode);
          pipelines.add(datanodePipeline);
          if (datanode.getUuid().equals(pipeline.getLeaderId())) {
            leaderCount.getAndIncrement();
          }
          int openContainerPerPipeline =
                  reconContainerManager.getPipelineToOpenContainer()
                  .getOrDefault(pipelineID, 0);
          openContainers.getAndAdd(openContainerPerPipeline);
        } catch (PipelineNotFoundException ex) {
          LOG.warn("Cannot get pipeline {} for datanode {}, pipeline not found",
              pipelineID.getId(), hostname, ex);
        } catch (IOException ioEx) {
          LOG.warn("Cannot get leader node of pipeline with id {}.",
              pipelineID.getId(), ioEx);
        }
      });
      try {
        Set<ContainerID> allContainers = nodeManager.getContainers(datanode);

        builder.withContainers(allContainers.size());
        builder.withOpenContainers(openContainers.get());
      } catch (NodeNotFoundException ex) {
        LOG.warn("Cannot get containers, datanode {} not found.",
            datanode.getUuid(), ex);
      }

      DatanodeInfo dnInfo = (DatanodeInfo) datanode;
      datanodes.add(builder.withHostname(nodeManager.getHostName(datanode))
          .withDatanodeStorageReport(storageReport)
          .withLastHeartbeat(nodeManager.getLastHeartbeat(datanode))
          .withState(nodeState)
          .withOperationalState(nodeOpState)
          .withPipelines(pipelines)
          .withLeaderCount(leaderCount.get())
          .withUUid(datanode.getUuidString())
          .withVersion(nodeManager.getVersion(datanode))
          .withSetupTime(nodeManager.getSetupTime(datanode))
          .withRevision(nodeManager.getRevision(datanode))
          .withBuildDate(nodeManager.getBuildDate(datanode))
          .withLayoutVersion(
              dnInfo.getLastKnownLayoutVersion().getMetadataLayoutVersion())
          .withNetworkLocation(datanode.getNetworkLocation())
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
    long committed = nodeStat.getCommitted().get();
    return new DatanodeStorageReport(capacity, used, remaining, committed);
  }

  /**
   * This GET API provides the information of all datanodes for which decommissioning is initiated.
   * @return the wrapped  Response output
   */
  @GET
  @Path("/decommission/info")
  public Response getDatanodesDecommissionInfo() {
    // Command to execute
    List<String> commandArgs = Arrays.asList("ozone", "admin", "datanode", "status", "decommission", "--json");

    return getDecommissionStatusResponse(commandArgs);
  }

  /**
   * This GET API provides the information of a specific datanode for which decommissioning is initiated.
   * @return the wrapped  Response output
   */
  @GET
  @Path("/decommission/info/{uuid}")
  public Response getDecommissionInfoForDatanode(@PathParam("uuid") String uuid) {
    Preconditions.checkNotNull(uuid, "uuid of a datanode cannot be null !!!");
    Preconditions.checkArgument(uuid.isEmpty(), "uuid of a datanode cannot be empty !!!");

    // Command to execute
    List<String> commandArgs =
        Arrays.asList("ozone", "admin", "datanode", "status", "decommission", "--id", uuid, "--json");

    return getDecommissionStatusResponse(commandArgs);
  }

  private static Response getDecommissionStatusResponse(List<String> commandArgs) {
    Response.ResponseBuilder builder = Response.status(Response.Status.OK);
    Map<Integer, String> commandOutputMap = ReconUtils.executeCommand(commandArgs);

    for (Map.Entry<Integer, String> entry : commandOutputMap.entrySet()) {
      Integer exitCode = entry.getKey();
      String processOutput = entry.getValue();
      if (exitCode != 0) {
        builder.status(Response.Status.INTERNAL_SERVER_ERROR);
        builder.entity(
            ReconUtils.joinListArgs(commandArgs) + " command execution is not successful : " + processOutput);
        return builder.build();
      } else {
        // Create ObjectMapper
        ObjectMapper objectMapper = new ObjectMapper();
        LOG.info("processOutput: {}", processOutput);
        // Deserialize JSON to Java object
        List<DecommissionStatusInfoResponse> decommissionStatusInfoResponseList = null;
        try {
          decommissionStatusInfoResponseList =
              objectMapper.readValue(processOutput, new TypeReference<List<DecommissionStatusInfoResponse>>() { });
          builder.entity(decommissionStatusInfoResponseList);
          return builder.build();
        } catch (JsonProcessingException jsonProcessingException) {
          LOG.error("Unexpected JSON Error: {}", jsonProcessingException);
          throw new WebApplicationException(jsonProcessingException, Response.Status.INTERNAL_SERVER_ERROR);
        } catch (Exception exception) {
          LOG.error("Unexpected Error: {}", exception);
          throw new WebApplicationException(exception, Response.Status.INTERNAL_SERVER_ERROR);
        }
      }
    }
    return builder.build();
  }
}
