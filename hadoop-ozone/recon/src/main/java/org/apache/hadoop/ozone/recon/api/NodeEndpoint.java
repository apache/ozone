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

package org.apache.hadoop.ozone.recon.api;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.DecommissionUtils;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.recon.api.types.DatanodeMetadata;
import org.apache.hadoop.ozone.recon.api.types.DatanodePipeline;
import org.apache.hadoop.ozone.recon.api.types.DatanodeStorageReport;
import org.apache.hadoop.ozone.recon.api.types.DatanodesResponse;
import org.apache.hadoop.ozone.recon.api.types.RemoveDataNodesResponseWrapper;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
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
  private StorageContainerLocationProtocol scmClient;
  private String errorMessage = "Error getting pipeline and container metrics for ";

  @Inject
  NodeEndpoint(OzoneStorageContainerManager reconSCM,
               StorageContainerLocationProtocol scmClient) {
    this.nodeManager =
        (ReconNodeManager) reconSCM.getScmNodeManager();
    this.reconContainerManager =
        (ReconContainerManager) reconSCM.getContainerManager();
    this.pipelineManager = (ReconPipelineManager) reconSCM.getPipelineManager();
    this.scmClient = scmClient;
  }

  /**
   * Return the list of datanodes with detailed information about each datanode.
   * @return {@link Response}
   */
  @GET
  public Response getDatanodes() {
    List<DatanodeMetadata> datanodes = new ArrayList<>();
    nodeManager.getAllNodes().forEach(datanode -> {
      DatanodeStorageReport storageReport = getStorageReport(datanode);
      NodeState nodeState = null;
      try {
        nodeState = nodeManager.getNodeStatus(datanode).getHealth();
      } catch (NodeNotFoundException e) {
        LOG.warn("Cannot get nodeState for datanode {}", datanode, e);
      }
      Set<PipelineID> pipelineIDs = nodeManager.getPipelines(datanode);
      List<DatanodePipeline> pipelines = new ArrayList<>();
      AtomicInteger leaderCount = new AtomicInteger();
      AtomicInteger openContainers = new AtomicInteger();

      pipelineIDs.forEach(pipelineID -> {
        try {
          Pipeline pipeline = pipelineManager.getPipeline(pipelineID);
          String leaderNode = pipeline.getLeaderNode().getHostName();
          DatanodePipeline datanodePipeline =
              new DatanodePipeline(pipelineID.getId(),
                  pipeline.getReplicationConfig(), leaderNode);
          pipelines.add(datanodePipeline);
          if (datanode.getID().equals(pipeline.getLeaderId())) {
            leaderCount.getAndIncrement();
          }
          int openContainerPerPipeline =
                  reconContainerManager.getPipelineToOpenContainer()
                  .getOrDefault(pipelineID, 0);
          openContainers.getAndAdd(openContainerPerPipeline);
        } catch (PipelineNotFoundException ex) {
          LOG.warn("Cannot get pipeline {} for datanode {}, pipeline not found",
              pipelineID.getId(), datanode, ex);
        } catch (IOException ioEx) {
          LOG.warn("Cannot get leader node of pipeline with id {}.",
              pipelineID.getId(), ioEx);
        }
      });

      final DatanodeMetadata.Builder builder = DatanodeMetadata.newBuilder()
          .setOpenContainers(openContainers.get());

      try {
        builder.setContainers(nodeManager.getContainerCount(datanode));
      } catch (NodeNotFoundException ex) {
        LOG.warn("Failed to getContainerCount for {}", datanode, ex);
      }

      datanodes.add(builder.setDatanode(datanode)
          .setDatanodeStorageReport(storageReport)
          .setLastHeartbeat(nodeManager.getLastHeartbeat(datanode))
          .setState(nodeState)
          .setPipelines(pipelines)
          .setLeaderCount(leaderCount.get())
          .build());
    });

    return Response.ok(new DatanodesResponse(datanodes)).build();
  }

  /**
   * Returns DatanodeStorageReport for the given Datanode.
   * @param datanode DatanodeDetails
   * @return DatanodeStorageReport
   */
  private DatanodeStorageReport getStorageReport(DatanodeDetails datanode) {
    SCMNodeStat nodeStat =
        nodeManager.getNodeStat(datanode).get();
    SpaceUsageSource.Fixed fsUsage = nodeManager.getTotalFilesystemUsage(datanode);
    DatanodeStorageReport.Builder builder = DatanodeStorageReport.newBuilder()
        .setCapacity(nodeStat.getCapacity().get())
        .setUsed(nodeStat.getScmUsed().get())
        .setRemaining(nodeStat.getRemaining().get())
        .setCommitted(nodeStat.getCommitted().get())
        .setMinimumFreeSpace(nodeStat.getFreeSpaceToSpare().get());

    if (fsUsage != null) {
      builder.setFilesystemCapacity(fsUsage.getCapacity())
          .setFilesystemAvailable(fsUsage.getAvailable())
          .setFilesystemUsed(fsUsage.getUsedSpace());
    }
    return builder.build();
  }

  /**
   * Removes datanodes from Recon's memory and nodes table in Recon DB.
   * @param uuids the list of datanode uuid's
   *
   * @return JSON response with failed, not found and successfully removed datanodes list.
   */
  @PUT
  @Path("/remove")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response removeDatanodes(List<String> uuids) {
    List<DatanodeMetadata> failedDatanodes = new ArrayList<>();
    List<DatanodeMetadata> notFoundDatanodes = new ArrayList<>();
    List<DatanodeMetadata> removedDatanodes = new ArrayList<>();
    Map<String, String> failedNodeErrorResponseMap = new HashMap<>();

    Objects.requireNonNull(uuids, "Datanode list argument should not be null");
    Preconditions.checkArgument(!uuids.isEmpty(), "Datanode list argument should not be empty");
    try {
      for (String uuid : uuids) {
        final DatanodeInfo nodeByUuid = nodeManager.getNode(DatanodeID.fromUuidString(uuid));
        if (nodeByUuid != null) {
          final DatanodeMetadata metadata = DatanodeMetadata.newBuilder()
              .setDatanode(nodeByUuid)
              .setState(nodeManager.getNodeStatus(nodeByUuid).getHealth())
              .build();

          if (preChecksSuccess(nodeByUuid, failedNodeErrorResponseMap)) {
            removedDatanodes.add(metadata);
            nodeManager.removeNode(nodeByUuid);
            LOG.info("Node {} removed successfully !!!", uuid);
          } else {
            failedDatanodes.add(metadata);
          }
        } else {
          LOG.error("Node not found: {}", uuid);
          notFoundDatanodes.add(DatanodeMetadata.newBuilder()
                  .setHostname("")
                  .setState(NodeState.DEAD)
              .setUuid(uuid).build());
        }
      }
    } catch (Exception exp) {
      LOG.error("Unexpected Error while removing datanodes {}", uuids, exp);
      throw new WebApplicationException(exp, Response.Status.INTERNAL_SERVER_ERROR);
    }

    RemoveDataNodesResponseWrapper removeDataNodesResponseWrapper = new RemoveDataNodesResponseWrapper();

    if (!failedDatanodes.isEmpty()) {
      DatanodesResponse failedNodesResp =
          new DatanodesResponse(failedDatanodes.size(), Collections.emptyList());
      failedNodesResp.setFailedNodeErrorResponseMap(failedNodeErrorResponseMap);
      removeDataNodesResponseWrapper.getDatanodesResponseMap().put("failedDatanodes", failedNodesResp);
    }

    if (!notFoundDatanodes.isEmpty()) {
      final DatanodesResponse notFoundNodesResp = new DatanodesResponse(notFoundDatanodes);
      removeDataNodesResponseWrapper.getDatanodesResponseMap().put("notFoundDatanodes", notFoundNodesResp);
    }

    if (!removedDatanodes.isEmpty()) {
      final DatanodesResponse removedNodesResp = new DatanodesResponse(removedDatanodes);
      removeDataNodesResponseWrapper.getDatanodesResponseMap().put("removedDatanodes", removedNodesResp);
    }
    return Response.ok(removeDataNodesResponseWrapper).build();
  }

  private boolean preChecksSuccess(DatanodeDetails nodeByUuid, Map<String, String> failedNodeErrorResponseMap) {
    final NodeStatus nodeStatus;
    AtomicBoolean isContainerOrPipeLineOpen = new AtomicBoolean(false);
    try {
      nodeStatus = nodeManager.getNodeStatus(nodeByUuid);
      if (nodeStatus.isDead()) {
        checkContainers(nodeByUuid, isContainerOrPipeLineOpen);
        if (isContainerOrPipeLineOpen.get()) {
          failedNodeErrorResponseMap.put(nodeByUuid.getUuidString(), "Open Containers/Pipelines");
          return false;
        }
        checkPipelines(nodeByUuid, isContainerOrPipeLineOpen);
        if (isContainerOrPipeLineOpen.get()) {
          failedNodeErrorResponseMap.put(nodeByUuid.getUuidString(), "Open Containers/Pipelines");
          return false;
        }
        return true;
      }
    } catch (NodeNotFoundException e) {
      LOG.error("Node : {} not found", nodeByUuid);
      return false;
    }
    failedNodeErrorResponseMap.put(nodeByUuid.getUuidString(), "DataNode should be in DEAD node status.");
    return false;
  }

  private void checkPipelines(DatanodeDetails nodeByUuid, AtomicBoolean isContainerOrPipeLineOpen) {
    nodeManager.getPipelines(nodeByUuid)
        .forEach(id -> {
          try {
            final Pipeline pipeline = pipelineManager.getPipeline(id);
            if (pipeline.isOpen()) {
              LOG.warn("Pipeline : {} is still open for datanode: {}, pre-check failed, datanode not eligible " +
                  "for remove.", id.getId(), nodeByUuid);
              isContainerOrPipeLineOpen.set(true);
            }
          } catch (PipelineNotFoundException pipelineNotFoundException) {
            LOG.warn("Pipeline {} is not managed by PipelineManager.", id, pipelineNotFoundException);
          }
        });
  }

  private void checkContainers(DatanodeDetails nodeByUuid, AtomicBoolean isContainerOrPipeLineOpen)
      throws NodeNotFoundException {
    nodeManager.getContainers(nodeByUuid)
        .forEach(id -> {
          try {
            final ContainerInfo container = reconContainerManager.getContainer(id);
            if (container.getState() == HddsProtos.LifeCycleState.OPEN) {
              LOG.warn("Failed to remove datanode {} due to OPEN container: {}", nodeByUuid, container);
              isContainerOrPipeLineOpen.set(true);
            }
          } catch (ContainerNotFoundException cnfe) {
            LOG.warn("Container {} is not managed by ContainerManager.",
                id, cnfe);
          }
        });
  }

  /**
   * This GET API provides the information of all datanodes for which decommissioning is initiated.
   * @return the wrapped  Response output
   */
  @GET
  @Path("/decommission/info")
  public Response getDatanodesDecommissionInfo() {
    try {
      return getDecommissionStatusResponse(null, null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This GET API provides the information of a specific datanode for which decommissioning is initiated.
   * API accepts both uuid or ipAddress, uuid will be given preference if both provided.
   * @return the wrapped  Response output
   */
  @GET
  @Path("/decommission/info/datanode")
  public Response getDecommissionInfoForDatanode(@QueryParam("uuid") String uuid,
                                                 @QueryParam("ipAddress") String ipAddress) {
    if (StringUtils.isEmpty(uuid)) {
      Objects.requireNonNull(ipAddress, "Either uuid or ipAddress of a datanode should be provided !!!");
      Preconditions.checkArgument(!ipAddress.isEmpty(),
          "Either uuid or ipAddress of a datanode should be provided !!!");
    }
    try {
      return getDecommissionStatusResponse(uuid, ipAddress);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Response getDecommissionStatusResponse(String uuid, String ipAddress) throws IOException {
    Response.ResponseBuilder builder = Response.status(Response.Status.OK);
    Map<String, Object> responseMap = new HashMap<>();
    Stream<HddsProtos.Node> allNodes = scmClient.queryNode(DECOMMISSIONING,
        null, HddsProtos.QueryScope.CLUSTER, "", ClientVersion.CURRENT_VERSION).stream();
    List<HddsProtos.Node> decommissioningNodes =
        DecommissionUtils.getDecommissioningNodesList(allNodes, uuid, ipAddress);
    String metricsJson = scmClient.getMetrics("Hadoop:service=StorageContainerManager,name=NodeDecommissionMetrics");
    int numDecomNodes = -1;
    JsonNode jsonNode = null;
    if (metricsJson != null) {
      jsonNode = DecommissionUtils.getBeansJsonNode(metricsJson);
      numDecomNodes = DecommissionUtils.getNumDecomNodes(jsonNode);
    }
    List<Map<String, Object>> dnDecommissionInfo =
        getDecommissioningNodesDetails(decommissioningNodes, jsonNode, numDecomNodes);
    try {
      responseMap.put("DatanodesDecommissionInfo", dnDecommissionInfo);
      builder.entity(responseMap);
      return builder.build();
    } catch (Exception exception) {
      LOG.error("Unexpected Error: {}", exception);
      throw new WebApplicationException(exception, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private List<Map<String, Object>> getDecommissioningNodesDetails(List<HddsProtos.Node> decommissioningNodes,
                                                                   JsonNode jsonNode,
                                                                   int numDecomNodes) throws IOException {
    List<Map<String, Object>> decommissioningNodesDetails = new ArrayList<>();

    for (HddsProtos.Node node : decommissioningNodes) {
      DatanodeDetails datanode = DatanodeDetails.getFromProtoBuf(
          node.getNodeID());
      Map<String, Object> datanodeMap = new LinkedHashMap<>();
      datanodeMap.put("datanodeDetails", datanode);
      datanodeMap.put("metrics", getCounts(datanode, jsonNode, numDecomNodes));
      datanodeMap.put("containers", getContainers(datanode));
      decommissioningNodesDetails.add(datanodeMap);
    }
    return decommissioningNodesDetails;
  }

  private Map<String, Object> getCounts(DatanodeDetails datanode, JsonNode counts, int numDecomNodes) {
    Map<String, Object> countsMap = new LinkedHashMap<>();
    String errMsg = getErrorMessage() + datanode.getHostName();
    try {
      countsMap = DecommissionUtils.getCountsMap(datanode, counts, numDecomNodes, countsMap, errMsg);
      if (countsMap != null) {
        return countsMap;
      }
      LOG.error(errMsg);
    } catch (IOException e) {
      LOG.error(errMsg + ": {} ", e);
    }
    return countsMap;
  }

  private Map<String, Object> getContainers(DatanodeDetails datanode)
      throws IOException {
    Map<String, List<ContainerID>> containers = scmClient.getContainersOnDecomNode(datanode);
    return containers.entrySet().stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> entry.getValue().stream().
                map(ContainerID::toString).
                collect(Collectors.toList())));
  }

  public String getErrorMessage() {
    return errorMessage;
  }
}
