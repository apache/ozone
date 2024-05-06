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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.ozone.recon.api.types.DatanodeMetadata;
import org.apache.hadoop.ozone.recon.api.types.DatanodePipeline;
import org.apache.hadoop.ozone.recon.api.types.DatanodeStorageReport;
import org.apache.hadoop.ozone.recon.api.types.DatanodesResponse;
import org.apache.hadoop.ozone.recon.api.types.RemoveDataNodesResponseWrapper;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
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

    Preconditions.checkNotNull(uuids, "Datanode list argument should not be null");
    Preconditions.checkArgument(!uuids.isEmpty(), "Datanode list argument should not be empty");
    try {
      for (String uuid : uuids) {
        DatanodeDetails nodeByUuid = nodeManager.getNodeByUuid(uuid);
        try {
          if (preChecksSuccess(nodeByUuid, failedNodeErrorResponseMap)) {
            removedDatanodes.add(DatanodeMetadata.newBuilder()
                .withHostname(nodeManager.getHostName(nodeByUuid))
                .withUUid(uuid)
                .withState(nodeManager.getNodeStatus(nodeByUuid).getHealth())
                .build());
            nodeManager.removeNode(nodeByUuid);
            LOG.info("Node {} removed successfully !!!", uuid);
          } else {
            failedDatanodes.add(DatanodeMetadata.newBuilder()
                .withHostname(nodeManager.getHostName(nodeByUuid))
                .withUUid(uuid)
                .withOperationalState(nodeByUuid.getPersistedOpState())
                .withState(nodeManager.getNodeStatus(nodeByUuid).getHealth())
                .build());
          }
        } catch (NodeNotFoundException nnfe) {
          LOG.error("Selected node {} not found : {} ", uuid, nnfe);
          notFoundDatanodes.add(DatanodeMetadata.newBuilder()
                  .withHostname("")
                  .withState(NodeState.DEAD)
              .withUUid(uuid).build());
        }
      }
    } catch (Exception exp) {
      LOG.error("Unexpected Error while removing datanodes : {} ", exp);
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
      DatanodesResponse notFoundNodesResp =
          new DatanodesResponse(notFoundDatanodes.size(), notFoundDatanodes);
      removeDataNodesResponseWrapper.getDatanodesResponseMap().put("notFoundDatanodes", notFoundNodesResp);
    }

    if (!removedDatanodes.isEmpty()) {
      DatanodesResponse removedNodesResp =
          new DatanodesResponse(removedDatanodes.size(), removedDatanodes);
      removeDataNodesResponseWrapper.getDatanodesResponseMap().put("removedDatanodes", removedNodesResp);
    }
    return Response.ok(removeDataNodesResponseWrapper).build();
  }

  private boolean preChecksSuccess(DatanodeDetails nodeByUuid, Map<String, String> failedNodeErrorResponseMap)
      throws NodeNotFoundException {
    if (null == nodeByUuid) {
      throw new NodeNotFoundException("Node  not found !!!");
    }
    NodeStatus nodeStatus = null;
    AtomicBoolean isContainerOrPipeLineOpen = new AtomicBoolean(false);
    try {
      nodeStatus = nodeManager.getNodeStatus(nodeByUuid);
      boolean isNodeDecommissioned = nodeByUuid.getPersistedOpState() == NodeOperationalState.DECOMMISSIONED;
      if (isNodeDecommissioned || nodeStatus.isDead()) {
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
    failedNodeErrorResponseMap.put(nodeByUuid.getUuidString(), "DataNode should be in either DECOMMISSIONED " +
        "operational state or DEAD node state.");
    return false;
  }

  private void checkPipelines(DatanodeDetails nodeByUuid, AtomicBoolean isContainerOrPipeLineOpen) {
    nodeManager.getPipelines(nodeByUuid)
        .forEach(id -> {
          try {
            final Pipeline pipeline = pipelineManager.getPipeline(id);
            if (pipeline.isOpen()) {
              LOG.warn("Pipeline : {} is still open for datanode: {}, pre-check failed, datanode not eligible " +
                  "for remove.", id.getId(), nodeByUuid.getUuid());
              isContainerOrPipeLineOpen.set(true);
              return;
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
              LOG.warn("Container : {} is still open for datanode: {}, pre-check failed, datanode not eligible " +
                  "for remove.", container.getContainerID(), nodeByUuid.getUuid());
              isContainerOrPipeLineOpen.set(true);
              return;
            }
          } catch (ContainerNotFoundException cnfe) {
            LOG.warn("Container {} is not managed by ContainerManager.",
                id, cnfe);
          }
        });
  }
}
