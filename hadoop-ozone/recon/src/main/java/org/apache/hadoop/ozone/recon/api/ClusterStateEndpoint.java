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
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.api.types.ClusterStateResponse;
import org.apache.hadoop.ozone.recon.api.types.DatanodeStorageReport;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.scm.ReconPipelineManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * Endpoint to fetch current state of ozone cluster.
 */
@Path("/clusterState")
@Produces(MediaType.APPLICATION_JSON)
public class ClusterStateEndpoint {

  private static final Logger LOG =
      LoggerFactory.getLogger(ClusterStateEndpoint.class);

  private ReconNodeManager nodeManager;
  private ReconPipelineManager pipelineManager;
  private ReconContainerManager containerManager;
  private ReconOMMetadataManager omMetadataManager;

  @Inject
  ClusterStateEndpoint(OzoneStorageContainerManager reconSCM,
                       ReconOMMetadataManager omMetadataManager) {
    this.nodeManager =
        (ReconNodeManager) reconSCM.getScmNodeManager();
    this.pipelineManager = (ReconPipelineManager) reconSCM.getPipelineManager();
    this.containerManager =
        (ReconContainerManager) reconSCM.getContainerManager();
    this.omMetadataManager = omMetadataManager;
  }

  /**
   * Return a summary report on current cluster state.
   * @return {@link Response}
   */
  @GET
  public Response getClusterState() {
    List<DatanodeDetails> datanodeDetails = nodeManager.getAllNodes();
    int containers = this.containerManager.getContainerIDs().size();
    int pipelines = this.pipelineManager.getPipelines().size();
    int healthyDatanodes = nodeManager.getNodeCount(NodeState.HEALTHY);
    SCMNodeStat stats = nodeManager.getStats();
    DatanodeStorageReport storageReport =
        new DatanodeStorageReport(stats.getCapacity().get(),
            stats.getScmUsed().get(), stats.getRemaining().get());
    ClusterStateResponse.Builder builder = ClusterStateResponse.newBuilder();
    try {
      builder.setVolumes(
          omMetadataManager.getVolumeTable().getEstimatedKeyCount());
    } catch (Exception ex) {
      LOG.error("Unable to get Volumes count in ClusterStateResponse.", ex);
    }
    try {
      builder.setBuckets(
          omMetadataManager.getBucketTable().getEstimatedKeyCount());
    } catch (Exception ex) {
      LOG.error("Unable to get Buckets count in ClusterStateResponse.", ex);
    }
    try {
      builder.setKeys(
          omMetadataManager.getKeyTable().getEstimatedKeyCount());
    } catch (Exception ex) {
      LOG.error("Unable to get Keys count in ClusterStateResponse.", ex);
    }
    ClusterStateResponse response = builder
        .setStorageReport(storageReport)
        .setPipelines(pipelines)
        .setContainers(containers)
        .setTotalDatanodes(datanodeDetails.size())
        .setHealthyDatanodes(healthyDatanodes)
        .build();
    return Response.ok(response).build();
  }
}
