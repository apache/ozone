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

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.VOLUME_TABLE;

import java.io.IOException;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.api.types.ClusterStateResponse;
import org.apache.hadoop.ozone.recon.api.types.ClusterStorageReport;
import org.apache.hadoop.ozone.recon.api.types.ContainerStateCounts;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManagerV2;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.scm.ReconPipelineManager;
import org.apache.hadoop.ozone.recon.spi.ReconGlobalStatsManager;
import org.apache.hadoop.ozone.recon.tasks.GlobalStatsValue;
import org.apache.hadoop.ozone.recon.tasks.OmTableInsightTask;
import org.apache.ozone.recon.schema.ContainerSchemaDefinitionV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Endpoint to fetch the current state of the ozone cluster.
 */
@Path("/clusterState")
@Produces(MediaType.APPLICATION_JSON)
public class ClusterStateEndpoint {

  private static final Logger LOG =
      LoggerFactory.getLogger(ClusterStateEndpoint.class);
  public static final int MISSING_CONTAINER_COUNT_LIMIT = 1001;

  private final ReconNodeManager nodeManager;
  private final ReconPipelineManager pipelineManager;
  private final ReconContainerManager containerManager;
  private final ReconGlobalStatsManager reconGlobalStatsManager;
  private final OzoneConfiguration ozoneConfiguration;
  private final ContainerHealthSchemaManagerV2 containerHealthSchemaManagerV2;

  @Inject
  ClusterStateEndpoint(OzoneStorageContainerManager reconSCM,
                       ReconGlobalStatsManager reconGlobalStatsManager,
                       ContainerHealthSchemaManagerV2
                           containerHealthSchemaManagerV2,
                       OzoneConfiguration ozoneConfiguration) {
    this.nodeManager =
        (ReconNodeManager) reconSCM.getScmNodeManager();
    this.pipelineManager = (ReconPipelineManager) reconSCM.getPipelineManager();
    this.containerManager =
        (ReconContainerManager) reconSCM.getContainerManager();
    this.reconGlobalStatsManager = reconGlobalStatsManager;
    this.containerHealthSchemaManagerV2 = containerHealthSchemaManagerV2;
    this.ozoneConfiguration = ozoneConfiguration;
  }

  /**
   * Return a summary report on the current cluster state.
   * @return {@link Response}
   */
  @GET
  public Response getClusterState() {
    ContainerStateCounts containerStateCounts = new ContainerStateCounts();
    int pipelines = this.pipelineManager.getPipelines().size();

    List<ContainerHealthSchemaManagerV2.UnhealthyContainerRecordV2> missingContainers =
        containerHealthSchemaManagerV2
        .getUnhealthyContainers(
            ContainerSchemaDefinitionV2.UnHealthyContainerStates.MISSING,
            0L, 0L, MISSING_CONTAINER_COUNT_LIMIT);

    containerStateCounts.setMissingContainerCount(
        missingContainers.size() == MISSING_CONTAINER_COUNT_LIMIT ?
            MISSING_CONTAINER_COUNT_LIMIT : missingContainers.size());

    containerStateCounts.setOpenContainersCount(
        this.containerManager.getContainerStateCount(
            HddsProtos.LifeCycleState.OPEN));

    containerStateCounts.setDeletedContainersCount(
        this.containerManager.getContainerStateCount(
            HddsProtos.LifeCycleState.DELETED));

    int healthyDataNodes =
        nodeManager.getNodeCount(NodeStatus.inServiceHealthy()) +
            nodeManager.getNodeCount(NodeStatus.inServiceHealthyReadOnly());

    SCMNodeStat stats = nodeManager.getStats();
    long fsCapacity = 0;
    long fsUsed = 0;
    long fsAvailable = 0;
    List<DatanodeInfo> dataNodes = nodeManager.getAllNodes();
    if (dataNodes == null || dataNodes.isEmpty()) {
      LOG.warn("No dataNodes available for filesystem usage calculation");
    } else {
      int reportedNodes = 0;
      int totalNodes = dataNodes.size();
      for (DatanodeInfo datanode : dataNodes) {
        SpaceUsageSource.Fixed fsUsage = nodeManager.getTotalFilesystemUsage(datanode);
        if (fsUsage != null) {
          fsCapacity += fsUsage.getCapacity();
          fsAvailable += fsUsage.getAvailable();
          fsUsed += fsUsage.getUsedSpace();
          reportedNodes++;
        } else {
          LOG.debug("DataNode {} has not reported filesystem usage",
              datanode.getUuidString());
        }
      }
      if (reportedNodes < totalNodes) {
        LOG.warn("Filesystem usage incomplete: {}/{} dataNodes reported",
            reportedNodes, totalNodes);
      }
    }

    ClusterStorageReport storageReport = ClusterStorageReport.newBuilder()
        .setCapacity(stats.getCapacity().get())
        .setCommitted(stats.getCommitted().get())
        .setUsed(stats.getScmUsed().get())
        .setMinimumFreeSpace(stats.getFreeSpaceToSpare().get())
        .setRemaining(stats.getRemaining().get())
        .setReserved(stats.getReserved().get())
        .setFilesystemUsed(fsUsed)
        .setFilesystemCapacity(fsCapacity)
        .setFilesystemAvailable(fsAvailable)
        .build();

    ClusterStateResponse.Builder builder = ClusterStateResponse.newBuilder();
    Long totalKeys = 0L;
    Long keysPendingDeletion = 0L;
    Long deletedDirs = 0L;
    
    try {
      // Read from RocksDB instead of SQL
      GlobalStatsValue volumeRecord = reconGlobalStatsManager.getGlobalStatsValue(
          OmTableInsightTask.getTableCountKeyFromTable(VOLUME_TABLE));
      GlobalStatsValue bucketRecord = reconGlobalStatsManager.getGlobalStatsValue(
          OmTableInsightTask.getTableCountKeyFromTable(BUCKET_TABLE));
      // Keys from OBJECT_STORE buckets.
      GlobalStatsValue keyRecord = reconGlobalStatsManager.getGlobalStatsValue(
          OmTableInsightTask.getTableCountKeyFromTable(KEY_TABLE));
      // Keys from FILE_SYSTEM_OPTIMIZED buckets
      GlobalStatsValue fileRecord = reconGlobalStatsManager.getGlobalStatsValue(
          OmTableInsightTask.getTableCountKeyFromTable(FILE_TABLE));
      // Keys from the DeletedTable
      GlobalStatsValue deletedKeyRecord = reconGlobalStatsManager.getGlobalStatsValue(
          OmTableInsightTask.getTableCountKeyFromTable(DELETED_TABLE));
      // Directories from the DeletedDirectoryTable
      GlobalStatsValue deletedDirRecord = reconGlobalStatsManager.getGlobalStatsValue(
          OmTableInsightTask.getTableCountKeyFromTable(DELETED_DIR_TABLE));

      if (volumeRecord != null) {
        builder.setVolumes(volumeRecord.getValue());
      }
      if (bucketRecord != null) {
        builder.setBuckets(bucketRecord.getValue());
      }

      if (keyRecord != null) {
        totalKeys += keyRecord.getValue();
      }
      if (fileRecord != null) {
        totalKeys += fileRecord.getValue();
      }
      if (deletedKeyRecord != null) {
        keysPendingDeletion += deletedKeyRecord.getValue();
      }
      if (deletedDirRecord != null) {
        deletedDirs += deletedDirRecord.getValue();
      }
    } catch (IOException e) {
      LOG.error("Error retrieving global stats from RocksDB", e);
      // Set defaults if error occurred
      builder.setVolumes(0L);
      builder.setBuckets(0L);
    }

    builder.setKeys(totalKeys);
    builder.setKeysPendingDeletion(keysPendingDeletion);
    builder.setDeletedDirs(deletedDirs);

    // Subtract deleted containers from total containers.
    containerStateCounts.setTotalContainerCount(
        this.containerManager.getContainers().size() -
            containerStateCounts.getDeletedContainersCount());
    ClusterStateResponse response = builder
        .setStorageReport(storageReport)
        .setPipelines(pipelines)
        .setContainers(containerStateCounts.getTotalContainerCount())
        .setMissingContainers(containerStateCounts.getMissingContainerCount())
        .setTotalDatanodes(nodeManager.getAllNodeCount())
        .setHealthyDatanodes(healthyDataNodes)
        .setOpenContainers(containerStateCounts.getOpenContainersCount())
        .setDeletedContainers(containerStateCounts.getDeletedContainersCount())
        .setScmServiceId(ozoneConfiguration.get(OZONE_SCM_SERVICE_IDS_KEY))
        .setOmServiceId(ozoneConfiguration.get(OZONE_OM_SERVICE_IDS_KEY))
        .build();
    return Response.ok(response).build();
  }
}
