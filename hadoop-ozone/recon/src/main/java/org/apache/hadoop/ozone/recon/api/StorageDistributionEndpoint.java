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

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.DatanodeStorageReport;
import org.apache.hadoop.ozone.recon.api.types.DeletionPendingBytesByComponent;
import org.apache.hadoop.ozone.recon.api.types.GlobalNamespaceReport;
import org.apache.hadoop.ozone.recon.api.types.GlobalStorageReport;
import org.apache.hadoop.ozone.recon.api.types.KeyInsightInfoResponse;
import org.apache.hadoop.ozone.recon.api.types.StorageCapacityDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.UsedSpaceBreakDown;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.tasks.OmTableInsightTask;
import org.apache.ozone.recon.schema.generated.tables.daos.GlobalStatsDao;
import org.apache.ozone.recon.schema.generated.tables.pojos.GlobalStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This endpoint handles requests related to storage distribution across
 * different datanodes in a Recon instance. It provides detailed reports
 * on storage capacity, utilization, and associated metrics.
 * <p>
 * The data is aggregated from multiple sources, including node manager
 * statistics, and is used to construct responses with information
 * about global storage and namespace usage, storage usage breakdown,
 * and deletion operations in progress.
 * <p>
 * An instance of {@link ReconNodeManager} is used to fetch detailed
 * node-specific statistics required for generating the report.
 */
@Path("/storageDistribution")
@Produces("application/json")
@AdminOnly
public class StorageDistributionEndpoint {
  private final ReconNodeManager nodeManager;
  private final OMDBInsightEndpoint omdbInsightEndpoint;
  private final NSSummaryEndpoint nsSummaryEndpoint;
  private final StorageContainerLocationProtocol scmClient;
  private static Logger log = LoggerFactory.getLogger(StorageDistributionEndpoint.class);
  private Map<DatanodeDetails, Long> blockDeletionMetricsMap = new ConcurrentHashMap<>();
  private GlobalStatsDao globalStatsDao;

  @Inject
  public StorageDistributionEndpoint(OzoneStorageContainerManager reconSCM,
                                     OMDBInsightEndpoint omDbInsightEndpoint,
                                     NSSummaryEndpoint nsSummaryEndpoint,
                                     GlobalStatsDao globalStatsDao,
                                     StorageContainerLocationProtocol scmClient) {
    this.nodeManager = (ReconNodeManager) reconSCM.getScmNodeManager();
    this.omdbInsightEndpoint = omDbInsightEndpoint;
    this.nsSummaryEndpoint = nsSummaryEndpoint;
    this.scmClient = scmClient;
    this.globalStatsDao = globalStatsDao;
  }

  @GET
  public Response getStorageDistribution() {
    try {
      initializeBlockDeletionMetricsMap();
      List<DatanodeStorageReport> nodeStorageReports = collectDatanodeReports();
      GlobalStorageReport globalStorageReport = calculateGlobalStorageReport();

      Map<String, Long> namespaceMetrics = new HashMap<>();
      try {
        namespaceMetrics = calculateNamespaceMetrics();
      } catch (Exception e) {
        log.error("Error calculating namespace metrics", e);
        // Initialize with default values
        namespaceMetrics.put("totalUsedNamespace", 0L);
        namespaceMetrics.put("totalOpenKeySize", 0L);
        namespaceMetrics.put("totalCommittedSize", 0L);
        namespaceMetrics.put("pendingDirectorySize", 0L);
        namespaceMetrics.put("pendingKeySize", 0L);
        namespaceMetrics.put("totalKeys", 0L);
      }

      StorageCapacityDistributionResponse response = buildStorageDistributionResponse(
              nodeStorageReports, globalStorageReport, namespaceMetrics);
      return Response.ok(response).build();
    } catch (Exception e) {
      log.error("Error getting storage distribution", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
              .entity("Error retrieving storage distribution: " + e.getMessage())
              .build();
    }
  }

  private GlobalStorageReport calculateGlobalStorageReport() {
    try {
      SCMNodeStat stats = nodeManager.getStats();
      if (stats == null) {
        log.warn("Node manager stats are null, returning default values");
        return new GlobalStorageReport(0L, 0L, 0L);
      }

      long scmUsed = stats.getScmUsed() != null ? stats.getScmUsed().get() : 0L;
      long remaining = stats.getRemaining() != null ? stats.getRemaining().get() : 0L;
      long capacity = stats.getCapacity() != null ? stats.getCapacity().get() : 0L;

      return new GlobalStorageReport(scmUsed, remaining, capacity);
    } catch (Exception e) {
      log.error("Error calculating global storage report", e);
      return new GlobalStorageReport(0L, 0L, 0L);
    }
  }

  private Map<String, Long> calculateNamespaceMetrics() {
    Map<String, Long> metrics = new HashMap<>();
    Map<String, Long> totalPendingAtOmSide = calculatePendingSizes();
    long totalOpenKeySize = calculateOpenKeySizes();
    long totalCommittedSize = calculateCommittedSize();
    long pendingDirectorySize = totalPendingAtOmSide.getOrDefault("pendingDirectorySize", 0L);
    long pendingKeySize = totalPendingAtOmSide.getOrDefault("pendingKeySize", 0L);
    long totalUsedNamespace = pendingDirectorySize + pendingKeySize + totalOpenKeySize + totalCommittedSize;

    long totalKeys = 0L;
    // Keys from OBJECT_STORE buckets.
    GlobalStats keyRecord = globalStatsDao.findById(
            OmTableInsightTask.getTableCountKeyFromTable(KEY_TABLE));
    // Keys from FILE_SYSTEM_OPTIMIZED buckets
    GlobalStats fileRecord = globalStatsDao.findById(
            OmTableInsightTask.getTableCountKeyFromTable(FILE_TABLE));
    if (keyRecord != null) {
      totalKeys += keyRecord.getValue();
    }
    if (fileRecord != null) {
      totalKeys += fileRecord.getValue();
    }

    metrics.put("pendingDirectorySize", pendingDirectorySize);
    metrics.put("pendingKeySize", pendingKeySize);
    metrics.put("totalOpenKeySize", totalOpenKeySize);
    metrics.put("totalCommittedSize", totalCommittedSize);
    metrics.put("totalUsedNamespace", totalUsedNamespace);
    metrics.put("totalKeys", totalKeys);
    return metrics;
  }

  private StorageCapacityDistributionResponse buildStorageDistributionResponse(
          List<DatanodeStorageReport> nodeStorageReports,
          GlobalStorageReport storageMetrics,
          Map<String, Long> namespaceMetrics) {
    DeletedBlocksTransactionSummary scmSummary = null;
    try {
      scmSummary = scmClient.getDeletedBlockSummary();
    } catch (IOException e) {
      log.error("Failed to get deleted block summary from SCM", e);
    }

    long totalPendingAtDnSide = 0L;
    try {
      totalPendingAtDnSide = blockDeletionMetricsMap.values().stream().reduce(0L, Long::sum);
    } catch (Exception e) {
      log.error("Error calculating pending deletion metrics", e);
    }

    DeletionPendingBytesByComponent deletionPendingBytesByStage =
            createDeletionPendingBytesByStage(
                    namespaceMetrics.getOrDefault("pendingDirectorySize", 0L),
                    namespaceMetrics.getOrDefault("pendingKeySize", 0L),
                    scmSummary != null ? scmSummary.getTotalBlockReplicatedSize() : 0L,
                    totalPendingAtDnSide);

    // Safely get values from namespaceMetrics with null checks
    Long totalUsedNamespace = namespaceMetrics.get("totalUsedNamespace");
    Long totalOpenKeySize = namespaceMetrics.get("totalOpenKeySize");
    Long totalCommittedSize = namespaceMetrics.get("totalCommittedSize");
    Long totalKeys = namespaceMetrics.get("totalKeys");
    Long totalContainerPreAllocated = nodeStorageReports.stream()
        .map(report -> report.getCommitted())
        .reduce(0L, Long::sum);

    return StorageCapacityDistributionResponse.newBuilder()
            .setDataNodeUsage(nodeStorageReports)
            .setGlobalStorage(storageMetrics)
            .setGlobalNamespace(new GlobalNamespaceReport(
                    totalUsedNamespace != null ? totalUsedNamespace : 0L,
                    totalKeys != null ? totalKeys : 0L))
            .setUsedSpaceBreakDown(new UsedSpaceBreakDown(
                    totalOpenKeySize != null ? totalOpenKeySize : 0L,
                    totalCommittedSize != null ? totalCommittedSize : 0L,
                    totalContainerPreAllocated != null ? totalContainerPreAllocated : 0L,
                    deletionPendingBytesByStage))
            .build();
  }

  private List<DatanodeStorageReport> collectDatanodeReports() {
    return nodeManager.getAllNodes().stream()
        .map(this::getStorageReport)
        .filter(report -> report != null) // Filter out null reports
        .collect(Collectors.toList());
  }

  private Map<String, Long> calculatePendingSizes() {
    Map<String, Long> result = new HashMap<>();
    KeyInsightInfoResponse response = (KeyInsightInfoResponse)
        omdbInsightEndpoint.getDeletedDirInfo(-1, "").getEntity();

    Map<String, Long> pendingKeySize = new HashMap<>();
    omdbInsightEndpoint.createKeysSummaryForDeletedKey(pendingKeySize);
    result.put("pendingDirectorySize", response.getReplicatedDataSize());
    result.put("pendingKeySize", pendingKeySize.getOrDefault("totalReplicatedDataSize", 0L));
    return result;
  }

  private long calculateOpenKeySizes() {
    Map<String, Long> openKeySummary = new HashMap<>();
    omdbInsightEndpoint.createKeysSummaryForOpenKey(openKeySummary);
    Map<String, Long> openKeyMPUSummary = new HashMap<>();
    omdbInsightEndpoint.createKeysSummaryForOpenMPUKey(openKeyMPUSummary);
    long openKeyDataSize = openKeySummary.getOrDefault("totalReplicatedDataSize", 0L);
    long totalMPUKeySize = openKeyMPUSummary.getOrDefault("totalReplicatedDataSize", 0L);
    return openKeyDataSize + totalMPUKeySize;
  }

  private long calculateCommittedSize() {
    try {
      Response rootResponse = nsSummaryEndpoint.getDiskUsage("/", false, true, false);
      if (rootResponse.getStatus() != Response.Status.OK.getStatusCode()) {
        log.warn("Failed to get disk usage, status: {}", rootResponse.getStatus());
        return 0L;
      }
      DUResponse duRootRes = (DUResponse) rootResponse.getEntity();
      return duRootRes != null ? duRootRes.getSizeWithReplica() : 0L;
    } catch (IOException e) {
      log.error("IOException while calculating committed size", e);
      return 0L;
    }
  }

  private DeletionPendingBytesByComponent createDeletionPendingBytesByStage(long pendingDirectorySize,
                                                                        long pendingKeySize,
                                                                        long totalPendingAtScmSide,
                                                                        long totalPendingAtDnSide) {
    long totalPending = pendingDirectorySize + pendingKeySize + totalPendingAtScmSide + totalPendingAtDnSide;
    Map<String, Map<String, Long>> stageItems = new HashMap<>();
    Map<String, Long> omMap = new HashMap<>();
    omMap.put("totalBytes", pendingDirectorySize + pendingKeySize);
    omMap.put("pendingDirectoryBytes", pendingDirectorySize);
    omMap.put("pendingKeyBytes", pendingKeySize);
    Map<String, Long> scmMap = new HashMap<>();
    scmMap.put("pendingBytes", totalPendingAtScmSide);
    Map<String, Long> dnMap = new HashMap<>();
    dnMap.put("pendingBytes", totalPendingAtDnSide);
    stageItems.put("OM", omMap);
    stageItems.put("SCM", scmMap);
    stageItems.put("DN", dnMap);
    return new DeletionPendingBytesByComponent(totalPending, stageItems);
  }

  private void initializeBlockDeletionMetricsMap() {
    nodeManager.getNodeStats().keySet().parallelStream().forEach(nodeId -> {
      try {
        long dnPending = ReconUtils.getMetricsFromDatanode(nodeId,
                "HddsDatanode",
                "BlockDeletingService",
                "TotalPendingBlockBytes");
        blockDeletionMetricsMap.put(nodeId, dnPending);
      } catch (IOException e) {
        blockDeletionMetricsMap.put(nodeId, -1L);
      }
    });
  }

  private DatanodeStorageReport getStorageReport(DatanodeDetails datanode) {
    try {
      SCMNodeMetric nodeMetric = nodeManager.getNodeStat(datanode);
      if (nodeMetric == null) {
        log.warn("Node statistics not available for datanode: {}", datanode);
        return null; // Return null for unavailable nodes
      }

      SCMNodeStat nodeStat = nodeMetric.get();
      if (nodeStat == null) {
        log.warn("Node stat is null for datanode: {}", datanode);
        return null; // Return null for unavailable stats
      }

      long capacity = nodeStat.getCapacity() != null ? nodeStat.getCapacity().get() : 0L;
      long used = nodeStat.getScmUsed() != null ? nodeStat.getScmUsed().get() : 0L;
      long remaining = nodeStat.getRemaining() != null ? nodeStat.getRemaining().get() : 0L;
      long committed = nodeStat.getCommitted() != null ? nodeStat.getCommitted().get() : 0L;
      long pendingDeletion = blockDeletionMetricsMap.getOrDefault(datanode, 0L);
      long minFreeSpace  = nodeStat.getFreeSpaceToSpare() != null ? nodeStat.getFreeSpaceToSpare().get() : 0L;
      if (pendingDeletion < 0) {
        log.warn("Block deletion metrics unavailable for datanode: {}", datanode);
        pendingDeletion = 0L;
      }
      DatanodeStorageReport storageReport = DatanodeStorageReport.newBuilder()
          .setCapacity(capacity)
          .setUsed(used)
          .setRemaining(remaining)
          .setCommitted(committed)
          .setPendingDeletion(pendingDeletion)
          .setMinimumFreeSpace(minFreeSpace)
          .setDatanodeUuid(datanode.getUuidString())
          .setHostName(datanode.getHostName())
          .build();
      return storageReport;
    } catch (Exception e) {
      log.error("Error getting storage report for datanode: {}", datanode, e);
      return null; // Return null on any error
    }
  }
}
