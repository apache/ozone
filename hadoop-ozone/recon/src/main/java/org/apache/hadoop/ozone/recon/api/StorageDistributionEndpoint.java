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
import java.util.Objects;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.DatanodeStorageReport;
import org.apache.hadoop.ozone.recon.api.types.GlobalNamespaceReport;
import org.apache.hadoop.ozone.recon.api.types.GlobalStorageReport;
import org.apache.hadoop.ozone.recon.api.types.StorageCapacityDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.UsedSpaceBreakDown;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.spi.ReconGlobalStatsManager;
import org.apache.hadoop.ozone.recon.tasks.GlobalStatsValue;
import org.apache.hadoop.ozone.recon.tasks.OmTableInsightTask;
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
  private final NSSummaryEndpoint nsSummaryEndpoint;
  private static final Logger LOG = LoggerFactory.getLogger(StorageDistributionEndpoint.class);
  private final ReconGlobalStatsManager reconGlobalStatsManager;
  private final ReconGlobalMetricsService reconGlobalMetricsService;

  @Inject
  public StorageDistributionEndpoint(OzoneStorageContainerManager reconSCM,
                                     NSSummaryEndpoint nsSummaryEndpoint,
                                     ReconGlobalStatsManager reconGlobalStatsManager,
                                     ReconGlobalMetricsService reconGlobalMetricsService) {
    this.nodeManager = (ReconNodeManager) reconSCM.getScmNodeManager();
    this.nsSummaryEndpoint = nsSummaryEndpoint;
    this.reconGlobalStatsManager = reconGlobalStatsManager;
    this.reconGlobalMetricsService = reconGlobalMetricsService;
  }

  @GET
  public Response getStorageDistribution() {
    try {
      List<DatanodeStorageReport> nodeStorageReports = collectDatanodeReports();
      GlobalStorageReport globalStorageReport = calculateGlobalStorageReport();

      Map<String, Long> namespaceMetrics = new HashMap<>();
      try {
        namespaceMetrics = calculateNamespaceMetrics();
      } catch (Exception e) {
        LOG.error("Error calculating namespace metrics", e);
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
      LOG.error("Error getting storage distribution", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
              .entity("Error retrieving storage distribution: " + e.getMessage())
              .build();
    }
  }

  private GlobalStorageReport calculateGlobalStorageReport() {
    try {
      SCMNodeStat stats = nodeManager.getStats();
      if (stats == null) {
        LOG.warn("Node manager stats are null, returning default values");
        return new GlobalStorageReport(0L, 0L, 0L);
      }

      long scmUsed = stats.getScmUsed() != null ? stats.getScmUsed().get() : 0L;
      long remaining = stats.getRemaining() != null ? stats.getRemaining().get() : 0L;
      long capacity = stats.getCapacity() != null ? stats.getCapacity().get() : 0L;

      return new GlobalStorageReport(scmUsed, remaining, capacity);
    } catch (Exception e) {
      LOG.error("Error calculating global storage report", e);
      return new GlobalStorageReport(0L, 0L, 0L);
    }
  }

  private Map<String, Long> calculateNamespaceMetrics() throws IOException {
    Map<String, Long> metrics = new HashMap<>();
    Map<String, Long> totalPendingAtOmSide = reconGlobalMetricsService.calculatePendingSizes();
    long totalOpenKeySize = calculateOpenKeySizes();
    long totalCommittedSize = calculateCommittedSize();
    long pendingDirectorySize = totalPendingAtOmSide.getOrDefault("pendingDirectorySize", 0L);
    long pendingKeySize = totalPendingAtOmSide.getOrDefault("pendingKeySize", 0L);
    long totalUsedNamespace = pendingDirectorySize + pendingKeySize + totalOpenKeySize + totalCommittedSize;

    long totalKeys = 0L;
    // Keys from OBJECT_STORE buckets.
    GlobalStatsValue keyRecord = reconGlobalStatsManager.getGlobalStatsValue(
            OmTableInsightTask.getTableCountKeyFromTable(KEY_TABLE));
    // Keys from FILE_SYSTEM_OPTIMIZED buckets
    GlobalStatsValue fileRecord = reconGlobalStatsManager.getGlobalStatsValue(
            OmTableInsightTask.getTableCountKeyFromTable(FILE_TABLE));
    if (keyRecord != null) {
      totalKeys += keyRecord.getValue();
    }
    if (fileRecord != null) {
      totalKeys += fileRecord.getValue();
    }

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

    // Safely get values from namespaceMetrics with null checks
    Long totalUsedNamespace = namespaceMetrics.get("totalUsedNamespace");
    Long totalOpenKeySize = namespaceMetrics.get("totalOpenKeySize");
    Long totalCommittedSize = namespaceMetrics.get("totalCommittedSize");
    Long totalKeys = namespaceMetrics.get("totalKeys");
    Long totalContainerPreAllocated = nodeStorageReports.stream()
        .map(DatanodeStorageReport::getCommitted)
        .reduce(0L, Long::sum);

    return StorageCapacityDistributionResponse.newBuilder()
            .setDataNodeUsage(nodeStorageReports)
            .setGlobalStorage(storageMetrics)
            .setGlobalNamespace(new GlobalNamespaceReport(
                    totalUsedNamespace != null ? totalUsedNamespace : 0L,
                    totalKeys != null ? totalKeys : 0L))
            .setUsedSpaceBreakDown(new UsedSpaceBreakDown(
                    totalOpenKeySize != null ? totalOpenKeySize : 0L,
                    totalCommittedSize != null ? totalCommittedSize : 0L, totalContainerPreAllocated))
            .build();
  }

  private List<DatanodeStorageReport> collectDatanodeReports() {
    return nodeManager.getAllNodes().stream()
        .map(this::getStorageReport)
        .filter(Objects::nonNull) // Filter out null reports
        .collect(Collectors.toList());
  }

  private long calculateOpenKeySizes() {
    Map<String, Long> openKeySummary = reconGlobalMetricsService.getOpenKeySummary();
    Map<String, Long> openKeyMPUSummary = reconGlobalMetricsService.getMPUKeySummary();
    long openKeyDataSize = openKeySummary.getOrDefault("totalReplicatedDataSize", 0L);
    long totalMPUKeySize = openKeyMPUSummary.getOrDefault("totalReplicatedDataSize", 0L);
    return openKeyDataSize + totalMPUKeySize;
  }

  private long calculateCommittedSize() {
    try {
      Response rootResponse = nsSummaryEndpoint.getDiskUsage("/", false, true, false);
      if (rootResponse.getStatus() != Response.Status.OK.getStatusCode()) {
        LOG.warn("Failed to get disk usage, status: {}", rootResponse.getStatus());
        return 0L;
      }
      DUResponse duRootRes = (DUResponse) rootResponse.getEntity();
      return duRootRes != null ? duRootRes.getSizeWithReplica() : 0L;
    } catch (IOException e) {
      LOG.error("IOException while calculating committed size", e);
      return 0L;
    }
  }

  private DatanodeStorageReport getStorageReport(DatanodeDetails datanode) {
    try {
      SCMNodeMetric nodeMetric = nodeManager.getNodeStat(datanode);
      if (nodeMetric == null) {
        LOG.warn("Node statistics not available for datanode: {}", datanode);
        return null; // Return null for unavailable nodes
      }

      SCMNodeStat nodeStat = nodeMetric.get();
      if (nodeStat == null) {
        LOG.warn("Node stat is null for datanode: {}", datanode);
        return null; // Return null for unavailable stats
      }

      long capacity = nodeStat.getCapacity() != null ? nodeStat.getCapacity().get() : 0L;
      long used = nodeStat.getScmUsed() != null ? nodeStat.getScmUsed().get() : 0L;
      long remaining = nodeStat.getRemaining() != null ? nodeStat.getRemaining().get() : 0L;
      long committed = nodeStat.getCommitted() != null ? nodeStat.getCommitted().get() : 0L;
      long minFreeSpace  = nodeStat.getFreeSpaceToSpare() != null ? nodeStat.getFreeSpaceToSpare().get() : 0L;
      long reservedSpace = nodeStat.getReserved() != null ? nodeStat.getReserved().get() : 0L;
      SpaceUsageSource.Fixed fsUsage = nodeManager.getTotalFilesystemUsage(datanode);
      DatanodeStorageReport.Builder builder = DatanodeStorageReport.newBuilder()
          .setCapacity(capacity)
          .setUsed(used)
          .setRemaining(remaining)
          .setCommitted(committed)
          .setMinimumFreeSpace(minFreeSpace)
          .setReserved(reservedSpace)
          .setDatanodeUuid(datanode.getUuidString())
          .setHostName(datanode.getHostName());
      if (fsUsage != null) {
        builder.setFilesystemCapacity(fsUsage.getCapacity())
            .setFilesystemAvailable(fsUsage.getAvailable())
            .setFilesystemUsed(fsUsage.getUsedSpace());
      }
      return builder.build();
    } catch (Exception e) {
      LOG.error("Error getting storage report for datanode: {}", datanode, e);
      return null; // Return null on any error
    }
  }
}
