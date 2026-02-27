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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.DataNodeMetricsServiceResponse;
import org.apache.hadoop.ozone.recon.api.types.DatanodePendingDeletionMetrics;
import org.apache.hadoop.ozone.recon.api.types.DatanodeStorageReport;
import org.apache.hadoop.ozone.recon.api.types.GlobalNamespaceReport;
import org.apache.hadoop.ozone.recon.api.types.GlobalStorageReport;
import org.apache.hadoop.ozone.recon.api.types.OpenKeyBytesInfo;
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
  private final DataNodeMetricsService dataNodeMetricsService;

  @Inject
  public StorageDistributionEndpoint(OzoneStorageContainerManager reconSCM,
                                     NSSummaryEndpoint nsSummaryEndpoint,
                                     ReconGlobalStatsManager reconGlobalStatsManager,
                                     ReconGlobalMetricsService reconGlobalMetricsService,
                                     DataNodeMetricsService dataNodeMetricsService) {
    this.nodeManager = (ReconNodeManager) reconSCM.getScmNodeManager();
    this.nsSummaryEndpoint = nsSummaryEndpoint;
    this.reconGlobalStatsManager = reconGlobalStatsManager;
    this.reconGlobalMetricsService = reconGlobalMetricsService;
    this.dataNodeMetricsService = dataNodeMetricsService;
  }

  @GET
  public Response getStorageDistribution() {
    try {
      List<DatanodeStorageReport> nodeStorageReports = collectDatanodeReports();
      GlobalStorageReport globalStorageReport = calculateGlobalStorageReport();
      OpenKeyBytesInfo totalOpenKeySize;
      try {
        totalOpenKeySize = calculateOpenKeySizes();
      } catch (Exception e) {
        LOG.error("Error calculating open key sizes", e);
        totalOpenKeySize = new OpenKeyBytesInfo(0L, 0L);
      }

      Map<String, Long> namespaceMetrics = new HashMap<>();
      try {
        namespaceMetrics = calculateNamespaceMetrics(totalOpenKeySize);
      } catch (Exception e) {
        LOG.error("Error calculating namespace metrics", e);
        // Initialize with default values
        namespaceMetrics.put("totalUsedNamespace", 0L);
        namespaceMetrics.put("totalCommittedSize", 0L);
        namespaceMetrics.put("pendingDirectorySize", 0L);
        namespaceMetrics.put("pendingKeySize", 0L);
        namespaceMetrics.put("totalKeys", 0L);
      }

      StorageCapacityDistributionResponse response = buildStorageDistributionResponse(
              nodeStorageReports, globalStorageReport, namespaceMetrics, totalOpenKeySize);
      return Response.ok(response).build();
    } catch (Exception e) {
      LOG.error("Error getting storage distribution", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
              .entity("Error retrieving storage distribution: " + e.getMessage())
              .build();
    }
  }

  /**
   * Downloads the distribution of data node metrics in a CSV file format.
   * This method collects metrics related to pending deletions and various storage statistics
   * for each data node. If the metrics collection is still in progress, it returns an
   * intermediate response. If the collection is complete and metrics data are available,
   * it generates and streams a CSV file containing detailed statistics.
   *
   * The CSV includes the following headers: HostName, Datanode UUID, Filesystem Capacity,
   * Filesystem Used Space, Filesystem Remaining Space, Ozone Capacity, Ozone Used Space,
   * Ozone Remaining Space, PreAllocated Container Space, Reserved Space, Minimum Free
   * Space, and Pending Block Size.
   *
   * @return A Response object. Depending on the state of metrics collection, this can be:
   *         - An HTTP 202 (Accepted) response with a status and metrics data if the
   *           collection is not yet complete.
   *         - An HTTP 500 (Internal Server Error) if the metrics data is missing despite
   *           the collection status being marked as finished.
   *         - An HTTP 202 (Accepted) response containing a CSV file of data node metrics
   *           if the collection is complete and valid metrics data are available.
   */
  @GET
  @Path("/download")
  public Response downloadDataNodeStorageDistribution() {

    DataNodeMetricsServiceResponse metricsResponse =
        dataNodeMetricsService.getCollectedMetrics(null);

    if (metricsResponse.getStatus() != DataNodeMetricsService.MetricCollectionStatus.FINISHED) {
      return Response.status(Response.Status.ACCEPTED)
          .entity(metricsResponse)
          .type(MediaType.APPLICATION_JSON)
          .build();
    }

    List<DatanodePendingDeletionMetrics> pendingDeletionMetrics =
        metricsResponse.getPendingDeletionPerDataNode();

    if (pendingDeletionMetrics == null) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Metrics data is missing despite FINISHED status.")
          .type(MediaType.TEXT_PLAIN)
          .build();
    }

    Map<String, DatanodeStorageReport> reportByUuid =
        collectDatanodeReports().stream()
            .collect(Collectors.toMap(
                DatanodeStorageReport::getDatanodeUuid,
                Function.identity()));

    List<DataNodeStoragePendingDeletionView> data = pendingDeletionMetrics.stream()
        .map(metric -> {
          return new DataNodeStoragePendingDeletionView(metric, reportByUuid.get(metric.getDatanodeUuid()));
        })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    List<String> headers = Arrays.asList(
        "HostName",
        "Datanode UUID",
        "Filesystem Capacity",
        "Filesystem Used Space",
        "Filesystem Remaining Space",
        "Ozone Capacity",
        "Ozone Used Space",
        "Ozone Remaining Space",
        "PreAllocated Container Space",
        "Reserved Space",
        "Minimum Free Space",
        "Pending Block Size"
    );

    List<Function<DataNodeStoragePendingDeletionView, Object>> columns =
        Arrays.asList(
            v -> v.getMetric() != null ? v.getMetric().getHostName() : "Unknown",
            v -> v.getMetric() != null ? v.getMetric().getDatanodeUuid() : "Unknown",
            v -> v.getReport() != null ? v.getReport().getFilesystemCapacity() : -1,
            v -> v.getReport() != null ? v.getReport().getFilesystemUsed() : -1,
            v -> v.getReport() != null ? v.getReport().getFilesystemAvailable() : -1,
            v -> v.getReport() != null ? v.getReport().getCapacity() : -1,
            v -> v.getReport() != null ? v.getReport().getUsed() : -1,
            v -> v.getReport() != null ? v.getReport().getRemaining() : -1,
            v -> v.getReport() != null ? v.getReport().getCommitted() : -1,
            v -> v.getReport() != null ? v.getReport().getReserved() : -1,
            v -> v.getReport() != null ? v.getReport().getMinimumFreeSpace() : -1,
            v -> v.getReport() != null ? v.getMetric().getPendingBlockSize() : -1
        );

    return ReconUtils.downloadCsv("datanode_storage_and_pending_deletion_stats.csv", headers, data, columns);
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

  private Map<String, Long> calculateNamespaceMetrics(OpenKeyBytesInfo totalOpenKeySize) throws IOException {
    Map<String, Long> metrics = new HashMap<>();
    Map<String, Long> totalPendingAtOmSide = reconGlobalMetricsService.calculatePendingSizes();
    long totalCommittedSize = calculateCommittedSize();
    long pendingDirectorySize = totalPendingAtOmSide.getOrDefault("pendingDirectorySize", 0L);
    long pendingKeySize = totalPendingAtOmSide.getOrDefault("pendingKeySize", 0L);
    long totalUsedNamespace = pendingDirectorySize + pendingKeySize +
        totalOpenKeySize.getTotalOpenKeyBytes() + totalCommittedSize;

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
    metrics.put("totalCommittedSize", totalCommittedSize);
    metrics.put("totalUsedNamespace", totalUsedNamespace);
    metrics.put("totalKeys", totalKeys);
    return metrics;
  }

  private StorageCapacityDistributionResponse buildStorageDistributionResponse(
          List<DatanodeStorageReport> nodeStorageReports,
          GlobalStorageReport storageMetrics,
          Map<String, Long> namespaceMetrics,
          OpenKeyBytesInfo totalOpenKeySize) {

    // Safely get values from namespaceMetrics with null checks
    Long totalUsedNamespace = namespaceMetrics.get("totalUsedNamespace");
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
                    totalOpenKeySize, totalCommittedSize != null ? totalCommittedSize : 0L, totalContainerPreAllocated))
            .build();
  }

  public List<DatanodeStorageReport> collectDatanodeReports() {
    return nodeManager.getAllNodes().stream()
        .map(this::getStorageReport)
        .filter(Objects::nonNull) // Filter out null reports
        .collect(Collectors.toList());
  }

  private OpenKeyBytesInfo calculateOpenKeySizes() {
    Map<String, Long> openKeySummary = reconGlobalMetricsService.getOpenKeySummary();
    Map<String, Long> openKeyMPUSummary = reconGlobalMetricsService.getMPUKeySummary();
    long openKeyDataSize = openKeySummary.getOrDefault("totalReplicatedDataSize", 0L);
    long totalMPUKeySize = openKeyMPUSummary.getOrDefault("totalReplicatedDataSize", 0L);
    return new OpenKeyBytesInfo(openKeyDataSize, totalMPUKeySize);
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

  /**
   * Represents a view that combines pending deletion metrics and storage report data
   * for a specific Datanode. This view is used to encapsulate both metric and storage
   * details for understanding the state of a datanode in terms of storage and pending deletions.
   */
  private static class DataNodeStoragePendingDeletionView {
    private final DatanodePendingDeletionMetrics metric;
    private final DatanodeStorageReport report;

    DataNodeStoragePendingDeletionView(DatanodePendingDeletionMetrics metric, DatanodeStorageReport report) {
      this.metric = metric;
      this.report = report;
    }

    DatanodePendingDeletionMetrics getMetric() {
      return metric;
    }

    DatanodeStorageReport getReport() {
      return report;
    }
  }
}
