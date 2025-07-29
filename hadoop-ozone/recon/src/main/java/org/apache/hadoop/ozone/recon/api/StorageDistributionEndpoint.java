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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.api.types.CommittedBytes;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.DatanodeStorageReport;
import org.apache.hadoop.ozone.recon.api.types.DeletionPendingBytesByStage;
import org.apache.hadoop.ozone.recon.api.types.GlobalNamespaceReport;
import org.apache.hadoop.ozone.recon.api.types.GlobalStorageReport;
import org.apache.hadoop.ozone.recon.api.types.StorageCapacityDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.UsedSpaceBreakDown;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;

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
@Path("/storagedistribution")
@Produces("application/json")
public class StorageDistributionEndpoint {
  private final ReconNodeManager nodeManager;
  private final OMDBInsightEndpoint omdbInsightEndpoint;
  private final NSSummaryEndpoint nsSummaryEndpoint;
  private final StorageContainerLocationProtocol scmClient;

  @Inject
  public StorageDistributionEndpoint(OzoneStorageContainerManager reconSCM,
                                     OMDBInsightEndpoint omDbInsightEndpoint,
                                     NSSummaryEndpoint nsSummaryEndpoint,
                                     StorageContainerLocationProtocol scmClient) {
    this.nodeManager = (ReconNodeManager) reconSCM.getScmNodeManager();
    this.omdbInsightEndpoint = omDbInsightEndpoint;
    this.nsSummaryEndpoint = nsSummaryEndpoint;
    this.scmClient = scmClient;
  }

  @GET
  public Response getStorageDistribution() {
    List<DatanodeStorageReport> nodeStorageReports = collectDatanodeReports();
    GlobalStorageReport globalStorageReport = calculateGlobalStorageReport();
    Map<String, Long> namespaceMetrics = calculateNamespaceMetrics();
    StorageCapacityDistributionResponse response = buildStorageDistributionResponse(
        nodeStorageReports, globalStorageReport, namespaceMetrics);
    return Response.ok(response).build();
  }

  private GlobalStorageReport calculateGlobalStorageReport() {
    SCMNodeStat stats = nodeManager.getStats();
    return new GlobalStorageReport(
        stats.getScmUsed().get(),
        stats.getRemaining().get(),
        stats.getCapacity().get()
    );
  }

  private Map<String, Long> calculateNamespaceMetrics() {
    Map<String, Long> metrics = new HashMap<>();
    long totalPendingAtOmSide = calculatePendingSizes();
    long totalOpenKeySize = calculateOpenKeySizes();
    long totalCommittedSize = calculateCommittedSize();
    long totalUsedNamespace = totalPendingAtOmSide + totalOpenKeySize + totalCommittedSize;
    metrics.put("totalPendingAtOmSide", totalPendingAtOmSide);
    metrics.put("totalOpenKeySize", totalOpenKeySize);
    metrics.put("totalCommittedSize", totalCommittedSize);
    metrics.put("totalUsedNamespace", totalUsedNamespace);
    return metrics;
  }

  private StorageCapacityDistributionResponse buildStorageDistributionResponse(
      List<DatanodeStorageReport> nodeStorageReports,
      GlobalStorageReport storageMetrics,
      Map<String, Long> namespaceMetrics) {
    DeletedBlocksTransactionSummary scmSummary;
    try {
      scmSummary = scmClient.getDeletedBlockSummary();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    long totalPendingAtDnSide = nodeStorageReports.stream().mapToLong(DatanodeStorageReport::getPendingDeletions).sum();

    DeletionPendingBytesByStage deletionPendingBytesByStage =
        createDeletionPendingBytesByStage(namespaceMetrics.get("totalPendingAtOmSide"),
            scmSummary.getTotalBlockReplicatedSize(),
            totalPendingAtDnSide);
    return StorageCapacityDistributionResponse.newBuilder()
        .setDataNodeUsage(nodeStorageReports)
        .setGlobalStorage(storageMetrics)
        .setGlobalNamespace(new GlobalNamespaceReport(namespaceMetrics.get("totalUsedNamespace"), 0))
        .setUsedSpaceBreakDown(new UsedSpaceBreakDown(
            namespaceMetrics.get("totalOpenKeySize"),
            new CommittedBytes(namespaceMetrics.get("totalCommittedSize"), 0, 0, 0),
            deletionPendingBytesByStage))
        .build();
  }

  private List<DatanodeStorageReport> collectDatanodeReports() {
    return nodeManager.getAllNodes().stream()
        .map(this::getStorageReport)
        .collect(Collectors.toList());
  }

  private long calculatePendingSizes() {
    Map<String, Long> pendingDeletedDirSizes = new HashMap<>();
    omdbInsightEndpoint.calculateTotalPendingDeletedDirSizes(pendingDeletedDirSizes);
    Map<String, Long> pendingKeySize = new HashMap<>();
    omdbInsightEndpoint.createKeysSummaryForDeletedKey(pendingKeySize);
    long totalPendingDeletedDirSize = pendingDeletedDirSizes.getOrDefault("totalReplicatedDataSize", 0L);
    long totalPendingKeySize = pendingKeySize.getOrDefault("totalReplicatedDataSize", 0L);
    return totalPendingDeletedDirSize + totalPendingKeySize;
  }

  private long calculateOpenKeySizes() {
    Map<String, Long> openKeySummary = new HashMap<>();
    omdbInsightEndpoint.createKeysSummaryForOpenKey(openKeySummary);
    omdbInsightEndpoint.createKeysSummaryForOpenMPUKey(openKeySummary);
    Map<String, Long> pendingKeySize = new HashMap<>();
    omdbInsightEndpoint.createKeysSummaryForDeletedKey(pendingKeySize);
    long openKeyDataSize = openKeySummary.getOrDefault("totalReplicatedDataSize", 0L);
    long totalMPUKeySize = pendingKeySize.getOrDefault("totalDataSize", 0L);
    return openKeyDataSize + totalMPUKeySize;
  }

  private long calculateCommittedSize() {
    try {
      Response rootResponse = nsSummaryEndpoint.getDiskUsage("/", false, true, false);
      DUResponse duRootRes = (DUResponse) rootResponse.getEntity();
      return duRootRes.getSizeWithReplica();
    } catch (IOException e) {
      return -1;
    }
  }

  private DeletionPendingBytesByStage createDeletionPendingBytesByStage(long totalPendingAtOmSide,
                                                                        long totalPendingAtScmSide,
                                                                        long totalPendingAtDnSide) {
    long totalPending = totalPendingAtOmSide + totalPendingAtScmSide + totalPendingAtDnSide;
    Map<String, Map<String, Long>> stageItems = new HashMap<>();
    Map<String, Long> omMap = new HashMap<>();
    omMap.put("pendingBytes", totalPendingAtOmSide);
    Map<String, Long> scmMap = new HashMap<>();
    scmMap.put("pendingBytes", totalPendingAtScmSide);
    Map<String, Long> dnMap = new HashMap<>();
    dnMap.put("pendingBytes", totalPendingAtDnSide);
    stageItems.put("OM", omMap);
    stageItems.put("SCM", scmMap);
    stageItems.put("DN", dnMap);
    return new DeletionPendingBytesByStage(totalPending, stageItems);
  }

  private DatanodeStorageReport getStorageReport(DatanodeDetails datanode) {
    SCMNodeStat nodeStat =
        nodeManager.getNodeStat(datanode).get();
    long capacity = nodeStat.getCapacity().get();
    long used = nodeStat.getScmUsed().get();
    long remaining = nodeStat.getRemaining().get();
    long committed = nodeStat.getCommitted().get();
    long pendingDeletions = 0; // TODO nodeStat.getPendingDeletions().get();
    return new DatanodeStorageReport(capacity, used, remaining, committed, pendingDeletions);
  }
}
