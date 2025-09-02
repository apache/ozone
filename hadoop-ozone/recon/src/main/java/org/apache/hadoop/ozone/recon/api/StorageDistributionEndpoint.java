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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.net.ssl.HttpsURLConnection;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.DatanodeStorageReport;
import org.apache.hadoop.ozone.recon.api.types.DeletionPendingBytesByStage;
import org.apache.hadoop.ozone.recon.api.types.GlobalNamespaceReport;
import org.apache.hadoop.ozone.recon.api.types.GlobalStorageReport;
import org.apache.hadoop.ozone.recon.api.types.StorageCapacityDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.UsedSpaceBreakDown;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
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
public class StorageDistributionEndpoint {
  private final ReconNodeManager nodeManager;
  private final OMDBInsightEndpoint omdbInsightEndpoint;
  private final NSSummaryEndpoint nsSummaryEndpoint;
  private final StorageContainerLocationProtocol scmClient;
  private static Logger log = LoggerFactory.getLogger(StorageDistributionEndpoint.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final int HTTP_TIMEOUT_MS = 5000;
  private Map<DatanodeDetails, Long> blockDeletionMetricsMap = new HashMap<>();

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
    initializeBlockDeletionMetricsMap();
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
    Map<String, Long> totalPendingAtOmSide = calculatePendingSizes();
    long totalOpenKeySize = calculateOpenKeySizes();
    long totalCommittedSize = calculateCommittedSize();
    long pendingDirectorySize = totalPendingAtOmSide.getOrDefault("pendingDirectorySize", 0L);
    long pendingKeySize = totalPendingAtOmSide.getOrDefault("pendingKeySize", 0L);
    long totalUsedNamespace = pendingDirectorySize + pendingKeySize + totalOpenKeySize + totalCommittedSize;
    metrics.put("pendingDirectorySize", pendingDirectorySize);
    metrics.put("pendingKeySize", pendingKeySize);
    metrics.put("totalOpenKeySize", totalOpenKeySize);
    metrics.put("totalCommittedSize", totalCommittedSize);
    metrics.put("totalUsedNamespace", totalUsedNamespace);
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
      log.warn("Failed to get deleted block summary from SCM", e);
    }
    long totalPendingAtDnSide = blockDeletionMetricsMap.values().stream().reduce(0L, Long::sum);

    DeletionPendingBytesByStage deletionPendingBytesByStage =
        createDeletionPendingBytesByStage(namespaceMetrics.getOrDefault("pendingDirectorySize", 0L),
            namespaceMetrics.getOrDefault("pendingKeySize", 0L),
            scmSummary != null ? scmSummary.getTotalBlockReplicatedSize() : 0L,
            totalPendingAtDnSide);
    return StorageCapacityDistributionResponse.newBuilder()
        .setDataNodeUsage(nodeStorageReports)
        .setGlobalStorage(storageMetrics)
        .setGlobalNamespace(new GlobalNamespaceReport(namespaceMetrics.get("totalUsedNamespace"), 0))
        .setUsedSpaceBreakDown(new UsedSpaceBreakDown(
            namespaceMetrics.get("totalOpenKeySize"),
            namespaceMetrics.get("totalCommittedSize"),
            deletionPendingBytesByStage))
        .build();
  }

  private List<DatanodeStorageReport> collectDatanodeReports() {
    return nodeManager.getAllNodes().stream()
        .map(this::getStorageReport)
        .collect(Collectors.toList());
  }

  private Map<String, Long> calculatePendingSizes() {
    Map<String, Long> result = new HashMap<>();
    Map<String, Long> pendingDeletedDirSizes = new HashMap<>();
    omdbInsightEndpoint.calculateTotalPendingDeletedDirSizes(pendingDeletedDirSizes);
    Map<String, Long> pendingKeySize = new HashMap<>();
    omdbInsightEndpoint.createKeysSummaryForDeletedKey(pendingKeySize);
    result.put("pendingDirectorySize", pendingDeletedDirSizes.getOrDefault("totalReplicatedDataSize", 0L));
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

  private DeletionPendingBytesByStage createDeletionPendingBytesByStage(long pendingDirectorySize,
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
    return new DeletionPendingBytesByStage(totalPending, stageItems);
  }

  private void initializeBlockDeletionMetricsMap() {
    nodeManager.getNodeStats().keySet().forEach(nodeId -> {
      try {
        blockDeletionMetricsMap.put(nodeId, getBlockDeletionMetricsFromDatanode(nodeId));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private DatanodeStorageReport getStorageReport(DatanodeDetails datanode) {
    SCMNodeStat nodeStat =
        nodeManager.getNodeStat(datanode).get();
    long capacity = nodeStat.getCapacity().get();
    long used = nodeStat.getScmUsed().get();
    long remaining = nodeStat.getRemaining().get();
    long committed = nodeStat.getCommitted().get();
    long pendingDeletion = blockDeletionMetricsMap.getOrDefault(datanode, 0L);
    return new DatanodeStorageReport(capacity, used, remaining, committed, pendingDeletion);
  }

  private HttpURLConnection makeHttpGetCall(String urlString) throws IOException {
    Objects.requireNonNull(urlString, "urlString");
    URL url = new URL(urlString);
    final HttpURLConnection conn = openURLConnection(url);
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(HTTP_TIMEOUT_MS);
    conn.setReadTimeout(HTTP_TIMEOUT_MS);
    conn.setRequestProperty("Accept", "application/json");
    return conn;
  }

  private HttpURLConnection openURLConnection(URL url) throws IOException {
    final String protocol = url.getProtocol().toLowerCase(Locale.ROOT);
    switch (protocol) {
    case "https":
      return (HttpsURLConnection) url.openConnection();
    case "http":
      return (HttpURLConnection) url.openConnection();
    default:
      throw new IOException("Unsupported protocol: " + protocol + " for URL: " + url);
    }
  }

  /** Parse block deletion metrics from JMX JSON response. */
  private long parseBlockDeletionMetrics(String jsonResponse) {
    if (jsonResponse == null || jsonResponse.isEmpty()) {
      return 0L;
    }
    try {
      JsonNode root = OBJECT_MAPPER.readTree(jsonResponse);
      JsonNode beans = root.get("beans");
      if (beans != null && beans.isArray()) {
        for (JsonNode bean : beans) {
          String name = bean.path("name").asText("");
          if (name.contains("BlockDeletingService")) {
            return extractBlockDeletionMetrics(bean);
          }
        }
      }
    } catch (Exception e) {
      log.warn("Failed to parse block deletion metrics JSON: {}", e.toString());
    }
    return 0L;
  }

  /** Extract block deletion metrics from JMX bean node. */
  private long extractBlockDeletionMetrics(JsonNode beanNode) {
    return beanNode.path("TotalPendingBlockBytes").asLong(0L);
  }

  private long getBlockDeletionMetricsFromDatanode(DatanodeDetails datanode) throws IOException {
    // Construct metrics URL for DataNode JMX endpoint
    String metricsUrl = String.format("http://%s:%d/jmx?qry=Hadoop:service=HddsDatanode,name=BlockDeletingService",
        datanode.getIpAddress(),
        datanode.getPort(DatanodeDetails.Port.Name.HTTP).getValue());

    HttpURLConnection conn = makeHttpGetCall(metricsUrl);
    try {
      String jsonResponse = getResponseData(conn);
      return parseBlockDeletionMetrics(jsonResponse);
    } finally {
      try {
        conn.disconnect();
      } catch (Exception ignored) {
        // no-op
      }
    }
  }

  private String getResponseData(HttpURLConnection conn) throws IOException {
    int code = conn.getResponseCode();
    // 2xx: read normal body
    if (code >= 200 && code < 300) {
      return readStream(conn.getInputStream());
    }
    String err = null;
    try {
      if (conn.getErrorStream() != null) {
        err = readStream(conn.getErrorStream());
      }
    } catch (IOException ignored) {
      // ignore read errors on error stream
    }
    log.warn("HTTP {} from {}. Error body: {}", code, conn.getURL(), err);
    return "";
  }

  /** Small utility to read an entire stream as a UTF-8 String. */
  private String readStream(java.io.InputStream in) throws IOException {
    StringBuilder sb = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line).append('\n');
      }
    }
    return sb.toString();
  }
}
