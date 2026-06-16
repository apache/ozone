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

package org.apache.hadoop.ozone.recon.chatbot.recon;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.recon.api.BucketEndpoint;
import org.apache.hadoop.ozone.recon.api.ClusterStateEndpoint;
import org.apache.hadoop.ozone.recon.api.ContainerEndpoint;
import org.apache.hadoop.ozone.recon.api.NSSummaryEndpoint;
import org.apache.hadoop.ozone.recon.api.NodeEndpoint;
import org.apache.hadoop.ozone.recon.api.OMDBInsightEndpoint;
import org.apache.hadoop.ozone.recon.api.PipelineEndpoint;
import org.apache.hadoop.ozone.recon.api.TaskStatusService;
import org.apache.hadoop.ozone.recon.api.UtilizationEndpoint;
import org.apache.hadoop.ozone.recon.api.VolumeEndpoint;
import org.apache.hadoop.ozone.recon.MetricsServiceProviderFactory;
import org.apache.hadoop.ozone.recon.spi.MetricsServiceProvider;
import java.net.HttpURLConnection;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Invokes existing Recon JAX-RS endpoint beans directly (no HTTP loopback).
 *
 */
@Singleton
public class ReconEndpointRouter {

  private final ClusterStateEndpoint clusterStateEndpoint;
  private final NodeEndpoint nodeEndpoint;
  private final PipelineEndpoint pipelineEndpoint;
  private final ContainerEndpoint containerEndpoint;
  private final OMDBInsightEndpoint omdbInsightEndpoint;
  private final VolumeEndpoint volumeEndpoint;
  private final BucketEndpoint bucketEndpoint;
  private final TaskStatusService taskStatusService;
  private final UtilizationEndpoint utilizationEndpoint;
  private final NSSummaryEndpoint nsSummaryEndpoint;
  private final ReconApiAllowlist reconApiAllowlist;
  private final MetricsServiceProvider metricsServiceProvider;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Inject
  public ReconEndpointRouter(
      ClusterStateEndpoint clusterStateEndpoint,
      NodeEndpoint nodeEndpoint,
      PipelineEndpoint pipelineEndpoint,
      ContainerEndpoint containerEndpoint,
      OMDBInsightEndpoint omdbInsightEndpoint,
      VolumeEndpoint volumeEndpoint,
      BucketEndpoint bucketEndpoint,
      TaskStatusService taskStatusService,
      UtilizationEndpoint utilizationEndpoint,
      NSSummaryEndpoint nsSummaryEndpoint,
      ReconApiAllowlist reconApiAllowlist,
      MetricsServiceProviderFactory metricsServiceProviderFactory) {
    this.clusterStateEndpoint = clusterStateEndpoint;
    this.nodeEndpoint = nodeEndpoint;
    this.pipelineEndpoint = pipelineEndpoint;
    this.containerEndpoint = containerEndpoint;
    this.omdbInsightEndpoint = omdbInsightEndpoint;
    this.volumeEndpoint = volumeEndpoint;
    this.bucketEndpoint = bucketEndpoint;
    this.taskStatusService = taskStatusService;
    this.utilizationEndpoint = utilizationEndpoint;
    this.nsSummaryEndpoint = nsSummaryEndpoint;
    this.reconApiAllowlist = reconApiAllowlist;
    this.metricsServiceProvider = metricsServiceProviderFactory.getMetricsServiceProvider();
  }

  public boolean hasRoute(String toolName) {
    return reconApiAllowlist.isRegistered(toolName);
  }

  public Response route(String toolName, Map<String, String> params) throws java.io.IOException {
    // limit is always pre-clamped by ReconQueryExecutor; 1000 is a defensive fallback only.
    int limit = parseInt(params.get("limit"), 1000);
    String startPrefix = params.get("startPrefix") == null ? "" : params.get("startPrefix");

    if ("api_v1_clusterState".equals(toolName)) {
      return clusterStateEndpoint.getClusterState();
    } else if ("api_v1_datanodes".equals(toolName)) {
      return nodeEndpoint.getDatanodes();
    } else if ("api_v1_pipelines".equals(toolName)) {
      return pipelineEndpoint.getPipelines();
    } else if ("api_v1_containers".equals(toolName)) {
      return containerEndpoint.getContainers(limit, 0L);
    } else if ("api_v1_containers_missing".equals(toolName)) {
      return containerEndpoint.getMissingContainers(limit);
    } else if ("api_v1_containers_unhealthy".equals(toolName)) {
      return containerEndpoint.getUnhealthyContainers(limit, parseLong(params.get("maxContainerId"), 0L), parseLong(params.get("minContainerId"), 0L));
    } else if ("api_v1_containers_unhealthy_state".equals(toolName)) {
      String state = params.get("state");
      return containerEndpoint.getUnhealthyContainers(state, limit, parseLong(params.get("maxContainerId"), 0L), parseLong(params.get("minContainerId"), 0L));
    } else if ("api_v1_containers_deleted".equals(toolName)) {
      return containerEndpoint.getSCMDeletedContainers(limit, 0L);
    } else if ("api_v1_containers_mismatch".equals(toolName)) {
      return containerEndpoint.getContainerMisMatchInsights(limit, 0L, params.get("missingIn") == null ? "" : params.get("missingIn"));
    } else if ("api_v1_containers_mismatch_deleted".equals(toolName)) {
      return containerEndpoint.getOmContainersDeletedInSCM(limit, 0L);
    } else if ("api_v1_containers_quasiClosed".equals(toolName)) {
      return containerEndpoint.getQuasiClosedContainers(limit, parseLong(params.get("minContainerId"), 0L));
    } else if ("api_v1_containers_unhealthy_export".equals(toolName)) {
      return containerEndpoint.listExportJobs();
    } else if ("api_v1_keys_open".equals(toolName)) {
      return omdbInsightEndpoint.getOpenKeyInfo(limit, "", startPrefix, parseBoolean(params.get("includeFso"), false), parseBoolean(params.get("includeNonFso"), false));
    } else if ("api_v1_keys_open_summary".equals(toolName)) {
      return omdbInsightEndpoint.getOpenKeySummary();
    } else if ("api_v1_keys_open_mpu_summary".equals(toolName)) {
      return omdbInsightEndpoint.getOpenMPUKeySummary();
    } else if ("api_v1_keys_deletePending_summary".equals(toolName)) {
      return omdbInsightEndpoint.getDeletedKeySummary();
    } else if ("api_v1_keys_deletePending".equals(toolName)) {
      return omdbInsightEndpoint.getDeletedKeyInfo(limit, "", startPrefix);
    } else if ("api_v1_keys_deletePending_dirs".equals(toolName)) {
      return omdbInsightEndpoint.getDeletedDirInfo(limit, "");
    } else if ("api_v1_keys_deletePending_dirs_summary".equals(toolName)) {
      return omdbInsightEndpoint.getDeletedDirectorySummary();
    } else if ("api_v1_keys_listKeys".equals(toolName)) {
      return omdbInsightEndpoint.listKeys(params.get("replicationType"), params.get("creationDate"), parseLong(params.get("keySize"), 0L), startPrefix, "", limit);
    } else if ("api_v1_volumes".equals(toolName)) {
      return volumeEndpoint.getVolumes(limit, "");
    } else if ("api_v1_buckets".equals(toolName)) {
      return bucketEndpoint.getBuckets(params.get("volume"), limit, "");
    } else if ("api_v1_task_status".equals(toolName)) {
      return taskStatusService.getTaskStats();
    } else if ("api_v1_utilization_fileCount".equals(toolName)) {
      return utilizationEndpoint.getFileCounts(params.get("volume"), params.get("bucket"), parseLong(params.get("fileSize"), 0L));
    } else if ("api_v1_utilization_containerCount".equals(toolName)) {
      return utilizationEndpoint.getContainerCounts(parseLong(params.get("containerSize"), 0L));
    } else if ("api_v1_namespace_summary".equals(toolName)) {
      return nsSummaryEndpoint.getBasicInfo(params.get("path"));
    } else if ("api_v1_namespace_usage".equals(toolName)) {
      return nsSummaryEndpoint.getDiskUsage(params.get("path"), parseBoolean(params.get("files"), false), parseBoolean(params.get("replica"), false), parseBoolean(params.get("sortSubPaths"), false));
    } else if ("api_v1_namespace_quota".equals(toolName)) {
      return nsSummaryEndpoint.getQuotaUsage(params.get("path"));
    } else if ("api_v1_namespace_dist".equals(toolName)) {
      return nsSummaryEndpoint.getFileSizeDistribution(params.get("path"));
    } else if ("api_v1_metrics_api".equals(toolName)) {
      return routeMetrics(params.get("api"), params);
    }

    throw new IllegalArgumentException("No in-process route for " + toolName);
  }

  private Response routeMetrics(String api, Map<String, String> params) throws java.io.IOException {
    if (metricsServiceProvider == null) {
      throw new java.io.IOException("Metrics endpoint is not configured.");
    }
    
    String query = params.get("query");
    if (query == null) {
      query = "";
    }
    
    try {
      HttpURLConnection connection = metricsServiceProvider.getMetricsResponse(api, query);
      int responseCode = connection.getResponseCode();
      
      InputStream inputStream;
      if (responseCode >= 200 && responseCode < 300) {
        inputStream = connection.getInputStream();
      } else {
        inputStream = connection.getErrorStream();
      }
      
      StringBuilder sb = new StringBuilder();
      if (inputStream != null) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))) {
          String line;
          while ((line = br.readLine()) != null) {
            sb.append(line);
          }
        }
      }
      
      if (responseCode < 200 || responseCode >= 300) {
        throw new java.io.IOException("Metrics API request failed with status " + responseCode + ": " + sb.toString());
      }
      
      JsonNode jsonNode = MAPPER.readTree(sb.toString());
      return Response.ok(jsonNode).build();
    } catch (Exception e) {
      if (e instanceof java.io.IOException) {
        throw (java.io.IOException) e;
      }
      throw new java.io.IOException("Failed to query metrics", e);
    }
  }

  private int parseInt(String val, int def) {
    if (val == null || val.isEmpty()) return def;
    try {
      return Integer.parseInt(val);
    } catch (NumberFormatException e) {
      return def;
    }
  }

  private long parseLong(String val, long def) {
    if (val == null || val.isEmpty()) return def;
    try {
      return Long.parseLong(val);
    } catch (NumberFormatException e) {
      return def;
    }
  }

  private boolean parseBoolean(String val, boolean def) {
    if (val == null || val.isEmpty()) return def;
    return Boolean.parseBoolean(val);
  }
}
