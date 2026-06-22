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
import java.io.IOException;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.recon.ReconConstants;
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

/**
 * Dispatches chatbot tool names to in-process Recon JAX-RS endpoint beans (no HTTP loopback).
 *
 * <p>Which tools may run is enforced upstream by {@code ChatbotAgent.validateToolCall} against
 * {@link ReconApiAllowlist}; {@link #hasRoute(String)} mirrors that allowlist. An unknown
 * {@code toolName} here still throws {@link IllegalArgumentException} as a defensive fallback.
 *
 * <p>All routes call injected endpoint beans directly in the same JVM.
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

  @Inject
  @SuppressWarnings("checkstyle:ParameterNumber")
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
      ReconApiAllowlist reconApiAllowlist) {
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
  }

  public boolean hasRoute(String toolName) {
    return reconApiAllowlist.isRegistered(toolName);
  }

  public Response route(String toolName, Map<String, String> params) throws IOException {
    // limit is pre-clamped by ReconQueryExecutor; MAX_RECORDS_PER_CALL is a defensive fallback only.
    int limit = parseInt(params.get(ReconConstants.RECON_QUERY_LIMIT),
        ReconQueryExecutor.MAX_RECORDS_PER_CALL);
    String startPrefix = params.get(ReconConstants.RECON_QUERY_START_PREFIX) == null
        ? "" : params.get(ReconConstants.RECON_QUERY_START_PREFIX);

    switch (toolName) {
    case "api_v1_clusterState":
      return clusterStateEndpoint.getClusterState();
    case "api_v1_datanodes":
      return nodeEndpoint.getDatanodes();
    case "api_v1_pipelines":
      return pipelineEndpoint.getPipelines();
    case "api_v1_containers":
      return containerEndpoint.getContainers(limit, 0L);
    case "api_v1_containers_missing":
      return containerEndpoint.getMissingContainers(limit);
    case "api_v1_containers_unhealthy":
      return routeUnhealthyContainers(params, limit);
    case "api_v1_containers_unhealthy_state":
      return routeUnhealthyContainersByState(params, limit);
    case "api_v1_containers_deleted":
      return containerEndpoint.getSCMDeletedContainers(limit, 0L);
    case "api_v1_containers_mismatch":
      return routeContainersMismatch(params, limit);
    case "api_v1_containers_mismatch_deleted":
      return containerEndpoint.getOmContainersDeletedInSCM(limit, 0L);
    case "api_v1_containers_quasiClosed":
      return containerEndpoint.getQuasiClosedContainers(
          limit, parseLong(params.get(ReconConstants.RECON_QUERY_MIN_CONTAINER_ID), 0L));
    case "api_v1_containers_unhealthy_export":
      return containerEndpoint.listExportJobs();
    case "api_v1_keys_open":
      return routeOpenKeys(params, limit, startPrefix);
    case "api_v1_keys_open_summary":
      return omdbInsightEndpoint.getOpenKeySummary();
    case "api_v1_keys_open_mpu_summary":
      return omdbInsightEndpoint.getOpenMPUKeySummary();
    case "api_v1_keys_deletePending_summary":
      return omdbInsightEndpoint.getDeletedKeySummary();
    case "api_v1_keys_deletePending":
      return omdbInsightEndpoint.getDeletedKeyInfo(limit, "", startPrefix);
    case "api_v1_keys_deletePending_dirs":
      return omdbInsightEndpoint.getDeletedDirInfo(limit, "");
    case "api_v1_keys_deletePending_dirs_summary":
      return omdbInsightEndpoint.getDeletedDirectorySummary();
    case "api_v1_keys_listKeys":
      return routeListKeys(params, limit, startPrefix);
    case "api_v1_volumes":
      return volumeEndpoint.getVolumes(limit, "");
    case "api_v1_buckets":
      return bucketEndpoint.getBuckets(
          params.get(ReconConstants.RECON_QUERY_VOLUME), limit, "");
    case "api_v1_task_status":
      return taskStatusService.getTaskStats();
    case "api_v1_utilization_fileCount":
      return routeFileCount(params);
    case "api_v1_utilization_containerCount":
      return utilizationEndpoint.getContainerCounts(
          parseLong(params.get(ReconConstants.RECON_QUERY_CONTAINER_SIZE), 0L));
    case "api_v1_namespace_summary":
      return nsSummaryEndpoint.getBasicInfo(params.get(ReconConstants.RECON_ENTITY_PATH));
    case "api_v1_namespace_usage":
      return routeNamespaceUsage(params);
    case "api_v1_namespace_quota":
      return nsSummaryEndpoint.getQuotaUsage(params.get(ReconConstants.RECON_ENTITY_PATH));
    case "api_v1_namespace_dist":
      return nsSummaryEndpoint.getFileSizeDistribution(params.get(ReconConstants.RECON_ENTITY_PATH));
    default:
      throw new IllegalArgumentException("No in-process route for " + toolName);
    }
  }

  private Response routeUnhealthyContainers(Map<String, String> params, int limit) {
    long maxContainerId = parseLong(params.get(ReconConstants.RECON_QUERY_MAX_CONTAINER_ID), 0L);
    long minContainerId = parseLong(params.get(ReconConstants.RECON_QUERY_MIN_CONTAINER_ID), 0L);
    return containerEndpoint.getUnhealthyContainers(limit, maxContainerId, minContainerId);
  }

  private Response routeUnhealthyContainersByState(Map<String, String> params, int limit) {
    String state = params.get(ReconConstants.RECON_QUERY_CONTAINER_STATE);
    long maxContainerId = parseLong(params.get(ReconConstants.RECON_QUERY_MAX_CONTAINER_ID), 0L);
    long minContainerId = parseLong(params.get(ReconConstants.RECON_QUERY_MIN_CONTAINER_ID), 0L);
    return containerEndpoint.getUnhealthyContainers(state, limit, maxContainerId, minContainerId);
  }

  private Response routeContainersMismatch(Map<String, String> params, int limit) {
    String missingIn = params.get(ReconConstants.RECON_QUERY_FILTER) == null
        ? "" : params.get(ReconConstants.RECON_QUERY_FILTER);
    return containerEndpoint.getContainerMisMatchInsights(limit, 0L, missingIn);
  }

  private Response routeOpenKeys(Map<String, String> params, int limit, String startPrefix) {
    boolean includeFso = parseBoolean(
        params.get(ReconConstants.RECON_OPEN_KEY_INCLUDE_FSO), false);
    boolean includeNonFso = parseBoolean(
        params.get(ReconConstants.RECON_OPEN_KEY_INCLUDE_NON_FSO), false);
    return omdbInsightEndpoint.getOpenKeyInfo(limit, "", startPrefix, includeFso, includeNonFso);
  }

  private Response routeListKeys(Map<String, String> params, int limit, String startPrefix) {
    long keySize = parseLong(params.get(ReconConstants.RECON_QUERY_KEY_SIZE), 0L);
    return omdbInsightEndpoint.listKeys(
        params.get(ReconConstants.RECON_QUERY_REPLICATION_TYPE),
        params.get(ReconConstants.RECON_QUERY_CREATION_DATE),
        keySize,
        startPrefix,
        "",
        limit);
  }

  private Response routeNamespaceUsage(Map<String, String> params) throws IOException {
    boolean files = parseBoolean(params.get(ReconConstants.RECON_NAMESPACE_USAGE_FILES), false);
    boolean replica = parseBoolean(params.get(ReconConstants.RECON_NAMESPACE_USAGE_REPLICA), false);
    boolean sortSubPaths = parseBoolean(
        params.get(ReconConstants.RECON_NAMESPACE_USAGE_SORT_SUB_PATHS), false);
    return nsSummaryEndpoint.getDiskUsage(
        params.get(ReconConstants.RECON_ENTITY_PATH), files, replica, sortSubPaths);
  }

  private Response routeFileCount(Map<String, String> params) {
    long fileSize = parseLong(params.get(ReconConstants.RECON_QUERY_FILE_SIZE), 0L);
    return utilizationEndpoint.getFileCounts(
        params.get(ReconConstants.RECON_QUERY_VOLUME),
        params.get(ReconConstants.RECON_QUERY_BUCKET),
        fileSize);
  }

  private int parseInt(String val, int def) {
    if (val == null || val.isEmpty()) {
      return def;
    }
    try {
      return Integer.parseInt(val);
    } catch (NumberFormatException e) {
      return def;
    }
  }

  private long parseLong(String val, long def) {
    if (val == null || val.isEmpty()) {
      return def;
    }
    try {
      return Long.parseLong(val);
    } catch (NumberFormatException e) {
      return def;
    }
  }

  private boolean parseBoolean(String val, boolean def) {
    if (val == null || val.isEmpty()) {
      return def;
    }
    return Boolean.parseBoolean(val);
  }
}
