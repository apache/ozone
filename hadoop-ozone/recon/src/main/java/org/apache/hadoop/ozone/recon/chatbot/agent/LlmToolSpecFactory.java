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

package org.apache.hadoop.ozone.recon.chatbot.agent;

import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_ENTITY_PATH;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_NAMESPACE_USAGE_FILES;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_NAMESPACE_USAGE_REPLICA;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_NAMESPACE_USAGE_SORT_SUB_PATHS;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OPEN_KEY_INCLUDE_FSO;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OPEN_KEY_INCLUDE_NON_FSO;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_BUCKET;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_CONTAINER_SIZE;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_CONTAINER_STATE;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_CREATION_DATE;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_FILE_SIZE;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_FILTER;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_KEY_SIZE;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_MAX_CONTAINER_ID;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_MIN_CONTAINER_ID;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_REPLICATION_TYPE;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_START_PREFIX;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_VOLUME;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMClient.ToolSpec;

/**
 * Builds native LLM tool specifications (names, descriptions, parameters).
 * Descriptions are semantic (when to use / not use) and complement recon-tool-semantics.md.
 */
@Singleton
public class LlmToolSpecFactory {

  private final List<ToolSpec> toolSpecs;

  @Inject
  public LlmToolSpecFactory() {
    this.toolSpecs = Collections.unmodifiableList(buildToolSpecs());
  }

  public List<ToolSpec> getToolSpecs() {
    return toolSpecs;
  }

  private List<ToolSpec> buildToolSpecs() {
    List<ToolSpec> specs = new ArrayList<>();
    Map<String, Object> limitOnly = paramMap(RECON_QUERY_LIMIT, "integer");
    addClusterAndContainerToolSpecs(specs, limitOnly);
    addKeyToolSpecs(specs, limitOnly);
    addVolumeUtilizationAndNamespaceToolSpecs(specs, limitOnly);
    return specs;
  }

  private void addClusterAndContainerToolSpecs(List<ToolSpec> specs, Map<String, Object> limitOnly) {
    specs.add(createSpec("api_v1_clusterState",
        "High-level cluster snapshot: storage capacity/usage, pipeline and container counts, "
            + "and aggregate key statistics. Use for broad health or capacity questions "
            + "(e.g. 'how much storage is used?', 'cluster overview'). Prefer this over "
            + "calling many sub-endpoints when the user wants an overall picture. "
            + "Do NOT use for per-datanode detail (use api_v1_datanodes) or listing individual keys.",
        null));

    specs.add(createSpec("api_v1_datanodes",
        "Live datanode inventory: hostname, state (HEALTHY/DEAD/etc.), storage reports, "
            + "and heartbeat metadata. Use when the user asks about datanodes, nodes, "
            + "storage nodes, or node health/counts. Do NOT use for container replica placement "
            + "history (use container replica tools) or pipeline leadership.",
        null));

    specs.add(createSpec("api_v1_pipelines",
        "SCM pipeline list with leaders, datanode members, and pipeline state. Use for pipeline "
            + "status, leader election, or 'how many pipelines' questions. Related to clusterState "
            + "but provides per-pipeline detail.",
        null));

    specs.add(createSpec("api_v1_containers",
        "List of all containers with IDs, key counts, and pipeline associations (max 1000). "
            + "Use for general container inventory. For unhealthy/missing/deleted/mismatch views "
            + "use the specialized container tools instead.",
        limitOnly));

    specs.add(createSpec("api_v1_containers_missing",
        "Containers reported missing in SCM (lost/unreachable). Use when the user mentions "
            + "'missing', 'lost', or containers not found. Distinct from deleted containers "
            + "(api_v1_containers_deleted) and from unhealthy under-replicated state "
            + "(api_v1_containers_unhealthy_state with state=MISSING).",
        limitOnly));

    specs.add(createSpec("api_v1_containers_unhealthy",
        "All unhealthy containers across every state with aggregate counts "
            + "(missing, under/over/mis-replicated). Use when the user asks broadly about "
            + "'unhealthy', 'bad', or 'replication problems' without naming one state. "
            + "If they name a specific state (UNDER_REPLICATED, MISSING, etc.), prefer "
            + "api_v1_containers_unhealthy_state with the state parameter.",
        paramMap(RECON_QUERY_LIMIT, "integer", RECON_QUERY_MAX_CONTAINER_ID, "integer",
            RECON_QUERY_MIN_CONTAINER_ID, "integer")));

    Map<String, Object> unhealthyStateParams = paramMap(
        RECON_QUERY_CONTAINER_STATE, "string", RECON_QUERY_LIMIT, "integer",
        RECON_QUERY_MAX_CONTAINER_ID, "integer", RECON_QUERY_MIN_CONTAINER_ID, "integer");
    specs.add(createSpec("api_v1_containers_unhealthy_state",
        "Unhealthy containers filtered to one SCM state. Required: state one of MISSING, "
            + "UNDER_REPLICATED, OVER_REPLICATED, MIS_REPLICATED. Use for targeted questions "
            + "like 'show under-replicated containers' or 'list missing containers'. "
            + "Prefer api_v1_containers_unhealthy when the user wants all unhealthy types combined.",
        unhealthyStateParams));

    specs.add(createSpec("api_v1_containers_deleted",
        "Containers deleted in SCM (removed from active service). Use when the user asks about "
            + "'deleted' or 'removed' containers — not 'missing' containers.",
        limitOnly));

    Map<String, Object> mismatchParams = paramMap(
        RECON_QUERY_LIMIT, "integer", RECON_QUERY_FILTER, "string");
    specs.add(createSpec("api_v1_containers_mismatch",
        "OM/SCM container consistency gaps. Use when metadata differs between OM and SCM. "
            + "Set missingIn to OM or SCM to find containers absent from that side. "
            + "Keywords: mismatch, inconsistent, missing in OM/SCM.",
        mismatchParams));

    specs.add(createSpec("api_v1_containers_mismatch_deleted",
        "Containers deleted in SCM but still present in OM (stale OM records). Use for "
            + "'deleted in SCM but in OM' or reconciliation cleanup scenarios.",
        limitOnly));

    specs.add(createSpec("api_v1_containers_quasiClosed",
        "Quasi-closed containers (transitional closure state). Use only when the user "
            + "explicitly mentions quasi-closed containers.",
        paramMap(RECON_QUERY_LIMIT, "integer", RECON_QUERY_MIN_CONTAINER_ID, "integer")));

    specs.add(createSpec("api_v1_containers_unhealthy_export",
        "List/export jobs for unhealthy container data exports. Use when the user asks about "
            + "export jobs or downloading unhealthy container reports — not for listing "
            + "unhealthy containers themselves.",
        null));
  }

  private void addKeyToolSpecs(List<ToolSpec> specs, Map<String, Object> limitOnly) {
    Map<String, Object> openKeysParams = paramMap(
        RECON_QUERY_LIMIT, "integer", RECON_QUERY_START_PREFIX, "string",
        RECON_OPEN_KEY_INCLUDE_FSO, "boolean", RECON_OPEN_KEY_INCLUDE_NON_FSO, "boolean");
    specs.add(createSpec("api_v1_keys_open",
        "Open (uncommitted/in-progress) keys — active writes not yet finalized. Use when "
            + "the user mentions open, in-progress, uncommitted, or unfinished uploads. "
            + "Set includeFso true for FSO buckets, includeNonFso true for OBS/legacy layouts "
            + "(both true when bucket type is unknown). Optional startPrefix scopes to a path. "
            + "Do NOT use for committed file listings (api_v1_keys_listKeys) or aggregate "
            + "open-key counts only (api_v1_keys_open_summary).",
        openKeysParams));

    specs.add(createSpec("api_v1_keys_open_summary",
        "Aggregate counts/stats for open keys cluster-wide or scoped. Use when the user wants "
            + "how many open keys exist without listing each key. Pair with clusterState for "
            + "'total keys vs open keys' questions.",
        null));

    specs.add(createSpec("api_v1_keys_open_mpu_summary",
        "Summary of open multipart upload (MPU) keys. Use when the user mentions MPU, "
            + "multipart uploads, or incomplete multipart writes.",
        null));

    specs.add(createSpec("api_v1_keys_deletePending_summary",
        "Aggregate summary of keys marked for deletion. Use for counts/overview of pending "
            + "deletes — not for listing individual pending-delete keys.",
        null));

    Map<String, Object> deletePendingParams = paramMap(
        RECON_QUERY_LIMIT, "integer", RECON_QUERY_START_PREFIX, "string");
    specs.add(createSpec("api_v1_keys_deletePending",
        "List keys pending deletion under an optional prefix. Use when the user asks about "
            + "delete-pending or tombstoned keys (files). For directory-level pending deletes "
            + "use api_v1_keys_deletePending_dirs.",
        deletePendingParams));

    specs.add(createSpec("api_v1_keys_deletePending_dirs",
        "List directories pending deletion. Use when the user asks about pending-delete "
            + "directories or dir-level cleanup — not individual files.",
        paramMap(RECON_QUERY_LIMIT, "integer")));

    specs.add(createSpec("api_v1_keys_deletePending_dirs_summary",
        "Summary counts for directories pending deletion. Use for overview only.",
        null));

    Map<String, Object> listKeysParams = paramMap(
        RECON_QUERY_START_PREFIX, "string", RECON_QUERY_LIMIT, "integer",
        RECON_QUERY_REPLICATION_TYPE, "string", RECON_QUERY_CREATION_DATE, "string",
        RECON_QUERY_KEY_SIZE, "integer");
    specs.add(createSpec("api_v1_keys_listKeys",
        "List committed keys/files under a bucket-scoped prefix with optional filters "
            + "(replicationType RATIS/EC, creationDate, minimum keySize). REQUIRED: startPrefix "
            + "at least /<volume>/<bucket> — never '/' alone. Use to enumerate or filter files "
            + "('list files in bucket', 'large keys', 'EC keys'). Do NOT use for disk-usage totals "
            + "(api_v1_namespace_usage), open/uncommitted keys (api_v1_keys_open), or namespace "
            + "counts without listing keys (api_v1_namespace_summary).",
        listKeysParams));

  }

  private void addVolumeUtilizationAndNamespaceToolSpecs(List<ToolSpec> specs, Map<String, Object> limitOnly) {
    specs.add(createSpec("api_v1_volumes",
        "Ozone volume list (max 1000). Use when the user asks to list volumes or how many volumes "
            + "exist. For buckets within a volume use api_v1_buckets with the volume parameter.",
        paramMap(RECON_QUERY_LIMIT, "integer")));

    specs.add(createSpec("api_v1_buckets",
        "Bucket list (max 1000), optionally filtered by volume. Use when the user asks about "
            + "buckets in a volume or bucket inventory. Set volume when the user names a volume.",
        paramMap(RECON_QUERY_VOLUME, "string", RECON_QUERY_LIMIT, "integer")));

    specs.add(createSpec("api_v1_task_status",
        "Recon background sync task status (OM/SCM delta lag, last update timestamps). Use when "
            + "the user asks whether Recon is up to date, task lag, or 'when did Recon last sync'.",
        null));

    specs.add(createSpec("api_v1_utilization_fileCount",
        "Distribution of file counts by size tier (optionally per volume/bucket). Use for "
            + "histogram-style 'how many small vs large files' questions — not for listing "
            + "individual files (api_v1_keys_listKeys).",
        paramMap(RECON_QUERY_VOLUME, "string", RECON_QUERY_BUCKET, "string",
            RECON_QUERY_FILE_SIZE, "integer")));

    specs.add(createSpec("api_v1_utilization_containerCount",
        "Distribution of container counts by size tier. Use for container size histogram "
            + "questions at cluster level.",
        paramMap(RECON_QUERY_CONTAINER_SIZE, "integer")));

    Map<String, Object> nsParams = paramMap(RECON_ENTITY_PATH, "string");
    specs.add(createSpec("api_v1_namespace_summary",
        "Namespace metadata summary for a path (counts, quotas overview without full usage math). "
            + "Use for 'what is under this path' summary. For disk usage totals prefer "
            + "api_v1_namespace_usage; for listing files prefer api_v1_keys_listKeys.",
        nsParams));

    specs.add(createSpec("api_v1_namespace_usage",
        "Disk usage (du-style totals) for a path with optional sub-path breakdown. Use when "
            + "the user asks 'how much space', 'disk usage', 'total size' — NOT for listing "
            + "individual files (api_v1_keys_listKeys). Set path to /volume, /volume/bucket, "
            + "or deeper. Optional files/replica/sortSubPaths flags control breakdown detail.",
        paramMap(RECON_ENTITY_PATH, "string", RECON_NAMESPACE_USAGE_FILES, "boolean",
            RECON_NAMESPACE_USAGE_REPLICA, "boolean", RECON_NAMESPACE_USAGE_SORT_SUB_PATHS, "boolean")));

    specs.add(createSpec("api_v1_namespace_quota",
        "Quota usage for a namespace path (volume/bucket/key quota consumption). Use when the "
            + "user asks about quota limits or quota usage — not general disk usage "
            + "(api_v1_namespace_usage).",
        nsParams));

    specs.add(createSpec("api_v1_namespace_dist",
        "File size distribution under a namespace path. Use for size histogram / distribution "
            + "questions at a path — not for listing keys.",
        nsParams));

  }

  private static Map<String, Object> paramMap(String... nameTypePairs) {
    Map<String, Object> params = new HashMap<>();
    for (int i = 0; i < nameTypePairs.length; i += 2) {
      params.put(nameTypePairs[i], nameTypePairs[i + 1]);
    }
    return params;
  }

  private ToolSpec createSpec(String name, String description, Map<String, Object> params) {
    if (params == null) {
      params = new HashMap<>();
    }
    return new ToolSpec(name, description, params);
  }
}
