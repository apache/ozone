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

import com.google.inject.Singleton;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Security allowlist of Recon API tools the chatbot may query.
 */
@Singleton
public class ReconApiAllowlist {

  private static final Set<String> EXACT_ROUTES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "api_v1_clusterState",
      "api_v1_datanodes",
      "api_v1_pipelines",
      "api_v1_containers",
      "api_v1_containers_missing",
      "api_v1_containers_unhealthy",
      "api_v1_containers_unhealthy_state",
      "api_v1_containers_deleted",
      "api_v1_containers_mismatch",
      "api_v1_containers_mismatch_deleted",
      "api_v1_containers_quasiClosed",
      "api_v1_containers_unhealthy_export",
      "api_v1_keys_open",
      "api_v1_keys_open_summary",
      "api_v1_keys_open_mpu_summary",
      "api_v1_keys_deletePending_summary",
      "api_v1_keys_deletePending",
      "api_v1_keys_deletePending_dirs",
      "api_v1_keys_deletePending_dirs_summary",
      "api_v1_keys_listKeys",
      "api_v1_volumes",
      "api_v1_buckets",
      "api_v1_task_status",
      "api_v1_utilization_fileCount",
      "api_v1_utilization_containerCount",
      "api_v1_namespace_summary",
      "api_v1_namespace_usage",
      "api_v1_namespace_quota",
      "api_v1_namespace_dist"
  )));

  public boolean isRegistered(String toolName) {
    if (toolName == null) {
      return false;
    }
    return EXACT_ROUTES.contains(toolName);
  }

  /**
   * Returns the immutable set of registered tool names. Used to keep the allowlist, the LLM tool
   * catalog, and the router in sync (see TestReconToolCatalogConsistency).
   */
  public Set<String> getRegisteredTools() {
    return EXACT_ROUTES;
  }
}
