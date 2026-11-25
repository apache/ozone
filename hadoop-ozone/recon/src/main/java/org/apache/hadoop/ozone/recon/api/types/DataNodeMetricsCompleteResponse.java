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

package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.apache.hadoop.ozone.recon.api.DataNodeMetricsService;

/**
 * Response returned when metrics collection is complete.
 * Includes the metric payload fields.
 */
public class DataNodeMetricsCompleteResponse {

  @JsonProperty("status")
  private final DataNodeMetricsService.MetricCollectionStatus status;

  @JsonProperty("totalNodesQueried")
  private final int totalNodesQueried;

  @JsonProperty("totalNodeQueriesFailed")
  private final long totalNodeQueryFailures;

  @JsonProperty("totalPendingDeletionSize")
  private final Long totalPendingDeletionSize;

  @JsonProperty("pendingDeletionPerDataNode")
  private final List<DatanodePendingDeletionMetrics> pendingDeletionPerDataNode;

  @JsonCreator
  public DataNodeMetricsCompleteResponse(
      @JsonProperty("status") DataNodeMetricsService.MetricCollectionStatus status,
      @JsonProperty("totalNodesQueried") int totalNodesQueried,
      @JsonProperty("totalNodeQueriesFailed") long totalNodeQueriesFailed,
      @JsonProperty("totalPendingDeletionSize") Long totalPendingDeletionSize,
      @JsonProperty("pendingDeletionPerDataNode") List<DatanodePendingDeletionMetrics> pendingDeletionPerDataNode) {
    this.status = status;
    this.totalNodesQueried = totalNodesQueried;
    this.totalNodeQueryFailures = totalNodeQueriesFailed;
    this.totalPendingDeletionSize = totalPendingDeletionSize;
    this.pendingDeletionPerDataNode = pendingDeletionPerDataNode;
  }

  public DataNodeMetricsService.MetricCollectionStatus getStatus() {
    return status;
  }

  public int getTotalNodesQueried() {
    return totalNodesQueried;
  }

  public long getTotalNodeQueryFailures() {
    return totalNodeQueryFailures;
  }

  public Long getTotalPendingDeletionSize() {
    return totalPendingDeletionSize;
  }

  public List<DatanodePendingDeletionMetrics> getPendingDeletionPerDataNode() {
    return pendingDeletionPerDataNode;
  }
}
