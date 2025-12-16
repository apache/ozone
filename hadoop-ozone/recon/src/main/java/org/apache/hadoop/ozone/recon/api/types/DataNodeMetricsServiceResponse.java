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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.apache.hadoop.ozone.recon.api.DataNodeMetricsService;

/**
 * Represents a response from the DataNodeMetricsService.
 * This class encapsulates the result of a metrics collection task,
 * including the collection status, total pending deletions across all data nodes,
 * and details about pending deletions for each data node.
 *
 * Instances of this class are created using the {@link Builder} class.
 */
public class DataNodeMetricsServiceResponse {
  @JsonProperty("status")
  private DataNodeMetricsService.MetricCollectionStatus status;
  @JsonProperty("totalPendingDeletionSize")
  private Long totalPendingDeletionSize;
  @JsonProperty("pendingDeletionPerDataNode")
  private List<DatanodePendingDeletionMetrics> pendingDeletionPerDataNode;
  @JsonProperty("totalNodesQueried")
  private int totalNodesQueried;
  @JsonProperty("totalNodeQueriesFailed")
  private long totalNodeQueryFailures;

  public DataNodeMetricsServiceResponse(Builder builder) {
    this.status = builder.status;
    this.totalPendingDeletionSize = builder.totalPendingDeletionSize;
    this.pendingDeletionPerDataNode = builder.pendingDeletion;
    this.totalNodesQueried = builder.totalNodesQueried;
    this.totalNodeQueryFailures = builder.totalNodeQueryFailures;
  }

  public DataNodeMetricsServiceResponse() {
    this.status = DataNodeMetricsService.MetricCollectionStatus.NOT_STARTED;
    this.totalPendingDeletionSize = 0L;
    this.pendingDeletionPerDataNode = null;
    this.totalNodesQueried = 0;
    this.totalNodeQueryFailures = 0;
  }

  public DataNodeMetricsService.MetricCollectionStatus getStatus() {
    return status;
  }

  public Long getTotalPendingDeletionSize() {
    return totalPendingDeletionSize;
  }

  public List<DatanodePendingDeletionMetrics> getPendingDeletionPerDataNode() {
    return pendingDeletionPerDataNode;
  }

  public int getTotalNodesQueried() {
    return totalNodesQueried;
  }

  public long getTotalNodeQueryFailures() {
    return totalNodeQueryFailures;
  }

  public  static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for constructing instances of {@link DataNodeMetricsServiceResponse}.
   * This class provides a fluent interface for setting the various properties
   * of a DataNodeMetricsServiceResponse object before creating a new immutable instance.
   * The Builder is designed to be used in a staged and intuitive manner.
   * The properties that can be configured include:
   * - Status of the metric collection process.
   * - Total number of blocks pending deletion across all data nodes.
   * - Metrics related to pending deletions from individual data nodes.
   */
  public static final class Builder {
    private DataNodeMetricsService.MetricCollectionStatus status;
    private Long totalPendingDeletionSize;
    private List<DatanodePendingDeletionMetrics> pendingDeletion;
    private int totalNodesQueried;
    private long totalNodeQueryFailures;

    public Builder setStatus(DataNodeMetricsService.MetricCollectionStatus status) {
      this.status = status;
      return this;
    }

    public Builder setTotalPendingDeletionSize(Long totalPendingDeletionSize) {
      this.totalPendingDeletionSize = totalPendingDeletionSize;
      return this;
    }

    public Builder setPendingDeletion(List<DatanodePendingDeletionMetrics> pendingDeletion) {
      this.pendingDeletion = pendingDeletion;
      return this;
    }

    public Builder setTotalNodesQueried(int totalNodesQueried) {
      this.totalNodesQueried = totalNodesQueried;
      return this;
    }

    public Builder setTotalNodeQueryFailures(long totalNodeQueryFailures) {
      this.totalNodeQueryFailures = totalNodeQueryFailures;
      return this;
    }

    public DataNodeMetricsServiceResponse build() {
      return new DataNodeMetricsServiceResponse(this);
    }
  }
}
