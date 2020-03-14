/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * Class that represents the API Response structure of ClusterState.
 */
public class ClusterStateResponse {
  /**
   * Total count of the pipelines.
   */
  @JsonProperty("pipelines")
  private int pipelines;

  /**
   * Count of datanodes.
   */
  @JsonProperty("datanodes")
  private DatanodesCount datanodes;

  /**
   * Storage Report of the cluster.
   */
  @JsonProperty("storageReport")
  private DatanodeStorageReport storageReport;

  /**
   * Total count of containers in the cluster.
   */
  @JsonProperty("containers")
  private int containers;

  /**
   * Total count of volumes in the cluster.
   */
  @JsonProperty("volumes")
  private long volumes;

  /**
   * Total count of buckets in the cluster.
   */
  @JsonProperty("buckets")
  private long buckets;

  /**
   * Total count of keys in the cluster.
   */
  @JsonProperty("keys")
  private long keys;

  /**
   * Returns new builder class that builds a ClusterStateResponse.
   *
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for ClusterStateResponse.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static final class Builder {
    private int pipelines;
    private DatanodesCount datanodes;
    private DatanodeStorageReport storageReport;
    private int containers;
    private long volumes;
    private long buckets;
    private long keys;

    public Builder() {
      // Default values
      this.containers = 0;
      this.volumes = 0;
      this.buckets = 0;
      this.keys = 0;
      this.pipelines = 0;
    }

    public Builder setPipelines(int pipelines) {
      this.pipelines = pipelines;
      return this;
    }

    public Builder setDatanodes(DatanodesCount datanodes) {
      this.datanodes = datanodes;
      return this;
    }

    public Builder setStorageReport(DatanodeStorageReport storageReport) {
      this.storageReport = storageReport;
      return this;
    }

    public Builder setContainers(int containers) {
      this.containers = containers;
      return this;
    }

    public Builder setVolumes(long volumes) {
      this.volumes = volumes;
      return this;
    }

    public Builder setBuckets(long buckets) {
      this.buckets = buckets;
      return this;
    }

    public Builder setKeys(long keys) {
      this.keys = keys;
      return this;
    }

    public ClusterStateResponse build() {
      Preconditions.checkNotNull(this.datanodes);
      Preconditions.checkNotNull(this.storageReport);

      ClusterStateResponse clusterStateResponse = new ClusterStateResponse();
      clusterStateResponse.buckets = this.buckets;
      clusterStateResponse.keys = this.keys;
      clusterStateResponse.pipelines = this.pipelines;
      clusterStateResponse.volumes = this.volumes;
      clusterStateResponse.datanodes = this.datanodes;
      clusterStateResponse.storageReport = this.storageReport;
      clusterStateResponse.containers = this.containers;
      return clusterStateResponse;
    }
  }

  public int getPipelines() {
    return pipelines;
  }

  public DatanodesCount getDatanodes() {
    return datanodes;
  }

  public DatanodeStorageReport getStorageReport() {
    return storageReport;
  }

  public int getContainers() {
    return containers;
  }

  public long getVolumes() {
    return volumes;
  }

  public long getBuckets() {
    return buckets;
  }

  public long getKeys() {
    return keys;
  }
}
