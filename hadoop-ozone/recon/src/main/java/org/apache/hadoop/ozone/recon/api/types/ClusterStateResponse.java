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
import java.util.Objects;

/**
 * Class that represents the API Response structure of ClusterState.
 */
public final class ClusterStateResponse {
  /**
   * Total count of the pipelines.
   */
  @JsonProperty("pipelines")
  private int pipelines;

  /**
   * Total count of datanodes.
   */
  @JsonProperty("totalDatanodes")
  private int totalDatanodes;

  /**
   * Count of healthy datanodes.
   */
  @JsonProperty("healthyDatanodes")
  private int healthyDatanodes;

  /**
   * Storage Report of the cluster.
   */
  @JsonProperty("storageReport")
  private ClusterStorageReport storageReport;

  /**
   * Total count of containers in the cluster.
   */
  @JsonProperty("containers")
  private int containers;

  /**
   * Total count of missing containers in the cluster.
   */
  @JsonProperty("missingContainers")
  private int missingContainers;

  /**
   * Total count of open containers in the cluster.
   */
  @JsonProperty("openContainers")
  private int openContainers;

  /**
   * Total count of deleted containers in the cluster.
   */
  @JsonProperty("deletedContainers")
  private int deletedContainers;

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
   * Total count of keys marked for deletion in the cluster.
   */
  @JsonProperty("keysPendingDeletion")
  private long keysPendingDeletion;

  /**
   * Total count of directories marked for deletion in the cluster.
   */
  @JsonProperty
  private long deletedDirs;

  @JsonProperty("scmServiceId")
  private String scmServiceId;

  @JsonProperty("omServiceId")
  private String omServiceId;

  /**
   * Returns new builder class that builds a ClusterStateResponse.
   *
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  private ClusterStateResponse(Builder b) {
    this.buckets = b.buckets;
    this.keys = b.keys;
    this.pipelines = b.pipelines;
    this.volumes = b.volumes;
    this.totalDatanodes = b.totalDatanodes;
    this.healthyDatanodes = b.healthyDatanodes;
    this.storageReport = b.storageReport;
    this.containers = b.containers;
    this.missingContainers = b.missingContainers;
    this.openContainers = b.openContainers;
    this.keysPendingDeletion = b.keysPendingDeletion;
    this.deletedDirs = b.deletedDirs;
    this.deletedContainers = b.deletedContainers;
    this.scmServiceId = b.scmServiceId;
    this.omServiceId = b.omServiceId;
  }

  /**
   * Builder for ClusterStateResponse.
   */
  public static final class Builder {
    private int pipelines;
    private int totalDatanodes;
    private int healthyDatanodes;
    private ClusterStorageReport storageReport;
    private int containers;
    private int missingContainers;
    private int openContainers;
    private int deletedContainers;
    private long volumes;
    private long buckets;
    private long keys;
    private long keysPendingDeletion;
    private long deletedDirs;
    private String scmServiceId;
    private String omServiceId;

    public Builder() {
      // Default values
      this.containers = 0;
      this.missingContainers = 0;
      this.openContainers = 0;
      this.deletedContainers = 0;
      this.volumes = 0;
      this.buckets = 0;
      this.keys = 0;
      this.pipelines = 0;
      this.totalDatanodes = 0;
      this.healthyDatanodes = 0;
      this.keysPendingDeletion = 0;
      this.deletedDirs = 0;
    }

    public Builder setPipelines(int pipelines) {
      this.pipelines = pipelines;
      return this;
    }

    public Builder setTotalDatanodes(int totalDatanodes) {
      this.totalDatanodes = totalDatanodes;
      return this;
    }

    public Builder setHealthyDatanodes(int healthyDatanodes) {
      this.healthyDatanodes = healthyDatanodes;
      return this;
    }

    public Builder setStorageReport(ClusterStorageReport storageReport) {
      this.storageReport = storageReport;
      return this;
    }

    public Builder setContainers(int containers) {
      this.containers = containers;
      return this;
    }

    public Builder setMissingContainers(int missingContainers) {
      this.missingContainers = missingContainers;
      return this;
    }

    public Builder setOpenContainers(int openContainers) {
      this.openContainers = openContainers;
      return this;
    }

    public Builder setDeletedContainers(int deletedContainers) {
      this.deletedContainers = deletedContainers;
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

    public void setKeysPendingDeletion(long keysPendingDeletion) {
      this.keysPendingDeletion = keysPendingDeletion;
    }

    public void setDeletedDirs(long deletedDirs) {
      this.deletedDirs = deletedDirs;
    }

    public Builder setKeys(long keys) {
      this.keys = keys;
      return this;
    }

    public Builder setScmServiceId(String scmServiceId) {
      this.scmServiceId = scmServiceId;
      return this;
    }

    public Builder setOmServiceId(String omServiceId) {
      this.omServiceId = omServiceId;
      return this;
    }

    public ClusterStateResponse build() {
      Objects.requireNonNull(storageReport, "storageReport == null");

      return new ClusterStateResponse(this);
    }
  }

  public int getPipelines() {
    return pipelines;
  }

  public int getTotalDatanodes() {
    return totalDatanodes;
  }

  public int getHealthyDatanodes() {
    return healthyDatanodes;
  }

  public ClusterStorageReport getStorageReport() {
    return storageReport;
  }

  public int getContainers() {
    return containers;
  }

  public long getVolumes() {
    return volumes;
  }

  public int getMissingContainers() {
    return missingContainers;
  }

  public int getOpenContainers() {
    return openContainers;
  }

  public int getDeletedContainers() {
    return deletedContainers;
  }

  public long getBuckets() {
    return buckets;
  }

  public long getKeys() {
    return keys;
  }

  public long getKeysPendingDeletion() {
    return keysPendingDeletion;
  }

  public long getDeletedDirs() {
    return deletedDirs;
  }

  public String getScmServiceId() {
    return scmServiceId;
  }

  public String getOmServiceId() {
    return omServiceId;
  }

}
