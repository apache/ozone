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

/**
 * Represents the response structure for storage capacity distribution in the system.
 * Provides aggregated information about global storage, namespace, space usage breakdown,
 * and individual data node storage reports.
 *
 * The response contains the following key components:
 *
 * - Global storage statistics for the entire cluster.
 * - Namespace report providing high-level metadata of the namespace.
 * - Detailed breakdown of used storage space by category.
 * - A list of metadata reports pertaining to storage usage on individual data nodes.
 */
public class StorageCapacityDistributionResponse {

  @JsonProperty("globalStorage")
  private GlobalStorageReport globalStorage;

  @JsonProperty("globalNamespace")
  private GlobalNamespaceReport globalNamespace;

  @JsonProperty("usedSpaceBreakdown")
  private  UsedSpaceBreakDown usedSpaceBreakDown;

  @JsonProperty("dataNodeUsage")
  private List<DatanodeStorageReport> dataNodeUsage;

  public StorageCapacityDistributionResponse() {
  }

  public StorageCapacityDistributionResponse(Builder builder) {
    this.globalStorage = builder.globalStorage;
    this.globalNamespace = builder.globalNamespace;
    this.usedSpaceBreakDown = builder.usedSpaceBreakDown;
    this.dataNodeUsage = builder.dataNodeUsage;
  }

  public GlobalStorageReport getGlobalStorage() {
    return globalStorage;
  }

  public GlobalNamespaceReport getGlobalNamespace() {
    return globalNamespace;
  }

  public UsedSpaceBreakDown getUsedSpaceBreakDown() {
    return usedSpaceBreakDown;
  }

  public List<DatanodeStorageReport> getDataNodeUsage() {
    return dataNodeUsage;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for constructing instances of {@link StorageCapacityDistributionResponse}.
   *
   * This class follows the builder pattern, allowing for a flexible and readable
   * construction of a StorageCapacityDistributionResponse object. It provides
   * methods to set individual components of the response before building
   * the final object.
   *
   * The following components can be set using this builder:
   * - Global storage report, represented by {@link GlobalStorageReport}.
   * - Global namespace report, represented by {@link GlobalNamespaceReport}.
   * - Breakdown of used storage space by category, represented by {@link UsedSpaceBreakDown}.
   * - A list of data node storage usage reports, represented by {@link DatanodeStorageReport}.
   *
   * The build method generates a StorageCapacityDistributionResponse instance using
   * the values set in this builder. Unset values will remain null.
   */
  public static final class Builder {
    private GlobalStorageReport globalStorage;
    private GlobalNamespaceReport globalNamespace;
    private UsedSpaceBreakDown usedSpaceBreakDown;
    private List<DatanodeStorageReport> dataNodeUsage;

    public Builder() {
      this.globalStorage = null;
      this.globalNamespace = null;
      this.usedSpaceBreakDown = null;
      this.dataNodeUsage = null;
    }

    public Builder setGlobalStorage(GlobalStorageReport globalStorage) {
      this.globalStorage = globalStorage;
      return this;
    }

    public Builder setGlobalNamespace(GlobalNamespaceReport globalNamespace) {
      this.globalNamespace = globalNamespace;
      return this;
    }

    public Builder setDataNodeUsage(List<DatanodeStorageReport> dataNodeUsage) {
      this.dataNodeUsage = dataNodeUsage;
      return this;
    }

    public Builder setUsedSpaceBreakDown(UsedSpaceBreakDown usedSpaceBreakDown) {
      this.usedSpaceBreakDown = usedSpaceBreakDown;
      return this;
    }

    public StorageCapacityDistributionResponse build() {
      return new StorageCapacityDistributionResponse(this);
    }
  }
}
