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

/**
 * Represents a report detailing global storage usage metrics.
 *
 * <p>This class encapsulates aggregated storage metrics across the cluster
 * and defines the relationship between filesystem capacity, reserved space,
 * and Ozone-managed storage.</p>
 *
 * <h3>Storage Hierarchy</h3>
 *
 * <pre>
 * {@code
 * Global Storage Layout
 *
 * |<------------------------ totalFileSystemCapacity ------------------------->|
 * |<-- totalReservedSpace -->|<----------- totalOzoneCapacity ---------------->|
 *                            |<-totalOzoneUsedSpace->|<- totalOzoneFreeSpace ->|
 *
 *
 * Relationships:
 *
 * totalFileSystemCapacity = totalOzoneCapacity + totalReservedSpace
 *
 * totalOzoneCapacity = totalOzoneUsedSpace + totalOzoneFreeSpace
 * }
 * </pre>
 *
 * <h3>Metric Definitions</h3>
 *
 * <ul>
 *   <li><b>totalFileSystemCapacity</b>:
 *       Total OS-reported filesystem capacity across all datanodes.</li>
 *
 *   <li><b>totalReservedSpace</b>:
 *       Space reserved and not available for Ozone allocation.</li>
 *
 *   <li><b>totalOzoneCapacity</b>:
 *       Portion of filesystem capacity available for Ozone
 *       (i.e., filesystem capacity minus reserved space).</li>
 *
 *   <li><b>totalOzoneUsedSpace</b>:
 *       Space currently consumed by Ozone data.</li>
 *
 *   <li><b>totalOzoneFreeSpace</b>:
 *       Remaining allocatable space within Ozone capacity.</li>
 *
 *   <li><b>totalOzonePreAllocatedContainerSpace</b>:
 *       Space pre-allocated for containers but not yet fully utilized.</li>
 *
 *   <li><b>totalMinimumFreeSpace</b>:
 *       Minimum free space that must be maintained as per configuration.</li>
 * </ul>
 *
 * <p>This differentiation helps in understanding how raw filesystem capacity
 * is divided between reserved space and Ozone-managed space, and further how
 * Ozone capacity is split between used and free storage.</p>
 */
public class GlobalStorageReport {

  @JsonProperty("totalFileSystemCapacity")
  private long totalFileSystemCapacity;

  @JsonProperty("totalReservedSpace")
  private long totalReservedSpace;

  @JsonProperty("totalOzoneCapacity")
  private long totalOzoneCapacity;

  @JsonProperty("totalOzoneUsedSpace")
  private long totalOzoneUsedSpace;

  @JsonProperty("totalOzoneFreeSpace")
  private long totalOzoneFreeSpace;

  @JsonProperty("totalOzonePreAllocatedContainerSpace")
  private long totalOzonePreAllocatedContainerSpace;

  @JsonProperty("totalMinimumFreeSpace")
  private long totalMinimumFreeSpace;

  public long getTotalFileSystemCapacity() {
    return totalFileSystemCapacity;
  }

  public long getTotalReservedSpace() {
    return totalReservedSpace;
  }

  public long getTotalOzoneCapacity() {
    return totalOzoneCapacity;
  }

  public long getTotalOzoneUsedSpace() {
    return totalOzoneUsedSpace;
  }

  public long getTotalOzoneFreeSpace() {
    return totalOzoneFreeSpace;
  }

  public long getTotalOzonePreAllocatedContainerSpace() {
    return totalOzonePreAllocatedContainerSpace;
  }

  public long getTotalMinimumFreeSpace() {
    return totalMinimumFreeSpace;
  }

  public GlobalStorageReport() {
  }

  public GlobalStorageReport(Builder builder) {
    this.totalFileSystemCapacity = builder.totalReservedSpace + builder.totalOzoneCapacity;
    this.totalReservedSpace = builder.totalReservedSpace;
    this.totalOzoneCapacity = builder.totalOzoneCapacity;
    this.totalOzoneUsedSpace = builder.totalOzoneUsedSpace;
    this.totalOzoneFreeSpace = builder.totalOzoneFreeSpace;
    this.totalOzonePreAllocatedContainerSpace = builder.totalOzonePreAllocatedContainerSpace;
    this.totalMinimumFreeSpace = builder.totalMinimumFreeSpace;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for creating an instance of GlobalStorageReport.
   * Provides a fluent interface to set various storage parameters.
   */
  public static final class Builder {
    private long totalReservedSpace;
    private long totalOzoneCapacity;
    private long totalOzoneUsedSpace;
    private long totalOzoneFreeSpace;
    private long totalOzonePreAllocatedContainerSpace;
    private long totalMinimumFreeSpace;

    public Builder() {
      totalReservedSpace = 0;
      totalOzoneCapacity = 0;
      totalOzoneUsedSpace = 0;
      totalOzoneFreeSpace = 0;
      totalOzonePreAllocatedContainerSpace = 0;
      totalMinimumFreeSpace = 0;
    }

    public Builder setTotalReservedSpace(long totalReservedSpace) {
      this.totalReservedSpace = totalReservedSpace;
      return this;
    }

    public Builder setTotalOzoneCapacity(long totalOzoneCapacity) {
      this.totalOzoneCapacity = totalOzoneCapacity;
      return this;
    }

    public Builder setTotalOzoneUsedSpace(long totalOzoneUsedSpace) {
      this.totalOzoneUsedSpace = totalOzoneUsedSpace;
      return this;
    }

    public Builder setTotalOzoneFreeSpace(long totalOzoneFreeSpace) {
      this.totalOzoneFreeSpace = totalOzoneFreeSpace;
      return this;
    }

    public Builder setTotalOzonePreAllocatedContainerSpace(long totalOzonePreAllocatedContainerSpace) {
      this.totalOzonePreAllocatedContainerSpace = totalOzonePreAllocatedContainerSpace;
      return this;
    }

    public Builder setTotalMinimumFreeSpace(long totalMinimumFreeSpace) {
      this.totalMinimumFreeSpace = totalMinimumFreeSpace;
      return this;
    }

    public GlobalStorageReport build() {
      return new GlobalStorageReport(this);
    }
  }
}
