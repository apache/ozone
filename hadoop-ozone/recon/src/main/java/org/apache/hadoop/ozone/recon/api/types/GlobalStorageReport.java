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
 * This class provides information about the total used space,
 * total free space, and the total storage capacity. It is used
 * to encapsulate and transport these metrics, which can assist
 * in monitoring and analyzing storage allocation and usage.
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
