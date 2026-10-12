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

/**
 * Metadata object that contains per-disk storage information for a Datanode.
 */
public final class DatanodeDiskInfo {
  private String storageUuid;
  private String storageLocation;
  private long capacity;
  private long used;
  private long remaining;
  private long committed;
  private Long openContainerCount;

  private DatanodeDiskInfo(Builder builder) {
    this.storageUuid = builder.storageUuid;
    this.storageLocation = builder.storageLocation;
    this.capacity = builder.capacity;
    this.used = builder.used;
    this.remaining = builder.remaining;
    this.committed = builder.committed;
    this.openContainerCount = builder.openContainerCount;
  }

  public String getStorageUuid() {
    return storageUuid;
  }

  public String getStorageLocation() {
    return storageLocation;
  }

  public long getCapacity() {
    return capacity;
  }

  public long getUsed() {
    return used;
  }

  public long getRemaining() {
    return remaining;
  }

  public long getCommitted() {
    return committed;
  }

  public Long getOpenContainerCount() {
    return openContainerCount;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for DatanodeDiskInfo.
   */
  public static final class Builder {
    private String storageUuid = "";
    private String storageLocation = "";
    private long capacity = 0;
    private long used = 0;
    private long remaining = 0;
    private long committed = 0;
    private Long openContainerCount;

    private Builder() {
    }

    public Builder setStorageUuid(String storageUuid) {
      this.storageUuid = storageUuid;
      return this;
    }

    public Builder setStorageLocation(String storageLocation) {
      this.storageLocation = storageLocation;
      return this;
    }

    public Builder setCapacity(long capacity) {
      this.capacity = capacity;
      return this;
    }

    public Builder setUsed(long used) {
      this.used = used;
      return this;
    }

    public Builder setRemaining(long remaining) {
      this.remaining = remaining;
      return this;
    }

    public Builder setCommitted(long committed) {
      this.committed = committed;
      return this;
    }

    public Builder setOpenContainerCount(long openContainerCount) {
      this.openContainerCount = openContainerCount;
      return this;
    }

    public DatanodeDiskInfo build() {
      return new DatanodeDiskInfo(this);
    }
  }
}
