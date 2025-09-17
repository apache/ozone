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

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metadata object that contains storage report of a Datanode.
 */
public final class DatanodeStorageReport {
  private String hostName;
  private long capacity;
  private long used;
  private long remaining;
  private long committed;
  private long pendingDeletion;
  private long minimumFreeSpace;

  private DatanodeStorageReport(Builder builder) {
    this.hostName = builder.hostName;
    this.capacity = builder.capacity;
    this.used = builder.used;
    this.remaining = builder.remaining;
    this.committed = builder.committed;
    this.pendingDeletion = builder.pendingDeletion;
    this.minimumFreeSpace = builder.minimumFreeSpace;
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

  public long getPendingDeletion() {
    return pendingDeletion;
  }

  public String getHostName() {
    return hostName;
  }

  public long getMinimumFreeSpace() {
    return minimumFreeSpace;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for DataNodeStorage Report.
   */
  public static final class Builder {
    private String hostName = "";
    private long capacity = 0;
    private long used = 0;
    private long remaining = 0;
    private long committed = 0;
    private long pendingDeletion = 0;
    private long minimumFreeSpace = 0;

    private static final Logger LOG =
        LoggerFactory.getLogger(Builder.class);

    private Builder() {
    }

    public Builder setHostName(String hostName) {
      this.hostName = hostName;
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

    public Builder setPendingDeletion(long pendingDeletion) {
      this.pendingDeletion = pendingDeletion;
      return this;
    }

    public Builder setMinimumFreeSpace(long minimumFreeSpace) {
      this.minimumFreeSpace = minimumFreeSpace;
      return this;
    }

    public void validate() {
      Objects.requireNonNull(hostName, "hostName cannot be null");

      if (capacity < 0) {
        throw new IllegalArgumentException("capacity cannot be negative");
      }
      if (used < 0) {
        throw new IllegalArgumentException("used cannot be negative");
      }
      if (remaining < 0) {
        throw new IllegalArgumentException("remaining cannot be negative");
      }
      if (committed < 0) {
        throw new IllegalArgumentException("committed cannot be negative");
      }
      if (pendingDeletion < 0) {
        throw new IllegalArgumentException("pendingDeletion cannot be negative");
      }
      // Logical consistency checks
      if (used + remaining > capacity) {
        LOG.warn("Inconsistent storage report for {}: used({}) + remaining({}) > capacity({})",
            hostName, used, remaining, capacity);
      }
    }

    public DatanodeStorageReport build() {
      return new DatanodeStorageReport(this);
    }
  }
}
