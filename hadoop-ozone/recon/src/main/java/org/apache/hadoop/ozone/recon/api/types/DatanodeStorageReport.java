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
  private String datanodeUuid;
  private String hostName;
  private long capacity;
  private long used;
  private long remaining;
  private long committed;
  private long minimumFreeSpace;
  private long reserved;
  private long filesystemCapacity;
  private long filesystemUsed;
  private long filesystemAvailable;

  public DatanodeStorageReport() {
  }

  private DatanodeStorageReport(Builder builder) {
    this.datanodeUuid = builder.datanodeUuid;
    this.hostName = builder.hostName;
    this.capacity = builder.capacity;
    this.used = builder.used;
    this.remaining = builder.remaining;
    this.committed = builder.committed;
    this.minimumFreeSpace = builder.minimumFreeSpace;
    this.reserved = builder.reserved;
    this.filesystemCapacity = builder.filesystemCapacity;
    this.filesystemUsed = builder.filesystemUsed;
    this.filesystemAvailable = builder.filesystemAvailable;
    builder.validate();
  }

  public String getDatanodeUuid() {
    return datanodeUuid;
  }

  public String getHostName() {
    return hostName;
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

  public long getMinimumFreeSpace() {
    return minimumFreeSpace;
  }

  public long getReserved() {
    return reserved;
  }

  public long getFilesystemCapacity() {
    return filesystemCapacity;
  }

  public long getFilesystemUsed() {
    return filesystemUsed;
  }

  public long getFilesystemAvailable() {
    return filesystemAvailable;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for DataNodeStorage Report.
   */
  public static final class Builder {
    private String datanodeUuid = "";
    private String hostName = "";
    private long capacity = 0;
    private long used = 0;
    private long remaining = 0;
    private long committed = 0;
    private long minimumFreeSpace = 0;
    private long reserved = 0;
    private long filesystemCapacity = 0;
    private long filesystemUsed = 0;
    private long filesystemAvailable = 0;

    private static final Logger LOG =
        LoggerFactory.getLogger(Builder.class);

    private Builder() {
    }

    public Builder setDatanodeUuid(String datanodeUuid) {
      this.datanodeUuid = datanodeUuid;
      return this;
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

    public Builder setMinimumFreeSpace(long minimumFreeSpace) {
      this.minimumFreeSpace = minimumFreeSpace;
      return this;
    }

    public Builder setReserved(long reserved) {
      this.reserved = reserved;
      return this;
    }

    public Builder setFilesystemCapacity(long filesystemCapacity) {
      this.filesystemCapacity = filesystemCapacity;
      return this;
    }

    public Builder setFilesystemUsed(long filesystemUsed) {
      this.filesystemUsed = filesystemUsed;
      return this;
    }

    public Builder setFilesystemAvailable(long filesystemAvailable) {
      this.filesystemAvailable = filesystemAvailable;
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

      if (minimumFreeSpace < 0) {
        throw new IllegalArgumentException("minimumFreeSpace cannot be negative");
      }

      if (reserved < 0) {
        throw new IllegalArgumentException("reserved cannot be negative");
      }

      if (filesystemCapacity < 0) {
        throw new IllegalArgumentException("filesystemCapacity cannot be negative");
      }

      if (filesystemAvailable < 0) {
        throw new IllegalArgumentException("filesystemAvailable cannot be negative");
      }

      if (filesystemUsed < 0) {
        throw new IllegalArgumentException("filesystemUsed cannot be negative");
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
