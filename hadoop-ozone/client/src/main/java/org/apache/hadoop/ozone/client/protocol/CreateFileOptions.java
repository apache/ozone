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

package org.apache.hadoop.ozone.client.protocol;

import jakarta.annotation.Nullable;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StoragePolicy;

/**
 * Options for {@link ClientProtocol#createFile(String, String, String, CreateFileOptions)}.
 * Encapsulates all optional parameters to allow future extensibility without adding a new overload for each of them.
 */
public final class CreateFileOptions {

  private final long size;
  private final ReplicationConfig replicationConfig;
  private final boolean overWrite;
  private final boolean recursive;
  private final StoragePolicy storagePolicy;

  private CreateFileOptions(Builder b) {
    this.size = b.size;
    this.replicationConfig = b.replicationConfig;
    this.overWrite = b.overWrite;
    this.recursive = b.recursive;
    this.storagePolicy = b.storagePolicy;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** @return the size of the data to be written. */
  public long getSize() {
    return size;
  }

  /** @return the replication config, or null to use the bucket's default. */
  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  /** @return true if an existing file at the location is overwritten. */
  public boolean isOverWrite() {
    return overWrite;
  }

  /** @return true if the file is created even if parent directories do not exist. */
  public boolean isRecursive() {
    return recursive;
  }

  /** @return the storage policy of the file, or null to inherit the bucket's. */
  @Nullable
  public StoragePolicy getStoragePolicy() {
    return storagePolicy;
  }

  @Override
  public String toString() {
    return "CreateFileOptions{size=" + size
        + ", replicationConfig=" + replicationConfig
        + ", overWrite=" + overWrite
        + ", recursive=" + recursive
        + ", storagePolicy=" + storagePolicy + "}";
  }

  /**
   * Builder for CreateFileOptions.
   */
  public static final class Builder {
    private long size;
    private ReplicationConfig replicationConfig;
    private boolean overWrite;
    private boolean recursive;
    private StoragePolicy storagePolicy;

    public Builder setSize(long size) {
      this.size = size;
      return this;
    }

    public Builder setReplicationConfig(ReplicationConfig replicationConfig) {
      this.replicationConfig = replicationConfig;
      return this;
    }

    public Builder setOverWrite(boolean overWrite) {
      this.overWrite = overWrite;
      return this;
    }

    public Builder setRecursive(boolean recursive) {
      this.recursive = recursive;
      return this;
    }

    public Builder setStoragePolicy(@Nullable StoragePolicy storagePolicy) {
      this.storagePolicy = storagePolicy;
      return this;
    }

    public CreateFileOptions build() {
      return new CreateFileOptions(this);
    }
  }
}
