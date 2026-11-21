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

import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nullable;
import java.util.Map;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StoragePolicy;

/**
 * Options for {@link ClientProtocol#createKey(String, String, String, CreateKeyOptions)}.
 * Encapsulates all optional parameters to allow future extensibility without adding a new overload for each of them.
 */
public final class CreateKeyOptions {

  private final long size;
  private final ReplicationConfig replicationConfig;
  private final Map<String, String> metadata;
  private final Map<String, String> tags;
  private final boolean derivedKeyPiggyBacking;
  private final StoragePolicy storagePolicy;

  private CreateKeyOptions(Builder b) {
    this.size = b.size;
    this.replicationConfig = b.replicationConfig;
    this.metadata = b.metadata == null ? ImmutableMap.of() : ImmutableMap.copyOf(b.metadata);
    this.tags = b.tags == null ? ImmutableMap.of() : ImmutableMap.copyOf(b.tags);
    this.derivedKeyPiggyBacking = b.derivedKeyPiggyBacking;
    this.storagePolicy = b.storagePolicy;
  }

  /** @return the size of the data to be written. */
  public long getSize() {
    return size;
  }

  /** @return the replication config, or null to use the bucket's default. */
  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  /** @return the custom key metadata, or an empty map if none was set. Never null. */
  public Map<String, String> getMetadata() {
    return metadata;
  }

  /** @return the tags used for S3 object tags, or an empty map if none was set. Never null. */
  public Map<String, String> getTags() {
    return tags;
  }

  /** @return true if the derived key is piggybacked on the create request, when the OM supports it. */
  public boolean isDerivedKeyPiggyBacking() {
    return derivedKeyPiggyBacking;
  }

  /** @return the storage policy of the key, or null to inherit the bucket's. */
  @Nullable
  public StoragePolicy getStoragePolicy() {
    return storagePolicy;
  }

  /**
   * Builder for CreateKeyOptions.
   */
  public static final class Builder {
    private long size;
    private ReplicationConfig replicationConfig;
    private Map<String, String> metadata;
    private Map<String, String> tags;
    private boolean derivedKeyPiggyBacking;
    private StoragePolicy storagePolicy;

    public Builder setSize(long size) {
      this.size = size;
      return this;
    }

    public Builder setReplicationConfig(ReplicationConfig replicationConfig) {
      this.replicationConfig = replicationConfig;
      return this;
    }

    public Builder setMetadata(Map<String, String> metadata) {
      this.metadata = metadata;
      return this;
    }

    public Builder setTags(Map<String, String> tags) {
      this.tags = tags;
      return this;
    }

    public Builder setDerivedKeyPiggyBacking(boolean derivedKeyPiggyBacking) {
      this.derivedKeyPiggyBacking = derivedKeyPiggyBacking;
      return this;
    }

    public Builder setStoragePolicy(@Nullable StoragePolicy storagePolicy) {
      this.storagePolicy = storagePolicy;
      return this;
    }

    public CreateKeyOptions build() {
      return new CreateKeyOptions(this);
    }
  }
}
