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

package org.apache.hadoop.ozone.s3.util;

import jakarta.annotation.Nullable;
import java.util.Objects;
import org.apache.hadoop.hdds.client.StoragePolicy;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;

/**
 * What an S3 storage class means in Ozone.
 *
 * <p>Every field is optional. An unset field means the storage class takes no position on it, so
 * the value is resolved elsewhere, for example from the bucket. A new property is added as one more
 * optional field here, without changing {@link S3StorageClass} or its callers.
 *
 * <p>The spec holds a replication type rather than a full replication config. For EC, the layout is
 * resolved at write time by {@link S3StorageClassResolver}, so it is not fixed by the class.
 */
public final class S3StorageClassSpec {

  private final ReplicationType replicationType;
  private final ReplicationFactor replicationFactor;
  private final StoragePolicy storagePolicy;

  private S3StorageClassSpec(Builder b) {
    this.replicationType = b.replicationType;
    this.replicationFactor = b.replicationFactor;
    this.storagePolicy = b.storagePolicy;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /** Replication type, or null if the class does not set one. */
  @Nullable
  public ReplicationType getReplicationType() {
    return replicationType;
  }

  /** Replication factor for RATIS, or null if not set or not applicable. */
  @Nullable
  public ReplicationFactor getReplicationFactor() {
    return replicationFactor;
  }

  /** Storage policy, or null to inherit it from the bucket or the cluster default. */
  @Nullable
  public StoragePolicy getStoragePolicy() {
    return storagePolicy;
  }

  /** Returns a copy of this spec with the given storage policy. */
  S3StorageClassSpec withStoragePolicy(@Nullable StoragePolicy policy) {
    return newBuilder()
        .setReplicationType(replicationType)
        .setReplicationFactor(replicationFactor)
        .setStoragePolicy(policy)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof S3StorageClassSpec)) {
      return false;
    }
    S3StorageClassSpec that = (S3StorageClassSpec) o;
    return replicationType == that.replicationType
        && replicationFactor == that.replicationFactor
        && Objects.equals(storagePolicy, that.storagePolicy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(replicationType, replicationFactor, storagePolicy);
  }

  @Override
  public String toString() {
    return "S3StorageClassSpec{replicationType=" + replicationType
        + ", replicationFactor=" + replicationFactor
        + ", storagePolicy=" + storagePolicy + '}';
  }

  /**
   * Builder for {@link S3StorageClassSpec}. Fields left unset stay null.
   */
  public static final class Builder {
    private ReplicationType replicationType;
    private ReplicationFactor replicationFactor;
    private StoragePolicy storagePolicy;

    private Builder() {
    }

    public Builder setReplicationType(@Nullable ReplicationType type) {
      this.replicationType = type;
      return this;
    }

    public Builder setReplicationFactor(@Nullable ReplicationFactor factor) {
      this.replicationFactor = factor;
      return this;
    }

    public Builder setStoragePolicy(@Nullable StoragePolicy policy) {
      this.storagePolicy = policy;
      return this;
    }

    public S3StorageClassSpec build() {
      return new S3StorageClassSpec(this);
    }
  }
}
