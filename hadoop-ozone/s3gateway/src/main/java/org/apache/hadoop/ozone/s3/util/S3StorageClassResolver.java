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

import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_STORAGE_CLASS_REDUCED_REDUNDANCY_STORAGE_POLICY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_STORAGE_CLASS_STANDARD_STORAGE_POLICY;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StoragePolicy;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;

/**
 * Maps between S3 storage class names and Ozone replication and storage policy.
 *
 * <p>The two directions are deliberately not symmetric:
 * <ul>
 *   <li><b>Forward</b> (request header to Ozone) is strict. An unknown name is the client's mistake
 *   and is rejected.</li>
 *   <li><b>Reverse</b> (stored key to response header) never fails. It describes data that already
 *   exists, so every combination of replication and storage policy maps to some class.
 *   Replication decides the class, and only a COLD storage policy changes it.</li>
 * </ul>
 */
public final class S3StorageClassResolver {

  /** EC layout used when neither the request nor the bucket specifies one. */
  private static final ECReplicationConfig DEFAULT_EC_REPLICATION_CONFIG =
      new ECReplicationConfig(3, 2);

  private S3StorageClassResolver() {
  }

  /**
   * Forward mapping: the spec for a write that requests the given storage class.
   *
   * <p>For {@code STANDARD} and {@code REDUCED_REDUNDANCY}, the storage policy comes from the
   * class's gateway setting. When the setting is empty, the spec has no storage policy and the
   * write inherits the bucket's.
   *
   * @param s3StorageClass the {@code x-amz-storage-class} header value
   * @param conf gateway configuration
   * @throws IllegalArgumentException if the storage class is unknown, or its configured storage
   *         policy is not valid
   */
  @Nonnull
  public static S3StorageClassSpec toSpec(String s3StorageClass, ConfigurationSource conf) {
    S3StorageClass storageClass = S3StorageClass.fromS3StorageClass(s3StorageClass);
    S3StorageClassSpec spec = storageClass.getSpec();
    switch (storageClass) {
    case STANDARD:
      return spec.withStoragePolicy(
          getConfiguredStoragePolicy(conf, OZONE_S3G_STORAGE_CLASS_STANDARD_STORAGE_POLICY));
    case REDUCED_REDUNDANCY:
      return spec.withStoragePolicy(getConfiguredStoragePolicy(conf,
          OZONE_S3G_STORAGE_CLASS_REDUCED_REDUNDANCY_STORAGE_POLICY));
    default:
      return spec;
    }
  }

  /**
   * The replication config for a write with the given spec.
   *
   * <p>For EC, the layout comes from the {@code storage-config} request header, then from the
   * bucket's replication if it is EC, and otherwise defaults to EC 3-2.
   *
   * @param spec the spec from {@link #toSpec}
   * @param storageConfigHeader the {@code storage-config} header value, or null
   * @param bucketReplicationConfig the bucket's default replication, or null
   * @throws IllegalArgumentException if the spec has no replication type, or
   *         {@code storageConfigHeader} is not a valid EC layout
   */
  @Nonnull
  public static ReplicationConfig toReplicationConfig(S3StorageClassSpec spec,
      @Nullable String storageConfigHeader, @Nullable ReplicationConfig bucketReplicationConfig) {
    ReplicationType type = spec.getReplicationType();
    if (type == ReplicationType.EC) {
      if (StringUtils.isNotEmpty(storageConfigHeader)) {
        return new ECReplicationConfig(storageConfigHeader);
      }
      if (bucketReplicationConfig instanceof ECReplicationConfig) {
        return bucketReplicationConfig;
      }
      return DEFAULT_EC_REPLICATION_CONFIG;
    }
    if (type == ReplicationType.RATIS) {
      return RatisReplicationConfig.getInstance(spec.getReplicationFactor());
    }
    throw new IllegalArgumentException("Storage class spec has no supported replication type: "
        + spec);
  }

  /**
   * Reverse mapping: the storage class to report for a stored key.
   *
   * <p>This never fails. Replication decides the class, and only a COLD storage policy changes it:
   * <pre>
   *   COLD + RATIS                       -&gt; GLACIER
   *   otherwise, by replication:
   *     EC                               -&gt; STANDARD_IA
   *     STAND_ALONE, or a single replica -&gt; REDUCED_REDUNDANCY
   *     anything else                    -&gt; STANDARD
   * </pre>
   * A null storage policy, as on keys written before storage policy support, uses replication
   * alone. A null replication reports {@code STANDARD}.
   *
   * @param replicationConfig the key's replication, or null
   * @param storagePolicy the key's storage policy, or null
   */
  @Nonnull
  public static S3StorageClass toS3StorageClass(@Nullable ReplicationConfig replicationConfig,
      @Nullable StoragePolicy storagePolicy) {
    if (replicationConfig == null) {
      return S3StorageClass.STANDARD;
    }
    if (storagePolicy == OzoneStoragePolicy.COLD
        && replicationConfig.getReplicationType() == ReplicationType.RATIS) {
      return S3StorageClass.GLACIER;
    }
    return fromReplicationConfig(replicationConfig);
  }

  /**
   * The storage class implied by replication alone. This is the mapping used before storage policy
   * support, kept unchanged so keys without a storage policy report the same class as before.
   */
  @Nonnull
  static S3StorageClass fromReplicationConfig(@Nonnull ReplicationConfig replicationConfig) {
    ReplicationType type = replicationConfig.getReplicationType();
    if (type == ReplicationType.EC) {
      return S3StorageClass.STANDARD_IA;
    }
    if (type == ReplicationType.STAND_ALONE || replicationConfig.getRequiredNodes() == 1) {
      return S3StorageClass.REDUCED_REDUNDANCY;
    }
    return S3StorageClass.STANDARD;
  }

  /**
   * Reads a per-class storage policy setting. Empty means no policy.
   *
   * @throws IllegalArgumentException if the value is not HOT or WARM
   */
  @Nullable
  private static StoragePolicy getConfiguredStoragePolicy(ConfigurationSource conf, String key) {
    String value = conf.getTrimmed(key, "");
    if (value.isEmpty()) {
      return null;
    }
    OzoneStoragePolicy policy;
    try {
      policy = OzoneStoragePolicy.valueOf(value.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid storage policy '" + value + "' for " + key
          + ". Expected HOT, WARM or empty.", e);
    }
    if (policy == OzoneStoragePolicy.COLD) {
      // COLD would make these writes report GLACIER instead of the class they asked for.
      throw new IllegalArgumentException("Storage policy COLD is not allowed for " + key
          + ". Expected HOT, WARM or empty.");
    }
    return policy;
  }
}
