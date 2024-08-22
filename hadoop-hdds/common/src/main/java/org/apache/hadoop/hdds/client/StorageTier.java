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

package org.apache.hadoop.hdds.client;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTierProto;

/**
 * Ozone specific storage tiers.
 */
public enum StorageTier {
  SSD("SSD", StorageType.SSD),
  DISK("DISK", StorageType.DISK),
  ARCHIVE("ARCHIVE", StorageType.ARCHIVE),
  EMPTY("EMPTY");

  private final String tierName;
  private final List<StorageType> storageTypes;
  private final boolean uniformStorageType;
  private static final Map<StorageTier, Map<ReplicationConfig, List<StorageType>>>
      CACHE = new EnumMap<>(StorageTier.class);

  StorageTier(String tierName) {
    this.tierName = tierName;
    this.storageTypes = Collections.emptyList();
    this.uniformStorageType = true;
  }

  // Constructor for uniform storage tiers
  StorageTier(String tierName, StorageType uniformStorageType) {
    this.tierName = tierName;
    this.storageTypes = Collections.singletonList(uniformStorageType);
    this.uniformStorageType = true;
  }

  // Constructor for non-uniform storage tiers
  StorageTier(String tierName, StorageType... storageTypes) {
    this.tierName = tierName;
    if (Arrays.stream(storageTypes).distinct().count() <= 1) {
      throw new IllegalArgumentException("StorageTier '" + tierName +
          "' requires at least two different StorageType instances." +
          " but only " + Arrays.stream(storageTypes).distinct().count() +
          " StorageType were provided.");
    }
    this.storageTypes = Arrays.asList(storageTypes);
    this.uniformStorageType = false;
  }

  static {
    // Precompute storage type mappings for each replication config
    for (StorageTier tier : StorageTier.values()) {
      Map<ReplicationConfig, List<StorageType>> tierCache = new HashMap<>();
      List<ReplicationConfig> replicationConfigs = Arrays.asList(
          RatisReplicationConfig.getInstance(ONE),
          RatisReplicationConfig.getInstance(THREE),
          StandaloneReplicationConfig.getInstance(ONE),
          StandaloneReplicationConfig.getInstance(THREE)
      );

      for (ReplicationConfig config : replicationConfigs) {
        tierCache.put(config, tier.computeStorageTypes(config));
      }
      CACHE.put(tier, tierCache);
    }
  }

  public StorageTierProto toProto() {
    switch (this) {
    case SSD:
      return StorageTierProto.SSD_TIER;
    case DISK:
      return StorageTierProto.DISK_TIER;
    case ARCHIVE:
      return StorageTierProto.ARCHIVE_TIER;
    default:
      throw new IllegalStateException(
          "Illegal StorageTier: " + this);
    }
  }

  public static StorageTier fromProto(StorageTierProto tier) {
    switch (tier) {
    case SSD_TIER:
      return SSD;
    case DISK_TIER:
      return DISK;
    case ARCHIVE_TIER:
      return ARCHIVE;
    default:
      throw new IllegalStateException(
          "Illegal StorageTierProto: " + tier);
    }
  }

  public String getTierName() {
    return tierName;
  }

  public boolean isUniformStorageType() {
    return uniformStorageType;
  }

  /**
   * Computes the list of StorageTypes based on replication configuration.
   *
   * @param replicationConfig The replication configuration.
   * @return The list of StorageTypes for the given tier and replication configuration.
   */
  private List<StorageType> computeStorageTypes(
      ReplicationConfig replicationConfig) {
    if (isUniformStorageType()) {
      int numberOfNodes = replicationConfig.getRequiredNodes();
      if (storageTypes.isEmpty()) {
        return Collections.emptyList();
      }
      return new ArrayList<>(Collections.nCopies(numberOfNodes, storageTypes.get(0)));
    } else {
      throw new UnsupportedOperationException(
          "Unsupported not UniformStorage Storage Tier: " + replicationConfig);
    }
  }

  /**
   * Maps a StorageTier to its corresponding StorageType based on replication type.
   *
   * @param replicationConfig The replication configuration.
   * @return The list of StorageTypes corresponding to the given tier and replication configuration.
   * @throws IllegalArgumentException if the replication configuration is not supported.
   */
  public List<StorageType> getStorageTypes(
      ReplicationConfig replicationConfig) {
    Map<ReplicationConfig, List<StorageType>> tierCache = CACHE.get(this);

    if (tierCache != null) {
      List<StorageType> cachedStorageType = tierCache.get(replicationConfig);
      if (cachedStorageType != null) {
        return cachedStorageType;
      }
    }

    throw new IllegalArgumentException("Unsupported ReplicationConfig: " +
        replicationConfig + " for StorageTier: " + getTierName());
  }

}