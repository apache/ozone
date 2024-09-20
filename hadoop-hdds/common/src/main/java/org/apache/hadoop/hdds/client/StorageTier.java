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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
  private final boolean isUniform;
  private static StorageTier defaultTier = DISK;

  StorageTier(String tierName) {
    this.tierName = tierName;
    this.storageTypes = Collections.emptyList();
    this.isUniform = true;
  }

  // Constructor for uniform storage tiers
  StorageTier(String tierName, StorageType uniformStorageType) {
    this.tierName = tierName;
    this.storageTypes = Collections.singletonList(uniformStorageType);
    this.isUniform = true;
  }

  // Constructor for non-uniform storage tiers
  StorageTier(String tierName, StorageType... storageTypes) {
    this.tierName = tierName;
    long storageTypeCount = Arrays.stream(storageTypes).distinct().count();
    if (Arrays.stream(storageTypes).distinct().count() <= 1) {
      throw new IllegalArgumentException("StorageTier '" + tierName +
          "' requires at least two different StorageType instances." +
          " but only " + storageTypeCount +
          " StorageType were provided.");
    }
    this.storageTypes = Arrays.asList(storageTypes);
    this.isUniform = false;
  }

  public static StorageTier getDefaultTier() {
    return defaultTier;
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

  public boolean isUniform() {
    return isUniform;
  }

  public StorageType getUniformStorageType() {
    if (!isUniform()) {
      throw new IllegalArgumentException("Uniform storage type is not supported");
    }
    return storageTypes.get(0);
  }

  /**
   * Maps a StorageTier to its corresponding StorageType based on replication type.
   *
   * @param nodeCount The node count of the storageTier required.
   * @return The list of StorageTypes corresponding to the given tier and replication configuration.
   * @throws IllegalArgumentException if the replication configuration is not supported.
   */
  public List<StorageType> getStorageTypes(int nodeCount) {
    if (nodeCount < 0) {
      throw new IllegalArgumentException("Unsupported node count: " +
          nodeCount + " for StorageTier: " + getTierName());
    }

    if (isUniform()) {
      if (storageTypes.isEmpty()) {
        return Collections.emptyList();
      }
      return Collections.nCopies(nodeCount, storageTypes.get(0));
    }

    throw new UnsupportedOperationException(
        "Unsupported not uniform StorageTier: " + this);
  }
}
