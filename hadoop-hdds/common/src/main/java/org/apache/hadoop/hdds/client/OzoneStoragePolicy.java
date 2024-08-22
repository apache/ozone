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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StoragePolicyProto;

/**
 * Enum defining different storage policies using StorageTier.
 */
public enum OzoneStoragePolicy implements StoragePolicy {

  HOT("Hot", StorageTier.SSD, StorageTier.DISK),
  WARM("Warm", StorageTier.DISK, StorageTier.EMPTY),
  COLD("Cold", StorageTier.ARCHIVE, StorageTier.EMPTY);

  private final String name;
  private final StorageTier creationTier;
  private final StorageTier creationFallbackTier;

  OzoneStoragePolicy(String name, StorageTier creationTier,
      StorageTier creationFallbackTier) {
    this.name = name;
    this.creationTier = creationTier;
    this.creationFallbackTier = creationFallbackTier;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public StorageTier getCreationTier() {
    return creationTier;
  }

  @Override
  public StorageTier getCreationFallbackTier() {
    return creationFallbackTier;
  }

  /**
   * Converts the current StoragePolicyType to its protobuf representation.
   * @return the corresponding StoragePolicyProto.
   */
  public StoragePolicyProto toProto() {
    switch (this) {
    case HOT:
      return StoragePolicyProto.HOT;
    case WARM:
      return StoragePolicyProto.WARM;
    case COLD:
      return StoragePolicyProto.COLD;
    default:
      throw new IllegalArgumentException(
          "Error: StoragePolicyType not found, type=" + this);
    }
  }

  /**
   * Converts a protobuf StoragePolicyProto to the corresponding StoragePolicyType.
   * @param proto the StoragePolicyProto to convert.
   * @return the corresponding StoragePolicyType.
   */
  public static OzoneStoragePolicy fromProto(StoragePolicyProto proto) {
    if (proto == null) {
      throw new IllegalArgumentException("StoragePolicyProto cannot be null");
    }
    switch (proto) {
    case HOT:
      return HOT;
    case WARM:
      return WARM;
    case COLD:
      return COLD;
    default:
      throw new IllegalArgumentException("Error: StoragePolicyProto not found, proto=" + proto);
    }
  }

  @Override
  public String toString() {
    return "OzoneStoragePolicy{"
        + "name=" + name
        + ", creationTier=" + creationTier
        + ", creationFallbackTier=" + creationFallbackTier
        + '}';
  }
}
