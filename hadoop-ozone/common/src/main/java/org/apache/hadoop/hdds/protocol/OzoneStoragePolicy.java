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

package org.apache.hadoop.hdds.protocol;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.StoragePolicyProto;

/**
 * Enum representing named storage policies that map semantic intent
 * (HOT, WARM, COLD) to physical {@link StorageType} values.
 *
 * <ul>
 *   <li><b>HOT</b> – primary SSD, fallback DISK</li>
 *   <li><b>WARM</b> – primary DISK, no fallback</li>
 *   <li><b>COLD</b> – primary ARCHIVE, no fallback</li>
 * </ul>
 */
public enum OzoneStoragePolicy {

  HOT(StorageType.SSD, StorageType.DISK),
  WARM(StorageType.DISK, null),
  COLD(StorageType.ARCHIVE, null);

  private final StorageType primaryType;
  private final StorageType fallbackType;

  OzoneStoragePolicy(StorageType primaryType, StorageType fallbackType) {
    this.primaryType = primaryType;
    this.fallbackType = fallbackType;
  }

  public StorageType getPrimaryStorageType() {
    return primaryType;
  }

  /**
   * Returns the fallback storage type, or {@code null} if no fallback
   * is available (write fails when primary is unavailable).
   */
  public StorageType getFallbackStorageType() {
    return fallbackType;
  }

  public static OzoneStoragePolicy getDefault() {
    return WARM;
  }

  public StoragePolicyProto toProto() {
    switch (this) {
    case HOT:
      return StoragePolicyProto.HOT;
    case WARM:
      return StoragePolicyProto.WARM;
    case COLD:
      return StoragePolicyProto.COLD;
    default:
      throw new IllegalStateException(
          "BUG: OzoneStoragePolicy not found, policy=" + this);
    }
  }

  /**
   * Converts a protobuf {@link StoragePolicyProto} to the corresponding
   * {@link OzoneStoragePolicy}. Returns {@code null} for
   * {@link StoragePolicyProto#STORAGE_POLICY_UNSET}, which means
   * "not set / inherit from parent".
   */
  public static OzoneStoragePolicy fromProto(StoragePolicyProto proto) {
    if (proto == null) {
      return null;
    }
    switch (proto) {
    case HOT:
      return HOT;
    case WARM:
      return WARM;
    case COLD:
      return COLD;
    case STORAGE_POLICY_UNSET:
      return null;
    default:
      throw new IllegalStateException(
          "BUG: StoragePolicyProto not found, proto=" + proto);
    }
  }

  /**
   * Case-insensitive parse from string. Intended for CLI / config parsing.
   *
   * @param policy the policy name (e.g. "hot", "WARM")
   * @return the matching {@link OzoneStoragePolicy}
   * @throws IllegalArgumentException if the string does not match any policy
   */
  public static OzoneStoragePolicy fromString(String policy) {
    return valueOf(policy.toUpperCase());
  }
}
