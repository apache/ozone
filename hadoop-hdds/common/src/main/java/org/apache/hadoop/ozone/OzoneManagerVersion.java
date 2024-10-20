/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.ComponentVersion;

import java.util.Arrays;
import java.util.Map;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

/**
 * Versioning for Ozone Manager.
 */
public enum OzoneManagerVersion implements ComponentVersion {
  DEFAULT_VERSION(0, "Initial version"),
  S3G_PERSISTENT_CONNECTIONS(1,
      "New S3G persistent connection support is present in OM."),
  ERASURE_CODED_STORAGE_SUPPORT(2, "OzoneManager version that supports"
      + "ECReplicationConfig"),
  OPTIMIZED_GET_KEY_INFO(3, "OzoneManager version that supports optimized"
      + " key lookups using cached container locations."),

  LIGHTWEIGHT_LIST_KEYS(4, "OzoneManager version that supports lightweight"
      + " listKeys API."),

  OBJECT_TAG(5, "OzoneManager version that supports object tags"),

  ATOMIC_REWRITE_KEY(6, "OzoneManager version that supports rewriting key as atomic operation"),
  HBASE_SUPPORT(7, "OzoneManager version that supports HBase integration"),
  LIGHTWEIGHT_LIST_STATUS(8, "OzoneManager version that supports lightweight"
      + " listStatus API."),

  FUTURE_VERSION(-1, "Used internally in the client when the server side is "
      + " newer and an unknown server version has arrived to the client.");

  public static final OzoneManagerVersion CURRENT = latest();

  private static final Map<Integer, OzoneManagerVersion> BY_PROTO_VALUE =
      Arrays.stream(values())
          .collect(toMap(OzoneManagerVersion::toProtoValue, identity()));
  private static final long SUPPORTED_FEATURE_BITMAP = calculateVersionBitmap();
  private final int version;
  private final String description;

  /**
   * This is limited to 63 because the version bitmap is stored in a `long` type,
   * which has 64 bits. One bit is reserved for each version number, starting from 0.
   * Therefore, the highest representable version number is 63.
   */
  public static final int MAX_SUPPORTED_VERSION = 63;

  static {
    for (OzoneManagerVersion version : OzoneManagerVersion.values()) {
      if (version.toProtoValue() > MAX_SUPPORTED_VERSION) {
        throw new IllegalStateException(
                "Version " + version + " exceeds the maximum supported version: " + MAX_SUPPORTED_VERSION);
      }
    }
  }

  OzoneManagerVersion(int version, String description) {
    this.version = version;
    this.description = description;
  }

  @Override
  public String description() {
    return description;
  }

  @Override
  public int toProtoValue() {
    return version;
  }

  public static OzoneManagerVersion fromProtoValue(int value) {
    return BY_PROTO_VALUE.getOrDefault(value, FUTURE_VERSION);
  }

  private static OzoneManagerVersion latest() {
    OzoneManagerVersion[] versions = OzoneManagerVersion.values();
    return versions[versions.length - 2];
  }

  /**
   * Checks if the given feature version is supported by the OM Service.
   *
   * @param omFeatureBitmap The feature bitmap from the remote OM.
   * @param checkedFeature  The feature to check.
   * @return true if the feature is supported, false otherwise.
   */
  public static boolean isOmFeatureSupported(long omFeatureBitmap, OzoneManagerVersion checkedFeature) {
    if (checkedFeature.toProtoValue() < 0) {
      return false;
    }
    return (omFeatureBitmap & (1L << checkedFeature .toProtoValue())) != 0;
  }

  /**
   * Get the current bitmap representing supported features.
   *
   * @return The bitmap of supported features.
   */
  public static long getSupportedFeatureBitmap() {
    return SUPPORTED_FEATURE_BITMAP;
  }

  /**
   * Calculate the version bitmap. Each bit position corresponds to a version.
   * Example: Version 0 -> bit 0, Version 1 -> bit 1, etc.
   * Note:
   * - Uses a 64-bit `long`, supporting versions 0 to 63.
   * - Versions beyond 63 are not supported.
   */
  private static long calculateVersionBitmap() {
    long bitmap = 0L;
    for (OzoneManagerVersion version : values()) {
      if (version.version >= 0) { // Ignore FUTURE_VERSION (-1)
        bitmap |= (1L << version.version);
      }
    }
    return bitmap;
  }
}
