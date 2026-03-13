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

package org.apache.hadoop.ozone;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.hdds.ComponentVersion;

/**
 * Versioning for Ozone Manager.
 */
public enum OzoneManagerVersion implements ComponentVersion {

  //////////////////////////////  //////////////////////////////

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

  S3_OBJECT_TAGGING_API(9, "OzoneManager version that supports S3 object tagging APIs, such as " +
      "PutObjectTagging, GetObjectTagging, and DeleteObjectTagging"),

  S3_PART_AWARE_GET(10, "OzoneManager version that supports S3 get for a specific multipart " +
                        "upload part number"),

  S3_LIST_MULTIPART_UPLOADS_PAGINATION(11,
      "OzoneManager version that supports S3 list multipart uploads API with pagination"),

  ZDU(100, "OzoneManager version that supports zero downtime upgrade"),

  FUTURE_VERSION(-1, "Used internally in the client when the server side is "
      + " newer and an unknown server version has arrived to the client.");

  //////////////////////////////  //////////////////////////////

  private static final SortedMap<Integer, OzoneManagerVersion> BY_VALUE =
      Arrays.stream(values())
          .collect(toMap(OzoneManagerVersion::serialize, identity(), (v1, v2) -> v1, TreeMap::new));

  public static final OzoneManagerVersion SOFTWARE_VERSION = BY_VALUE.get(BY_VALUE.lastKey());

  private final int version;
  private final String description;

  OzoneManagerVersion(int version, String description) {
    this.version = version;
    this.description = description;
  }

  @Override
  public String description() {
    return description;
  }

  @Override
  public int serialize() {
    return version;
  }

  /**
   * @param value The serialized version to convert.
   * @return The version corresponding to this serialized value, or {@link #FUTURE_VERSION} if no matching version is
   *    found.
   */
  public static OzoneManagerVersion deserialize(int value) {
    return BY_VALUE.getOrDefault(value, FUTURE_VERSION);
  }


  /**
   * @return The next version immediately following this one and excluding FUTURE_VERSION,
   *    or null if there is no such version.
   */
  @Override
  public OzoneManagerVersion nextVersion() {
    int nextOrdinal = ordinal() + 1;
    if (nextOrdinal >= values().length - 1) {
      return null;
    }
    return values()[nextOrdinal];
  }

  @Override
  public String toString() {
    return name() + " (" + serialize() + ")";
  }
}
