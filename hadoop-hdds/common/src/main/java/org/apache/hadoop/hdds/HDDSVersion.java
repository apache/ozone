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

package org.apache.hadoop.hdds;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Versioning for datanode.
 */
public enum HDDSVersion implements ComponentVersion {

  //////////////////////////////  //////////////////////////////

  DEFAULT_VERSION(0, "Initial version"),

  SEPARATE_RATIS_PORTS_AVAILABLE(1, "Version with separated Ratis port."),
  COMBINED_PUTBLOCK_WRITECHUNK_RPC(2, "WriteChunk can optionally support " +
          "a PutBlock request"),
  STREAM_BLOCK_SUPPORT(3,
      "This version has support for reading a block by streaming chunks."),

  ZDU(100, "Version that supports zero downtime upgrade"),

  FUTURE_VERSION(-1, "Used internally in the client when the server side is "
      + " newer and an unknown server version has arrived to the client.");

  //////////////////////////////  //////////////////////////////

  private static final SortedMap<Integer, HDDSVersion> BY_VALUE =
      Arrays.stream(values())
          .collect(toMap(HDDSVersion::serialize, identity(), (v1, v2) -> v1, TreeMap::new));

  public static final HDDSVersion SOFTWARE_VERSION = BY_VALUE.get(BY_VALUE.lastKey());

  private final int version;
  private final String description;

  HDDSVersion(int version, String description) {
    this.version = version;
    this.description = description;
  }

  @Override
  public String description() {
    return description;
  }

  /**
   * @return The next version immediately following this one and excluding FUTURE_VERSION,
   *    or null if there is no such version.
   */
  @Override
  public HDDSVersion nextVersion() {
    int nextOrdinal = ordinal() + 1;
    if (nextOrdinal >= values().length - 1) {
      return null;
    }
    return values()[nextOrdinal];
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
  public static HDDSVersion deserialize(int value) {
    return BY_VALUE.getOrDefault(value, FUTURE_VERSION);
  }

  @Override
  public boolean isSupportedBy(int serializedVersion) {
    // In order for the other serialized version to support this version's features,
    // the other version must be equal or larger to this version.
    return deserialize(serializedVersion).compareTo(this) >= 0;
  }

  @Override
  public String toString() {
    return name() + " (" + serialize() + ")";
  }
}
