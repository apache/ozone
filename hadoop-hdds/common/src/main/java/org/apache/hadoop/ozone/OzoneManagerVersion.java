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

  FUTURE_VERSION(-1, "Used internally in the client when the server side is "
      + " newer and an unknown server version has arrived to the client.");

  public static final OzoneManagerVersion CURRENT = latest();
  public static final int CURRENT_VERSION = CURRENT.version;

  private static final Map<Integer, OzoneManagerVersion> BY_PROTO_VALUE =
      Arrays.stream(values())
          .collect(toMap(OzoneManagerVersion::toProtoValue, identity()));

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
}
