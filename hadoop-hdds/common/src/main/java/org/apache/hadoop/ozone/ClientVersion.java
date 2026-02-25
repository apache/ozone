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
import java.util.Comparator;
import java.util.Map;
import org.apache.hadoop.hdds.ComponentVersion;

/**
 * Versioning for protocol clients.
 */
public enum ClientVersion implements ComponentVersion {

  DEFAULT_VERSION(0, "Initial version"),

  VERSION_HANDLES_UNKNOWN_DN_PORTS(1,
      "Client version that handles the REPLICATION port in DatanodeDetails."),

  ERASURE_CODING_SUPPORT(2, "This client version has support for Erasure"
      + " Coding."),

  BUCKET_LAYOUT_SUPPORT(3,
      "This client version has support for Object Store and File " +
          "System Optimized Bucket Layouts."),

  FUTURE_VERSION(-1, "Used internally when the server side is older and an"
      + " unknown client version has arrived from the client.");

  public static final ClientVersion CURRENT = latest();
  public static final int CURRENT_VERSION = CURRENT.version;

  private static final Map<Integer, ClientVersion> BY_PROTO_VALUE =
      Arrays.stream(values())
          .collect(toMap(ClientVersion::toProtoValue, identity()));

  private final int version;
  private final String description;

  ClientVersion(int version, String description) {
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

  @Override
  public String toString() {
    return name() + " (" + serialize() + ")";
  }

  public static ClientVersion fromProtoValue(int value) {
    return BY_PROTO_VALUE.getOrDefault(value, FUTURE_VERSION);
  }

  private static ClientVersion latest() {
    return Arrays.stream(ClientVersion.values())
        .max(Comparator.comparingInt(ComponentVersion::toProtoValue)).orElse(null);
  }

}
