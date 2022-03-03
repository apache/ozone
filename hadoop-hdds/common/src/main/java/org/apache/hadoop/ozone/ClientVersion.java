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

/**
 * Versioning for protocol clients.
 */
public enum ClientVersion {

  // old client, doesn't even send version number in requests
  DEFAULT_VERSION(0, "Initial version"),

  // DatanodeDetails#getFromProtobuf handles unknown types of ports
  VERSION_HANDLES_UNKNOWN_DN_PORTS(1,
      "Client version that handles the REPLICATION port in DatanodeDetails.");

  public static final ClientVersion CURRENT = latest();
  public static final int CURRENT_VERSION = CURRENT.version;

  private final int version;
  private final String description;

  ClientVersion(int version, String description) {
    this.version = version;
    this.description = description;
  }

  public int version() {
    return version;
  }

  public String description() {
    return description;
  }

  private static ClientVersion latest() {
    ClientVersion[] versions = ClientVersion.values();
    return versions[versions.length - 1];
  }

}
