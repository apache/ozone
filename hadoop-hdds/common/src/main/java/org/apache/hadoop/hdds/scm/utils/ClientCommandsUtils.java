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

package org.apache.hadoop.hdds.scm.utils;

import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * These methods should be merged with other similar utility classes.
 */
public final class ClientCommandsUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ClientCommandsUtils.class);

  /** Utility classes should not be constructed. **/
  private ClientCommandsUtils() {

  }

  public static ContainerProtos.ReadChunkVersion getReadChunkVersion(
      ContainerProtos.ReadChunkRequestProto readChunkRequest) {
    if (readChunkRequest.hasReadChunkVersion()) {
      return readChunkRequest.getReadChunkVersion();
    } else {
      return ContainerProtos.ReadChunkVersion.V0;
    }
  }

  public static ContainerProtos.ReadChunkVersion getReadChunkVersion(
      ContainerProtos.GetSmallFileRequestProto getSmallFileRequest) {
    if (getSmallFileRequest.hasReadChunkVersion()) {
      return getSmallFileRequest.getReadChunkVersion();
    } else {
      return ContainerProtos.ReadChunkVersion.V0;
    }
  }

  /**
   * Returns the write pipeline (component) version the datanode should execute the write at,
   * derived from the value the client forwarded from the SCM-provided pipeline version.
   * {@link HDDSVersion#ZDU} is the lowest version write-path versioning can use, so it is the
   * floor: any version below ZDU (including an absent field from a client predating zero downtime
   * upgrade support, or a value this datanode cannot deserialize) is rounded up to ZDU. ZDU is the
   * first value that is unambiguous across both the component-version and the (legacy)
   * layout-feature domains, so any {@code isAllowed} comparison on a datanode that has not
   * finalized ZDU is safe. No write-path versioning feature predates ZDU, so this loses no
   * behavior. New clients may still forward a pre-ZDU current version (e.g. when HDDS is not yet
   * finalized for ZDU); the datanode rounds it up here so there is no issue.
   */
  public static HDDSVersion getWritePipelineVersion(ContainerProtos.ContainerCommandRequestProto request) {
    // Absent, unrecognized (deserializes to UNKNOWN_VERSION == -1), and pre-ZDU versions all fall
    // below the ZDU floor, so a single comparison rounds every one of them up to ZDU.
    int serializedVersion = request.hasWritePipelineVersion()
        ? request.getWritePipelineVersion() : HDDSVersion.ZDU.serialize();
    HDDSVersion writeVersion = HDDSVersion.deserialize(serializedVersion);
    if (writeVersion == HDDSVersion.UNKNOWN_VERSION) {
      // Should not normally happen: the version originates from SCM's view of the datanodes.
      LOG.error("Datanode was given an unrecognized write pipeline version {}; using {} instead.",
          serializedVersion, HDDSVersion.ZDU);
    }
    return writeVersion.serialize() < HDDSVersion.ZDU.serialize() ? HDDSVersion.ZDU : writeVersion;
  }
}
