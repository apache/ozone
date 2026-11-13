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

package org.apache.hadoop.hdds.upgrade;

import java.io.IOException;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.HDDSVersion;

/**
 * Component version manager for HDDS (Datanodes and SCM).
 */
public final class HDDSVersionUtils {
  private HDDSVersionUtils() {
  }

  /**
   * If the apparent version stored on the disk is &gt;= {@link HDDSVersion#ZDU} serialized, the apparent version is
   * resolved via {@link HDDSVersion#deserialize(int)}.
   * If the value is below that threshold, the apparent version is resolved as a {@link HDDSLayoutFeature}. Integers in
   * the gap between the largest {@link HDDSLayoutFeature} and ZDU are not valid legacy layout values.
   *
   * If the serialized version does not match any of these known versions, {@link HDDSVersion#UNKNOWN_VERSION} is
   * returned.
   */
  public static ComponentVersion deserializeHDDSVersionOrLayoutVersion(int serializedVersion) {
    if (serializedVersion >= HDDSVersion.ZDU.serialize()) {
      return HDDSVersion.deserialize(serializedVersion);
    } else {
      ComponentVersion fromLayout = HDDSLayoutFeature.deserialize(serializedVersion);
      if (fromLayout != null) {
        return fromLayout;
      } else {
        return HDDSVersion.UNKNOWN_VERSION;
      }
    }
  }

  /**
   * If the apparent version stored on the disk is &gt;= {@link HDDSVersion#ZDU} serialized, the apparent version is
   * resolved via {@link HDDSVersion#deserialize(int)}. Values with no matching {@link HDDSVersion} fail startup with
   * the persisted integer in the exception message.
   * If the value is below that threshold, the apparent version is resolved as a {@link HDDSLayoutFeature}. Integers in
   * the gap between the largest {@link HDDSLayoutFeature} and ZDU are not valid legacy layout values; startup fails
   * with the persisted integer in the exception message.
   */
  public static ComponentVersion deserializedPersistedApparentVersion(int serializedApparentVersion)
      throws IOException {
    ComponentVersion persistedVersion = deserializeHDDSVersionOrLayoutVersion(serializedApparentVersion);
    if (persistedVersion == HDDSVersion.UNKNOWN_VERSION) {
      throw new IOException("Initialization failed. Disk contains unknown apparent version "
          + serializedApparentVersion + " for software version " + HDDSVersion.SOFTWARE_VERSION
          + ". Make sure this component was not downgraded after finalization");
    }
    return persistedVersion;
  }
}
