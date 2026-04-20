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
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.upgrade.ComponentVersionManager;
import org.apache.hadoop.ozone.upgrade.UpgradeException;

/**
 * Component version manager for HDDS (Datanodes and SCM).
 */
public class HDDSVersionManager extends ComponentVersionManager {
  public HDDSVersionManager(Storage storage) throws IOException {
    super(storage, computeApparentVersion(storage.getApparentVersion()), HDDSVersion.SOFTWARE_VERSION);
  }

  /**
   * If the apparent version stored on the disk is &gt;= {@link HDDSVersion#ZDU} serialized, the apparent version is
   * resolved via {@link HDDSVersion#deserialize(int)}. Values with no matching {@link HDDSVersion} fail startup with
   * the persisted integer in the exception message.
   * If the value is below that threshold, the apparent version is resolved as a {@link HDDSLayoutFeature}. Integers in
   * the gap between the largest {@link HDDSLayoutFeature} and ZDU are not valid legacy layout values; startup fails
   * with the persisted integer in the exception message.
   */
  private static ComponentVersion computeApparentVersion(int serializedApparentVersion) throws IOException {
    if (serializedApparentVersion >= HDDSVersion.ZDU.serialize()) {
      HDDSVersion fromHdds = HDDSVersion.deserialize(serializedApparentVersion);
      if (fromHdds != HDDSVersion.FUTURE_VERSION) {
        return fromHdds;
      }
    } else {
      ComponentVersion fromLayout = HDDSLayoutFeature.deserialize(serializedApparentVersion);
      if (fromLayout != null) {
        return fromLayout;
      }
    }
    throw new IOException("Initialization failed. Disk contains unknown apparent version " + serializedApparentVersion +
        " for software version " + HDDSVersion.SOFTWARE_VERSION + ". Make sure this component was not downgraded" +
        " after finalization");
  }

  @Override
  protected void runUpgradeAction(ComponentVersion componentVersion) throws UpgradeException {
    // TODO HDDS-14826: Register upgrade actions based on annotations
  }
}
