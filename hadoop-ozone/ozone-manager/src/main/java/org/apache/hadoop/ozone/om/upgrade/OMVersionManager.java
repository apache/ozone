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

package org.apache.hadoop.ozone.om.upgrade;

import java.io.IOException;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.upgrade.ComponentVersionManager;

/**
 * Component version manager for Ozone Manager.
 */
public class OMVersionManager extends ComponentVersionManager {
  public OMVersionManager(int serializedApparentVersion) throws IOException {
    super(computeApparentVersion(serializedApparentVersion), OzoneManagerVersion.SOFTWARE_VERSION);
  }

  /**
   * If the apparent version stored on the disk is >= 100, it indicates the component has been finalized for the
   * ZDU feature, and the apparent version corresponds to a version in {@link OzoneManagerVersion}.
   * If the apparent version stored on the disk is < 100, it indicates the component is not yet finalized for the
   * ZDU feature, and the apparent version corresponds to a version in {@link OMLayoutFeature}.
   */
  private static ComponentVersion computeApparentVersion(int serializedApparentVersion) {
    if (serializedApparentVersion < OzoneManagerVersion.ZDU.serialize()) {
      return OMLayoutFeature.deserialize(serializedApparentVersion);
    } else {
      return OzoneManagerVersion.deserialize(serializedApparentVersion);
    }
  }

  // TODO HDDS-14826: Register upgrade actions based on annotations
}
