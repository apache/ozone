/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.upgrade;

import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.OzoneConsts;

import java.io.IOException;

/**
 * Utility class to retrieve the version of a feature that corresponds to the
 * metadata layout version specified by the provided
 * {@link HDDSLayoutVersionManager}.
 */
public final class DatanodeMetadataFeatures {
  private static HDDSLayoutVersionManager versionManager;

  private DatanodeMetadataFeatures() { }

  public static synchronized void initialize(
      HDDSLayoutVersionManager manager) {
    versionManager = manager;
  }

  public static synchronized String getSchemaVersion() throws IOException {
    if (versionManager == null) {
      // version manager can be null for testing. Use the latest version in
      // this case.
      return OzoneConsts.SCHEMA_V2;
    } else if (versionManager.getMetadataLayoutVersion() <
        HDDSLayoutFeature.FIRST_UPGRADE_VERSION.layoutVersion()) {
      return OzoneConsts.SCHEMA_V1;
    } else {
      return OzoneConsts.SCHEMA_V2;
    }
  }
}
