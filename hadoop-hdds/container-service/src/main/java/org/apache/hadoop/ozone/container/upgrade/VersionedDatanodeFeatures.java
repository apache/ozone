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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;

import java.io.File;
import java.io.IOException;

/**
 * Utility class to retrieve the version of a feature that corresponds to the
 * metadata layout version specified by the provided
 * {@link HDDSLayoutVersionManager}.
 */
public final class VersionedDatanodeFeatures {
  private static HDDSLayoutVersionManager versionManager;

  private VersionedDatanodeFeatures() { }

  public static void initialize(
      HDDSLayoutVersionManager manager) {
    versionManager = manager;
  }

  public static boolean isFinalized(
      HDDSLayoutFeature layoutFeature) {
    // version manager can be null for testing. Use the latest version in
    // this case.
    return versionManager == null ||
        versionManager.getMetadataLayoutVersion() >=
            layoutFeature.layoutVersion();
  }

  /**
   * Utilities for container Schema V2 layout feature.
   * Containers created prior to the feature's finalization will use schema
   * v1, and schema v2 will be used for all containers created after
   * finalization.
   */
  public static class SchemaV2 {
    public static String chooseSchemaVersion() {
      if (isFinalized(HDDSLayoutFeature.DATANODE_SCHEMA_V2)) {
        return OzoneConsts.SCHEMA_V2;
      } else {
        return OzoneConsts.SCHEMA_V1;
      }
    }
  }

  /**
   * Utilities for SCM HA layout feature.
   * Prior to SCM HA finalization, datanode volumes used the format
   * {@literal <volume>/hdds/<scm-id>}. After SCM HA, the expected format is
   * {@literal <volume>/hdds/<cluster-id>}.
   *
   * In pre-finalize for SCM HA, datanodes
   * will still use the SCM ID for container file paths. The exception is if
   * the cluster is already using cluster ID paths (since SCM HA was merged
   * before the upgrade framework). In this case, cluster ID paths should
   * continue to be used.
   *
   * On finalization of SCM HA, datanodes
   * will create a symlink from the SCM ID directory to the cluster ID
   * directory and use cluster ID in container file paths. If a cluster ID
   * directory is already present, no changes are made.
   */
  public static class ScmHA {
    private static String scmID;

    public static void initialize(String scmId) {
      scmID = scmId;
    }

    public static String chooseContainerPathID(StorageVolume volume,
        String clusterID) {
      Preconditions.checkNotNull(scmID,
          "Not yet initialized with scmID");
      File clusterIDDir = new File(volume.getStorageDir(), clusterID);

      if (isFinalized(HDDSLayoutFeature.SCM_HA) || clusterIDDir.exists()) {
        return clusterID;
      } else {
        return scmID;
      }
    }

    public static boolean upgradeVolumeIfNeeded(StorageVolume volume,
        String clusterID) throws IOException {
      File clusterIDDir = new File(volume.getStorageDir(), clusterID);
      boolean needsUpgrade = isFinalized(HDDSLayoutFeature.SCM_HA) &&
          !clusterIDDir.exists();

      if (needsUpgrade) {
        ScmHAFinalizeUpgradeActionDatanode.upgradeVolume(volume, clusterID);
      }
      return needsUpgrade;
    }
  }
}
