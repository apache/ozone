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

package org.apache.hadoop.ozone.container.upgrade;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;

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
        versionManager.isAllowed(layoutFeature);
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
    /**
     * Choose whether to use cluster ID or SCM ID based on the format of the
     * volume and SCM HA finalization status.
     */
    public static String chooseContainerPathID(StorageVolume volume,
        String clusterID) throws IOException {
      File clusterIDDir = new File(volume.getStorageDir(), clusterID);

      // SCM ID may be null for testing, but these non-upgrade tests will use
      // the latest version with cluster ID anyways.
      if (isFinalized(HDDSLayoutFeature.SCM_HA) || clusterIDDir.exists()) {
        return clusterID;
      } else {
        File[] subdirs = volume.getStorageDir().listFiles(File::isDirectory);
        if (subdirs == null) {
          throw new IOException("Failed to read volume " +
              volume.getStorageDir());
        } else if (subdirs.length != 1) {
          throw new IOException("Invalid volume directory " +
              volume.getStorageDir() +
              " has more than one directory before SCM HA finalization.");
        }
        return subdirs[0].getName();
      }
    }

    public static boolean upgradeVolumeIfNeeded(StorageVolume volume,
        String clusterID) {
      File clusterIDDir = new File(volume.getStorageDir(), clusterID);
      boolean needsUpgrade = isFinalized(HDDSLayoutFeature.SCM_HA) &&
          !clusterIDDir.exists();

      boolean success = true;

      if (needsUpgrade) {
        success = ScmHAFinalizeUpgradeActionDatanode.upgradeVolume(volume,
            clusterID);
      }
      return success;
    }
  }

  /**
   * Utilities for container Schema V3 layout feature.
   * This schema put all container metadata info into a per-disk
   * rocksdb instance instead of a per-container instance.
   */
  public static class SchemaV3 {
    public static String chooseSchemaVersion(ConfigurationSource conf) {
      if (isFinalizedAndEnabled(conf)) {
        return OzoneConsts.SCHEMA_V3;
      } else {
        return SchemaV2.chooseSchemaVersion();
      }
    }

    public static boolean isFinalizedAndEnabled(ConfigurationSource conf) {
      DatanodeConfiguration dcf = conf.getObject(DatanodeConfiguration.class);
      if (isFinalized(HDDSLayoutFeature.DATANODE_SCHEMA_V3)
          && dcf.getContainerSchemaV3Enabled()) {
        return true;
      }
      return false;
    }
  }
}
