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

package org.apache.hadoop.ozone.container.common.utils;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.InconsistentStorageStateException;
import org.apache.hadoop.ozone.container.common.HDDSVolumeLayoutVersion;
import org.apache.hadoop.ozone.container.common.volume.DbVolume;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A util class for {@link StorageVolume}.
 */
public final class StorageVolumeUtil {

  private static final String VERSION_FILE   = "VERSION";
  private static final String STORAGE_ID_PREFIX = "DS-";

  private StorageVolumeUtil() {
  }

  public static void onFailure(StorageVolume volume) {
    if (volume != null) {
      VolumeSet volumeSet = volume.getVolumeSet();
      if (volumeSet != null && volumeSet instanceof MutableVolumeSet) {
        ((MutableVolumeSet) volumeSet).checkVolumeAsync(volume);
      }
    }
  }

  public static List<HddsVolume> getHddsVolumesList(
      List<StorageVolume> volumes) {
    return volumes.stream().
        map(v -> (HddsVolume) v).collect(Collectors.toList());
  }

  public static List<DbVolume> getDbVolumesList(
      List<StorageVolume> volumes) {
    return volumes.stream().
        map(v -> (DbVolume) v).collect(Collectors.toList());
  }

  public static File getVersionFile(File rootDir) {
    return new File(rootDir, VERSION_FILE);
  }

  public static String generateUuid() {
    return STORAGE_ID_PREFIX + UUID.randomUUID();
  }

  /**
   * Returns storageID if it is valid. Throws an exception otherwise.
   */
  @VisibleForTesting
  public static String getStorageID(Properties props, File versionFile)
      throws InconsistentStorageStateException {
    return getProperty(props, OzoneConsts.STORAGE_ID, versionFile);
  }

  /**
   * Returns clusterID if it is valid. It should match the clusterID from the
   * Datanode. Throws an exception otherwise.
   */
  @VisibleForTesting
  public static String getClusterID(Properties props, File versionFile,
      String clusterID) throws InconsistentStorageStateException {
    String cid = getProperty(props, OzoneConsts.CLUSTER_ID, versionFile);

    if (clusterID == null) {
      return cid;
    }
    if (!clusterID.equals(cid)) {
      throw new InconsistentStorageStateException("Mismatched " +
          "ClusterIDs. Version File : " + versionFile + " has clusterID: " +
          cid + " and Datanode has clusterID: " + clusterID);
    }
    return cid;
  }

  /**
   * Returns datanodeUuid if it is valid. It should match the UUID of the
   * Datanode. Throws an exception otherwise.
   */
  @VisibleForTesting
  public static String getDatanodeUUID(Properties props, File versionFile,
      String datanodeUuid)
      throws InconsistentStorageStateException {
    String datanodeID = getProperty(props, OzoneConsts.DATANODE_UUID,
        versionFile);

    if (datanodeUuid != null && !datanodeUuid.equals(datanodeID)) {
      throw new InconsistentStorageStateException("Mismatched " +
          "DatanodeUUIDs. Version File : " + versionFile + " has datanodeUuid: "
          + datanodeID + " and Datanode has datanodeUuid: " + datanodeUuid);
    }
    return datanodeID;
  }

  /**
   * Returns creationTime if it is valid. Throws an exception otherwise.
   */
  @VisibleForTesting
  public static long getCreationTime(Properties props, File versionFile)
      throws InconsistentStorageStateException {
    String cTimeStr = getProperty(props, OzoneConsts.CTIME, versionFile);

    long cTime = Long.parseLong(cTimeStr);
    long currentTime = Time.now();
    if (cTime > currentTime || cTime < 0) {
      throw new InconsistentStorageStateException("Invalid Creation time in " +
          "Version File : " + versionFile + " - " + cTime + ". Current system" +
          " time is " + currentTime);
    }
    return cTime;
  }

  /**
   * Returns layOutVersion if it is valid. Throws an exception otherwise.
   */
  @VisibleForTesting
  public static int getLayOutVersion(Properties props, File versionFile) throws
      InconsistentStorageStateException {
    String lvStr = getProperty(props, OzoneConsts.LAYOUTVERSION, versionFile);

    int lv = Integer.parseInt(lvStr);
    if (HDDSVolumeLayoutVersion.getLatestVersion().getVersion() != lv) {
      throw new InconsistentStorageStateException("Invalid layOutVersion. " +
          "Version file has layOutVersion as " + lv + " and latest Datanode " +
          "layOutVersion is " +
          HDDSVolumeLayoutVersion.getLatestVersion().getVersion());
    }
    return lv;
  }

  public static String getProperty(
      Properties props, String propName, File
      versionFile
  )
      throws InconsistentStorageStateException {
    String value = props.getProperty(propName);
    if (StringUtils.isBlank(value)) {
      throw new InconsistentStorageStateException("Invalid " + propName +
          ". Version File : " + versionFile + " has null or empty " + propName);
    }
    return value;
  }

  /**
   * Check Volume is in consistent state or not.
   * Prior to SCM HA, volumes used the format {@code <volume>/hdds/<scm-id>}.
   * Post SCM HA, new volumes will use the format {@code <volume>/hdds/<cluster
   * -id>}.
   * Existing volumes using SCM ID would have been reformatted to have {@code
   * <volume>/hdds/<cluster-id>} as a symlink pointing to {@code <volume
   * >/hdds/<scm-id>}.
   *
   * @param volume
   * @param scmId
   * @param clusterId
   * @param conf
   * @param logger
   * @param dbVolumeSet
   * @return true - if volume is in consistent state, otherwise false.
   */
  public static boolean checkVolume(StorageVolume volume, String scmId,
      String clusterId, ConfigurationSource conf, Logger logger,
      MutableVolumeSet dbVolumeSet) {
    File volumeRoot = volume.getStorageDir();
    String volumeRootPath = volumeRoot.getPath();
    File clusterDir = new File(volumeRoot, clusterId);

    try {
      volume.format(clusterId);
    } catch (IOException ex) {
      logger.error("Error during formatting volume {}.",
          volumeRootPath, ex);
      return false;
    }

    File[] rootFiles = volumeRoot.listFiles();

    if (rootFiles == null) {
      // This is the case for IOException, where listFiles returns null.
      // So, we fail the volume.
      return false;
    } else if (rootFiles.length == 1) {
      // DN started for first time or this is a newly added volume.
      // The one file is the version file.
      // So we create cluster ID directory, or SCM ID directory if
      // pre-finalized for SCM HA.
      // Either the SCM ID or cluster ID will be used in naming the
      // volume's subdirectory, depending on the datanode's layout version.
      String id = VersionedDatanodeFeatures.ScmHA.chooseContainerPathID(conf,
          scmId, clusterId);
      try {
        volume.createWorkingDir(id, dbVolumeSet);
      } catch (IOException e) {
        logger.error("Prepare working dir failed for volume {}.",
            volumeRootPath, e);
        return false;
      }
      return true;
    } else if (rootFiles.length == 2) {
      // If we are finalized for SCM HA and there is no cluster ID directory,
      // the volume may have been unhealthy during finalization and been
      // skipped. Create cluster ID symlink now.
      // Else, We are still pre-finalized.
      // The existing directory should be left for backwards compatibility.
      return VersionedDatanodeFeatures.ScmHA.
          upgradeVolumeIfNeeded(volume, clusterId);
    } else {
      if (!clusterDir.exists()) {
        logger.error("Volume {} is in an inconsistent state. {} files found " +
            "but cluster ID directory {} does not exist.", volumeRootPath,
            rootFiles.length, clusterDir);
        return false;
      }
      return true;
    }
  }
}
