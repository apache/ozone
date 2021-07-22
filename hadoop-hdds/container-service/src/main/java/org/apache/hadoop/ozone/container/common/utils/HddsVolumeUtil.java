/**
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
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.InconsistentStorageStateException;
import org.apache.hadoop.ozone.container.common.HDDSVolumeLayoutVersion;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import sun.jvm.hotspot.debugger.posix.elf.ELFSectionHeader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;
import java.util.UUID;

/**
 * A util class for {@link HddsVolume}.
 */
public final class HddsVolumeUtil {

  // Private constructor for Utility class. Unused.
  private HddsVolumeUtil() {
  }

  private static final String VERSION_FILE   = "VERSION";
  private static final String STORAGE_ID_PREFIX = "DS-";

  public static File getVersionFile(File rootDir) {
    return new File(rootDir, VERSION_FILE);
  }

  public static String generateUuid() {
    return STORAGE_ID_PREFIX + UUID.randomUUID();
  }

  /**
   * Get hddsRoot from volume root. If volumeRoot points to hddsRoot, it is
   * returned as is.
   * For a volumeRoot /data/disk1, the hddsRoot is /data/disk1/hdds.
   * @param volumeRoot root of the volume.
   * @return hddsRoot of the volume.
   */
  public static String getHddsRoot(String volumeRoot) {
    if (volumeRoot.endsWith(HddsVolume.HDDS_VOLUME_DIR)) {
      return volumeRoot;
    } else {
      File hddsRoot = new File(volumeRoot, HddsVolume.HDDS_VOLUME_DIR);
      return hddsRoot.getPath();
    }
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
    if(HDDSVolumeLayoutVersion.getLatestVersion().getVersion() != lv) {
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
   * @param hddsVolume
   * @param scmId
   * @param clusterId
   * @param logger
   * @return true - if volume is in consistent state, otherwise false.
   */
  public static boolean checkVolume(HddsVolume hddsVolume, String scmId, String
      clusterId, Logger logger) {
    File hddsRoot = hddsVolume.getHddsRootDir();
    String volumeRoot = hddsRoot.getPath();
    File clusterDir = new File(hddsRoot, clusterId);
    File scmDir = new File(hddsRoot, scmId);
    String errorPrefix = "Volume " + volumeRoot + " is in an inconsistent " +
        "state.";

    try {
      hddsVolume.format(clusterId);
    } catch (IOException ex) {
      logger.error("Error during formatting volume {}.",
          volumeRoot, ex);
      return false;
    }

    File[] hddsFiles = hddsRoot.listFiles();

    if(hddsFiles == null) {
      // This is the case for IOException, where listFiles returns null.
      // So, we fail the volume.
      return false;
    } else if (hddsFiles.length == 1) {
      // DN started for first time or this is a newly added volume.
      // The one file is the version file.
      // So we create cluster ID directory.
      if (!clusterDir.mkdir()) {
        logger.error("Unable to create cluster directory {}", clusterDir);
        return false;
      }
      return true;
    } else if(hddsFiles.length == 2) {
      // If there are 2 files, they should be the version file and the
      // cluster ID directory.
      if (!clusterDir.exists()) {
        logger.error("{} 2 files found but cluster ID directory {} does not " +
            "exist.", errorPrefix, clusterDir);
        return false;
      }
    } else if(hddsFiles.length == 3) {
      // If there are 3 files, they should be version file, SCM ID directory,
      // and cluster ID symlink to SCM ID directory.
      if (!clusterDir.exists()) {
        logger.error("{} 3 files found but cluster ID directory {} does not " +
            "exist.", errorPrefix, clusterDir);
        return false;
      }

      if (!scmDir.exists()) {
        logger.error("{} 3 files found but SCM ID directory {} does not exist.",
            errorPrefix, scmDir);
        return false;
      }

      boolean symlinkCorrect;
      try {
        symlinkCorrect = Files.isSymbolicLink(clusterDir.toPath()) &&
            Files.readSymbolicLink(clusterDir.toPath()).equals(scmDir.toPath());
      } catch (IOException ex) {
        symlinkCorrect = false;
        logger.error("Error while reading {} as a symlink.",
            clusterDir.toPath(), ex);
      }

      if (symlinkCorrect) {
        return true;
      } else {
        logger.error("{} 3 files found but failed to cluster ID directory {} " +
            " as a symlink to SCM ID directory {}.", errorPrefix, clusterDir,
            scmDir);
        return false;
      }
    }

    logger.error("The HDDS root directory {} should contain a VERSION file," +
        " a cluster ID directory, and optionally an SCM ID directory. " +
    "Please remove any other extra files from the directory " +
        "so that DataNode startup can proceed.", hddsRoot.getAbsolutePath());
    return false;
  }
}
