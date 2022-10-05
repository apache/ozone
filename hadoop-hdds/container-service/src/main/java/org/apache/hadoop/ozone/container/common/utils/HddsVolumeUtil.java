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

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.volume.DbVolume;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil.onFailure;

/**
 * A util class for {@link HddsVolume}.
 */
public final class HddsVolumeUtil {

  private static final Logger LOG =
      LoggerFactory.getLogger(HddsVolumeUtil.class);

  // Private constructor for Utility class. Unused.
  private HddsVolumeUtil() {
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
   * Initialize db instance, rocksdb will load the existing instance
   * if present and format a new one if not.
   * @param containerDBPath
   * @param conf
   * @throws IOException
   */
  public static void initPerDiskDBStore(String containerDBPath,
      ConfigurationSource conf) throws IOException {
    DatanodeStore store = BlockUtils.getUncachedDatanodeStore(containerDBPath,
        OzoneConsts.SCHEMA_V3, conf, false);
    BlockUtils.addDB(store, containerDBPath, conf, OzoneConsts.SCHEMA_V3);
  }

  /**
   * Load already formatted db instances for all HddsVolumes.
   * @param hddsVolumeSet
   * @param dbVolumeSet
   * @param logger
   */
  public static void loadAllHddsVolumeDbStore(MutableVolumeSet hddsVolumeSet,
      MutableVolumeSet dbVolumeSet, Logger logger) {
    // Scan subdirs under the db volumes and build a one-to-one map
    // between each HddsVolume -> DbVolume.
    mapDbVolumesToDataVolumesIfNeeded(hddsVolumeSet, dbVolumeSet);

    for (HddsVolume volume : StorageVolumeUtil.getHddsVolumesList(
        hddsVolumeSet.getVolumesList())) {
      try {
        volume.loadDbStore();
      } catch (IOException e) {
        onFailure(volume);
        if (logger != null) {
          logger.error("Load db store for HddsVolume {} failed",
              volume.getStorageDir().getAbsolutePath(), e);
        }
      }
    }
  }

  private static void mapDbVolumesToDataVolumesIfNeeded(
      MutableVolumeSet hddsVolumeSet, MutableVolumeSet dbVolumeSet) {
    if (dbVolumeSet == null || dbVolumeSet.getVolumesList().isEmpty()) {
      return;
    }

    List<HddsVolume> hddsVolumes = StorageVolumeUtil.getHddsVolumesList(
        hddsVolumeSet.getVolumesList());
    List<DbVolume> dbVolumes = StorageVolumeUtil.getDbVolumesList(
        dbVolumeSet.getVolumesList());
    Map<String, DbVolume> globalDbVolumeMap = new HashMap<>();

    // build a datanode global map of storageID -> dbVolume
    dbVolumes.forEach(dbVolume ->
        dbVolume.getHddsVolumeIDs().forEach(storageID ->
            globalDbVolumeMap.put(storageID, dbVolume)));

    // map each hddsVolume to a dbVolume
    hddsVolumes.forEach(hddsVolume ->
        hddsVolume.setDbVolume(globalDbVolumeMap.getOrDefault(
            hddsVolume.getStorageID(), null)));
  }

  /**
   * Get the HddsVolume according to the path.
   * @param volumes volume list to match from
   * @param pathStr path to match
   */
  public static HddsVolume matchHddsVolume(List<HddsVolume> volumes,
      String pathStr) throws IOException {
    assert pathStr != null;
    List<HddsVolume> resList = new ArrayList<>();
    for (HddsVolume hddsVolume: volumes) {
      if (pathStr.startsWith(hddsVolume.getVolumeRootDir())) {
        resList.add(hddsVolume);
      }
    }
    if (resList.size() == 1) {
      return resList.get(0);
    } else if (resList.size() > 1) {
      throw new IOException("Get multi volumes " +
          resList.stream().map(StorageVolume::getVolumeRootDir).collect(
              Collectors.joining(",")) + " matching path " + pathStr);
    }
    return null;
  }
}
