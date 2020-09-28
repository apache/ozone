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

package org.apache.hadoop.ozone.container.upgrade;


import static org.apache.hadoop.ozone.container.common.volume.HddsVolume.HDDS_VOLUME_DIR;
import static org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet.getDatanodeStorageDirs;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeatureCatalog.HDDSLayoutFeature;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.container.common.helpers.DatanodeVersionFile;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.upgrade.AbstractLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class to manage layout versions and features for Storage Container Manager
 * and DataNodes.
 */
@SuppressWarnings("FinalClass")
public class DataNodeLayoutVersionManager extends
    AbstractLayoutVersionManager {
  private static final Logger LOG = LoggerFactory.getLogger(
      DataNodeLayoutVersionManager.class);
  private static DataNodeLayoutVersionManager dataNodeLayoutVersionManager;

  private DataNodeLayoutVersionManager() {
  }

  /**
   * Read only instance to DataNode Version Manager.
   * @return version manager instance.
   */
  public static synchronized LayoutVersionManager getInstance() {
    if (dataNodeLayoutVersionManager == null) {
      throw new RuntimeException("DataNode Layout Version Manager not yet " +
          "initialized.");
    }
    return dataNodeLayoutVersionManager;
  }

  /**
   * Initialize DataNode version manager from version file stored on the
   * DataNode.
   * @param conf - Ozone Configuration
   * @return version manager instance.
   */
  public static synchronized DataNodeLayoutVersionManager initialize(
      ConfigurationSource conf)
      throws IOException {
    if (dataNodeLayoutVersionManager == null) {
      dataNodeLayoutVersionManager = new DataNodeLayoutVersionManager();
      int layoutVersion = 0;
      Collection<String> rawLocations = getDatanodeStorageDirs(conf);
      for (String locationString : rawLocations) {
        StorageLocation location = StorageLocation.parse(locationString);
        File hddsRootDir = new File(location.getUri().getPath(),
            HDDS_VOLUME_DIR);
        // Read the version from VersionFile Stored on the data node.
        File versionFile = HddsVolumeUtil.getVersionFile(hddsRootDir);
        if (!versionFile.exists()) {
          // Volume Root is non empty but VERSION file does not exist.
          LOG.warn("VERSION file does not exist in volume {},"
                  + " current volume state: {}.",
              hddsRootDir.getPath(), HddsVolume.VolumeState.INCONSISTENT);
          continue;
        } else {
          LOG.debug("Reading version file {} from disk.", versionFile);
        }
        Properties props = DatanodeVersionFile.readFrom(versionFile);
        if (props.isEmpty()) {
          continue;
        }
        int storedVersion = HddsVolumeUtil.getLayOutVersion(props, versionFile);
        if (storedVersion > layoutVersion) {
          layoutVersion = storedVersion;
        }
      }
      dataNodeLayoutVersionManager.init(layoutVersion,
          HDDSLayoutFeature.values());
    }
    return dataNodeLayoutVersionManager;
  }

  @VisibleForTesting
  protected synchronized static void resetLayoutVersionManager() {
    if (dataNodeLayoutVersionManager != null) {
      dataNodeLayoutVersionManager.reset();
      dataNodeLayoutVersionManager = null;
    }
  }
}
