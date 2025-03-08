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

package org.apache.hadoop.ozone.repair.datanode.schemaupgrade;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;

/**
 * This is the handler that process container upgrade checker.
 */
public class UpgradeChecker {

  public static Pair<HDDSLayoutFeature, HDDSLayoutFeature> getLayoutFeature(
      DatanodeDetails dnDetail, OzoneConfiguration conf) throws IOException {
    DatanodeLayoutStorage layoutStorage =
        new DatanodeLayoutStorage(conf, dnDetail.getUuidString());
    HDDSLayoutVersionManager layoutVersionManager =
        new HDDSLayoutVersionManager(layoutStorage.getLayoutVersion());

    final int metadataLayoutVersion =
        layoutVersionManager.getMetadataLayoutVersion();
    final HDDSLayoutFeature metadataLayoutFeature =
        (HDDSLayoutFeature) layoutVersionManager.getFeature(
            metadataLayoutVersion);

    final int softwareLayoutVersion =
        layoutVersionManager.getSoftwareLayoutVersion();
    final HDDSLayoutFeature softwareLayoutFeature =
        (HDDSLayoutFeature) layoutVersionManager.getFeature(
            softwareLayoutVersion);

    return Pair.of(softwareLayoutFeature, metadataLayoutFeature);
  }

  public static List<HddsVolume> getAllVolume(DatanodeDetails detail,
      OzoneConfiguration configuration) throws IOException {
    final MutableVolumeSet dataVolumeSet = UpgradeUtils
        .getHddsVolumes(configuration, StorageVolume.VolumeType.DATA_VOLUME,
            detail.getUuidString());
    return StorageVolumeUtil.getHddsVolumesList(dataVolumeSet.getVolumesList());
  }

  public static boolean isAlreadyUpgraded(HddsVolume hddsVolume) {
    final File migrateFile =
        UpgradeUtils.getVolumeUpgradeCompleteFile(hddsVolume);
    return migrateFile.exists();
  }
}
