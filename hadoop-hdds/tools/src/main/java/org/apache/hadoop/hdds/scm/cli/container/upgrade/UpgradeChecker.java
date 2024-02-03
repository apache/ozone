/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.cli.container.upgrade;

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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * This is the handler that process container upgrade checker.
 */
public class UpgradeChecker {

  /*
   * Verify that the datanode is in the shutdown state or running.
   */
  public Pair<Boolean, String> checkDatanodeRunning() {
    String command =
        "ps aux | grep org.apache.hadoop.ozone.HddsDatanodeService " +
            "| grep -v grep";
    try {
      Process exec = Runtime.getRuntime().exec(new String[]{"/bin/bash", "-c",
          command});
      boolean notTimeout = exec.waitFor(10, TimeUnit.SECONDS);
      if (!notTimeout) {
        return Pair.of(true,
            String.format("Execution of the command '%s' timeout", command));
      }
      if (exec.exitValue() == 0) {
        return Pair.of(true, "HddsDatanodeService is running." +
            " This upgrade command requires datanode to be off and in" +
            " the IN_MAINTENANCE mode. Please put the datanode in" +
            " the desired state first, then try this command later again.");
      } else if (exec.exitValue() == 1) {
        return Pair.of(false, "HddsDatanodeService is not running.");
      } else {
        return Pair.of(true,
            String.format("Return code of the command '%s' is %d", command,
                exec.exitValue()));
      }
    } catch (IOException | InterruptedException e) {
      return Pair.of(true,
          String.format("Run command '%s' has error '%s'",
              command, e.getMessage()));
    }
  }

  public Pair<HDDSLayoutFeature, HDDSLayoutFeature> getLayoutFeature(
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

  public List<HddsVolume> getAllVolume(DatanodeDetails detail,
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
