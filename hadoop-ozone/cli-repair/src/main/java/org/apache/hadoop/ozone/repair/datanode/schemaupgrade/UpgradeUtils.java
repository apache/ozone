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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaTwoDBDefinition;

/**
 * Utils functions to help upgrade v2 to v3 container functions.
 */
final class UpgradeUtils {

  public static final Set<String> COLUMN_FAMILY_NAMES = Collections.unmodifiableSet(
      new DatanodeSchemaTwoDBDefinition("", new OzoneConfiguration())
          .getMap().keySet());

  public static final String BACKUP_CONTAINER_DATA_FILE_SUFFIX = ".backup";
  public static final String UPGRADE_COMPLETE_FILE_NAME = "upgrade.complete";
  public static final String UPGRADE_LOCK_FILE_NAME = "upgrade.lock";

  /** Never constructed. **/
  private UpgradeUtils() {

  }

  public static MutableVolumeSet getHddsVolumes(OzoneConfiguration conf,
      StorageVolume.VolumeType volumeType, String dnUuid) throws IOException {
    return new MutableVolumeSet(dnUuid, conf, null, volumeType, null);
  }

  public static DatanodeDetails getDatanodeDetails(OzoneConfiguration conf)
      throws IOException {
    String idFilePath = HddsServerUtil.getDatanodeIdFilePath(conf);
    Objects.requireNonNull(idFilePath, "idFilePath == null");
    File idFile = new File(idFilePath);
    Preconditions.checkState(idFile.exists(),
        "Datanode id file: " + idFilePath + " not exists");
    return ContainerUtils.readDatanodeDetailsFrom(idFile, conf);
  }

  public static File getVolumeUpgradeCompleteFile(HddsVolume volume) {
    return new File(volume.getHddsRootDir(), UPGRADE_COMPLETE_FILE_NAME);
  }

  public static File getVolumeUpgradeLockFile(HddsVolume volume) {
    return new File(volume.getHddsRootDir(), UPGRADE_LOCK_FILE_NAME);
  }

  public static boolean createFile(File file) throws IOException {
    final Date date = new Date();
    try (Writer writer = new OutputStreamWriter(Files.newOutputStream(file.toPath()),
        StandardCharsets.UTF_8)) {
      writer.write(date.toString());
    }
    return file.exists();
  }

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
    final MutableVolumeSet dataVolumeSet = getHddsVolumes(configuration, StorageVolume.VolumeType.DATA_VOLUME,
            detail.getUuidString());
    return StorageVolumeUtil.getHddsVolumesList(dataVolumeSet.getVolumesList());
  }

  public static boolean isAlreadyUpgraded(HddsVolume hddsVolume) {
    final File migrateFile =
        getVolumeUpgradeCompleteFile(hddsVolume);
    return migrateFile.exists();
  }
}
