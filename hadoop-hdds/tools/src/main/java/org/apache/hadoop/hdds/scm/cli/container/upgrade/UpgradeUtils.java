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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Date;

import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_NAME;

/**
 * Utils functions to help upgrade v2 to v3 container functions.
 */
public final class UpgradeUtils {

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
    Preconditions.checkNotNull(idFilePath);
    File idFile = new File(idFilePath);
    Preconditions.checkState(idFile.exists(),
        "Datanode id file: " + idFilePath + " not exists");
    return ContainerUtils.readDatanodeDetailsFrom(idFile);
  }

  public static File getContainerDBPath(HddsVolume volume) {
    return new File(volume.getDbParentDir(), CONTAINER_DB_NAME);
  }

  public static File getVolumeUpgradeCompleteFile(HddsVolume volume) {
    return new File(volume.getHddsRootDir(),
        UpgradeTask.UPGRADE_COMPLETE_FILE_NAME);
  }

  public static File getVolumeUpgradeLockFile(HddsVolume volume) {
    return new File(volume.getHddsRootDir(),
        UpgradeTask.UPGRADE_LOCK_FILE_NAME);
  }

  public static boolean createFile(File file) throws IOException {
    final Date date = new Date();
    try (Writer writer = new OutputStreamWriter(new FileOutputStream(file),
        StandardCharsets.UTF_8)) {
      writer.write(date.toString());
    }
    return file.exists();
  }

}
