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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_NAME;

/**
 * This is the handler that process container upgrade checker.
 */
public class UpgradeChecker {

  /*
   * Verify that the datanode is in the shutdown state.
   */
  public Pair<Boolean, String> checkDatanodeNotStarted() {
    String command =
        "ps aux | grep org.apache.hadoop.ozone.HddsDatanodeService " +
            "| grep -v grep";
    try {
      Process exec = Runtime.getRuntime().exec(command);
      exec.waitFor(10, TimeUnit.SECONDS);
      if (exec.exitValue() != 0 && exec.exitValue() != 1) {

        return Pair.of(false,
            String.format("Return code of the command %s was %d", command,
                exec.exitValue()));
      } else {
        final boolean dnStarted =
            IOUtils.toString(exec.getInputStream(), StandardCharsets.UTF_8)
                .contains("proc_datanode");
        if (dnStarted) {
          return Pair.of(false,
              "This command needs to be executed with datanode turned off.");
        }
      }
    } catch (IOException | InterruptedException e) {

      return Pair.of(false,
          String.format("Run the command %s has error", command));

    }
    return Pair.of(true, "");
  }

  public Pair<Integer, Integer> getLayoutVersion(OzoneConfiguration conf)
      throws IOException {
    SCMStorageConfig scmStorageConfig = new SCMStorageConfig(conf);
    HDDSLayoutVersionManager scmLayoutVersionManager =
        new HDDSLayoutVersionManager(
            scmStorageConfig.getLayoutVersion());
    final int metadataLayoutVersion =
        scmLayoutVersionManager.getMetadataLayoutVersion();
    final int softwareLayoutVersion =
        scmLayoutVersionManager.getSoftwareLayoutVersion();

    return Pair.of(softwareLayoutVersion, metadataLayoutVersion);
  }

  public List<HddsVolume> getAllVolume(DatanodeDetails detail,
      OzoneConfiguration configuration) throws IOException {
    final MutableVolumeSet dataVolumeSet = UpgradeUtils
        .getHddsVolumes(configuration, StorageVolume.VolumeType.DATA_VOLUME,
            detail.getUuidString());
    return StorageVolumeUtil.getHddsVolumesList(dataVolumeSet.getVolumesList());
  }

  public List<File> getVolumeDBPath(List<HddsVolume> hddsVolumes) {
    List<File> dbPaths = new ArrayList<>();

    for (HddsVolume storageVolume : hddsVolumes) {
      File clusterIdDir =
          new File(storageVolume.getStorageDir(), storageVolume.getClusterID());
      File storageIdDir =
          new File(clusterIdDir, storageVolume.getStorageID());
      final File containerDBPath =
          new File(storageIdDir, CONTAINER_DB_NAME);

      if (containerDBPath.exists() && containerDBPath.isDirectory()) {
        dbPaths.add(containerDBPath);
      }
    }

    return dbPaths;
  }

  public Map<File, Exception> dbBackup(List<File> dbPaths) {
    Map<File, Exception> failDBDir = new ConcurrentHashMap<>();
    for (File dbPath : dbPaths) {
      try {
        final File backup =
            new File(dbPath.getParentFile(), dbPath.getName() + ".bak");
        if (backup.exists() || backup.mkdir()) {
          FileUtils.copyDirectory(dbPath, backup, true);
        }
      } catch (IOException e) {
        failDBDir.put(dbPath, e);
      }
    }
    return failDBDir;
  }

  public static boolean checkAlreadyMigrate(HddsVolume hddsVolume) {
    final File migrateFile = UpgradeUtils.getVolumeMigrateFile(hddsVolume);
    return migrateFile.exists();
  }

}
