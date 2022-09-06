/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.helpers;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * Helper class for handling /tmp/container_delete_service
 * operations used for container delete when Schema V3 is enabled.
 */
public class CleanUpManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(CleanUpManager.class);

  private static final String FILE_SEPARATOR = File.separator;

  private static final String TMP_DELETE_SERVICE_DIR =
      FILE_SEPARATOR + "tmp" + FILE_SEPARATOR + "container_delete_service";

  private Path tmpDirPath;

  public CleanUpManager(HddsVolume hddsVolume) {
    setTmpDirPath(hddsVolume);

    if (Files.notExists(tmpDirPath)) {
      try {
        Files.createDirectories(tmpDirPath);
      } catch (IOException ex) {
        LOG.error("Error creating {}", tmpDirPath.toString(), ex);
      }
    }
  }

  public Path getTmpDirPath() {
    return tmpDirPath;
  }

  public static boolean checkContainerSchemaV3Enabled(
      KeyValueContainerData keyValueContainerData) {
    return (keyValueContainerData.getSchemaVersion()
        .equals(OzoneConsts.SCHEMA_V3));
  }

  public static boolean checkContainerSchemaV3Enabled(
      ConfigurationSource config) {
    return VersionedDatanodeFeatures.SchemaV3
        .isFinalizedAndEnabled(config);
  }

  private void setTmpDirPath(HddsVolume hddsVolume) {
    StringBuilder stringBuilder = new StringBuilder();

    // HddsVolume root directory path
    String hddsRoot = hddsVolume.getHddsRootDir().toString();

    // HddsVolume path
    String volPath = HddsVolumeUtil.getHddsRoot(hddsRoot);

    stringBuilder.append(volPath);
    stringBuilder.append(FILE_SEPARATOR);

    String clusterId = "";
    try {
      clusterId += hddsVolume.getClusterID();

      if (clusterId == null) {
        throw new IOException();
      }
    } catch (IOException ex) {
      LOG.error("Volume has not been initialized, " +
          "clusterId is null.", ex);
    }

    String pathId = "";
    try {
      pathId += VersionedDatanodeFeatures.ScmHA
          .chooseContainerPathID(hddsVolume, clusterId);
    } catch (IOException ex) {
      LOG.error("Failed to get the container path Id", ex);
    }

    stringBuilder.append(pathId);
    stringBuilder.append(TMP_DELETE_SERVICE_DIR);

    String tmpPath = stringBuilder.toString();
    this.tmpDirPath = Paths.get(tmpPath);
  }

  public boolean renameDir(KeyValueContainerData keyValueContainerData) {
    String containerPath = keyValueContainerData.getContainerPath();
    File container = new File(containerPath);
    String containerDirName = container.getName();

    String destinationDirPath = tmpDirPath
        .resolve(Paths.get(containerDirName)).toString();

    boolean success = container.renameTo(new File(destinationDirPath));

    if (success) {
      keyValueContainerData.setMetadataPath(destinationDirPath +
          FILE_SEPARATOR + OzoneConsts.CONTAINER_META_PATH);
      keyValueContainerData.setChunksPath(destinationDirPath +
          FILE_SEPARATOR + OzoneConsts.STORAGE_DIR_CHUNKS);
    }
    return success;
  }

  /**
   * Get direct files under /tmp/container_delete_service
   * and store them in a list.
   * @return iterator to the list of the leftover files
   */
  public ListIterator<File> getDeleteLeftovers() {
    List<File> leftovers = new ArrayList<>();

    try {
      File tmpDir = new File(tmpDirPath.toString());

      for (File file : tmpDir.listFiles()) {
        leftovers.add(file);
      }
    } catch (NullPointerException ex) {
      LOG.error("Tmp directory is null, path doesn't exist", ex);
    }

    ListIterator<File> leftoversListIt = leftovers.listIterator();

    return leftoversListIt;
  }

  /**
   * @return true if tmpDir is empty
   */
  public boolean tmpDirIsEmpty() {
    ListIterator<File> leftoversListIt = getDeleteLeftovers();

    return !leftoversListIt.hasNext();
  }

  /**
   * Delete all files under the /tmp/container_delete_service.
   */
  public synchronized void cleanTmpDir() {
    ListIterator<File> leftoversListIt = getDeleteLeftovers();

    while (leftoversListIt.hasNext()) {
      File file = leftoversListIt.next();
      try {
        if (file.isDirectory()) {
          FileUtils.deleteDirectory(file);
        } else {
          FileUtils.delete(file);
        }
      } catch (IOException ex) {
        LOG.error("Failed to delete directory or file inside " +
            "/tmp/container_delete_service.", ex);
      }
    }
  }

  /**
   * Delete the /container_delete_service and all of its contents.
   * @throws IOException
   */
  public void deleteTmpDir() {
    File deleteDir = new File(tmpDirPath.toString());
    try {
      FileUtils.deleteDirectory(deleteDir);
    } catch (IOException ex) {
      LOG.error("Error deleting {}", deleteDir.getPath(), ex);
    }
  }
}
