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

package org.apache.hadoop.ozone.container.common;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
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

  private final DatanodeConfiguration datanodeConf;

  private String tmpPath;

  public CleanUpManager(ConfigurationSource configurationSource,
                        HddsVolume hddsVolume) {
    this.datanodeConf =
        configurationSource.getObject(DatanodeConfiguration.class);
    tmpDirInit(hddsVolume);
  }

  public DatanodeConfiguration getDatanodeConf() {
    return datanodeConf;
  }

  public boolean checkContainerSchemaV3Enabled(
      KeyValueContainerData keyValueContainerData) {
    return (keyValueContainerData.getSchemaVersion()
        .equals(OzoneConsts.SCHEMA_V3));
  }

  private void tmpDirInit(HddsVolume hddsVolume) {
    //(String) path of /tmp from the datanode config
    String tmpDir = datanodeConf.getTmpDeleteDirectoryPath();

    // HddsVolume root directory path
    String hddsRoot = hddsVolume.getHddsRootDir().toString();

    // HddsVolume path
    String volPath = HddsVolumeUtil.getHddsRoot(hddsRoot);

    tmpPath = volPath + tmpDir;
    Path tmpDirPath = Paths.get(tmpPath);

    if (Files.notExists(tmpDirPath)) {
      try {
        Files.createDirectories(tmpDirPath);
      } catch (IOException e) {
        LOG.error("Error creating /tmp/container_delete_service", e);
      }
    }
  }

  public boolean renameDir(KeyValueContainerData keyValueContainerData)
      throws IOException {
    String containerPath = keyValueContainerData.getContainerPath();
    File container = new File(containerPath);
    String containerDirName = container.getName();

    String destinationDirPath = tmpPath + "/" + containerDirName;

    boolean success = container.renameTo(new File(destinationDirPath));

    if (success) {
      keyValueContainerData.setMetadataPath(destinationDirPath + "/metadata");
      keyValueContainerData.setChunksPath(destinationDirPath + "/chunks");
    }
    return success;
  }

  /**
   * Get direct files under /tmp/container_delete_service
   * and store them in a list.
   * @return iterator to the list of the leftover files
   */
  public ListIterator<File> getDeleteLeftovers() {
    List<File> leftovers = new ArrayList<File>();

    File tmpDir = new File(tmpPath);

    for (File file : tmpDir.listFiles()) {
      leftovers.add(file);
    }

    ListIterator<File> leftoversListIt = leftovers.listIterator();

    return leftoversListIt;
  }

  public boolean tmpDirIsEmpty() {
    ListIterator<File> leftoversListIt = getDeleteLeftovers();

    return leftoversListIt.hasNext();
  }

  /**
   * Delete all files under the /tmp/container_delete_service.
   * @throws IOException
   */
  public void cleanTmpDir() throws IOException {
    ListIterator<File> leftoversListIt = getDeleteLeftovers();

    while (leftoversListIt.hasNext()) {
      File file = leftoversListIt.next();
      if (file.isDirectory()) {
        FileUtils.deleteDirectory(file);
      } else {
        FileUtils.delete(file);
      }
    }
  }

  /**
   * Delete the /tmp/container_delete_service and all of its contents.
   * @throws IOException
   */
  public void deleteTmpDir() throws IOException {
    File deleteDir = new File(tmpPath);
    File tmpDir = deleteDir.getParentFile();
    FileUtils.deleteDirectory(tmpDir);
  }
}
