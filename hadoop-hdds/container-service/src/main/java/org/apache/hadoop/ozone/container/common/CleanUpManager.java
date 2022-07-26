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
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Helper class for moving the container to a new
 * location, before deletion.
 */
public class CleanUpManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(CleanUpManager.class);

  private final ConfigurationSource configurationSource;
  private DatanodeConfiguration datanodeConf;

  public CleanUpManager(ConfigurationSource configurationSource) {
    this.configurationSource = configurationSource;
    this.datanodeConf =
        configurationSource.getObject(DatanodeConfiguration.class);
  }

  public DatanodeConfiguration getDatanodeConf() {
    return datanodeConf;
  }

  public boolean checkContainerSchemaV3Enabled(
      KeyValueContainerData keyValueContainerData) {
    if (keyValueContainerData.getSchemaVersion()
        .equals(OzoneConsts.SCHEMA_V3)) {
      return true;
    } else {
      return false;
    }
  }

  public boolean renameDir(KeyValueContainerData keyValueContainerData)
      throws IOException {
    boolean success = false;
    String tmpDirPath = datanodeConf.getDiskTmpDirectoryPath();

    String containerPath = keyValueContainerData.getContainerPath();
    File container = new File(containerPath);

    String containerDirName = container.getName();

    String destinationDirPath = tmpDirPath + "/" + containerDirName;

    try {
      FileUtils.moveDirectory(container, new File(destinationDirPath));
      success = true;
    } catch (IOException ex) {
      LOG.error("Error while moving metadata and chunks under /tmp", ex);
    }

    keyValueContainerData.setMetadataPath(destinationDirPath + "/metadata");
    keyValueContainerData.setChunksPath(destinationDirPath + "/chunks");
    return success;
  }

  public boolean checkTmpDirIsEmpty() throws IOException {
    String tmpDirPath = datanodeConf.getDiskTmpDirectoryPath();
    File tmpDir = new File(tmpDirPath);

    try (Stream<Path> entries = Files.list(tmpDir.toPath())) {
      return !entries.findFirst().isPresent();
    }
  }

  /**
   * Get all filenames under /tmp and store them in a list.
   * @return list of the leftover filenames
   */
  public List<String> getDeleteLeftovers() {
    List<String> leftovers = new ArrayList<String>();
    String tmpDirPath = datanodeConf.getDiskTmpDirectoryPath();
    File tmpDir = new File(tmpDirPath);

    for (File file : tmpDir.listFiles()) {
      String fileName = file.getName();
      leftovers.add(fileName);
    }
    return leftovers;
  }

  /**
   * Delete all files under the /tmp.
   * @throws IOException
   */
  public void cleanTmpDir() throws IOException {
    String tmpDirPath = datanodeConf.getDiskTmpDirectoryPath();
    File tmpDir = new File(tmpDirPath);
    for (File file : tmpDir.listFiles()) {
      FileUtils.deleteDirectory(file);
    }
  }

  /**
   * Delete the /tmp and all of its contents.
   * @throws IOException
   */
  public void deleteTmpDir() throws IOException {
    String tmpDirPath = datanodeConf.getDiskTmpDirectoryPath();
    File tmpDir = new File(tmpDirPath);
    FileUtils.deleteDirectory(tmpDir);
  }
}
