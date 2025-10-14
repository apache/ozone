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

package org.apache.hadoop.ozone.om.snapshot;

import static org.apache.hadoop.ozone.om.OmSnapshotLocalDataYaml.YAML_FILE_EXTENSION;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshotLocalData;
import org.apache.hadoop.ozone.om.OmSnapshotLocalDataYaml;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.util.ObjectSerializer;
import org.apache.hadoop.ozone.util.YamlSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * Manages local data and metadata associated with Ozone Manager (OM) snapshots,
 * including the creation, storage, and representation of data as YAML files.
 */
public class OmSnapshotLocalDataManager implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(OmSnapshotLocalDataManager.class);

  private final ObjectSerializer<OmSnapshotLocalData> snapshotLocalDataSerializer;
  private final OMMetadataManager omMetadataManager;

  public OmSnapshotLocalDataManager(OMMetadataManager omMetadataManager) {
    this.omMetadataManager = omMetadataManager;
    this.snapshotLocalDataSerializer = new YamlSerializer<OmSnapshotLocalData>(
        new OmSnapshotLocalDataYaml.YamlFactory()) {

      @Override
      public void computeAndSetChecksum(Yaml yaml, OmSnapshotLocalData data) throws IOException {
        data.computeAndSetChecksum(yaml);
      }
    };
  }

  /**
   * Returns the path to the YAML file that stores local properties for the given snapshot.
   *
   * @param snapshotPath path to the snapshot checkpoint dir
   * @return the path to the snapshot's local property YAML file
   */
  public static String getSnapshotLocalPropertyYamlPath(Path snapshotPath) {
    return snapshotPath.toString() + YAML_FILE_EXTENSION;
  }

  /**
   * Returns the path to the YAML file that stores local properties for the given snapshot.
   *
   * @param snapshotInfo snapshot metadata
   * @return the path to the snapshot's local property YAML file
   */
  public String getSnapshotLocalPropertyYamlPath(SnapshotInfo snapshotInfo) {
    Path snapshotPath = OmSnapshotManager.getSnapshotPath(omMetadataManager, snapshotInfo);
    return getSnapshotLocalPropertyYamlPath(snapshotPath);
  }

  /**
   * Creates and writes snapshot local properties to a YAML file with not defragged SST file list.
   * @param snapshotStore snapshot metadata manager.
   * @param snapshotInfo snapshot info instance corresponding to snapshot.
   */
  public void createNewOmSnapshotLocalDataFile(RDBStore snapshotStore, SnapshotInfo snapshotInfo) throws IOException {
    Path snapshotLocalDataPath = Paths.get(
        getSnapshotLocalPropertyYamlPath(snapshotStore.getDbLocation().toPath()));
    Files.deleteIfExists(snapshotLocalDataPath);
    OmSnapshotLocalData snapshotLocalDataYaml = new OmSnapshotLocalData(snapshotInfo.getSnapshotId(),
        OmSnapshotManager.getSnapshotSSTFileList(snapshotStore), snapshotInfo.getPathPreviousSnapshotId());
    snapshotLocalDataSerializer.save(snapshotLocalDataPath.toFile(), snapshotLocalDataYaml);
  }

  public OmSnapshotLocalData getOmSnapshotLocalData(SnapshotInfo snapshotInfo) throws IOException {
    Path snapshotLocalDataPath = Paths.get(getSnapshotLocalPropertyYamlPath(snapshotInfo));
    return snapshotLocalDataSerializer.load(snapshotLocalDataPath.toFile());
  }

  public OmSnapshotLocalData getOmSnapshotLocalData(File snapshotDataPath) throws IOException {
    return snapshotLocalDataSerializer.load(snapshotDataPath);
  }

  @Override
  public void close() {
    if (snapshotLocalDataSerializer != null) {
      try {
        snapshotLocalDataSerializer.close();
      } catch (IOException e) {
        LOG.error("Failed to close snapshot local data serializer", e);
      }
    }
  }
}
