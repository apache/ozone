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

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.UUID;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshotLocalDataYaml;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.yaml.snakeyaml.Yaml;

/**
 * Manages local data and metadata associated with Ozone Manager (OM) snapshots,
 * including the creation, storage, and representation of data as YAML files.
 */
public class OmSnapshotLocalDataManager implements AutoCloseable {

  private final GenericObjectPool<Yaml> yamlPool;
  private final MutableGraph<VersionLocalDataNode> localDataGraph;
  private final OMMetadataManager omMetadataManager;

  public OmSnapshotLocalDataManager(OMMetadataManager omMetadataManager) {
    this.yamlPool = new GenericObjectPool(new OmSnapshotLocalDataYaml.YamlFactory());
    this.localDataGraph = GraphBuilder.directed().build();
    this.omMetadataManager = omMetadataManager;
    init();
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
    OmSnapshotLocalDataYaml snapshotLocalDataYaml = new OmSnapshotLocalDataYaml(snapshotInfo.getSnapshotId(),
        OmSnapshotManager.getSnapshotSSTFileList(snapshotStore), snapshotInfo.getPathPreviousSnapshotId());
    snapshotLocalDataYaml.writeToYaml(this, snapshotLocalDataPath.toFile());
  }

  private void init() {
    RDBStore store = (RDBStore) omMetadataManager.getStore();
    String checkpointPrefix = store.getDbLocation().getName();
    File snapshotDir = new File(store.getSnapshotsParentDir());
    for (File yamlFile :
        Objects.requireNonNull(snapshotDir.listFiles(
            (dir, name) -> name.startsWith(checkpointPrefix) && name.endsWith(YAML_FILE_EXTENSION)))) {
      System.out.println(yamlFile.getAbsolutePath());
    }
  }

  @Override
  public void close() {
    if (yamlPool != null) {
      yamlPool.close();
    }
  }

  private final class VersionLocalDataNode {
    private UUID snapshotId;
    private int version;
    private UUID previousSnapshotId;
    private int previousSnapshotVersion;

    private VersionLocalDataNode(UUID snapshotId, int version, UUID previousSnapshotId, int previousSnapshotVersion) {
      this.previousSnapshotId = previousSnapshotId;
      this.previousSnapshotVersion = previousSnapshotVersion;
      this.snapshotId = snapshotId;
      this.version = version;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof VersionLocalDataNode)) {
        return false;
      }

      VersionLocalDataNode that = (VersionLocalDataNode) o;
      return version == that.version && previousSnapshotVersion == that.previousSnapshotVersion &&
          snapshotId.equals(that.snapshotId) && Objects.equals(previousSnapshotId, that.previousSnapshotId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(snapshotId, version, previousSnapshotId, previousSnapshotVersion);
    }
  }

  public UncheckedAutoCloseableSupplier<Yaml> getSnapshotLocalYaml() throws IOException {
    try {
      Yaml yaml = yamlPool.borrowObject();
      return new UncheckedAutoCloseableSupplier<Yaml>() {

        @Override
        public void close() {
          yamlPool.returnObject(yaml);
        }

        @Override
        public Yaml get() {
          return yaml;
        }
      };
    } catch (Exception e) {
      throw new IOException("Failed to get snapshot local yaml", e);
    }
  }

}
