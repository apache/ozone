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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshotLocalData;
import org.apache.hadoop.ozone.om.OmSnapshotLocalData.VersionMeta;
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
  private final MutableGraph<LocalDataVersionNode> localDataGraph;
  private final Map<UUID, SnapshotVersionsMeta> versionNodeMap;
  private final OMMetadataManager omMetadataManager;

  public OmSnapshotLocalDataManager(OMMetadataManager omMetadataManager) throws IOException {
    this.localDataGraph = GraphBuilder.directed().build();
    this.omMetadataManager = omMetadataManager;
    this.snapshotLocalDataSerializer = new YamlSerializer<OmSnapshotLocalData>(
        new OmSnapshotLocalDataYaml.YamlFactory()) {

      @Override
      public void computeAndSetChecksum(Yaml yaml, OmSnapshotLocalData data) throws IOException {
        data.computeAndSetChecksum(yaml);
      }
    };
    this.versionNodeMap = new HashMap<>();
    init();
  }

  @VisibleForTesting
  Map<UUID, SnapshotVersionsMeta> getVersionNodeMap() {
    return versionNodeMap;
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
    return getSnapshotLocalPropertyYamlPath(snapshotInfo.getSnapshotId());
  }

  public String getSnapshotLocalPropertyYamlPath(UUID snapshotId) {
    Path snapshotPath = OmSnapshotManager.getSnapshotPath(omMetadataManager, snapshotId);
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
    return getOmSnapshotLocalData(snapshotInfo.getSnapshotId());
  }

  public OmSnapshotLocalData getOmSnapshotLocalData(UUID snapshotId) throws IOException {
    Path snapshotLocalDataPath = Paths.get(getSnapshotLocalPropertyYamlPath(snapshotId));
    OmSnapshotLocalData snapshotLocalData = snapshotLocalDataSerializer.load(snapshotLocalDataPath.toFile());
    if (!Objects.equals(snapshotLocalData.getSnapshotId(), snapshotId)) {
      throw new IOException("SnapshotId in path : " + snapshotLocalDataPath + " contains snapshotLocalData " +
          "corresponding to snapshotId " + snapshotLocalData.getSnapshotId() + ". Expected snapshotId " + snapshotId);
    }
    return snapshotLocalData;
  }

  public OmSnapshotLocalData getOmSnapshotLocalData(File snapshotDataPath) throws IOException {
    return snapshotLocalDataSerializer.load(snapshotDataPath);
  }

  private LocalDataVersionNode getVersionNode(UUID snapshotId, int version) {
    if (!versionNodeMap.containsKey(snapshotId)) {
      return null;
    }
    return versionNodeMap.get(snapshotId).getVersionNode(version);
  }

  private void addSnapshotVersionMeta(UUID snapshotId, SnapshotVersionsMeta snapshotVersionsMeta)
      throws IOException {
    if (!versionNodeMap.containsKey(snapshotId)) {
      for (LocalDataVersionNode versionNode : snapshotVersionsMeta.getSnapshotVersions().values()) {
        if (getVersionNode(versionNode.snapshotId, versionNode.version) != null) {
          throw new IOException("Unable to add " + versionNode + " since it already exists");
        }
        LocalDataVersionNode previousVersionNode = versionNode.previousSnapshotId == null ? null :
            getVersionNode(versionNode.previousSnapshotId, versionNode.previousSnapshotVersion);
        if (versionNode.previousSnapshotId != null && previousVersionNode == null) {
          throw new IOException("Unable to add " + versionNode + " since previous snapshot with version hasn't been " +
              "loaded");
        }
        localDataGraph.addNode(versionNode);
        if (previousVersionNode != null) {
          localDataGraph.putEdge(versionNode, previousVersionNode);
        }
      }
      versionNodeMap.put(snapshotId, snapshotVersionsMeta);
    }
  }

  void addVersionNodeWithDependents(OmSnapshotLocalData snapshotLocalData) throws IOException {
    if (versionNodeMap.containsKey(snapshotLocalData.getSnapshotId())) {
      return;
    }
    Set<UUID> visitedSnapshotIds = new HashSet<>();
    Stack<Pair<UUID, SnapshotVersionsMeta>> stack = new Stack<>();
    stack.push(Pair.of(snapshotLocalData.getSnapshotId(), new SnapshotVersionsMeta(snapshotLocalData)));
    while (!stack.isEmpty()) {
      Pair<UUID, SnapshotVersionsMeta> versionNodeToProcess = stack.peek();
      UUID snapId = versionNodeToProcess.getLeft();
      SnapshotVersionsMeta snapshotVersionsMeta = versionNodeToProcess.getRight();
      if (visitedSnapshotIds.contains(snapId)) {
        addSnapshotVersionMeta(snapId, snapshotVersionsMeta);
        stack.pop();
      } else {
        UUID prevSnapId = snapshotVersionsMeta.getPreviousSnapshotId();
        if (prevSnapId != null && !versionNodeMap.containsKey(prevSnapId)) {
          OmSnapshotLocalData prevSnapshotLocalData = getOmSnapshotLocalData(prevSnapId);
          stack.push(Pair.of(prevSnapshotLocalData.getSnapshotId(), new SnapshotVersionsMeta(prevSnapshotLocalData)));
        }
        visitedSnapshotIds.add(snapId);
      }
    }
  }

  private void init() throws IOException {
    RDBStore store = (RDBStore) omMetadataManager.getStore();
    String checkpointPrefix = store.getDbLocation().getName();
    File snapshotDir = new File(store.getSnapshotsParentDir());
    File[] localDataFiles = snapshotDir.listFiles(
        (dir, name) -> name.startsWith(checkpointPrefix) && name.endsWith(YAML_FILE_EXTENSION));
    if (localDataFiles == null) {
      throw new IOException("Error while listing yaml files inside directory: " + snapshotDir.getAbsolutePath());
    }
    Arrays.sort(localDataFiles, Comparator.comparing(File::getName));
    for (File localDataFile : localDataFiles) {
      OmSnapshotLocalData snapshotLocalData = snapshotLocalDataSerializer.load(localDataFile);
      File file = new File(getSnapshotLocalPropertyYamlPath(snapshotLocalData.getSnapshotId()));
      String expectedPath = file.getAbsolutePath();
      String actualPath = localDataFile.getAbsolutePath();
      if (!expectedPath.equals(actualPath)) {
        throw new IOException("Unexpected path for local data file with snapshotId:" + snapshotLocalData.getSnapshotId()
            + " : " + actualPath + ". " + "Expected: " + expectedPath);
      }
      addVersionNodeWithDependents(snapshotLocalData);
    }
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

  static final class LocalDataVersionNode {
    private final UUID snapshotId;
    private final int version;
    private final UUID previousSnapshotId;
    private final int previousSnapshotVersion;

    private LocalDataVersionNode(UUID snapshotId, int version, UUID previousSnapshotId, int previousSnapshotVersion) {
      this.previousSnapshotId = previousSnapshotId;
      this.previousSnapshotVersion = previousSnapshotVersion;
      this.snapshotId = snapshotId;
      this.version = version;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof LocalDataVersionNode)) {
        return false;
      }
      LocalDataVersionNode that = (LocalDataVersionNode) o;
      return version == that.version && previousSnapshotVersion == that.previousSnapshotVersion &&
          snapshotId.equals(that.snapshotId) && Objects.equals(previousSnapshotId, that.previousSnapshotId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(snapshotId, version, previousSnapshotId, previousSnapshotVersion);
    }
  }

  static final class SnapshotVersionsMeta {
    private final UUID previousSnapshotId;
    private final Map<Integer, LocalDataVersionNode> snapshotVersions;
    private int version;

    private SnapshotVersionsMeta(OmSnapshotLocalData snapshotLocalData) {
      this.previousSnapshotId = snapshotLocalData.getPreviousSnapshotId();
      this.snapshotVersions = getVersionNodes(snapshotLocalData);
      this.version = snapshotLocalData.getVersion();
    }

    private Map<Integer, LocalDataVersionNode> getVersionNodes(OmSnapshotLocalData snapshotLocalData) {
      UUID snapshotId = snapshotLocalData.getSnapshotId();
      UUID prevSnapshotId = snapshotLocalData.getPreviousSnapshotId();
      Map<Integer, LocalDataVersionNode> versionNodes = new HashMap<>();
      for (Map.Entry<Integer, VersionMeta> entry : snapshotLocalData.getVersionSstFileInfos().entrySet()) {
        versionNodes.put(entry.getKey(), new LocalDataVersionNode(snapshotId, entry.getKey(),
            prevSnapshotId, entry.getValue().getPreviousSnapshotVersion()));
      }
      return versionNodes;
    }

    UUID getPreviousSnapshotId() {
      return previousSnapshotId;
    }

    int getVersion() {
      return version;
    }

    Map<Integer, LocalDataVersionNode> getSnapshotVersions() {
      return snapshotVersions;
    }

    LocalDataVersionNode getVersionNode(int snapshotVersion) {
      return snapshotVersions.get(snapshotVersion);
    }
  }
}
