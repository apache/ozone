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

import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_SEPARATOR;
import static org.apache.hadoop.ozone.om.OmSnapshotLocalDataYaml.YAML_FILE_EXTENSION;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshotLocalData;
import org.apache.hadoop.ozone.om.OmSnapshotLocalDataYaml;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.util.YamlSerializer;
import org.apache.ozone.compaction.log.SstFileInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.rocksdb.LiveFileMetaData;
import org.yaml.snakeyaml.Yaml;

/**
 * Test class for OmSnapshotLocalDataManager.
 */
public class TestOmSnapshotLocalDataManager {

  private static YamlSerializer<OmSnapshotLocalData> snapshotLocalDataYamlSerializer;

  @Mock
  private OMMetadataManager omMetadataManager;

  @Mock
  private RDBStore rdbStore;

  @Mock
  private RDBStore snapshotStore;

  @TempDir
  private Path tempDir;

  private OmSnapshotLocalDataManager localDataManager;
  private AutoCloseable mocks;

  private File snapshotsDir;

  @BeforeAll
  public static void setupClass() {
    snapshotLocalDataYamlSerializer = new YamlSerializer<OmSnapshotLocalData>(
        new OmSnapshotLocalDataYaml.YamlFactory()) {

      @Override
      public void computeAndSetChecksum(Yaml yaml, OmSnapshotLocalData data) throws IOException {
        data.computeAndSetChecksum(yaml);
      }
    };
  }

  @AfterAll
  public static void teardownClass() throws IOException {
    snapshotLocalDataYamlSerializer.close();
    snapshotLocalDataYamlSerializer = null;
  }

  @BeforeEach
  public void setUp() throws IOException {
    mocks = MockitoAnnotations.openMocks(this);
    
    // Setup mock behavior
    when(omMetadataManager.getStore()).thenReturn(rdbStore);

    this.snapshotsDir = tempDir.resolve("snapshots").toFile();
    FileUtils.deleteDirectory(snapshotsDir);
    assertTrue(snapshotsDir.exists() || snapshotsDir.mkdirs());
    File dbLocation = tempDir.resolve("db").toFile();
    FileUtils.deleteDirectory(dbLocation);
    assertTrue(dbLocation.exists() || dbLocation.mkdirs());

    
    when(rdbStore.getSnapshotsParentDir()).thenReturn(snapshotsDir.getAbsolutePath());
    when(rdbStore.getDbLocation()).thenReturn(dbLocation);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (localDataManager != null) {
      localDataManager.close();
    }
    if (mocks != null) {
      mocks.close();
    }
  }

  @Test
  public void testConstructor() throws IOException {
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    assertNotNull(localDataManager);
  }

  @Test
  public void testGetSnapshotLocalPropertyYamlPathWithSnapshotInfo() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    SnapshotInfo snapshotInfo = createMockSnapshotInfo(snapshotId, null);
    
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    
    File yamlPath = new File(localDataManager.getSnapshotLocalPropertyYamlPath(snapshotInfo));
    assertNotNull(yamlPath);
    Path expectedYamlPath = Paths.get(snapshotsDir.getAbsolutePath(), "db" + OM_SNAPSHOT_SEPARATOR + snapshotId
        + YAML_FILE_EXTENSION);
    assertEquals(expectedYamlPath.toAbsolutePath().toString(), yamlPath.getAbsolutePath());
  }

  @Test
  public void testCreateNewOmSnapshotLocalDataFile() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    SnapshotInfo snapshotInfo = createMockSnapshotInfo(snapshotId, null);
    
    // Setup snapshot store mock
    File snapshotDbLocation = OmSnapshotManager.getSnapshotPath(omMetadataManager, snapshotId).toFile();
    assertTrue(snapshotDbLocation.exists() || snapshotDbLocation.mkdirs());

    List<LiveFileMetaData> sstFiles = new ArrayList<>();
    sstFiles.add(createMockLiveFileMetaData("file1.sst", KEY_TABLE, "key1", "key7"));
    sstFiles.add(createMockLiveFileMetaData("file2.sst", KEY_TABLE, "key3", "key9"));
    sstFiles.add(createMockLiveFileMetaData("file3.sst", FILE_TABLE, "key1", "key7"));
    sstFiles.add(createMockLiveFileMetaData("file4.sst", FILE_TABLE, "key1", "key7"));
    sstFiles.add(createMockLiveFileMetaData("file5.sst", DIRECTORY_TABLE, "key1", "key7"));
    sstFiles.add(createMockLiveFileMetaData("file6.sst", "colFamily1", "key1", "key7"));
    List<SstFileInfo> sstFileInfos = IntStream.range(0, sstFiles.size() - 1)
        .mapToObj(sstFiles::get).map(SstFileInfo::new).collect(Collectors.toList());
    when(snapshotStore.getDbLocation()).thenReturn(snapshotDbLocation);
    RocksDatabase rocksDatabase = mock(RocksDatabase.class);
    when(snapshotStore.getDb()).thenReturn(rocksDatabase);
    when(rocksDatabase.getLiveFilesMetaData()).thenReturn(sstFiles);
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    
    localDataManager.createNewOmSnapshotLocalDataFile(snapshotStore, snapshotInfo);
    
    // Verify file was created
    OmSnapshotLocalData snapshotLocalData = localDataManager.getOmSnapshotLocalData(snapshotId);
    assertEquals(1, snapshotLocalData.getVersionSstFileInfos().size());
    OmSnapshotLocalData.VersionMeta versionMeta = snapshotLocalData.getVersionSstFileInfos().get(0);
    OmSnapshotLocalData.VersionMeta expectedVersionMeta = new OmSnapshotLocalData.VersionMeta(0, sstFileInfos);
    assertEquals(expectedVersionMeta, versionMeta);
  }

  @Test
  public void testGetOmSnapshotLocalDataWithSnapshotInfo() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    SnapshotInfo snapshotInfo = createMockSnapshotInfo(snapshotId, null);
    
    // Create and write snapshot local data file
    OmSnapshotLocalData localData = createMockLocalData(snapshotId, null);
    
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    
    // Write the file manually for testing
    Path yamlPath = Paths.get(localDataManager.getSnapshotLocalPropertyYamlPath(snapshotInfo.getSnapshotId()));
    writeLocalDataToFile(localData, yamlPath);
    
    // Test retrieval
    OmSnapshotLocalData retrieved = localDataManager.getOmSnapshotLocalData(snapshotInfo);
    
    assertNotNull(retrieved);
    assertEquals(snapshotId, retrieved.getSnapshotId());
  }

  @Test
  public void testGetOmSnapshotLocalDataWithMismatchedSnapshotId() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    UUID wrongSnapshotId = UUID.randomUUID();
    
    // Create local data with wrong snapshot ID
    OmSnapshotLocalData localData = createMockLocalData(wrongSnapshotId, null);
    
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    
    Path yamlPath = Paths.get(localDataManager.getSnapshotLocalPropertyYamlPath(snapshotId));
    writeLocalDataToFile(localData, yamlPath);
    // Should throw IOException due to mismatched IDs
    assertThrows(IOException.class, () -> {
      localDataManager.getOmSnapshotLocalData(snapshotId);
    });
  }

  @Test
  public void testGetOmSnapshotLocalDataWithFile() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    
    OmSnapshotLocalData localData = createMockLocalData(snapshotId, null);
    
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    
    Path yamlPath = tempDir.resolve("test-snapshot.yaml");
    writeLocalDataToFile(localData, yamlPath);
    
    OmSnapshotLocalData retrieved = localDataManager
        .getOmSnapshotLocalData(yamlPath.toFile());
    
    assertNotNull(retrieved);
    assertEquals(snapshotId, retrieved.getSnapshotId());
  }

  @Test
  public void testAddVersionNodeWithDependents() throws IOException {
    List<UUID> versionIds = Stream.of(UUID.randomUUID(), UUID.randomUUID())
        .sorted(Comparator.comparing(String::valueOf)).collect(Collectors.toList());
    UUID snapshotId = versionIds.get(0);
    UUID previousSnapshotId = versionIds.get(1);
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    // Create snapshot directory structure and files
    createSnapshotLocalDataFile(snapshotId, previousSnapshotId);
    createSnapshotLocalDataFile(previousSnapshotId, null);
    OmSnapshotLocalData localData = createMockLocalData(snapshotId, previousSnapshotId);
    
    // Should not throw exception
    localDataManager.addVersionNodeWithDependents(localData);
  }

  @Test
  public void testAddVersionNodeWithDependentsAlreadyExists() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    
    createSnapshotLocalDataFile(snapshotId, null);
    
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    
    OmSnapshotLocalData localData = createMockLocalData(snapshotId, null);
    
    // First addition
    localDataManager.addVersionNodeWithDependents(localData);
    
    // Second addition - should handle gracefully
    localDataManager.addVersionNodeWithDependents(localData);
  }

  @Test
  public void testInitWithExistingYamlFiles() throws IOException {
    List<UUID> versionIds = Stream.of(UUID.randomUUID(), UUID.randomUUID())
        .sorted(Comparator.comparing(String::valueOf)).collect(Collectors.toList());
    UUID snapshotId = versionIds.get(0);
    UUID previousSnapshotId = versionIds.get(1);
    
    createSnapshotLocalDataFile(previousSnapshotId, null);
    createSnapshotLocalDataFile(snapshotId, previousSnapshotId);
    
    // Initialize - should load existing files
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    
    assertNotNull(localDataManager);
    Map<UUID, OmSnapshotLocalDataManager.SnapshotVersionsMeta> versionMap =
        localDataManager.getVersionNodeMap();
    assertEquals(2, versionMap.size());
    assertEquals(versionMap.keySet(), new HashSet<>(versionIds));
  }

  @Test
  public void testInitWithInvalidPathThrowsException() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    
    // Create a file with wrong location
    OmSnapshotLocalData localData = createMockLocalData(snapshotId, null);
    Path wrongPath = Paths.get(snapshotsDir.getAbsolutePath(), "db-wrong-name.yaml");
    writeLocalDataToFile(localData, wrongPath);
    
    // Should throw IOException during init
    assertThrows(IOException.class, () -> {
      new OmSnapshotLocalDataManager(omMetadataManager);
    });
  }

  @Test
  public void testClose() throws IOException {
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    
    // Should not throw exception
    localDataManager.close();
  }

  // Helper methods

  private SnapshotInfo createMockSnapshotInfo(UUID snapshotId, UUID previousSnapshotId) {
    SnapshotInfo.Builder builder = SnapshotInfo.newBuilder()
        .setSnapshotId(snapshotId)
        .setName("snapshot-" + snapshotId);
    
    if (previousSnapshotId != null) {
      builder.setPathPreviousSnapshotId(previousSnapshotId);
    }
    
    return builder.build();
  }

  private LiveFileMetaData createMockLiveFileMetaData(String fileName, String columnFamilyName, String smallestKey,
      String largestKey) {
    LiveFileMetaData liveFileMetaData = mock(LiveFileMetaData.class);
    when(liveFileMetaData.columnFamilyName()).thenReturn(StringUtils.string2Bytes(columnFamilyName));
    when(liveFileMetaData.fileName()).thenReturn(fileName);
    when(liveFileMetaData.smallestKey()).thenReturn(StringUtils.string2Bytes(smallestKey));
    when(liveFileMetaData.largestKey()).thenReturn(StringUtils.string2Bytes(largestKey));
    return liveFileMetaData;
  }

  private OmSnapshotLocalData createMockLocalData(UUID snapshotId, UUID previousSnapshotId) {
    List<LiveFileMetaData> sstFiles = new ArrayList<>();
    sstFiles.add(createMockLiveFileMetaData("file1.sst", "columnFamily1", "key1", "key7"));
    sstFiles.add(createMockLiveFileMetaData("file2.sst", "columnFamily1", "key3", "key10"));
    sstFiles.add(createMockLiveFileMetaData("file3.sst", "columnFamily2", "key1", "key8"));
    sstFiles.add(createMockLiveFileMetaData("file4.sst", "columnFamily2", "key0", "key10"));
    return new OmSnapshotLocalData(snapshotId, sstFiles, previousSnapshotId);
  }

  private void createSnapshotLocalDataFile(UUID snapshotId, UUID previousSnapshotId)
      throws IOException {
    OmSnapshotLocalData localData = createMockLocalData(snapshotId, previousSnapshotId);
    
    String fileName = "db" + OM_SNAPSHOT_SEPARATOR + snapshotId.toString() + YAML_FILE_EXTENSION;
    Path yamlPath = Paths.get(snapshotsDir.getAbsolutePath(), fileName);
    
    writeLocalDataToFile(localData, yamlPath);
  }

  private void writeLocalDataToFile(OmSnapshotLocalData localData, Path filePath)
      throws IOException {
    // This is a simplified version - in real implementation, 
    // you would use the YamlSerializer
    snapshotLocalDataYamlSerializer.save(filePath.toFile(), localData);
  }
}
