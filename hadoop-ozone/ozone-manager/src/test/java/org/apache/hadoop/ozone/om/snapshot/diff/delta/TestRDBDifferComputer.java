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

package org.apache.hadoop.ozone.om.snapshot.diff.delta;

import static org.apache.hadoop.hdds.utils.IOUtils.getINode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshotLocalData;
import org.apache.hadoop.ozone.om.OmSnapshotLocalData.VersionMeta;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataProvider;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.SubStatus;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.apache.ozone.rocksdiff.DifferSnapshotInfo;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for RDBDifferComputer.
 */
public class TestRDBDifferComputer {

  @TempDir
  private Path tempDir;

  @Mock
  private OmSnapshotManager omSnapshotManager;

  @Mock
  private OMMetadataManager activeMetadataManager;

  @Mock
  private OmSnapshotLocalDataManager localDataManager;

  @Mock
  private RDBStore rdbStore;

  @Mock
  private RocksDBCheckpointDiffer differ;

  @Mock
  private Consumer<SubStatus> activityReporter;

  private AutoCloseable mocks;
  private Path deltaDirPath;
  private RDBDifferComputer rdbDifferComputer;

  @BeforeEach
  public void setUp() throws IOException {
    mocks = MockitoAnnotations.openMocks(this);
    deltaDirPath = tempDir.resolve("delta");
    when(omSnapshotManager.getSnapshotLocalDataManager()).thenReturn(localDataManager);
    when(activeMetadataManager.getStore()).thenReturn(rdbStore);
    when(rdbStore.getRocksDBCheckpointDiffer()).thenReturn(differ);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (rdbDifferComputer != null) {
      rdbDifferComputer.close();
    }
    if (mocks != null) {
      mocks.close();
    }
  }

  /**
   * Tests that the constructor creates RDBDifferComputer successfully with differ.
   */
  @Test
  public void testConstructorWithDiffer() throws IOException {
    rdbDifferComputer = new RDBDifferComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    assertNotNull(rdbDifferComputer, "RDBDifferComputer should be created");
    assertTrue(Files.exists(deltaDirPath), "Delta directory should be created");
    verify(activeMetadataManager, times(1)).getStore();
    verify(rdbStore, times(1)).getRocksDBCheckpointDiffer();
  }

  /**
   * Tests constructor when differ is null (fallback scenario).
   */
  @Test
  public void testConstructorWithNullDiffer() throws IOException {
    when(rdbStore.getRocksDBCheckpointDiffer()).thenReturn(null);

    rdbDifferComputer = new RDBDifferComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    assertNotNull(rdbDifferComputer, "RDBDifferComputer should be created even with null differ");
    assertTrue(Files.exists(deltaDirPath), "Delta directory should be created");
  }

  /**
   * Tests computeDeltaFiles with successful differ computation.
   */
  @Test
  public void testComputeDeltaFilesWithDiffer() throws IOException {
    rdbDifferComputer = new RDBDifferComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    UUID fromSnapshotId = UUID.randomUUID();
    UUID toSnapshotId = UUID.randomUUID();
    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1", fromSnapshotId);
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2", toSnapshotId);
    Set<String> tablesToLookup = ImmutableSet.of("keyTable");
    TablePrefixInfo tablePrefixInfo = mock(TablePrefixInfo.class);

    // Mock snapshot local data
    ReadableOmSnapshotLocalDataProvider snapProvider = mock(ReadableOmSnapshotLocalDataProvider.class);
    Optional<OmSnapshotLocalData> fromSnapshotLocalData = Optional.of(createMockSnapshotLocalData(fromSnapshotId, 1));
    OmSnapshotLocalData toSnapshotLocalData = createMockSnapshotLocalData(toSnapshotId, 2);

    when(snapProvider.getPreviousSnapshotLocalData()).thenReturn(fromSnapshotLocalData);
    when(snapProvider.getSnapshotLocalData()).thenReturn(toSnapshotLocalData);
    when(localDataManager.getOmSnapshotLocalData(toSnapshotId, fromSnapshotId)).thenReturn(snapProvider);

    // Create mock SST files
    Path sstFile1 = tempDir.resolve("sst1.sst");
    Path sstFile2 = tempDir.resolve("sst2.sst");
    Files.createFile(sstFile1);
    Files.createFile(sstFile2);

    SstFileInfo sstFileInfo1 = new SstFileInfo("sst1.sst", "key1", "key2", "keyTable");
    SstFileInfo sstFileInfo2 = new SstFileInfo("sst2.sst", "key3", "key4", "keyTable");

    Map<Path, SstFileInfo> differResult = new HashMap<>();
    differResult.put(sstFile1, sstFileInfo1);
    differResult.put(sstFile2, sstFileInfo2);

    when(differ.getSSTDiffListWithFullPath(any(DifferSnapshotInfo.class), any(DifferSnapshotInfo.class),
        any(Map.class), any(TablePrefixInfo.class), anySet())).thenReturn(Optional.of(differResult));

    Optional<Map<Path, Pair<Path, SstFileInfo>>> result =
        rdbDifferComputer.computeDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup, tablePrefixInfo);

    assertTrue(result.isPresent(), "Result should be present");
    assertEquals(2, result.get().size(), "Should have 2 delta files");
    assertTrue(result.get().containsKey(sstFile1), "Should contain first SST file");
    assertTrue(result.get().containsKey(sstFile2), "Should contain second SST file");

    // Verify links were created in delta directory
    for (Map.Entry<Path, Pair<Path, SstFileInfo>> entry : result.get().entrySet()) {
      Path actualPath = entry.getKey();
      Path link = entry.getValue().getLeft();
      assertEquals(differResult.get(actualPath), entry.getValue().getValue());
      assertTrue(link.startsWith(deltaDirPath), "Link should be in delta directory");
      assertTrue(Files.exists(link), "Link should exist");
      assertEquals(getINode(actualPath), getINode(link));
    }

    verify(snapProvider, times(1)).close();
  }

  /**
   * Tests computeDeltaFiles when differ returns empty.
   */
  @Test
  public void testComputeDeltaFilesWithEmptyDifferResult() throws IOException {
    rdbDifferComputer = new RDBDifferComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    UUID fromSnapshotId = UUID.randomUUID();
    UUID toSnapshotId = UUID.randomUUID();
    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1", fromSnapshotId);
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2", toSnapshotId);
    Set<String> tablesToLookup = ImmutableSet.of("keyTable");
    TablePrefixInfo tablePrefixInfo = mock(TablePrefixInfo.class);

    // Mock snapshot local data
    ReadableOmSnapshotLocalDataProvider snapProvider = mock(ReadableOmSnapshotLocalDataProvider.class);
    Optional<OmSnapshotLocalData> fromSnapshotLocalData = Optional.of(createMockSnapshotLocalData(fromSnapshotId, 1));
    OmSnapshotLocalData toSnapshotLocalData = createMockSnapshotLocalData(toSnapshotId, 2);

    when(snapProvider.getPreviousSnapshotLocalData()).thenReturn(fromSnapshotLocalData);
    when(snapProvider.getSnapshotLocalData()).thenReturn(toSnapshotLocalData);
    when(localDataManager.getOmSnapshotLocalData(toSnapshotId, fromSnapshotId)).thenReturn(snapProvider);

    when(differ.getSSTDiffListWithFullPath(any(DifferSnapshotInfo.class), any(DifferSnapshotInfo.class),
        any(Map.class), any(TablePrefixInfo.class), anySet())).thenReturn(Optional.empty());

    Optional<Map<Path, Pair<Path, SstFileInfo>>> result =
        rdbDifferComputer.computeDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup, tablePrefixInfo);

    assertFalse(result.isPresent(), "Result should be empty when differ returns empty");
    verify(snapProvider, times(1)).close();
  }

  /**
   * Tests computeDeltaFiles when differ is null.
   */
  @Test
  public void testComputeDeltaFilesWithNullDiffer() throws IOException {
    when(rdbStore.getRocksDBCheckpointDiffer()).thenReturn(null);
    rdbDifferComputer = new RDBDifferComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1", UUID.randomUUID());
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2", UUID.randomUUID());
    Set<String> tablesToLookup = ImmutableSet.of("keyTable");
    TablePrefixInfo tablePrefixInfo = mock(TablePrefixInfo.class);

    Optional<Map<Path, Pair<Path, SstFileInfo>>> result =
        rdbDifferComputer.computeDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup, tablePrefixInfo);

    assertFalse(result.isPresent(), "Result should be empty when differ is null");
  }

  /**
   * Tests computeDeltaFiles with multiple tables.
   */
  @Test
  public void testComputeDeltaFilesWithMultipleTables() throws IOException {
    rdbDifferComputer = new RDBDifferComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    UUID fromSnapshotId = UUID.randomUUID();
    UUID toSnapshotId = UUID.randomUUID();
    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1", fromSnapshotId);
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2", toSnapshotId);
    Set<String> tablesToLookup = ImmutableSet.of("keyTable", "fileTable", "directoryTable");
    TablePrefixInfo tablePrefixInfo = mock(TablePrefixInfo.class);

    // Mock snapshot local data
    ReadableOmSnapshotLocalDataProvider snapProvider = mock(ReadableOmSnapshotLocalDataProvider.class);
    Optional<OmSnapshotLocalData> fromSnapshotLocalData = Optional.of(createMockSnapshotLocalData(fromSnapshotId, 1));
    OmSnapshotLocalData toSnapshotLocalData = createMockSnapshotLocalData(toSnapshotId, 2);

    when(snapProvider.getPreviousSnapshotLocalData()).thenReturn(fromSnapshotLocalData);
    when(snapProvider.getSnapshotLocalData()).thenReturn(toSnapshotLocalData);
    when(localDataManager.getOmSnapshotLocalData(toSnapshotId, fromSnapshotId)).thenReturn(snapProvider);

    // Create mock SST files for different tables
    Path sstFile1 = tempDir.resolve("key1.sst");
    Path sstFile2 = tempDir.resolve("file1.sst");
    Path sstFile3 = tempDir.resolve("dir1.sst");
    Files.createFile(sstFile1);
    Files.createFile(sstFile2);
    Files.createFile(sstFile3);

    SstFileInfo sstFileInfo1 = new SstFileInfo("key1.sst", "key1", "key2", "keyTable");
    SstFileInfo sstFileInfo2 = new SstFileInfo("file1.sst", "file1", "file2", "fileTable");
    SstFileInfo sstFileInfo3 = new SstFileInfo("dir1.sst", "dir1", "dir2", "directoryTable");

    Map<Path, SstFileInfo> differResult = new HashMap<>();
    differResult.put(sstFile1, sstFileInfo1);
    differResult.put(sstFile2, sstFileInfo2);
    differResult.put(sstFile3, sstFileInfo3);

    when(differ.getSSTDiffListWithFullPath(any(DifferSnapshotInfo.class), any(DifferSnapshotInfo.class),
        any(Map.class), any(TablePrefixInfo.class), anySet())).thenReturn(Optional.of(differResult));

    Optional<Map<Path, Pair<Path, SstFileInfo>>> result =
        rdbDifferComputer.computeDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup, tablePrefixInfo);

    assertTrue(result.isPresent(), "Result should be present");
    assertEquals(3, result.get().size(), "Should have 3 delta files from different tables");
  }

  /**
   * Tests computeDeltaFiles with version mapping.
   */
  @Test
  public void testComputeDeltaFilesWithVersionMapping() throws IOException {
    rdbDifferComputer = new RDBDifferComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    UUID fromSnapshotId = UUID.randomUUID();
    UUID toSnapshotId = UUID.randomUUID();
    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1", fromSnapshotId);
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2", toSnapshotId);
    Set<String> tablesToLookup = ImmutableSet.of("keyTable");
    TablePrefixInfo tablePrefixInfo = mock(TablePrefixInfo.class);

    // Mock snapshot local data with version mapping
    ReadableOmSnapshotLocalDataProvider snapProvider = mock(ReadableOmSnapshotLocalDataProvider.class);
    Optional<OmSnapshotLocalData> fromSnapshotLocalData = Optional.of(createMockSnapshotLocalData(fromSnapshotId, 1));
    OmSnapshotLocalData toSnapshotLocalData = createMockSnapshotLocalDataWithVersions(toSnapshotId, 2);

    when(snapProvider.getPreviousSnapshotLocalData()).thenReturn(fromSnapshotLocalData);
    when(snapProvider.getSnapshotLocalData()).thenReturn(toSnapshotLocalData);
    when(localDataManager.getOmSnapshotLocalData(toSnapshotId, fromSnapshotId)).thenReturn(snapProvider);

    Path sstFile = tempDir.resolve("sst1.sst");
    Files.createFile(sstFile);
    SstFileInfo sstFileInfo = new SstFileInfo("sst1.sst", "key1", "key2", "keyTable");

    Map<Path, SstFileInfo> differResult = new HashMap<>();
    differResult.put(sstFile, sstFileInfo);

    when(differ.getSSTDiffListWithFullPath(any(DifferSnapshotInfo.class), any(DifferSnapshotInfo.class),
        any(Map.class), any(TablePrefixInfo.class), anySet())).thenReturn(Optional.of(differResult));

    Optional<Map<Path, Pair<Path, SstFileInfo>>> result =
        rdbDifferComputer.computeDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup, tablePrefixInfo);

    assertTrue(result.isPresent(), "Result should be present");

    // Verify that version map was passed to differ
    ArgumentCaptor<Map<Integer, Integer>> versionMapCaptor = ArgumentCaptor.forClass(Map.class);
    verify(differ).getSSTDiffListWithFullPath(any(DifferSnapshotInfo.class), any(DifferSnapshotInfo.class),
        versionMapCaptor.capture(), any(TablePrefixInfo.class), anySet());

    Map<Integer, Integer> capturedVersionMap = versionMapCaptor.getValue();
    assertNotNull(capturedVersionMap, "Version map should not be null");
    assertEquals(ImmutableMap.of(0, 0, 1, 0, 2, 1), capturedVersionMap);
  }

  /**
   * Tests that toDifferSnapshotInfo throws exception when no versions found.
   */
  @Test
  public void testToDifferSnapshotInfoWithNoVersions() throws IOException {
    rdbDifferComputer = new RDBDifferComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    UUID snapshotId = UUID.randomUUID();
    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1", snapshotId);
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2", UUID.randomUUID());
    Set<String> tablesToLookup = ImmutableSet.of("keyTable");
    TablePrefixInfo tablePrefixInfo = mock(TablePrefixInfo.class);

    // Mock snapshot local data with empty versions
    ReadableOmSnapshotLocalDataProvider snapProvider = mock(ReadableOmSnapshotLocalDataProvider.class);
    OmSnapshotLocalData fromSnapshotLocalDataMock = mock(OmSnapshotLocalData.class);
    OmSnapshotLocalData toSnapshotLocalData = createMockSnapshotLocalData(UUID.randomUUID(), 1);

    when(fromSnapshotLocalDataMock.getSnapshotId()).thenReturn(snapshotId);
    when(fromSnapshotLocalDataMock.getVersionSstFileInfos()).thenReturn(Collections.emptyMap());
    Optional<OmSnapshotLocalData> fromSnapshotLocalData = Optional.of(fromSnapshotLocalDataMock);
    when(snapProvider.getPreviousSnapshotLocalData()).thenReturn(fromSnapshotLocalData);
    when(snapProvider.getSnapshotLocalData()).thenReturn(toSnapshotLocalData);
    when(localDataManager.getOmSnapshotLocalData(any(UUID.class), any(UUID.class))).thenReturn(snapProvider);

    assertThrows(IOException.class, () ->
        rdbDifferComputer.computeDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup, tablePrefixInfo),
        "Should throw IOException when no versions found");
  }

  /**
   * Tests that close properly cleans up resources.
   */
  @Test
  public void testClose() throws IOException {
    rdbDifferComputer = new RDBDifferComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    assertTrue(Files.exists(deltaDirPath), "Delta directory should exist");

    rdbDifferComputer.close();

    assertFalse(Files.exists(deltaDirPath), "Delta directory should be cleaned up after close");
  }

  /**
   * Tests computeDeltaFiles with IOException from differ.
   */
  @Test
  public void testComputeDeltaFilesWithIOException() throws IOException {
    rdbDifferComputer = new RDBDifferComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    UUID fromSnapshotId = UUID.randomUUID();
    UUID toSnapshotId = UUID.randomUUID();
    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1", fromSnapshotId);
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2", toSnapshotId);
    Set<String> tablesToLookup = ImmutableSet.of("keyTable");
    TablePrefixInfo tablePrefixInfo = mock(TablePrefixInfo.class);

    // Mock snapshot local data
    ReadableOmSnapshotLocalDataProvider snapProvider = mock(ReadableOmSnapshotLocalDataProvider.class);
    Optional<OmSnapshotLocalData> fromSnapshotLocalData = Optional.of(createMockSnapshotLocalData(fromSnapshotId, 1));
    OmSnapshotLocalData toSnapshotLocalData = createMockSnapshotLocalData(toSnapshotId, 2);

    when(snapProvider.getPreviousSnapshotLocalData()).thenReturn(fromSnapshotLocalData);
    when(snapProvider.getSnapshotLocalData()).thenReturn(toSnapshotLocalData);
    when(localDataManager.getOmSnapshotLocalData(toSnapshotId, fromSnapshotId)).thenReturn(snapProvider);

    when(differ.getSSTDiffListWithFullPath(any(DifferSnapshotInfo.class), any(DifferSnapshotInfo.class),
        any(Map.class), any(TablePrefixInfo.class), anySet()))
        .thenThrow(new IOException("Test exception"));

    assertThrows(IOException.class, () ->
        rdbDifferComputer.computeDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup, tablePrefixInfo),
        "Should propagate IOException from differ");

    verify(snapProvider, times(1)).close();
  }

  /**
   * Tests that differ operations are synchronized.
   */
  @Test
  public void testDifferSynchronization() throws IOException {
    rdbDifferComputer = new RDBDifferComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    UUID fromSnapshotId = UUID.randomUUID();
    UUID toSnapshotId = UUID.randomUUID();
    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1", fromSnapshotId);
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2", toSnapshotId);
    Set<String> tablesToLookup = ImmutableSet.of("keyTable");
    TablePrefixInfo tablePrefixInfo = mock(TablePrefixInfo.class);

    // Mock snapshot local data
    ReadableOmSnapshotLocalDataProvider snapProvider = mock(ReadableOmSnapshotLocalDataProvider.class);
    Optional<OmSnapshotLocalData> fromSnapshotLocalData = Optional.of(createMockSnapshotLocalData(fromSnapshotId, 1));
    OmSnapshotLocalData toSnapshotLocalData = createMockSnapshotLocalData(toSnapshotId, 2);

    when(snapProvider.getPreviousSnapshotLocalData()).thenReturn(fromSnapshotLocalData);
    when(snapProvider.getSnapshotLocalData()).thenReturn(toSnapshotLocalData);
    when(localDataManager.getOmSnapshotLocalData(toSnapshotId, fromSnapshotId)).thenReturn(snapProvider);

    when(differ.getSSTDiffListWithFullPath(any(DifferSnapshotInfo.class), any(DifferSnapshotInfo.class),
        any(Map.class), any(TablePrefixInfo.class), anySet())).thenReturn(Optional.empty());

    // Multiple calls should work correctly (synchronized access to differ)
    for (int i = 0; i < 3; i++) {
      Optional<Map<Path, Pair<Path, SstFileInfo>>> result =
          rdbDifferComputer.computeDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup, tablePrefixInfo);
      assertFalse(result.isPresent(), "Result should be empty");
    }

    verify(differ, times(3)).getSSTDiffListWithFullPath(any(DifferSnapshotInfo.class),
        any(DifferSnapshotInfo.class), any(Map.class), any(TablePrefixInfo.class), anySet());
  }

  // Helper methods

  private SnapshotInfo createMockSnapshotInfo(String volumeName, String bucketName,
      String snapshotName, UUID snapshotId) {
    SnapshotInfo.Builder builder = SnapshotInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setName(snapshotName)
        .setSnapshotId(snapshotId);
    return builder.build();
  }

  private OmSnapshotLocalData createMockSnapshotLocalData(UUID snapshotId, int version) {
    OmSnapshotLocalData localData = mock(OmSnapshotLocalData.class);
    when(localData.getSnapshotId()).thenReturn(snapshotId);

    // Create version SST file info
    List<SstFileInfo> sstFiles = new ArrayList<>();
    sstFiles.add(new SstFileInfo("file1.sst", "key1", "key2", "keyTable"));

    VersionMeta versionMeta = new VersionMeta(0, sstFiles);
    Map<Integer, VersionMeta> versionMap = new TreeMap<>();
    versionMap.put(version, versionMeta);

    when(localData.getVersionSstFileInfos()).thenReturn(versionMap);
    when(localData.getVersion()).thenReturn(version);

    return localData;
  }

  private OmSnapshotLocalData createMockSnapshotLocalDataWithVersions(UUID snapshotId, int version) {
    OmSnapshotLocalData localData = mock(OmSnapshotLocalData.class);
    when(localData.getSnapshotId()).thenReturn(snapshotId);

    // Create multiple versions
    Map<Integer, VersionMeta> versionMap = new TreeMap<>();
    for (int i = 0; i <= version; i++) {
      List<SstFileInfo> sstFiles = new ArrayList<>();
      sstFiles.add(new SstFileInfo("file" + i + ".sst", "key" + i, "key" + (i + 1), "keyTable"));
      VersionMeta versionMeta = new VersionMeta(i > 0 ? i - 1 : 0, sstFiles);
      versionMap.put(i, versionMeta);
    }

    when(localData.getVersionSstFileInfos()).thenReturn(versionMap);
    when(localData.getVersion()).thenReturn(version);

    return localData;
  }
}


