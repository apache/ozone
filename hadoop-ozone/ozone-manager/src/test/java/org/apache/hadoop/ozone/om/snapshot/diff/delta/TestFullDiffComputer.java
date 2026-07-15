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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.SubStatus;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.RocksDB;

/**
 * Unit tests for FullDiffComputer.
 */
public class TestFullDiffComputer {

  @Mock
  private OmSnapshotManager omSnapshotManager;

  @Mock
  private OMMetadataManager activeMetadataManager;

  @Mock
  private OmSnapshotLocalDataManager localDataManager;

  @Mock
  private Consumer<SubStatus> activityReporter;

  @TempDir
  private Path tempDir;

  private Path deltaDirPath;

  private AutoCloseable mocks;
  private FullDiffComputer fullDiffComputer;

  @BeforeEach
  public void setUp() throws IOException {
    mocks = MockitoAnnotations.openMocks(this);
    deltaDirPath = tempDir.resolve("delta");
    when(omSnapshotManager.getSnapshotLocalDataManager()).thenReturn(localDataManager);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (fullDiffComputer != null) {
      fullDiffComputer.close();
    }
    if (mocks != null) {
      mocks.close();
    }
  }

  /**
   * Tests that the constructor creates a FullDiffComputer successfully.
   */
  @Test
  public void testConstructor() throws IOException {
    fullDiffComputer = new FullDiffComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    assertNotNull(fullDiffComputer, "FullDiffComputer should be created");
    assertTrue(Files.exists(deltaDirPath), "Delta directory should be created");
  }

  public static Stream<Arguments> computeDeltaFileCases() {
    return Stream.of(
        Arguments.of("Delta File with same source and target",
            ImmutableMap.of(new SstFileInfo("1", "ac", "ae", "cf1"), 1,
                new SstFileInfo("2", "ad", "ag", "cf1"), 2),
            ImmutableMap.of(new SstFileInfo("3", "ah", "ak", "cf1"), 1,
                new SstFileInfo("4", "af", "ai", "cf1"), 2),
            ImmutableMap.of("cf1", "a", "cf2", "z"), Collections.emptyMap(), ImmutableSet.of("cf1")),
        Arguments.of("Delta File with source having more files",
            ImmutableMap.of(new SstFileInfo("1", "ac", "ae", "cf1"), 1,
                new SstFileInfo("2", "ad", "ag", "cf1"), 2,
                new SstFileInfo("3", "af", "ah", "cf1"), 3),
            ImmutableMap.of(new SstFileInfo("3", "ah", "ak", "cf1"), 1,
                new SstFileInfo("4", "af", "ai", "cf1"), 2),
            ImmutableMap.of("cf1", "a", "cf2", "z"),
            ImmutableMap.of(Paths.get("snap1").resolve("3.sst"), new SstFileInfo("3", "af", "ah", "cf1")),
            ImmutableSet.of("cf1")),
        Arguments.of("Delta File with target having more files",
            ImmutableMap.of(new SstFileInfo("1", "ac", "ae", "cf1"), 1,
                new SstFileInfo("2", "ad", "ag", "cf1"), 2),
            ImmutableMap.of(new SstFileInfo("3", "ah", "ak", "cf1"), 1,
                new SstFileInfo("4", "af", "ai", "cf1"), 2,
                new SstFileInfo("2", "af", "ah", "cf1"), 3),
            ImmutableMap.of("cf1", "a", "cf2", "z"),
            ImmutableMap.of(Paths.get("snap2").resolve("2.sst"), new SstFileInfo("2", "af", "ah", "cf1")),
            ImmutableSet.of("cf1")),
        Arguments.of("Delta File computation with source files with invalid prefix",
            ImmutableMap.of(new SstFileInfo("1", "ac", "ae", "cf1"), 1,
                new SstFileInfo("2", "bh", "bi", "cf1"), 2),
            ImmutableMap.of(new SstFileInfo("3", "ah", "ak", "cf1"), 1,
                new SstFileInfo("4", "af", "ai", "cf1"), 2),
            ImmutableMap.of("cf1", "a", "cf2", "z"),
            ImmutableMap.of(Paths.get("snap2").resolve("4.sst"), new SstFileInfo("4", "af", "ai", "cf1")),
            ImmutableSet.of("cf1")),
        Arguments.of("Delta File computation with target files with invalid prefix",
            ImmutableMap.of(new SstFileInfo("1", "ac", "ae", "cf1"), 1,
                new SstFileInfo("2", "ah", "ai", "cf1"), 2),
            ImmutableMap.of(new SstFileInfo("3", "ah", "ak", "cf1"), 1,
                new SstFileInfo("4", "bf", "bi", "cf1"), 2),
            ImmutableMap.of("cf1", "a", "cf2", "z"),
            ImmutableMap.of(Paths.get("snap1").resolve("2.sst"), new SstFileInfo("2", "ah", "ai", "cf1")),
            ImmutableSet.of("cf1")),
        Arguments.of("Delta File computation with target files with multiple tables",
            ImmutableMap.of(new SstFileInfo("1", "ac", "ae", "cf1"), 1,
                new SstFileInfo("2", "ah", "ai", "cf1"), 2,
                new SstFileInfo("3", "ah", "ai", "cf3"), 3),
            ImmutableMap.of(new SstFileInfo("3", "ah", "ak", "cf1"), 1,
                new SstFileInfo("4", "af", "ai", "cf1"), 2,
                new SstFileInfo("5", "af", "ai", "cf4"), 5),
            ImmutableMap.of("cf1", "a", "cf2", "z"),
            Collections.emptyMap(), ImmutableSet.of("cf1")),
        Arguments.of("Delta File computation with target files with multiple tables to lookup on source",
            ImmutableMap.of(new SstFileInfo("1", "ac", "ae", "cf1"), 1,
                new SstFileInfo("2", "ah", "ai", "cf1"), 2,
                new SstFileInfo("3", "ah", "ai", "cf3"), 3),
            ImmutableMap.of(new SstFileInfo("3", "ah", "ak", "cf1"), 1,
                new SstFileInfo("4", "af", "ai", "cf1"), 2,
                new SstFileInfo("5", "af", "ai", "cf4"), 5),
            ImmutableMap.of("cf1", "a", "cf2", "z"),
            ImmutableMap.of(Paths.get("snap1").resolve("3.sst"), new SstFileInfo("3", "ah", "ai", "cf3")),
            ImmutableSet.of("cf1", "cf3")),
        Arguments.of("Delta File computation with target files with multiple tables to lookup on target",
            ImmutableMap.of(new SstFileInfo("1", "ac", "ae", "cf1"), 1,
                new SstFileInfo("2", "ah", "ai", "cf1"), 2,
                new SstFileInfo("3", "ah", "ai", "cf3"), 3),
            ImmutableMap.of(new SstFileInfo("3", "ah", "ak", "cf1"), 1,
                new SstFileInfo("4", "af", "ai", "cf1"), 2,
                new SstFileInfo("5", "af", "ai", "cf4"), 5),
            ImmutableMap.of("cf1", "a", "cf2", "z"),
            ImmutableMap.of(Paths.get("snap2").resolve("5.sst"), new SstFileInfo("5", "af", "ai", "cf4")),
            ImmutableSet.of("cf1", "cf4"))
    );
  }

  @ParameterizedTest
  @MethodSource("computeDeltaFileCases")
  public void testComputeDeltaFiles(String description,
      Map<SstFileInfo, Integer> sourceSnapshotFiles, Map<SstFileInfo, Integer> targetSnapshotFiles,
      Map<String, String> tablePrefixMap, Map<Path, SstFileInfo> expectedDiffFile,
      Set<String> tablesToLookup) throws IOException {
    fullDiffComputer = new FullDiffComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    File sstFileDirPath = tempDir.resolve("sstFiles").toFile();
    assertTrue(sstFileDirPath.mkdirs() || sstFileDirPath.exists());
    Map<Integer, Path> paths = Stream.concat(sourceSnapshotFiles.values().stream(),
            targetSnapshotFiles.values().stream())
        .distinct().collect(Collectors.toMap(Function.identity(), i -> {
          // Create mock SST files
          try {
            Path sstFilePath = sstFileDirPath.toPath().resolve(UUID.randomUUID() + ".sst").toAbsolutePath();
            assertTrue(sstFilePath.toFile().createNewFile() || sstFilePath.toFile().exists());
            return sstFilePath;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }));

    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1");
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2");
    Path snapDirectory = tempDir.resolve("snaps");
    OmSnapshot fromSnap = createMockSnapshot(snapDirectory, fromSnapshot,
        sourceSnapshotFiles.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
            entry -> paths.get(entry.getValue()))));
    OmSnapshot toSnap = createMockSnapshot(snapDirectory, toSnapshot,
        targetSnapshotFiles.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
            entry -> paths.get(entry.getValue()))));

    @SuppressWarnings("unchecked")
    UncheckedAutoCloseableSupplier<OmSnapshot> fromHandle = mock(UncheckedAutoCloseableSupplier.class);
    @SuppressWarnings("unchecked")
    UncheckedAutoCloseableSupplier<OmSnapshot> toHandle = mock(UncheckedAutoCloseableSupplier.class);

    when(fromHandle.get()).thenReturn(fromSnap);
    when(toHandle.get()).thenReturn(toSnap);
    when(omSnapshotManager.getActiveSnapshot("vol1", "bucket1", "snap1")).thenReturn(fromHandle);
    when(omSnapshotManager.getActiveSnapshot("vol1", "bucket1", "snap2")).thenReturn(toHandle);

    TablePrefixInfo tablePrefixInfo = new TablePrefixInfo(tablePrefixMap);

    Map<Path, SstFileInfo> result = fullDiffComputer.computeDeltaFiles(fromSnapshot, toSnapshot,
        tablesToLookup, tablePrefixInfo).orElse(Collections.emptyMap()).entrySet()
        .stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getValue()));
    when(activeMetadataManager.getTableBucketPrefix("vol1", "bucket1")).thenReturn(tablePrefixInfo);
    assertEquals(expectedDiffFile.entrySet().stream().collect(
        Collectors.toMap(entry -> snapDirectory.resolve(entry.getKey()), Map.Entry::getValue)),
        result);

    Set<Object> iNodes = fullDiffComputer.getDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup).stream()
        .map(Pair::getKey).map(path -> {
          try {
            return getINode(path);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).collect(Collectors.toSet());
    Set<Object> expectedInodes = result.keySet().stream().map(path -> {
      try {
        return getINode(path);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toSet());
    assertEquals(expectedInodes, iNodes);
  }

  /**
   * Tests that close properly cleans up resources.
   */
  @Test
  public void testClose() throws IOException {
    fullDiffComputer = new FullDiffComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    assertTrue(Files.exists(deltaDirPath), "Delta directory should exist");

    fullDiffComputer.close();

    assertFalse(Files.exists(deltaDirPath), "Delta directory should be cleaned up after close");
  }

  // Helper methods
  private SnapshotInfo createMockSnapshotInfo(String volumeName, String bucketName, String snapshotName) {
    SnapshotInfo.Builder builder = SnapshotInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setName(snapshotName)
        .setSnapshotId(UUID.randomUUID());
    return builder.build();
  }

  private LiveFileMetaData getMockLiveFileMetaData(Path dbLocation, SstFileInfo sstFileInfo) {
    LiveFileMetaData liveFileMetaData = mock(LiveFileMetaData.class);
    String path = dbLocation.toAbsolutePath().toString();
    String fileName = sstFileInfo.getFilePath(dbLocation).toFile().getName();
    when(liveFileMetaData.fileName()).thenReturn(fileName);
    when(liveFileMetaData.path()).thenReturn(path);
    when(liveFileMetaData.columnFamilyName()).thenReturn(StringUtils.string2Bytes(sstFileInfo.getColumnFamily()));
    when(liveFileMetaData.smallestKey()).thenReturn(StringUtils.string2Bytes(sstFileInfo.getStartKey()));
    when(liveFileMetaData.largestKey()).thenReturn(StringUtils.string2Bytes(sstFileInfo.getEndKey()));

    return liveFileMetaData;
  }

  private OmSnapshot createMockSnapshot(Path snapshotDir, SnapshotInfo snapshotInfo,
      Map<SstFileInfo, Path> sstFilesLinks) throws IOException {
    OmSnapshot snapshot = mock(OmSnapshot.class);
    OMMetadataManager metadataManager = mock(OMMetadataManager.class);
    RDBStore store = mock(RDBStore.class);
    RocksDatabase database = mock(RocksDatabase.class);
    when(store.getDb()).thenReturn(database);
    ManagedRocksDB managedRocksDB = mock(ManagedRocksDB.class);
    when(database.getManagedRocksDb()).thenReturn(managedRocksDB);
    RocksDB rocksDB = mock(RocksDB.class);
    when(managedRocksDB.get()).thenReturn(rocksDB);

    Path dbLocationPath = snapshotDir.resolve(snapshotInfo.getName());
    File dbLocation = dbLocationPath.toFile();
    List<LiveFileMetaData> liveFileMetaDataList = sstFilesLinks.keySet().stream()
        .map(sstFileInfo -> getMockLiveFileMetaData(dbLocationPath, sstFileInfo))
        .collect(Collectors.toList());
    when(rocksDB.getLiveFilesMetaData()).thenReturn(liveFileMetaDataList);
    assertTrue(dbLocation.mkdirs() || dbLocation.exists());

    for (Map.Entry<SstFileInfo, Path> sstFile : sstFilesLinks.entrySet()) {
      File path = sstFile.getKey().getFilePath(dbLocation.toPath()).toFile();
      Files.createLink(path.toPath(), sstFile.getValue());
    }
    when(snapshot.getMetadataManager()).thenReturn(metadataManager);
    when(metadataManager.getStore()).thenReturn(store);
    when(store.getDbLocation()).thenReturn(dbLocation);

    return snapshot;
  }
}
