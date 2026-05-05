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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.SubStatus;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for CompositeDeltaDiffComputer using Mockito.mockConstruction()
 * to properly isolate and test fallback logic.
 */
public class TestCompositeDeltaDiffComputer {

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
    if (mocks != null) {
      mocks.close();
    }
  }

  /**
   * Tests that RDBDifferComputer is created when fullDiff=false using mockConstruction.
   */
  @Test
  public void testRDBDifferComputerCreatedWhenNotFullDiff() throws IOException {
    try (MockedConstruction<RDBDifferComputer> rdbDifferMock = mockConstruction(RDBDifferComputer.class);
         MockedConstruction<FullDiffComputer> fullDiffMock = mockConstruction(FullDiffComputer.class)) {

      CompositeDeltaDiffComputer composite = new CompositeDeltaDiffComputer(
          omSnapshotManager, activeMetadataManager, deltaDirPath, activityReporter, false, false);

      // Verify RDBDifferComputer was constructed (fullDiff=false)
      assertEquals(1, rdbDifferMock.constructed().size(), "RDBDifferComputer should be constructed");
      assertEquals(1, fullDiffMock.constructed().size(), "FullDiffComputer should always be constructed");

      composite.close();
    }
  }

  /**
   * Tests that RDBDifferComputer is NOT created when fullDiff=true using mockConstruction.
   */
  @Test
  public void testRDBDifferComputerNotCreatedWhenFullDiff() throws IOException {
    try (MockedConstruction<RDBDifferComputer> rdbDifferMock = mockConstruction(RDBDifferComputer.class);
         MockedConstruction<FullDiffComputer> fullDiffMock = mockConstruction(FullDiffComputer.class)) {

      CompositeDeltaDiffComputer composite = new CompositeDeltaDiffComputer(
          omSnapshotManager, activeMetadataManager, deltaDirPath, activityReporter, true, false);

      // Verify RDBDifferComputer was NOT constructed (fullDiff=true)
      assertEquals(0, rdbDifferMock.constructed().size(), "RDBDifferComputer should NOT " +
          "be constructed when fullDiff=true");
      assertEquals(1, fullDiffMock.constructed().size(), "FullDiffComputer should always be constructed");

      composite.close();
    }
  }

  /**
   * Tests successful RDBDifferComputer computation without fallback.
   */
  @Test
  public void testSuccessfulRDBDifferComputationWithoutFallback() throws IOException {
    UUID fromSnapshotId = UUID.randomUUID();
    UUID toSnapshotId = UUID.randomUUID();
    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1", fromSnapshotId);
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2", toSnapshotId);
    Set<String> tablesToLookup = ImmutableSet.of("keyTable");
    TablePrefixInfo tablePrefixInfo = new TablePrefixInfo(ImmutableMap.of("keyTable", "a"));

    // Create expected results from RDBDiffer
    Path sstFile1 = tempDir.resolve("rdb1.sst");
    Path sstFile2 = tempDir.resolve("rdb2.sst");
    Files.createFile(sstFile1);
    Files.createFile(sstFile2);
    SstFileInfo sstInfo1 = new SstFileInfo("rdb1.sst", "key1", "key2", "keyTable");
    SstFileInfo sstInfo2 = new SstFileInfo("rdb2.sst", "key3", "key4", "keyTable");
    Map<Path, Pair<Path, SstFileInfo>> rdbDifferResult = new HashMap<>();
    rdbDifferResult.put(sstFile1, Pair.of(sstFile1, sstInfo1));
    rdbDifferResult.put(sstFile2, Pair.of(sstFile2, sstInfo2));

    try (MockedConstruction<RDBDifferComputer> rdbDifferMock = mockConstruction(RDBDifferComputer.class,
        (mock, context) -> {
          // Make RDBDifferComputer return results successfully
          when(mock.computeDeltaFiles(any(), any(), anySet(), any()))
              .thenReturn(Optional.of(rdbDifferResult));
        });
         MockedConstruction<FullDiffComputer> fullDiffMock = mockConstruction(FullDiffComputer.class)) {

      CompositeDeltaDiffComputer composite = new CompositeDeltaDiffComputer(
          omSnapshotManager, activeMetadataManager, deltaDirPath, activityReporter, false, false);

      Optional<Map<Path, Pair<Path, SstFileInfo>>> result =
          composite.computeDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup, tablePrefixInfo);

      // Verify RDBDiffer results are returned
      assertTrue(result.isPresent(), "Result should be present from RDBDiffer");
      assertEquals(2, result.get().size(), "Should have 2 files from RDBDiffer");
      assertEquals(rdbDifferResult, result.get(), "Should return RDBDifferComputer result");

      // Verify RDBDifferComputer was called but NOT FullDiffComputer
      RDBDifferComputer rdbDifferInstance = rdbDifferMock.constructed().get(0);
      verify(rdbDifferInstance, times(1)).computeDeltaFiles(any(), any(), anySet(), any());

      // Verify FullDiffComputer was NEVER called (no fallback needed)
      FullDiffComputer fullDiffInstance = fullDiffMock.constructed().get(0);
      verify(fullDiffInstance, times(0)).computeDeltaFiles(any(), any(), anySet(), any());

      // Verify only DAG_WALK status was reported (no FULL_DIFF)
      ArgumentCaptor<SubStatus> statusCaptor = ArgumentCaptor.forClass(SubStatus.class);
      verify(activityReporter, times(1)).accept(statusCaptor.capture());
      assertEquals(SubStatus.SST_FILE_DELTA_DAG_WALK, statusCaptor.getValue(),
          "Only DAG_WALK should be reported when RDBDiffer succeeds");

      composite.close();
    }
  }

  /**
   * Tests successful RDBDifferComputer with single file.
   */
  @Test
  public void testSuccessfulRDBDifferWithSingleFile() throws IOException {
    UUID fromSnapshotId = UUID.randomUUID();
    UUID toSnapshotId = UUID.randomUUID();
    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1", fromSnapshotId);
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2", toSnapshotId);
    Set<String> tablesToLookup = ImmutableSet.of("keyTable");
    TablePrefixInfo tablePrefixInfo = new TablePrefixInfo(ImmutableMap.of("keyTable", "a"));

    Path sstFile = tempDir.resolve("single.sst");
    Files.createFile(sstFile);
    SstFileInfo sstInfo = new SstFileInfo("single.sst", "key1", "key5", "keyTable");
    Map<Path, Pair<Path, SstFileInfo>> rdbDifferResult = new HashMap<>();
    rdbDifferResult.put(sstFile, Pair.of(sstFile, sstInfo));

    try (MockedConstruction<RDBDifferComputer> rdbDifferMock = mockConstruction(RDBDifferComputer.class,
        (mock, context) -> {
          when(mock.computeDeltaFiles(any(), any(), anySet(), any()))
              .thenReturn(Optional.of(rdbDifferResult));
        });
         MockedConstruction<FullDiffComputer> fullDiffMock = mockConstruction(FullDiffComputer.class)) {

      CompositeDeltaDiffComputer composite = new CompositeDeltaDiffComputer(
          omSnapshotManager, activeMetadataManager, deltaDirPath, activityReporter, false, false);

      Optional<Map<Path, Pair<Path, SstFileInfo>>> result =
          composite.computeDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup, tablePrefixInfo);

      assertTrue(result.isPresent(), "Result should be present");
      assertEquals(1, result.get().size(), "Should have 1 file");
      
      // Verify no fallback to FullDiff
      FullDiffComputer fullDiffInstance = fullDiffMock.constructed().get(0);
      verify(fullDiffInstance, times(0)).computeDeltaFiles(any(), any(), anySet(), any());

      composite.close();
    }
  }

  /**
   * Tests successful RDBDifferComputer with multiple tables.
   */
  @Test
  public void testSuccessfulRDBDifferWithMultipleTables() throws IOException {
    UUID fromSnapshotId = UUID.randomUUID();
    UUID toSnapshotId = UUID.randomUUID();
    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1", fromSnapshotId);
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2", toSnapshotId);
    Set<String> tablesToLookup = ImmutableSet.of("keyTable", "fileTable", "directoryTable");
    TablePrefixInfo tablePrefixInfo = new TablePrefixInfo(ImmutableMap.of(
        "keyTable", "a", "fileTable", "b", "directoryTable", "c"));

    // Create files for different tables
    Path keyFile = tempDir.resolve("key1.sst");
    Path fileFile = tempDir.resolve("file1.sst");
    Path dirFile = tempDir.resolve("dir1.sst");
    Files.createFile(keyFile);
    Files.createFile(fileFile);
    Files.createFile(dirFile);

    SstFileInfo keyInfo = new SstFileInfo("key1.sst", "key1", "key2", "keyTable");
    SstFileInfo fileInfo = new SstFileInfo("file1.sst", "file1", "file2", "fileTable");
    SstFileInfo dirInfo = new SstFileInfo("dir1.sst", "dir1", "dir2", "directoryTable");

    Map<Path, Pair<Path, SstFileInfo>> rdbDifferResult = new HashMap<>();
    rdbDifferResult.put(keyFile, Pair.of(keyFile, keyInfo));
    rdbDifferResult.put(fileFile, Pair.of(fileFile, fileInfo));
    rdbDifferResult.put(dirFile, Pair.of(dirFile, dirInfo));

    try (MockedConstruction<RDBDifferComputer> rdbDifferMock = mockConstruction(RDBDifferComputer.class,
        (mock, context) -> {
          when(mock.computeDeltaFiles(any(), any(), anySet(), any()))
              .thenReturn(Optional.of(rdbDifferResult));
        });
         MockedConstruction<FullDiffComputer> fullDiffMock = mockConstruction(FullDiffComputer.class)) {

      CompositeDeltaDiffComputer composite = new CompositeDeltaDiffComputer(
          omSnapshotManager, activeMetadataManager, deltaDirPath, activityReporter, false, false);

      Optional<Map<Path, Pair<Path, SstFileInfo>>> result =
          composite.computeDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup, tablePrefixInfo);

      assertTrue(result.isPresent(), "Result should be present");
      assertEquals(3, result.get().size(), "Should have 3 files from different tables");

      // Verify RDBDiffer handled all tables without fallback
      RDBDifferComputer rdbDifferInstance = rdbDifferMock.constructed().get(0);
      verify(rdbDifferInstance, times(1)).computeDeltaFiles(any(), any(), anySet(), any());

      FullDiffComputer fullDiffInstance = fullDiffMock.constructed().get(0);
      verify(fullDiffInstance, times(0)).computeDeltaFiles(any(), any(), anySet(), any());

      composite.close();
    }
  }

  /**
   * Tests successful RDBDifferComputer returning empty map (no changes).
   */
  @Test
  public void testSuccessfulRDBDifferWithNoChanges() throws IOException {
    UUID fromSnapshotId = UUID.randomUUID();
    UUID toSnapshotId = UUID.randomUUID();
    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1", fromSnapshotId);
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2", toSnapshotId);
    Set<String> tablesToLookup = ImmutableSet.of("keyTable");
    TablePrefixInfo tablePrefixInfo = new TablePrefixInfo(ImmutableMap.of("keyTable", "a"));

    // RDBDiffer returns empty map (no differences, but successful computation)
    Map<Path, Pair<Path, SstFileInfo>> emptyResult = new HashMap<>();

    try (MockedConstruction<RDBDifferComputer> rdbDifferMock = mockConstruction(RDBDifferComputer.class,
        (mock, context) -> {
          when(mock.computeDeltaFiles(any(), any(), anySet(), any()))
              .thenReturn(Optional.of(emptyResult));
        });
         MockedConstruction<FullDiffComputer> fullDiffMock = mockConstruction(FullDiffComputer.class)) {

      CompositeDeltaDiffComputer composite = new CompositeDeltaDiffComputer(
          omSnapshotManager, activeMetadataManager, deltaDirPath, activityReporter, false, false);

      Optional<Map<Path, Pair<Path, SstFileInfo>>> result =
          composite.computeDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup, tablePrefixInfo);

      // Empty result is still a valid success case - no fallback needed
      assertTrue(result.isPresent(), "Result should be present even if empty");
      assertEquals(0, result.get().size(), "Should have 0 files (no changes)");

      // Verify no fallback occurred
      FullDiffComputer fullDiffInstance = fullDiffMock.constructed().get(0);
      verify(fullDiffInstance, times(0)).computeDeltaFiles(any(), any(), anySet(), any());

      // Only DAG_WALK status should be reported
      ArgumentCaptor<SubStatus> statusCaptor = ArgumentCaptor.forClass(SubStatus.class);
      verify(activityReporter, times(1)).accept(statusCaptor.capture());
      assertEquals(SubStatus.SST_FILE_DELTA_DAG_WALK, statusCaptor.getValue());

      composite.close();
    }
  }

  /**
   * Tests fallback from RDBDifferComputer to FullDiffComputer using mockConstruction.
   */
  @Test
  public void testFallbackFromRDBDifferToFullDiff() throws IOException {
    UUID fromSnapshotId = UUID.randomUUID();
    UUID toSnapshotId = UUID.randomUUID();
    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1", fromSnapshotId);
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2", toSnapshotId);
    Set<String> tablesToLookup = ImmutableSet.of("keyTable");
    TablePrefixInfo tablePrefixInfo = new TablePrefixInfo(ImmutableMap.of("keyTable", "a"));

    // Create expected results
    Path sstFile = tempDir.resolve("test.sst");
    Files.createFile(sstFile);
    SstFileInfo sstInfo = new SstFileInfo("test.sst", "key1", "key2", "keyTable");
    Map<Path, Pair<Path, SstFileInfo>> fullDiffResult = new HashMap<>();
    fullDiffResult.put(sstFile, Pair.of(sstFile, sstInfo));

    try (MockedConstruction<RDBDifferComputer> rdbDifferMock = mockConstruction(RDBDifferComputer.class,
        (mock, context) -> {
          // Make RDBDifferComputer return empty to trigger fallback
          when(mock.computeDeltaFiles(any(), any(), anySet(), any()))
              .thenReturn(Optional.empty());
        });
         MockedConstruction<FullDiffComputer> fullDiffMock = mockConstruction(FullDiffComputer.class,
             (mock, context) -> {
               // Make FullDiffComputer return results
               when(mock.computeDeltaFiles(any(), any(), anySet(), any()))
                   .thenReturn(Optional.of(fullDiffResult));
             })) {

      CompositeDeltaDiffComputer composite = new CompositeDeltaDiffComputer(
          omSnapshotManager, activeMetadataManager, deltaDirPath, activityReporter, false, false);

      Optional<Map<Path, Pair<Path, SstFileInfo>>> result =
          composite.computeDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup, tablePrefixInfo);

      // Verify fallback occurred
      assertTrue(result.isPresent(), "Result should be present from fallback");
      assertEquals(fullDiffResult, result.get(), "Should return FullDiffComputer result");

      // Verify both computers were called
      RDBDifferComputer rdbDifferInstance = rdbDifferMock.constructed().get(0);
      FullDiffComputer fullDiffInstance = fullDiffMock.constructed().get(0);

      verify(rdbDifferInstance, times(1)).computeDeltaFiles(any(), any(), anySet(), any());
      verify(fullDiffInstance, times(1)).computeDeltaFiles(any(), any(), anySet(), any());

      // Verify activity statuses were reported
      ArgumentCaptor<SubStatus> statusCaptor = ArgumentCaptor.forClass(SubStatus.class);
      verify(activityReporter, times(2)).accept(statusCaptor.capture());
      List<SubStatus> statuses = statusCaptor.getAllValues();
      assertEquals(SubStatus.SST_FILE_DELTA_DAG_WALK, statuses.get(0));
      assertEquals(SubStatus.SST_FILE_DELTA_FULL_DIFF, statuses.get(1));

      composite.close();
    }
  }

  /**
   * Tests fallback on exception using mockConstruction.
   */
  @Test
  public void testFallbackOnException() throws IOException {
    UUID fromSnapshotId = UUID.randomUUID();
    UUID toSnapshotId = UUID.randomUUID();
    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1", fromSnapshotId);
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2", toSnapshotId);
    Set<String> tablesToLookup = ImmutableSet.of("keyTable");
    TablePrefixInfo tablePrefixInfo = new TablePrefixInfo(ImmutableMap.of("keyTable", "a"));

    Path sstFile = tempDir.resolve("test2.sst");
    Files.createFile(sstFile);
    SstFileInfo sstInfo = new SstFileInfo("test2.sst", "key3", "key4", "keyTable");
    Map<Path, Pair<Path, SstFileInfo>> fullDiffResult = new HashMap<>();
    fullDiffResult.put(sstFile, Pair.of(sstFile, sstInfo));

    try (MockedConstruction<RDBDifferComputer> rdbDifferMock = mockConstruction(RDBDifferComputer.class,
        (mock, context) -> {
          // Make RDBDifferComputer throw exception to trigger fallback
          when(mock.computeDeltaFiles(any(), any(), anySet(), any()))
              .thenThrow(new RuntimeException("Test exception"));
        });
         MockedConstruction<FullDiffComputer> fullDiffMock = mockConstruction(FullDiffComputer.class,
             (mock, context) -> {
               // Make FullDiffComputer return results
               when(mock.computeDeltaFiles(any(), any(), anySet(), any()))
                   .thenReturn(Optional.of(fullDiffResult));
             })) {

      CompositeDeltaDiffComputer composite = new CompositeDeltaDiffComputer(
          omSnapshotManager, activeMetadataManager, deltaDirPath, activityReporter, false, false);

      Optional<Map<Path, Pair<Path, SstFileInfo>>> result =
          composite.computeDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup, tablePrefixInfo);

      // Verify fallback occurred
      assertTrue(result.isPresent(), "Result should be present from fallback after exception");

      // Verify activity statuses
      ArgumentCaptor<SubStatus> statusCaptor = ArgumentCaptor.forClass(SubStatus.class);
      verify(activityReporter, times(2)).accept(statusCaptor.capture());
      List<SubStatus> statuses = statusCaptor.getAllValues();
      assertEquals(SubStatus.SST_FILE_DELTA_DAG_WALK, statuses.get(0));
      assertEquals(SubStatus.SST_FILE_DELTA_FULL_DIFF, statuses.get(1));

      composite.close();
    }
  }

  /**
   * Tests that FullDiffComputer is used directly when fullDiff=true.
   */
  @Test
  public void testFullDiffOnlyMode() throws IOException {
    UUID fromSnapshotId = UUID.randomUUID();
    UUID toSnapshotId = UUID.randomUUID();
    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1", fromSnapshotId);
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2", toSnapshotId);
    Set<String> tablesToLookup = ImmutableSet.of("keyTable");
    TablePrefixInfo tablePrefixInfo = new TablePrefixInfo(ImmutableMap.of("keyTable", "a"));

    Path sstFile = tempDir.resolve("test3.sst");
    Files.createFile(sstFile);
    SstFileInfo sstInfo = new SstFileInfo("test3.sst", "key5", "key6", "keyTable");
    Map<Path, Pair<Path, SstFileInfo>> fullDiffResult = new HashMap<>();
    fullDiffResult.put(sstFile, Pair.of(sstFile, sstInfo));

    try (MockedConstruction<RDBDifferComputer> rdbDifferMock = mockConstruction(RDBDifferComputer.class);
         MockedConstruction<FullDiffComputer> fullDiffMock = mockConstruction(FullDiffComputer.class,
             (mock, context) -> {
               when(mock.computeDeltaFiles(any(), any(), anySet(), any()))
                   .thenReturn(Optional.of(fullDiffResult));
             })) {

      CompositeDeltaDiffComputer composite = new CompositeDeltaDiffComputer(
          omSnapshotManager, activeMetadataManager, deltaDirPath, activityReporter, true, false);

      Optional<Map<Path, Pair<Path, SstFileInfo>>> result =
          composite.computeDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup, tablePrefixInfo);

      // Verify RDBDifferComputer was never constructed or called
      assertEquals(0, rdbDifferMock.constructed().size(), "RDBDifferComputer should not be constructed");

      // Verify FullDiffComputer was used
      assertTrue(result.isPresent(), "Result should be present");
      FullDiffComputer fullDiffInstance = fullDiffMock.constructed().get(0);
      verify(fullDiffInstance, times(1)).computeDeltaFiles(any(), any(), anySet(), any());

      // Verify only FULL_DIFF status was reported
      ArgumentCaptor<SubStatus> statusCaptor = ArgumentCaptor.forClass(SubStatus.class);
      verify(activityReporter, times(1)).accept(statusCaptor.capture());
      assertEquals(SubStatus.SST_FILE_DELTA_FULL_DIFF, statusCaptor.getValue());

      composite.close();
    }
  }

  /**
   * Tests proper cleanup of both computers.
   */
  @Test
  public void testCloseCallsBothComputers() throws IOException {
    try (MockedConstruction<RDBDifferComputer> rdbDifferMock = mockConstruction(RDBDifferComputer.class);
         MockedConstruction<FullDiffComputer> fullDiffMock = mockConstruction(FullDiffComputer.class)) {

      CompositeDeltaDiffComputer composite = new CompositeDeltaDiffComputer(
          omSnapshotManager, activeMetadataManager, deltaDirPath, activityReporter, false, false);

      composite.close();

      // Verify close was called on both
      RDBDifferComputer rdbDifferInstance = rdbDifferMock.constructed().get(0);
      FullDiffComputer fullDiffInstance = fullDiffMock.constructed().get(0);

      verify(rdbDifferInstance, times(1)).close();
      verify(fullDiffInstance, times(1)).close();
    }
  }

  /**
   * Tests that nonNativeDiff flag is properly passed to constructor.
   * Verifies CompositeDeltaDiffComputer can be created with nonNativeDiff=true.
   */
  @Test
  public void testNonNativeDiffFlagInConstructor() throws IOException {
    try (MockedConstruction<RDBDifferComputer> rdbDifferMock = mockConstruction(RDBDifferComputer.class);
         MockedConstruction<FullDiffComputer> fullDiffMock = mockConstruction(FullDiffComputer.class)) {

      // Create with nonNativeDiff = true
      CompositeDeltaDiffComputer composite = new CompositeDeltaDiffComputer(
          omSnapshotManager, activeMetadataManager, deltaDirPath, activityReporter, false, true);

      // Verify construction succeeds and both computers are created
      assertEquals(1, rdbDifferMock.constructed().size(), "RDBDifferComputer should be created");
      assertEquals(1, fullDiffMock.constructed().size(), "FullDiffComputer should be created");

      composite.close();
    }
  }

  /**
   * Tests that nonNativeDiff flag works correctly when disabled.
   * Verifies CompositeDeltaDiffComputer can be created with nonNativeDiff=false.
   */
  @Test
  public void testNonNativeDiffDisabled() throws IOException {
    try (MockedConstruction<RDBDifferComputer> rdbDifferMock = mockConstruction(RDBDifferComputer.class);
         MockedConstruction<FullDiffComputer> fullDiffMock = mockConstruction(FullDiffComputer.class)) {

      // Create with nonNativeDiff = false (default behavior)
      CompositeDeltaDiffComputer composite = new CompositeDeltaDiffComputer(
          omSnapshotManager, activeMetadataManager, deltaDirPath, activityReporter, false, false);

      // Verify construction succeeds and both computers are created
      assertEquals(1, rdbDifferMock.constructed().size(), "RDBDifferComputer should be created");
      assertEquals(1, fullDiffMock.constructed().size(), "FullDiffComputer should be created");

      composite.close();
    }
  }

  /**
   * Tests nonNativeDiff mode with computeDeltaFiles - verifies fromSnapshot files are added.
   * In nonNativeDiff mode, SST files from fromSnapshot are added to the delta to handle deletes.
   */
  @Test
  public void testNonNativeDiffComputeDeltaFilesEnabled() throws IOException {
    // Given nonNativeDiff is enabled and we have snapshots
    UUID fromSnapshotId = UUID.randomUUID();
    UUID toSnapshotId = UUID.randomUUID();
    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1", fromSnapshotId);
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2", toSnapshotId);
    Set<String> tablesToLookup = ImmutableSet.of("keyTable");
    TablePrefixInfo tablePrefixInfo = new TablePrefixInfo(ImmutableMap.of("keyTable", "a"));

    // Setup fromSnapshot SST files
    Path fromDbPath = tempDir.resolve("fromDb");
    Files.createDirectories(fromDbPath);
    Path fromSstFile1 = fromDbPath.resolve("000001.sst");
    Path fromSstFile2 = fromDbPath.resolve("000002.sst");
    Files.createFile(fromSstFile1);
    Files.createFile(fromSstFile2);
    
    SstFileInfo fromSstInfo1 = new SstFileInfo("000001", "a/key1", "a/key100", "keyTable");
    SstFileInfo fromSstInfo2 = new SstFileInfo("000002", "a/key101", "a/key200", "keyTable");
    Set<SstFileInfo> fromSnapshotSstFiles = ImmutableSet.of(fromSstInfo1, fromSstInfo2);

    // Mock fromSnapshot
    OmSnapshot fromSnap = org.mockito.Mockito.mock(OmSnapshot.class);
    OMMetadataManager fromMetaMgr = org.mockito.Mockito.mock(OMMetadataManager.class);
    RDBStore fromRdbStore = org.mockito.Mockito.mock(RDBStore.class);
    when(fromSnap.getMetadataManager()).thenReturn(fromMetaMgr);
    when(fromMetaMgr.getStore()).thenReturn(fromRdbStore);
    when(fromRdbStore.getDbLocation()).thenReturn(fromDbPath.toFile());
    
    @SuppressWarnings("unchecked")
    UncheckedAutoCloseableSupplier<OmSnapshot> fromSnapSupplier = 
        (UncheckedAutoCloseableSupplier<OmSnapshot>) org.mockito.Mockito.mock(UncheckedAutoCloseableSupplier.class);
    when(fromSnapSupplier.get()).thenReturn(fromSnap);
    when(omSnapshotManager.getActiveSnapshot(eq("vol1"), eq("bucket1"), eq("snap1")))
        .thenReturn(fromSnapSupplier);

    // Mock RDBDifferComputer to return a result
    Map<Path, Pair<Path, SstFileInfo>> rdbDifferResult = new HashMap<>();
    Path toSstFile = tempDir.resolve("000003.sst");
    Files.createFile(toSstFile);
    SstFileInfo toSstInfo = new SstFileInfo("000003.sst", "a/key1", "a/key50", "keyTable");
    rdbDifferResult.put(toSstFile, Pair.of(deltaDirPath.resolve("000003.sst"), toSstInfo));

    try (MockedConstruction<RDBDifferComputer> rdbDifferMock = mockConstruction(RDBDifferComputer.class,
        (mock, context) -> {
          when(mock.computeDeltaFiles(any(), any(), anySet(), any()))
              .thenReturn(Optional.of(rdbDifferResult));
        });
         MockedConstruction<FullDiffComputer> fullDiffMock = mockConstruction(FullDiffComputer.class);
         MockedStatic<FullDiffComputer> fullDiffStaticMock = mockStatic(FullDiffComputer.class)) {

      // Mock the static method getSSTFileSetForSnapshot
      fullDiffStaticMock.when(() -> FullDiffComputer.getSSTFileSetForSnapshot(any(), anySet(), any()))
          .thenReturn(fromSnapshotSstFiles);

      // When we create CompositeDeltaDiffComputer with nonNativeDiff=true
      CompositeDeltaDiffComputer composite = new CompositeDeltaDiffComputer(
          omSnapshotManager, activeMetadataManager, deltaDirPath, activityReporter, false, true);

      // Then computeDeltaFiles should complete successfully and include fromSnapshot files
      Optional<Map<Path, Pair<Path, SstFileInfo>>> result =
          composite.computeDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup, tablePrefixInfo);

      // Result should be present with both RDBDiffer result AND fromSnapshot files
      assertTrue(result.isPresent(), "Result should be present");
      Map<Path, Pair<Path, SstFileInfo>> deltaFiles = result.get();
      
      // Should have 1 from RDBDiffer + 2 from fromSnapshot = 3 total
      assertEquals(3, deltaFiles.size(),
          "Should have 3 files (1 RDBDiffer + 2 fromSnapshot), got: " + deltaFiles.size());
      assertEquals(ImmutableSet.of(fromSstFile1, fromSstFile2, toSstFile), deltaFiles.keySet());
      Map<Path, SstFileInfo> infoMap = ImmutableMap.of(fromSstFile1, fromSstInfo1, fromSstFile2, fromSstInfo2,
          toSstFile, toSstInfo);
      for (Map.Entry<Path, Pair<Path, SstFileInfo>> entry : deltaFiles.entrySet()) {
        assertEquals(infoMap.get(entry.getKey()), entry.getValue().getRight());
        assertEquals(deltaDirPath.toAbsolutePath(), entry.getValue().getLeft().toAbsolutePath().getParent());
      }
      assertEquals(getINode(fromSstFile1), getINode(deltaFiles.get(fromSstFile1).getLeft()));
      assertEquals(getINode(fromSstFile2), getINode(deltaFiles.get(fromSstFile2).getLeft()));

      composite.close();
    }
  }

  /**
   * Tests nonNativeDiff mode disabled with computeDeltaFiles.
   * Verifies normal behavior when nonNativeDiff=false.
   */
  @Test
  public void testNonNativeDiffComputeDeltaFilesDisabled() throws IOException {
    // Given nonNativeDiff is disabled
    UUID fromSnapshotId = UUID.randomUUID();
    UUID toSnapshotId = UUID.randomUUID();
    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1", fromSnapshotId);
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2", toSnapshotId);
    Set<String> tablesToLookup = ImmutableSet.of("keyTable");
    TablePrefixInfo tablePrefixInfo = new TablePrefixInfo(ImmutableMap.of("keyTable", "a"));

    // Mock RDBDifferComputer to return a result
    Map<Path, Pair<Path, SstFileInfo>> rdbDifferResult = new HashMap<>();
    Path sstFile = tempDir.resolve("000001.sst");
    Files.createFile(sstFile);
    SstFileInfo sstInfo = new SstFileInfo("000001.sst", "a/key1", "a/key50", "keyTable");
    rdbDifferResult.put(sstFile, Pair.of(deltaDirPath.resolve("000001.sst"), sstInfo));

    try (MockedConstruction<RDBDifferComputer> rdbDifferMock = mockConstruction(RDBDifferComputer.class,
        (mock, context) -> {
          when(mock.computeDeltaFiles(any(), any(), anySet(), any()))
              .thenReturn(Optional.of(rdbDifferResult));
        });
         MockedConstruction<FullDiffComputer> fullDiffMock = mockConstruction(FullDiffComputer.class)) {

      // When we create CompositeDeltaDiffComputer with nonNativeDiff=false
      CompositeDeltaDiffComputer composite = new CompositeDeltaDiffComputer(
          omSnapshotManager, activeMetadataManager, deltaDirPath, activityReporter, false, false);

      // Then computeDeltaFiles should complete successfully with RDBDiffer result
      Optional<Map<Path, Pair<Path, SstFileInfo>>> result =
          composite.computeDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup, tablePrefixInfo);

      // Result should contain RDBDiffer result
      assertTrue(result.isPresent(), "Result should be present");
      Map<Path, Pair<Path, SstFileInfo>> deltaFiles = result.get();
      assertEquals(1, deltaFiles.size(), "Should have RDBDiffer result");
      assertTrue(deltaFiles.containsKey(sstFile), "Should contain the SST file");

      composite.close();
    }
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
}

