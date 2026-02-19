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

import static org.apache.hadoop.hdds.StringUtils.bytes2String;
import static org.apache.hadoop.hdds.StringUtils.string2Bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataProvider;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.SubStatus;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for FileLinkDeltaFileComputer.
 */
public class TestFileLinkDeltaFileComputer {

  @TempDir
  private Path tempDir;

  @Mock
  private OmSnapshotManager omSnapshotManager;

  @Mock
  private OMMetadataManager activeMetadataManager;

  @Mock
  private OmSnapshotLocalDataManager localDataManager;

  @Mock
  private Consumer<SubStatus> activityReporter;

  private AutoCloseable mocks;
  private Path deltaDirPath;
  private TestableFileLinkDeltaFileComputer deltaFileComputer;

  @BeforeEach
  public void setUp() throws IOException {
    mocks = MockitoAnnotations.openMocks(this);
    deltaDirPath = tempDir.resolve("delta");
    when(omSnapshotManager.getSnapshotLocalDataManager()).thenReturn(localDataManager);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (deltaFileComputer != null) {
      deltaFileComputer.close();
    }
    if (mocks != null) {
      mocks.close();
    }
  }

  /**
   * Tests that the constructor creates the delta directory successfully.
   */
  @Test
  public void testConstructorCreatesDeltaDirectory() throws IOException {
    deltaFileComputer = new TestableFileLinkDeltaFileComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    assertTrue(Files.exists(deltaDirPath), "Delta directory should be created");
    assertTrue(Files.isDirectory(deltaDirPath), "Delta path should be a directory");
  }

  /**
   * Tests that the constructor handles an existing delta directory.
   */
  @Test
  public void testConstructorWithExistingDirectory() throws IOException {
    Files.createDirectories(deltaDirPath);
    assertTrue(Files.exists(deltaDirPath));

    deltaFileComputer = new TestableFileLinkDeltaFileComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    assertTrue(Files.exists(deltaDirPath), "Delta directory should exist");
  }

  /**
   * Tests creating a hard link to a file.
   */
  @Test
  public void testCreateLink() throws IOException {
    deltaFileComputer = new TestableFileLinkDeltaFileComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    // Create a source file
    Path sourceFile = tempDir.resolve("source.sst");
    Files.createFile(sourceFile);
    Files.write(sourceFile, string2Bytes("test data"));

    // Create a hard link
    Path linkPath = deltaFileComputer.createLink(sourceFile);

    assertNotNull(linkPath, "Link path should not be null");
    assertTrue(Files.exists(linkPath), "Link should be created");
    assertTrue(linkPath.getFileName().toString().endsWith(".sst"), "Link should preserve file extension");
    assertEquals("test data", bytes2String(Files.readAllBytes(linkPath)), "Link should point to same data");
  }

  /**
   * Tests creating multiple hard links increments the counter.
   */
  @Test
  public void testCreateMultipleLinks() throws IOException {
    deltaFileComputer = new TestableFileLinkDeltaFileComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    // Create multiple source files
    List<Path> sourceFiles = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      Path sourceFile = tempDir.resolve("source" + i + ".sst");
      Files.createFile(sourceFile);
      sourceFiles.add(sourceFile);
    }

    // Create hard links
    Set<String> linkNames = new HashSet<>();
    for (Path sourceFile : sourceFiles) {
      Path linkPath = deltaFileComputer.createLink(sourceFile);
      linkNames.add(Optional.ofNullable(linkPath.getFileName()).map(Path::toString).orElse("null"));
    }

    assertEquals(5, linkNames.size(), "All links should have unique names");
  }

  /**
   * Tests creating a link when the link already exists (concurrent scenario).
   */
  @Test
  public void testCreateLinkWhenLinkExists() throws IOException {
    deltaFileComputer = new TestableFileLinkDeltaFileComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    // Create a source file
    Path sourceFile = tempDir.resolve("source.sst");
    Files.createFile(sourceFile);

    // Create first link
    Path firstLink = deltaFileComputer.createLink(sourceFile);
    assertTrue(Files.exists(firstLink));

    // Manually create the next link file to simulate concurrent creation
    Path expectedNextLink = deltaDirPath.resolve("2.sst");
    Files.createFile(expectedNextLink);

    expectedNextLink = deltaDirPath.resolve("3.sst");
    // Try to create another link - it should handle the FileAlreadyExistsException
    Path secondLink = deltaFileComputer.createLink(sourceFile);
    assertEquals(expectedNextLink, secondLink);
  }

  /**
   * Tests the updateActivity method calls the activity reporter.
   */
  @Test
  public void testUpdateActivity() throws IOException {
    deltaFileComputer = new TestableFileLinkDeltaFileComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    SubStatus status = SubStatus.SST_FILE_DELTA_DAG_WALK;
    deltaFileComputer.updateActivity(status);

    verify(activityReporter, times(1)).accept(status);
  }

  /**
   * Tests the updateActivity method with multiple status updates.
   */
  @Test
  public void testMultipleActivityUpdates() throws IOException {
    deltaFileComputer = new TestableFileLinkDeltaFileComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    SubStatus[] statuses = {SubStatus.SST_FILE_DELTA_DAG_WALK, SubStatus.SST_FILE_DELTA_FULL_DIFF,
        SubStatus.DIFF_REPORT_GEN};
    for (SubStatus status : statuses) {
      deltaFileComputer.updateActivity(status);
    }

    ArgumentCaptor<SubStatus> captor = ArgumentCaptor.forClass(SubStatus.class);
    verify(activityReporter, times(3)).accept(captor.capture());
    assertEquals(3, captor.getAllValues().size());
  }

  /**
   * Tests the close method deletes the delta directory.
   */
  @Test
  public void testCloseDeletesDeltaDirectory() throws IOException {
    deltaFileComputer = new TestableFileLinkDeltaFileComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    assertTrue(Files.exists(deltaDirPath), "Delta directory should exist before close");

    deltaFileComputer.close();

    assertFalse(Files.exists(deltaDirPath), "Delta directory should be deleted after close");
  }

  /**
   * Tests close when delta directory doesn't exist.
   */
  @Test
  public void testCloseWithNonExistentDirectory() throws IOException {
    Path nonExistentPath = tempDir.resolve("nonexistent");
    deltaFileComputer = new TestableFileLinkDeltaFileComputer(omSnapshotManager, activeMetadataManager,
        nonExistentPath, activityReporter);

    // Delete the directory
    Files.deleteIfExists(nonExistentPath);

    // Close should not throw an exception
    deltaFileComputer.close();
  }

  /**
   * Tests close deletes directory with files in it.
   */
  @Test
  public void testCloseDeletesDirectoryWithFiles() throws IOException {
    deltaFileComputer = new TestableFileLinkDeltaFileComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    // Create source files and links
    for (int i = 0; i < 3; i++) {
      Path sourceFile = tempDir.resolve("source" + i + ".sst");
      Files.createFile(sourceFile);
      deltaFileComputer.createLink(sourceFile);
    }

    assertTrue(Files.list(deltaDirPath).count() > 0, "Delta directory should contain files");

    deltaFileComputer.close();

    assertFalse(Files.exists(deltaDirPath), "Delta directory with files should be deleted");
  }

  /**
   * Tests getLocalDataProvider delegates to snapshot manager.
   */
  @Test
  public void testGetLocalDataProvider() throws IOException {
    deltaFileComputer = new TestableFileLinkDeltaFileComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    UUID snapshotId = UUID.randomUUID();
    UUID toResolveId = UUID.randomUUID();
    ReadableOmSnapshotLocalDataProvider mockProvider = mock(ReadableOmSnapshotLocalDataProvider.class);

    when(localDataManager.getOmSnapshotLocalData(snapshotId, toResolveId)).thenReturn(mockProvider);

    ReadableOmSnapshotLocalDataProvider result = deltaFileComputer.getLocalDataProvider(snapshotId, toResolveId);

    assertEquals(mockProvider, result);
    verify(localDataManager, times(1)).getOmSnapshotLocalData(snapshotId, toResolveId);
  }

  /**
   * Tests getSnapshot delegates to snapshot manager.
   */
  @Test
  public void testGetSnapshot() throws IOException {
    deltaFileComputer = new TestableFileLinkDeltaFileComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    SnapshotInfo snapshotInfo = createMockSnapshotInfo("vol1", "bucket1", "snap1");
    @SuppressWarnings("unchecked")
    UncheckedAutoCloseableSupplier<OmSnapshot> mockSnapshot = mock(UncheckedAutoCloseableSupplier.class);

    when(omSnapshotManager.getActiveSnapshot("vol1", "bucket1", "snap1")).thenReturn(mockSnapshot);

    UncheckedAutoCloseableSupplier<OmSnapshot> result = deltaFileComputer.getSnapshot(snapshotInfo);

    assertEquals(mockSnapshot, result);
    verify(omSnapshotManager, times(1)).getActiveSnapshot("vol1", "bucket1", "snap1");
  }

  /**
   * Tests getActiveMetadataManager returns the correct instance.
   */
  @Test
  public void testGetActiveMetadataManager() throws IOException {
    deltaFileComputer = new TestableFileLinkDeltaFileComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    OMMetadataManager result = deltaFileComputer.getActiveMetadataManager();

    assertEquals(activeMetadataManager, result);
  }

  /**
   * Tests getDeltaFiles method invokes computeDeltaFiles correctly.
   */
  @Test
  public void testGetDeltaFiles() throws IOException {
    deltaFileComputer = new TestableFileLinkDeltaFileComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1");
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2");
    Set<String> tablesToLookup = ImmutableSet.of("keyTable", "fileTable");

    TablePrefixInfo tablePrefixInfo = mock(TablePrefixInfo.class);
    when(activeMetadataManager.getTableBucketPrefix("vol1", "bucket1")).thenReturn(tablePrefixInfo);

    // Set up the test implementation to return some delta files
    Map<Path, Pair<Path, SstFileInfo>> deltaMap = new HashMap<>();
    Path sstPath = tempDir.resolve("test.sst");
    Files.createFile(sstPath);
    SstFileInfo sstFileInfo = mock(SstFileInfo.class);
    deltaMap.put(deltaDirPath.resolve("1.sst"), Pair.of(sstPath, sstFileInfo));

    deltaFileComputer.setComputeDeltaFilesResult(Optional.of(deltaMap));

    Collection<Pair<Path, SstFileInfo>> result =
        deltaFileComputer.getDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup);

    assertEquals(1, result.size(), "Should have one delta file");
    verify(activeMetadataManager, times(1)).getTableBucketPrefix("vol1", "bucket1");
  }

  /**
   * Tests getDeltaFiles when computeDeltaFiles returns empty.
   */
  @Test
  public void testGetDeltaFilesReturnsEmpty() throws IOException {
    deltaFileComputer = new TestableFileLinkDeltaFileComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    SnapshotInfo fromSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap1");
    SnapshotInfo toSnapshot = createMockSnapshotInfo("vol1", "bucket1", "snap2");
    Set<String> tablesToLookup = ImmutableSet.of("keyTable");

    TablePrefixInfo tablePrefixInfo = mock(TablePrefixInfo.class);
    when(activeMetadataManager.getTableBucketPrefix("vol1", "bucket1")).thenReturn(tablePrefixInfo);

    deltaFileComputer.setComputeDeltaFilesResult(Optional.empty());

    assertThrows(IOException.class, () -> deltaFileComputer.getDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup));
  }

  /**
   * Tests that links preserve file extensions correctly.
   */
  @Test
  public void testLinkPreservesFileExtension() throws IOException {
    deltaFileComputer = new TestableFileLinkDeltaFileComputer(omSnapshotManager, activeMetadataManager,
        deltaDirPath, activityReporter);

    String[] extensions = {"sst", "txt", "log", "data"};
    for (String ext : extensions) {
      Path sourceFile = tempDir.resolve("source." + ext);
      Files.createFile(sourceFile);

      Path linkPath = deltaFileComputer.createLink(sourceFile);

      assertTrue(linkPath.getFileName().toString().endsWith("." + ext),
          "Link should preserve extension: " + ext);
    }
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

  /**
   * Concrete implementation of FileLinkDeltaFileComputer for testing.
   */
  private static class TestableFileLinkDeltaFileComputer extends FileLinkDeltaFileComputer {

    private Optional<Map<Path, Pair<Path, SstFileInfo>>> computeDeltaFilesResult = Optional.empty();

    TestableFileLinkDeltaFileComputer(OmSnapshotManager snapshotManager, OMMetadataManager activeMetadataManager,
        Path deltaDirPath, Consumer<SubStatus> activityReporter) throws IOException {
      super(snapshotManager, activeMetadataManager, deltaDirPath, activityReporter);
    }

    @Override
    Optional<Map<Path, Pair<Path, SstFileInfo>>> computeDeltaFiles(SnapshotInfo fromSnapshot,
        SnapshotInfo toSnapshot, Set<String> tablesToLookup, TablePrefixInfo tablePrefixInfo) throws IOException {
      return computeDeltaFilesResult;
    }

    void setComputeDeltaFilesResult(Optional<Map<Path, Pair<Path, SstFileInfo>>> result) {
      this.computeDeltaFilesResult = result;
    }
  }
}
