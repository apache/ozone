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

package org.apache.hadoop.ozone.om;

import static org.apache.commons.io.file.PathUtils.copyDirectory;
import static org.apache.hadoop.hdds.utils.HAUtils.getExistingFiles;
import static org.apache.hadoop.hdds.utils.IOUtils.getINode;
import static org.apache.hadoop.ozone.OzoneConsts.OM_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.SNAPSHOT_CANDIDATE_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.SNAPSHOT_INFO_TABLE;
import static org.apache.hadoop.ozone.om.OMDBCheckpointServlet.processFile;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.OM_HARDLINK_FILE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.VOLUME_TABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.InodeMetadataRocksDBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.slf4j.event.Level;

/**
 * Unit test ozone snapshot manager.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestOmSnapshotManager {

  private OzoneManager om;
  private SnapshotChainManager snapshotChainManager;
  private OmMetadataManagerImpl omMetadataManager;
  private OmSnapshotManager omSnapshotManager;
  private OmSnapshotLocalDataManager snapshotLocalDataManager;
  private static final String CANDIDATE_DIR_NAME = OM_DB_NAME +
      SNAPSHOT_CANDIDATE_DIR;
  private File leaderDir;
  private File leaderSnapDir1;
  private File leaderSnapDir2;
  private File followerSnapDir2;
  private File leaderCheckpointDir;
  private File candidateDir;
  private File s1File;
  private File f1File;

  @BeforeAll
  void init(@TempDir File tempDir) throws Exception {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempDir.toString());
    // Enable filesystem snapshot feature for the test regardless of the default
    configuration.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY,
        true);

    // Only allow one entry in cache so each new one causes an eviction
    configuration.setInt(
        OMConfigKeys.OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE, 1);
    configuration.setBoolean(
        OMConfigKeys.OZONE_OM_SNAPSHOT_ROCKSDB_METRICS_ENABLED, false);
    // Allow 2 fs snapshots
    configuration.setInt(
        OMConfigKeys.OZONE_OM_FS_SNAPSHOT_MAX_LIMIT, 2);

    OmTestManagers omTestManagers = new OmTestManagers(configuration);
    om = omTestManagers.getOzoneManager();
    omMetadataManager = (OmMetadataManagerImpl) om.getMetadataManager();
    omSnapshotManager = om.getOmSnapshotManager();
    snapshotLocalDataManager = om.getOmSnapshotManager().getSnapshotLocalDataManager();
    snapshotChainManager = omMetadataManager.getSnapshotChainManager();
  }

  @AfterAll
  void stop() {
    om.stop();
  }

  @AfterEach
  void cleanup() throws IOException {
    Table<String, SnapshotInfo> snapshotInfoTable = omMetadataManager.getSnapshotInfoTable();

    Iterator<UUID> iter = snapshotChainManager.iterator(true);
    while (iter.hasNext()) {
      UUID snapshotId = iter.next();
      String snapshotInfoKey = snapshotChainManager.getTableKey(snapshotId);
      SnapshotInfo snapshotInfo = snapshotInfoTable.get(snapshotInfoKey);
      snapshotChainManager.deleteSnapshot(snapshotInfo);
      snapshotInfoTable.delete(snapshotInfoKey);

      Path snapshotYaml = Paths.get(snapshotLocalDataManager.getSnapshotLocalPropertyYamlPath(snapshotInfo));
      Files.deleteIfExists(snapshotYaml);
    }
    omSnapshotManager.invalidateCache();
  }

  @Test
  public void testSnapshotFeatureFlagSafetyCheck() throws IOException {
    // Verify that the snapshot feature config safety check method
    // is returning the expected value.
    final TypedTable<String, SnapshotInfo> snapshotInfoTable = mock(TypedTable.class);
    HddsWhiteboxTestUtils.setInternalState(
        om.getMetadataManager(), SNAPSHOT_INFO_TABLE, snapshotInfoTable);

    when(snapshotInfoTable.isEmpty()).thenReturn(false);
    assertFalse(om.getOmSnapshotManager().canDisableFsSnapshot(om.getMetadataManager()));

    when(snapshotInfoTable.isEmpty()).thenReturn(true);
    assertTrue(om.getOmSnapshotManager().canDisableFsSnapshot(om.getMetadataManager()));
  }

  @Test
  public void testCloseOnEviction() throws IOException,
      InterruptedException, TimeoutException {

    GenericTestUtils.setLogLevel(RDBStore.class, Level.DEBUG);
    LogCapturer logCapture = LogCapturer.captureLogs(RDBStore.class);
    // set up db tables
    final TypedTable<String, OmVolumeArgs> volumeTable = mock(TypedTable.class);
    final TypedTable<String, OmBucketInfo> bucketTable = mock(TypedTable.class);
    final TypedTable<String, SnapshotInfo> snapshotInfoTable = mock(TypedTable.class);
    HddsWhiteboxTestUtils.setInternalState(
        omMetadataManager, VOLUME_TABLE, volumeTable);
    HddsWhiteboxTestUtils.setInternalState(
        omMetadataManager, BUCKET_TABLE, bucketTable);
    HddsWhiteboxTestUtils.setInternalState(
        omMetadataManager, SNAPSHOT_INFO_TABLE, snapshotInfoTable);

    final String volumeName = UUID.randomUUID().toString();
    final String dbVolumeKey = om.getMetadataManager().getVolumeKey(volumeName);
    final OmVolumeArgs omVolumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setAdminName("bilbo")
        .setOwnerName("bilbo")
        .build();
    when(volumeTable.get(dbVolumeKey)).thenReturn(omVolumeArgs);

    String bucketName = UUID.randomUUID().toString();
    final String dbBucketKey = om.getMetadataManager().getBucketKey(
        volumeName, bucketName);
    final OmBucketInfo omBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .build();
    when(bucketTable.get(dbBucketKey)).thenReturn(omBucketInfo);

    SnapshotInfo first = createSnapshotInfo(volumeName, bucketName);
    SnapshotInfo second = createSnapshotInfo(volumeName, bucketName);
    first.setGlobalPreviousSnapshotId(null);
    first.setPathPreviousSnapshotId(null);
    second.setGlobalPreviousSnapshotId(first.getSnapshotId());
    second.setPathPreviousSnapshotId(first.getSnapshotId());

    when(snapshotInfoTable.get(first.getTableKey())).thenReturn(first);
    when(snapshotInfoTable.get(second.getTableKey())).thenReturn(second);

    snapshotChainManager.addSnapshot(first);
    snapshotChainManager.addSnapshot(second);
    RDBBatchOperation rdbBatchOperation = RDBBatchOperation.newAtomicOperation();
    // create the first snapshot checkpoint
    OmSnapshotManager.createOmSnapshotCheckpoint(om.getMetadataManager(),
        first, rdbBatchOperation);
    om.getMetadataManager().getStore().commitBatchOperation(rdbBatchOperation);

    // retrieve it and setup store mock
    OmSnapshot firstSnapshot = omSnapshotManager
        .getActiveSnapshot(first.getVolumeName(), first.getBucketName(), first.getName())
        .get();
    DBStore firstSnapshotStore = mock(DBStore.class);
    HddsWhiteboxTestUtils.setInternalState(
        firstSnapshot.getMetadataManager(), "store", firstSnapshotStore);

    // create second snapshot checkpoint (which will be used for eviction)
    rdbBatchOperation = RDBBatchOperation.newAtomicOperation();
    OmSnapshotManager.createOmSnapshotCheckpoint(om.getMetadataManager(),
        second, rdbBatchOperation);
    om.getMetadataManager().getStore().commitBatchOperation(rdbBatchOperation);

    // confirm store not yet closed
    verify(firstSnapshotStore, times(0)).close();

    // read in second snapshot to evict first
    omSnapshotManager
        .getActiveSnapshot(second.getVolumeName(), second.getBucketName(), second.getName());

    // As a workaround, invalidate all cache entries in order to trigger
    // instances close in this test case, since JVM GC most likely would not
    // have triggered and closed the instances yet at this point.
    omSnapshotManager.invalidateCache();

    // confirm store was closed
    verify(firstSnapshotStore, timeout(3000).times(1)).close();

    // Verify RocksDBStoreMetrics registration is skipped.
    String msg = "Skipped Metrics registration during RocksDB init";
    GenericTestUtils.waitFor(() -> {
      return logCapture.getOutput().contains(msg);
    }, 100, 30_000);
  }

  @Test
  public void testValidateSnapshotLimit() throws IOException {
    TypedTable<String, SnapshotInfo> snapshotInfoTable = mock(TypedTable.class);
    HddsWhiteboxTestUtils.setInternalState(
        omMetadataManager, SNAPSHOT_INFO_TABLE, snapshotInfoTable);

    SnapshotInfo first = createSnapshotInfo("vol1", "buck1");
    SnapshotInfo second = createSnapshotInfo("vol1", "buck1");

    first.setGlobalPreviousSnapshotId(null);
    first.setPathPreviousSnapshotId(null);
    second.setGlobalPreviousSnapshotId(first.getSnapshotId());
    second.setPathPreviousSnapshotId(first.getSnapshotId());

    when(snapshotInfoTable.get(first.getTableKey())).thenReturn(first);
    when(snapshotInfoTable.get(second.getTableKey())).thenReturn(second);

    snapshotChainManager.addSnapshot(first);
    assertDoesNotThrow(() -> omSnapshotManager.snapshotLimitCheck());
    omSnapshotManager.decrementInFlightSnapshotCount();

    snapshotChainManager.addSnapshot(second);

    OMException exception = assertThrows(OMException.class, () -> omSnapshotManager.snapshotLimitCheck());
    assertEquals(OMException.ResultCodes.TOO_MANY_SNAPSHOTS, exception.getResult());

    snapshotChainManager.deleteSnapshot(second);

    assertDoesNotThrow(() -> omSnapshotManager.snapshotLimitCheck());
  }

  @BeforeEach
  void setupData(@TempDir File testDir) throws IOException {
    // Set up the leader with the following files:
    // leader/db.checkpoints/checkpoint1/f1.sst
    // leader/db.snapshots/checkpointState/snap1/s1.sst
    // leader/db.snapshots/checkpointState/snap2/noLink.sst
    // leader/db.snapshots/checkpointState/snap2/nonSstFile

    // Set up the follower with the following files, (as if they came
    // from the tarball from the leader)

    // follower/om.db.candidate/f1.sst
    // follower/om.db.candidate/db.snapshots/checkpointState/snap1/s1.sst
    // follower/om.db.candidate/db.snapshots/checkpointState/snap2/noLink.sst
    // follower/om.db.candidate/db.snapshots/checkpointState/snap2/nonSstFile

    // Note that the layout between leader and follower is slightly
    // different in that the f1.sst on the leader is in the
    // db.checkpoints/checkpoint1 directory but on the follower is
    // moved to the om.db.candidate directory; the links must be adjusted
    // accordingly.

    byte[] dummyData = {0};

    // Create dummy leader files to calculate links.
    leaderDir = new File(testDir, "leader");
    assertTrue(leaderDir.mkdirs());
    String pathSnap1 = OM_SNAPSHOT_CHECKPOINT_DIR + OM_KEY_PREFIX + "snap1";
    String pathSnap2 = OM_SNAPSHOT_CHECKPOINT_DIR + OM_KEY_PREFIX + "snap2";
    leaderSnapDir1 = new File(leaderDir.toString(), pathSnap1);
    assertTrue(leaderSnapDir1.mkdirs());
    Files.write(Paths.get(leaderSnapDir1.toString(), "s1.sst"), dummyData);

    leaderSnapDir2 = new File(leaderDir.toString(), pathSnap2);
    assertTrue(leaderSnapDir2.mkdirs());
    Files.write(Paths.get(leaderSnapDir2.toString(), "noLink.sst"), dummyData);
    Files.write(Paths.get(leaderSnapDir2.toString(), "nonSstFile"), dummyData);

    // Also create the follower files.
    candidateDir = new File(testDir, CANDIDATE_DIR_NAME);
    File followerSnapDir1 = new File(candidateDir.toString(), pathSnap1);
    followerSnapDir2 = new File(candidateDir.toString(), pathSnap2);
    copyDirectory(leaderDir.toPath(), candidateDir.toPath());
    f1File = new File(candidateDir, "f1.sst");
    Files.write(f1File.toPath(), dummyData);
    s1File = new File(followerSnapDir1, "s1.sst");
    // confirm s1 file got copied over.
    assertTrue(s1File.exists());

    // Finish creating leaders files that are not to be copied over, because
    //  f1.sst belongs in a different directory as explained above.
    leaderCheckpointDir = new File(leaderDir.toString(),
        OM_CHECKPOINT_DIR + OM_KEY_PREFIX + "checkpoint1");
    assertTrue(leaderCheckpointDir.mkdirs());
    Files.write(Paths.get(leaderCheckpointDir.toString(), "f1.sst"), dummyData);
  }

  /*
   * Create map of links to files on the leader:
   *     leader/db.snapshots/checkpointState/snap2/<link to f1.sst>
   *     leader/db.snapshots/checkpointState/snap2/<link to s1.sst>
   * and test that corresponding links are created on the Follower:
   *     follower/db.snapshots/checkpointState/snap2/f1.sst
   *     follower/db.snapshots/checkpointState/snap2/s1.sst
   */
  @Test
  public void testHardLinkCreation() throws IOException {

    // Map of links to files on the leader
    Map<Path, Path> hardLinkFiles = new HashMap<>();
    hardLinkFiles.put(Paths.get(leaderSnapDir2.toString(), "f1.sst"),
        Paths.get(leaderCheckpointDir.toString(), "f1.sst"));
    hardLinkFiles.put(Paths.get(leaderSnapDir2.toString(), "s1.sst"),
        Paths.get(leaderSnapDir1.toString(), "s1.sst"));

    // Create link list from leader map.
    Path hardLinkList =
        OmSnapshotUtils.createHardLinkList(
            leaderDir.toString().length() + 1, hardLinkFiles);

    Files.move(hardLinkList, Paths.get(candidateDir.toString(),
        OM_HARDLINK_FILE));

    Path snapshot2Path = Paths.get(candidateDir.getPath(),
        OM_SNAPSHOT_CHECKPOINT_DIR, followerSnapDir2.getName());

    // Pointers to follower links to be created.
    File f1FileLink = new File(snapshot2Path.toFile(), "f1.sst");
    File s1FileLink = new File(snapshot2Path.toFile(), "s1.sst");
    Object s1FileInode = getINode(s1File.toPath());
    Object f1FileInode = getINode(f1File.toPath());
    // Create links on the follower from list.
    InodeMetadataRocksDBCheckpoint obtainedCheckpoint =
        new InodeMetadataRocksDBCheckpoint(candidateDir.toPath());
    assertNotNull(obtainedCheckpoint);

    // Confirm expected follower links.
    assertTrue(s1FileLink.exists());
    assertEquals(s1FileInode,
        getINode(s1FileLink.toPath()), "link matches original file");

    assertTrue(f1FileLink.exists());
    assertEquals(f1FileInode,
        getINode(f1FileLink.toPath()), "link matches original file");
  }

  @Test
  public void testGetSnapshotInfo() throws IOException {
    SnapshotInfo s1 = createSnapshotInfo("vol", "buck");
    UUID latestGlobalSnapId = snapshotChainManager.getLatestGlobalSnapshotId();
    UUID latestPathSnapId =
        snapshotChainManager.getLatestPathSnapshotId(String.join("/", "vol", "buck"));
    s1.setPathPreviousSnapshotId(latestPathSnapId);
    s1.setGlobalPreviousSnapshotId(latestGlobalSnapId);
    snapshotChainManager.addSnapshot(s1);
    OMException ome = assertThrows(OMException.class,
        () -> om.getOmSnapshotManager().getSnapshot(s1.getSnapshotId()));
    assertEquals(OMException.ResultCodes.FILE_NOT_FOUND, ome.getResult());
    // not present in snapshot chain too
    SnapshotInfo s2 = createSnapshotInfo("vol", "buck");
    ome = assertThrows(OMException.class,
        () -> om.getOmSnapshotManager().getSnapshot(s2.getSnapshotId()));
    assertEquals(OMException.ResultCodes.FILE_NOT_FOUND, ome.getResult());

    // add to make cleanup work
    TypedTable<String, SnapshotInfo> snapshotInfoTable = mock(TypedTable.class);
    HddsWhiteboxTestUtils.setInternalState(
        omMetadataManager, SNAPSHOT_INFO_TABLE, snapshotInfoTable);
    when(snapshotInfoTable.get(s1.getTableKey())).thenReturn(s1);
  }

  /*
   * Test that exclude list is generated correctly.
   */
  @Test
  public void testExcludeUtilities() throws IOException {
    File noLinkFile = new File(followerSnapDir2, "noLink.sst");
    File nonSstFile = new File(followerSnapDir2, "nonSstFile");
    // Confirm that the list of existing sst files is as expected.
    List<String> existingSstList = getExistingFiles(candidateDir);
    Set<String> existingSstFiles = new HashSet<>(existingSstList);
    Set<String> expectedSstFileNames = new HashSet<>(Arrays.asList(
        s1File.getName(),
        noLinkFile.getName(),
        f1File.getName(),
        nonSstFile.getName()));
    assertEquals(expectedSstFileNames, existingSstFiles);
  }

  /*
   * Confirm that processFile() correctly determines whether a file
   * should be copied, linked, or excluded from the tarball entirely.
   * This test always passes in a null dest dir.
   */
  @Test
  void testProcessFileWithNullDestDirParameter(@TempDir File testDir) throws IOException {
    assertTrue(new File(testDir, "snap1").mkdirs());
    assertTrue(new File(testDir, "snap2").mkdirs());
    Path copyFile = Paths.get(testDir.toString(),
        "snap1/copyfile.sst");
    Path copyFileName = copyFile.getFileName();
    assertNotNull(copyFileName);
    Files.write(copyFile,
        "dummyData".getBytes(StandardCharsets.UTF_8));
    long expectedFileSize = Files.size(copyFile);
    Path excludeFile = Paths.get(testDir.toString(),
        "snap1/excludeFile.sst");
    Path excludeFileName = excludeFile.getFileName();
    assertNotNull(excludeFileName);
    Files.write(excludeFile,
        "dummyData".getBytes(StandardCharsets.UTF_8));
    Path linkToExcludedFile = Paths.get(testDir.toString(),
        "snap2/excludeFile.sst");
    Files.createLink(linkToExcludedFile, excludeFile);
    Path linkToCopiedFile = Paths.get(testDir.toString(),
        "snap2/copyfile.sst");
    Files.createLink(linkToCopiedFile, copyFile);
    Path addToCopiedFiles = Paths.get(testDir.toString(),
        "snap1/copyfile2.sst");
    Files.write(addToCopiedFiles,
        "dummyData".getBytes(StandardCharsets.UTF_8));
    Path addNonSstToCopiedFiles = Paths.get(testDir.toString(),
        "snap1/nonSst");
    Files.write(addNonSstToCopiedFiles,
        "dummyData".getBytes(StandardCharsets.UTF_8));

    Map<String, Map<Path, Path>> toExcludeFiles = new HashMap<>();
    toExcludeFiles.computeIfAbsent(excludeFileName.toString(), (k) -> new HashMap<>()).put(excludeFile,
        excludeFile);
    Map<String, Map<Path, Path>> copyFiles = new HashMap<>();
    copyFiles.computeIfAbsent(copyFileName.toString(), (k) -> new HashMap<>()).put(copyFile,
        copyFile);
    Map<Path, Path> hardLinkFiles = new HashMap<>();
    long fileSize;
    fileSize = processFile(excludeFile, copyFiles, hardLinkFiles,
        toExcludeFiles, null);
    assertEquals(copyFiles.size(), 1);
    assertEquals(hardLinkFiles.size(), 0);
    assertEquals(fileSize, 0);

    // Confirm the linkToExcludedFile gets added as a link.
    fileSize = processFile(linkToExcludedFile, copyFiles, hardLinkFiles,
        toExcludeFiles, null);
    assertEquals(copyFiles.size(), 1);
    assertEquals(hardLinkFiles.size(), 1);
    assertEquals(hardLinkFiles.get(linkToExcludedFile), excludeFile);
    assertEquals(fileSize, 0);
    hardLinkFiles = new HashMap<>();

    // Confirm the linkToCopiedFile gets added as a link.
    fileSize = processFile(linkToCopiedFile, copyFiles, hardLinkFiles,
        toExcludeFiles, null);
    assertEquals(copyFiles.size(), 1);
    assertEquals(hardLinkFiles.size(), 1);
    assertEquals(hardLinkFiles.get(linkToCopiedFile), copyFile);
    assertEquals(fileSize, 0);
    hardLinkFiles = new HashMap<>();

    // Confirm the addToCopiedFiles gets added to list of copied files
    fileSize = processFile(addToCopiedFiles, copyFiles, hardLinkFiles,
        toExcludeFiles, null);
    assertEquals(copyFiles.size(), 2);
    assertEquals(copyFiles.get(addToCopiedFiles.getFileName().toString()).get(addToCopiedFiles), addToCopiedFiles);
    assertEquals(fileSize, expectedFileSize);
    copyFiles = new HashMap<>();
    copyFiles.computeIfAbsent(copyFileName.toString(), (k) -> new HashMap<>()).put(copyFile, copyFile);

    // Confirm the addNonSstToCopiedFiles gets added to list of copied files
    fileSize = processFile(addNonSstToCopiedFiles, copyFiles, hardLinkFiles,
        toExcludeFiles, null);
    assertEquals(copyFiles.size(), 2);
    assertEquals(fileSize, 0);
    assertEquals(copyFiles.get(addNonSstToCopiedFiles.getFileName().toString()).get(addNonSstToCopiedFiles),
        addNonSstToCopiedFiles);
  }

  /*
   * Confirm that processFile() correctly determines whether a file
   * should be copied, linked, or excluded from the tarball entirely.
   * This test always passes in a non-null dest dir.
   */
  @Test
  void testProcessFileWithDestDirParameter(@TempDir File testDir) throws IOException {
    assertTrue(new File(testDir, "snap1").mkdirs());
    assertTrue(new File(testDir, "snap2").mkdirs());
    assertTrue(new File(testDir, "snap3").mkdirs());
    Path destDir = Paths.get(testDir.toString(), "destDir");
    assertTrue(new File(destDir.toString()).mkdirs());

    // Create test files.
    Path copyFile = Paths.get(testDir.toString(),
        "snap1/copyfile.sst");
    Path copyFileName = copyFile.getFileName();
    assertNotNull(copyFileName);
    Path destCopyFile = Paths.get(destDir.toString(),
        "snap1/copyfile.sst");
    Files.write(copyFile,
        "dummyData".getBytes(StandardCharsets.UTF_8));
    Path sameNameAsCopyFile = Paths.get(testDir.toString(),
        "snap3/copyFile.sst");
    Files.write(sameNameAsCopyFile,
        "dummyData".getBytes(StandardCharsets.UTF_8));
    Path destSameNameAsCopyFile = Paths.get(destDir.toString(),
        "snap3/copyFile.sst");
    long expectedFileSize = Files.size(copyFile);
    Path excludeFile = Paths.get(testDir.toString(),
        "snap1/excludeFile.sst");
    Path excludeFileName = excludeFile.getFileName();
    assertNotNull(excludeFileName);
    Path destExcludeFile = Paths.get(destDir.toString(),
        "snap1/excludeFile.sst");
    Files.write(excludeFile,
        "dummyData".getBytes(StandardCharsets.UTF_8));
    Path linkToExcludedFile = Paths.get(testDir.toString(),
        "snap2/excludeFile.sst");
    Path destLinkToExcludedFile = Paths.get(destDir.toString(),
        "snap2/excludeFile.sst");
    Files.createLink(linkToExcludedFile, excludeFile);
    Path sameNameAsExcludeFile = Paths.get(testDir.toString(),
        "snap3/excludeFile.sst");
    Files.write(sameNameAsExcludeFile,
        "dummyData".getBytes(StandardCharsets.UTF_8));
    Path destSameNameAsExcludeFile = Paths.get(destDir.toString(),
        "snap3/excludeFile.sst");
    Path linkToCopiedFile = Paths.get(testDir.toString(),
        "snap2/copyfile.sst");
    Path destLinkToCopiedFile = Paths.get(destDir.toString(),
        "snap2/copyfile.sst");
    Files.createLink(linkToCopiedFile, copyFile);
    Path addToCopiedFiles = Paths.get(testDir.toString(),
        "snap1/copyfile2.sst");
    Path destAddToCopiedFiles = Paths.get(destDir.toString(),
        "snap1/copyfile2.sst");
    Files.write(addToCopiedFiles,
        "dummyData".getBytes(StandardCharsets.UTF_8));
    Path addNonSstToCopiedFiles = Paths.get(testDir.toString(),
        "snap1/nonSst");
    Path destAddNonSstToCopiedFiles = Paths.get(destDir.toString(),
        "snap1/nonSst");
    Files.write(addNonSstToCopiedFiles,
        "dummyData".getBytes(StandardCharsets.UTF_8));

    // Create test data structures.
    Map<String, Map<Path, Path>> toExcludeFiles = new HashMap<>();
    toExcludeFiles.put(excludeFileName.toString(), ImmutableMap.of(excludeFile, destExcludeFile));
    Map<String, Map<Path, Path>> copyFiles = new HashMap<>();
    copyFiles.computeIfAbsent(copyFileName.toString(), (k) -> new HashMap<>()).put(copyFile, destCopyFile);
    Map<Path, Path> hardLinkFiles = new HashMap<>();
    long fileSize;

    fileSize = processFile(excludeFile, copyFiles, hardLinkFiles,
        toExcludeFiles, destExcludeFile.getParent());
    assertEquals(copyFiles.size(), 1);
    assertEquals(hardLinkFiles.size(), 0);
    assertEquals(fileSize, 0);

    // Confirm the linkToExcludedFile gets added as a link.
    fileSize = processFile(linkToExcludedFile, copyFiles, hardLinkFiles,
        toExcludeFiles, destLinkToExcludedFile.getParent());
    assertEquals(copyFiles.size(), 1);
    assertEquals(hardLinkFiles.size(), 1);
    assertEquals(hardLinkFiles.get(destLinkToExcludedFile),
        destExcludeFile);
    assertEquals(fileSize, 0);
    hardLinkFiles = new HashMap<>();

    // Confirm the file with same name as excluded file gets copied.
    fileSize = processFile(sameNameAsExcludeFile, copyFiles, hardLinkFiles,
        toExcludeFiles, destSameNameAsExcludeFile.getParent());
    assertEquals(copyFiles.size(), 2);
    assertEquals(hardLinkFiles.size(), 0);
    assertEquals(copyFiles.get(sameNameAsExcludeFile.getFileName().toString()).get(sameNameAsExcludeFile),
        destSameNameAsExcludeFile);
    assertEquals(fileSize, expectedFileSize);
    copyFiles = new HashMap<>();
    copyFiles.computeIfAbsent(copyFileName.toString(), (k) -> new HashMap<>()).put(copyFile, destCopyFile);


    // Confirm the file with same name as copy file gets copied.
    fileSize = processFile(sameNameAsCopyFile, copyFiles, hardLinkFiles,
        toExcludeFiles, destSameNameAsCopyFile.getParent());
    assertEquals(copyFiles.size(), 2);
    assertEquals(hardLinkFiles.size(), 0);
    assertEquals(copyFiles.get(sameNameAsCopyFile.getFileName().toString()).get(sameNameAsCopyFile),
        destSameNameAsCopyFile);
    assertEquals(fileSize, expectedFileSize);
    copyFiles = new HashMap<>();
    copyFiles.computeIfAbsent(copyFileName.toString(), (k) -> new HashMap<>()).put(copyFile, destCopyFile);


    // Confirm the linkToCopiedFile gets added as a link.
    fileSize = processFile(linkToCopiedFile, copyFiles, hardLinkFiles,
        toExcludeFiles, destLinkToCopiedFile.getParent());
    assertEquals(copyFiles.size(), 1);
    assertEquals(hardLinkFiles.size(), 1);
    assertEquals(hardLinkFiles.get(destLinkToCopiedFile),
        destCopyFile);
    assertEquals(fileSize, 0);
    hardLinkFiles = new HashMap<>();

    // Confirm the addToCopiedFiles gets added to list of copied files
    fileSize = processFile(addToCopiedFiles, copyFiles, hardLinkFiles,
        toExcludeFiles, destAddToCopiedFiles.getParent());
    assertEquals(copyFiles.size(), 2);
    assertEquals(copyFiles.get(addToCopiedFiles.getFileName().toString()).get(addToCopiedFiles),
        destAddToCopiedFiles);
    assertEquals(fileSize, expectedFileSize);
    copyFiles = new HashMap<>();
    copyFiles.computeIfAbsent(copyFileName.toString(), (k) -> new HashMap<>()).put(copyFile, destCopyFile);

    // Confirm the addNonSstToCopiedFiles gets added to list of copied files
    fileSize = processFile(addNonSstToCopiedFiles, copyFiles, hardLinkFiles,
        toExcludeFiles, destAddNonSstToCopiedFiles.getParent());
    assertEquals(copyFiles.size(), 2);
    assertEquals(fileSize, 0);
    assertEquals(copyFiles.get(addNonSstToCopiedFiles.getFileName().toString()).get(addNonSstToCopiedFiles),
        destAddNonSstToCopiedFiles);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 10, 100})
  public void testGetSnapshotPath(int version) {
    OMMetadataManager metadataManager = mock(OMMetadataManager.class);
    RDBStore store = mock(RDBStore.class);
    when(metadataManager.getStore()).thenReturn(store);
    File file = new File("test-db");
    when(store.getDbLocation()).thenReturn(file);
    String path = "dir1/dir2";
    when(store.getSnapshotsParentDir()).thenReturn(path);
    UUID snapshotId = UUID.randomUUID();
    String snapshotPath = OmSnapshotManager.getSnapshotPath(metadataManager, snapshotId, version).toString();
    String expectedPath = "dir1/dir2/test-db-" + snapshotId;
    if (version != 0) {
      expectedPath = expectedPath + "-" + version;
    }
    assertEquals(expectedPath, snapshotPath);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 10, 100})
  public void testGetSnapshotPathFromConf(int version) {
    try (MockedStatic<OMStorage> mocked = mockStatic(OMStorage.class)) {
      String omDir = "dir1/dir2";
      mocked.when(() -> OMStorage.getOmDbDir(any())).thenReturn(new File(omDir));
      OzoneConfiguration conf = mock(OzoneConfiguration.class);
      SnapshotInfo snapshotInfo = createSnapshotInfo("volumeName", "bucketname");
      String snapshotPath = OmSnapshotManager.getSnapshotPath(conf, snapshotInfo, version);
      String expectedPath = omDir + OM_KEY_PREFIX + OM_SNAPSHOT_CHECKPOINT_DIR + OM_KEY_PREFIX +
          OM_DB_NAME + "-" + snapshotInfo.getSnapshotId();
      if (version != 0) {
        expectedPath = expectedPath + "-" + version;
      }
      assertEquals(expectedPath, snapshotPath);
    }
  }

  @Test
  public void testCreateSnapshotIdempotent() throws Exception {
    // set up db tables
    LogCapturer logCapturer = LogCapturer.captureLogs(OmSnapshotManager.class);
    final TypedTable<String, OmVolumeArgs> volumeTable = mock(TypedTable.class);
    final TypedTable<String, OmBucketInfo> bucketTable = mock(TypedTable.class);
    final TypedTable<String, SnapshotInfo> snapshotInfoTable = mock(TypedTable.class);
    HddsWhiteboxTestUtils.setInternalState(
        om.getMetadataManager(), VOLUME_TABLE, volumeTable);
    HddsWhiteboxTestUtils.setInternalState(
        om.getMetadataManager(), BUCKET_TABLE, bucketTable);
    HddsWhiteboxTestUtils.setInternalState(
        om.getMetadataManager(), SNAPSHOT_INFO_TABLE, snapshotInfoTable);

    final String volumeName = UUID.randomUUID().toString();
    final String dbVolumeKey = om.getMetadataManager().getVolumeKey(volumeName);
    final OmVolumeArgs omVolumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setAdminName("bilbo")
        .setOwnerName("bilbo")
        .build();
    when(volumeTable.get(dbVolumeKey)).thenReturn(omVolumeArgs);

    String bucketName = UUID.randomUUID().toString();
    final String dbBucketKey = om.getMetadataManager().getBucketKey(
        volumeName, bucketName);
    final OmBucketInfo omBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .build();
    when(bucketTable.get(dbBucketKey)).thenReturn(omBucketInfo);

    SnapshotInfo first = createSnapshotInfo(volumeName, bucketName);
    first.setPathPreviousSnapshotId(null);
    when(snapshotInfoTable.get(first.getTableKey())).thenReturn(first);

    // Create first checkpoint for the snapshot checkpoint
    RDBBatchOperation rdbBatchOperation = RDBBatchOperation.newAtomicOperation();
    OmSnapshotManager.createOmSnapshotCheckpoint(om.getMetadataManager(),
        first, rdbBatchOperation);
    om.getMetadataManager().getStore().commitBatchOperation(rdbBatchOperation);
    assertThat(logCapturer.getOutput()).doesNotContain(
        "for snapshot " + first.getName() + " already exists.");
    logCapturer.clearOutput();

    // Create checkpoint again for the same snapshot.
    rdbBatchOperation = RDBBatchOperation.newAtomicOperation();
    OmSnapshotManager.createOmSnapshotCheckpoint(om.getMetadataManager(),
        first, rdbBatchOperation);
    om.getMetadataManager().getStore().commitBatchOperation(rdbBatchOperation);

    assertThat(logCapturer.getOutput())
        .contains("for snapshot " + first.getTableKey() + " already exists.");
  }

  private SnapshotInfo createSnapshotInfo(String volumeName,
                                          String bucketName) {
    SnapshotInfo snapshotInfo = SnapshotInfo.newInstance(volumeName,
        bucketName,
        UUID.randomUUID().toString(),
        UUID.randomUUID(),
        Time.now());
    snapshotInfo.setPathPreviousSnapshotId(null);
    snapshotInfo.setGlobalPreviousSnapshotId(null);
    return snapshotInfo;
  }
}
