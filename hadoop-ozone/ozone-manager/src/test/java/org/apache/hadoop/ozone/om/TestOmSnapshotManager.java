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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.commons.io.file.PathUtils.copyDirectory;
import static org.apache.hadoop.hdds.utils.HAUtils.getExistingSstFiles;
import static org.apache.hadoop.ozone.OzoneConsts.OM_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.SNAPSHOT_CANDIDATE_DIR;
import static org.apache.hadoop.ozone.om.OMDBCheckpointServlet.processFile;
import static org.apache.hadoop.ozone.OzoneConsts.SNAPSHOT_INFO_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.VOLUME_TABLE;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.OM_HARDLINK_FILE;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.getINode;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPrefix;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test ozone snapshot manager.
 */
public class TestOmSnapshotManager {

  private OzoneManager om;
  private File testDir;
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

  @Before
  public void init() throws Exception {
    OzoneConfiguration configuration = new OzoneConfiguration();
    testDir = GenericTestUtils.getRandomizedTestDir();
    configuration.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        testDir.toString());
    // Enable filesystem snapshot feature for the test regardless of the default
    configuration.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY,
        true);

    // Only allow one entry in cache so each new one causes an eviction
    configuration.setInt(
        OMConfigKeys.OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE, 1);

    OmTestManagers omTestManagers = new OmTestManagers(configuration);
    om = omTestManagers.getOzoneManager();
    setupData();
  }

  @After
  public void cleanup() throws Exception {
    om.stop();
    FileUtils.deleteDirectory(testDir);
  }

  @Test
  public void testCloseOnEviction() throws IOException {

    // set up db tables
    Table<String, OmVolumeArgs> volumeTable = mock(Table.class);
    Table<String, OmBucketInfo> bucketTable = mock(Table.class);
    Table<String, SnapshotInfo> snapshotInfoTable = mock(Table.class);
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
    SnapshotInfo second = createSnapshotInfo(volumeName, bucketName);
    when(snapshotInfoTable.get(first.getTableKey())).thenReturn(first);
    when(snapshotInfoTable.get(second.getTableKey())).thenReturn(second);

    // create the first snapshot checkpoint
    OmSnapshotManager.createOmSnapshotCheckpoint(om.getMetadataManager(),
        first);

    // retrieve it and setup store mock
    OmSnapshotManager omSnapshotManager = om.getOmSnapshotManager();
    OmSnapshot firstSnapshot = (OmSnapshot) omSnapshotManager
        .checkForSnapshot(first.getVolumeName(),
        first.getBucketName(), getSnapshotPrefix(first.getName()), false);
    DBStore firstSnapshotStore = mock(DBStore.class);
    HddsWhiteboxTestUtils.setInternalState(
        firstSnapshot.getMetadataManager(), "store", firstSnapshotStore);

    // create second snapshot checkpoint (which will be used for eviction)
    OmSnapshotManager.createOmSnapshotCheckpoint(om.getMetadataManager(),
        second);

    // confirm store not yet closed
    verify(firstSnapshotStore, times(0)).close();

    // read in second snapshot to evict first
    omSnapshotManager
        .checkForSnapshot(second.getVolumeName(),
        second.getBucketName(), getSnapshotPrefix(second.getName()), false);

    // As a workaround, invalidate all cache entries in order to trigger
    // instances close in this test case, since JVM GC most likely would not
    // have triggered and closed the instances yet at this point.
    omSnapshotManager.getSnapshotCache().invalidateAll();

    // confirm store was closed
    verify(firstSnapshotStore, timeout(3000).times(1)).close();
  }

  private void setupData() throws IOException {
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
    leaderDir = new File(testDir.toString(),
        "leader");
    Assert.assertTrue(leaderDir.mkdirs());
    String pathSnap1 = OM_SNAPSHOT_CHECKPOINT_DIR + OM_KEY_PREFIX + "snap1";
    String pathSnap2 = OM_SNAPSHOT_CHECKPOINT_DIR + OM_KEY_PREFIX + "snap2";
    leaderSnapDir1 = new File(leaderDir.toString(), pathSnap1);
    Assert.assertTrue(leaderSnapDir1.mkdirs());
    Files.write(Paths.get(leaderSnapDir1.toString(), "s1.sst"), dummyData);

    leaderSnapDir2 = new File(leaderDir.toString(), pathSnap2);
    Assert.assertTrue(leaderSnapDir2.mkdirs());
    Files.write(Paths.get(leaderSnapDir2.toString(), "noLink.sst"), dummyData);
    Files.write(Paths.get(leaderSnapDir2.toString(), "nonSstFile"), dummyData);

    // Also create the follower files.
    candidateDir = new File(testDir.toString(),
        CANDIDATE_DIR_NAME);
    File followerSnapDir1 = new File(candidateDir.toString(), pathSnap1);
    followerSnapDir2 = new File(candidateDir.toString(), pathSnap2);
    copyDirectory(leaderDir.toPath(), candidateDir.toPath());
    f1File = new File(candidateDir, "f1.sst");
    Files.write(f1File.toPath(), dummyData);
    s1File = new File(followerSnapDir1, "s1.sst");
    // confirm s1 file got copied over.
    Assert.assertTrue(s1File.exists());

    // Finish creating leaders files that are not to be copied over, because
    //  f1.sst belongs in a different directory as explained above.
    leaderCheckpointDir = new File(leaderDir.toString(),
        OM_CHECKPOINT_DIR + OM_KEY_PREFIX + "checkpoint1");
    Assert.assertTrue(leaderCheckpointDir.mkdirs());
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
  @SuppressFBWarnings({"NP_NULL_ON_SOME_PATH"})
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

    // Pointers to follower links to be created.
    File f1FileLink = new File(followerSnapDir2, "f1.sst");
    File s1FileLink = new File(followerSnapDir2, "s1.sst");

    // Create links on the follower from list.
    OmSnapshotUtils.createHardLinks(candidateDir.toPath());

    // Confirm expected follower links.
    Assert.assertTrue(s1FileLink.exists());
    Assert.assertEquals("link matches original file",
        getINode(s1File.toPath()), getINode(s1FileLink.toPath()));

    Assert.assertTrue(f1FileLink.exists());
    Assert.assertEquals("link matches original file",
        getINode(f1File.toPath()), getINode(f1FileLink.toPath()));
  }

  /*
   * Test that exclude list is generated correctly.
   */
  @Test
  public void testExcludeUtilities() throws IOException {
    File noLinkFile = new File(followerSnapDir2, "noLink.sst");

    // Confirm that the list of existing sst files is as expected.
    List<String> existingSstList = getExistingSstFiles(candidateDir);
    Set<String> existingSstFiles = new HashSet<>(existingSstList);
    int truncateLength = candidateDir.toString().length() + 1;
    Set<String> expectedSstFiles = new HashSet<>(Arrays.asList(
        s1File.toString().substring(truncateLength),
        noLinkFile.toString().substring(truncateLength),
        f1File.toString().substring(truncateLength)));
    Assert.assertEquals(expectedSstFiles, existingSstFiles);

    // Confirm that the excluded list is normalized as expected.
    //  (Normalizing means matches the layout on the leader.)
    Set<Path> normalizedSet =
        OMDBCheckpointServlet.normalizeExcludeList(existingSstList,
            leaderCheckpointDir.toString(), leaderDir.toString());
    Set<Path> expectedNormalizedSet = new HashSet<>(Arrays.asList(
        Paths.get(leaderSnapDir1.toString(), "s1.sst"),
        Paths.get(leaderSnapDir2.toString(), "noLink.sst"),
        Paths.get(leaderCheckpointDir.toString(), "f1.sst")));
    Assert.assertEquals(expectedNormalizedSet, normalizedSet);
  }

  /*
   * Confirm that processFile() correctly determines whether a file
   * should be copied, linked, or excluded from the tarball entirely.
   */
  @Test
  public void testProcessFile() {
    Path copyFile = Paths.get(testDir.toString(),
        "snap1/copyfile.sst");
    Path excludeFile = Paths.get(testDir.toString(),
        "snap1/excludeFile.sst");
    Path linkToExcludedFile = Paths.get(testDir.toString(),
        "snap2/excludeFile.sst");
    Path linkToCopiedFile = Paths.get(testDir.toString(),
        "snap2/copyfile.sst");
    Path addToCopiedFiles = Paths.get(testDir.toString(),
        "snap1/copyfile2.sst");
    Path addNonSstToCopiedFiles = Paths.get(testDir.toString(),
        "snap1/nonSst");

    Set<Path> toExcludeFiles = new HashSet<>(
        Collections.singletonList(excludeFile));
    Set<Path> copyFiles = new HashSet<>(Collections.singletonList(copyFile));
    List<String> excluded = new ArrayList<>();
    Map<Path, Path> hardLinkFiles = new HashMap<>();

    // Confirm the exclude file gets added to the excluded list,
    //  (and thus is excluded.)
    processFile(excludeFile, copyFiles, hardLinkFiles, toExcludeFiles,
        excluded);
    Assert.assertEquals(excluded.size(), 1);
    Assert.assertEquals((excluded.get(0)), excludeFile.toString());
    Assert.assertEquals(copyFiles.size(), 1);
    Assert.assertEquals(hardLinkFiles.size(), 0);
    excluded = new ArrayList<>();

    // Confirm the linkToExcludedFile gets added as a link.
    processFile(linkToExcludedFile, copyFiles, hardLinkFiles, toExcludeFiles,
        excluded);
    Assert.assertEquals(excluded.size(), 0);
    Assert.assertEquals(copyFiles.size(), 1);
    Assert.assertEquals(hardLinkFiles.size(), 1);
    Assert.assertEquals(hardLinkFiles.get(linkToExcludedFile), excludeFile);
    hardLinkFiles = new HashMap<>();

    // Confirm the linkToCopiedFile gets added as a link.
    processFile(linkToCopiedFile, copyFiles, hardLinkFiles, toExcludeFiles,
        excluded);
    Assert.assertEquals(excluded.size(), 0);
    Assert.assertEquals(copyFiles.size(), 1);
    Assert.assertEquals(hardLinkFiles.size(), 1);
    Assert.assertEquals(hardLinkFiles.get(linkToCopiedFile), copyFile);
    hardLinkFiles = new HashMap<>();

    // Confirm the addToCopiedFiles gets added to list of copied files
    processFile(addToCopiedFiles, copyFiles, hardLinkFiles, toExcludeFiles,
        excluded);
    Assert.assertEquals(excluded.size(), 0);
    Assert.assertEquals(copyFiles.size(), 2);
    Assert.assertTrue(copyFiles.contains(addToCopiedFiles));
    copyFiles = new HashSet<>(Collections.singletonList(copyFile));

    // Confirm the addNonSstToCopiedFiles gets added to list of copied files
    processFile(addNonSstToCopiedFiles, copyFiles, hardLinkFiles,
        toExcludeFiles, excluded);
    Assert.assertEquals(excluded.size(), 0);
    Assert.assertEquals(copyFiles.size(), 2);
    Assert.assertTrue(copyFiles.contains(addNonSstToCopiedFiles));
  }

  @Test
  public void testCreateSnapshotIdempotent() throws Exception {
    // set up db tables
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(OmSnapshotManager.LOG);
    Table<String, OmVolumeArgs> volumeTable = mock(Table.class);
    Table<String, OmBucketInfo> bucketTable = mock(Table.class);
    Table<String, SnapshotInfo> snapshotInfoTable = mock(Table.class);
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
    when(snapshotInfoTable.get(first.getTableKey())).thenReturn(first);

    // Create first checkpoint for the snapshot checkpoint
    OmSnapshotManager.createOmSnapshotCheckpoint(om.getMetadataManager(),
        first);
    Assert.assertFalse(logCapturer.getOutput().contains(
        "for snapshot " + first.getName() + " already exists."));
    logCapturer.clearOutput();

    // Create checkpoint again for the same snapshot.
    OmSnapshotManager.createOmSnapshotCheckpoint(om.getMetadataManager(),
        first);

    Assert.assertTrue(logCapturer.getOutput().contains(
        "for snapshot " + first.getName() + " already exists."));
  }

  private SnapshotInfo createSnapshotInfo(String volumeName,
                                          String bucketName) {
    return SnapshotInfo.newInstance(volumeName,
        bucketName,
        UUID.randomUUID().toString(),
        UUID.randomUUID(),
        Time.now());
  }
}
