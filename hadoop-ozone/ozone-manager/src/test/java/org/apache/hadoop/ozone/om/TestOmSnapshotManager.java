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
 * Unit test om snapshot manager.
 */
public class TestOmSnapshotManager {

  private OzoneManager om;
  private File testDir;
  private static final String CANDIDATE_DIR_NAME = OM_DB_NAME + SNAPSHOT_CANDIDATE_DIR;
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
        first.getBucketName(), getSnapshotPrefix(first.getName()));
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
        second.getBucketName(), getSnapshotPrefix(second.getName()));

    // As a workaround, invalidate all cache entries in order to trigger
    // instances close in this test case, since JVM GC most likely would not
    // have triggered and closed the instances yet at this point.
    omSnapshotManager.getSnapshotCache().invalidateAll();

    // confirm store was closed
    verify(firstSnapshotStore, timeout(3000).times(1)).close();
  }

  static class DirectoryData {
    String pathSnap1;
    String pathSnap2;
    File leaderDir;
    File leaderSnapdir1;
    File leaderSnapdir2;
    File leaderCheckpointDir;
    File candidateDir;
    File followerSnapdir1;
    File followerSnapdir2;
    File s1FileLink;
    File s1File;
    File f1FileLink;
    File f1File;
    File nolinkFile;
  }

  private DirectoryData setupData() throws IOException {
    // Setup the leader with the following files:
    // leader/db.checkpoints/dir1/f1.sst
    // leader/db.snapshots/checkpointState/dir1/s1.sst

    // And the following links:
    // leader/db.snapshots/checkpointState/dir2/<link to f1.sst>
    // leader/db.snapshots/checkpointState/dir2/<link to s1.sst>

    // Setup the follower with the following files, (as if they came
    // from the tarball from the leader)

    // follower/cand/f1.sst
    // follower/cand/db.snapshots/checkpointState/dir1/s1.sst

    // Note that the layout is slightly different in that the f1.sst on
    // the leader is in a checkpoint directory but on the follower is
    // moved to the candidate omdb directory; the links must be
    // adjusted accordingly.

    byte[] dummyData = {0};
    DirectoryData directoryData = new DirectoryData();

    // Create dummy leader files to calculate links
    directoryData.leaderDir = new File(testDir.toString(),
        "leader");
    Assert.assertTrue(directoryData.leaderDir.mkdirs());
    directoryData.pathSnap1 = OM_SNAPSHOT_CHECKPOINT_DIR + OM_KEY_PREFIX + "dir1";
    directoryData.pathSnap2 = OM_SNAPSHOT_CHECKPOINT_DIR + OM_KEY_PREFIX + "dir2";
    directoryData.leaderSnapdir1 = new File(directoryData.leaderDir.toString(), directoryData.pathSnap1);
    Assert.assertTrue(directoryData.leaderSnapdir1.mkdirs());
    Files.write(Paths.get(directoryData.leaderSnapdir1.toString(), "s1.sst"), dummyData);

    directoryData.leaderSnapdir2 = new File(directoryData.leaderDir.toString(), directoryData.pathSnap2);
    Assert.assertTrue(directoryData.leaderSnapdir2.mkdirs());
    Files.write(Paths.get(directoryData.leaderSnapdir2.toString(), "noLink.sst"), dummyData);
    Files.write(Paths.get(directoryData.leaderSnapdir2.toString(), "nonSstFile"), dummyData);


    // Also create the follower files
    directoryData.candidateDir = new File(testDir.toString(),
        CANDIDATE_DIR_NAME);
    directoryData.followerSnapdir1 = new File(directoryData.candidateDir.toString(), directoryData.pathSnap1);
    directoryData.followerSnapdir2 = new File(directoryData.candidateDir.toString(), directoryData.pathSnap2);
    copyDirectory(directoryData.leaderDir.toPath(), directoryData.candidateDir.toPath());
    Files.write(Paths.get(directoryData.candidateDir.toString(), "f1.sst"), dummyData);


    directoryData.leaderCheckpointDir = new File(directoryData.leaderDir.toString(),
        OM_CHECKPOINT_DIR + OM_KEY_PREFIX + "dir1");
    Assert.assertTrue(directoryData.leaderCheckpointDir.mkdirs());
    Files.write(Paths.get(directoryData.leaderCheckpointDir.toString(), "f1.sst"), dummyData);
    directoryData.s1FileLink = new File(directoryData.followerSnapdir2, "s1.sst");
    directoryData.s1File = new File(directoryData.followerSnapdir1, "s1.sst");
    directoryData.f1FileLink = new File(directoryData.followerSnapdir2, "f1.sst");
    directoryData.f1File = new File(directoryData.candidateDir, "f1.sst");
    directoryData.nolinkFile = new File(directoryData.followerSnapdir2, "noLink.sst");

    return directoryData;
  }

  @Test
  @SuppressFBWarnings({"NP_NULL_ON_SOME_PATH"})
  public void testHardLinkCreation() throws IOException {

    // test that following links are created on the follower
    //     follower/db.snapshots/checkpointState/dir2/f1.sst
    //     follower/db.snapshots/checkpointState/dir2/s1.sst

    DirectoryData directoryData = setupData();

    // Create map of links to dummy files on the leader
    Map<Path, Path> hardLinkFiles = new HashMap<>();
    hardLinkFiles.put(Paths.get(directoryData.leaderSnapdir2.toString(), "f1.sst"),
        Paths.get(directoryData.leaderCheckpointDir.toString(), "f1.sst"));
    hardLinkFiles.put(Paths.get(directoryData.leaderSnapdir2.toString(), "s1.sst"),
        Paths.get(directoryData.leaderSnapdir1.toString(), "s1.sst"));

    // Create link list.
    Path hardLinkList =
        OmSnapshotUtils.createHardLinkList(
            directoryData.leaderDir.toString().length() + 1, hardLinkFiles);

    Files.move(hardLinkList, Paths.get(directoryData.candidateDir.toString(),
        OM_HARDLINK_FILE));

    // Create links on the follower from list.
    OmSnapshotUtils.createHardLinks(directoryData.candidateDir.toPath());

    // Confirm expected follower links.
    Assert.assertTrue(directoryData.s1FileLink.exists());
    Assert.assertEquals("link matches original file",
        getINode(directoryData.s1File.toPath()), getINode(directoryData.s1FileLink.toPath()));

    Assert.assertTrue(directoryData.f1FileLink.exists());
    Assert.assertEquals("link matches original file",
        getINode(directoryData.f1File.toPath()), getINode(directoryData.f1FileLink.toPath()));
  }

  @Test
  public void testFileUtilities() throws IOException {

    DirectoryData directoryData = setupData();
    List<String> excludeList = getExistingSstFiles(directoryData.candidateDir);
    Set<String> existingSstFiles = new HashSet<>(excludeList);
    int truncateLength = directoryData.candidateDir.toString().length() + 1;
    Set<String> expectedSstFiles = new HashSet<>(Arrays.asList(
        directoryData.s1File.toString().substring(truncateLength),
        directoryData.nolinkFile.toString().substring(truncateLength),
        directoryData.f1File.toString().substring(truncateLength)));
    Assert.assertEquals(expectedSstFiles, existingSstFiles);

    Set<Path> normalizedSet =
        OMDBCheckpointServlet.normalizeExcludeList(excludeList,
            directoryData.leaderCheckpointDir.toString(), directoryData.leaderDir.toString());
    Set<Path> expectedNormalizedSet = new HashSet<>(Arrays.asList(
        Paths.get(directoryData.leaderSnapdir1.toString(), "s1.sst"),
        Paths.get(directoryData.leaderSnapdir2.toString(), "noLink.sst"),
        Paths.get(directoryData.leaderCheckpointDir.toString(), "f1.sst")));
    Assert.assertEquals(expectedNormalizedSet, normalizedSet);
  }

  @Test
  public void testProcessFile() {
    Path copyFile = Paths.get(testDir.toString(), "dir1/copyfile.sst");
    Path excludeFile = Paths.get(testDir.toString(), "dir1/excludefile.sst");
    Path linkToExcludedFile = Paths.get(testDir.toString(), "dir2/excludefile.sst");
    Path linkToCopiedFile = Paths.get(testDir.toString(), "dir2/copyfile.sst");
    Path addToCopiedFiles = Paths.get(testDir.toString(), "dir1/copyfile2.sst");
    Path addNonSstToCopiedFiles = Paths.get(testDir.toString(), "dir1/nonSst");

    Set<Path> toExcludeFiles = new HashSet<>(
        Collections.singletonList(excludeFile));
    Set<Path> copyFiles = new HashSet<>(Collections.singletonList(copyFile));
    List<String> excluded = new ArrayList<>();
    Map<Path, Path> hardLinkFiles = new HashMap<>();

    processFile(excludeFile, copyFiles, hardLinkFiles, toExcludeFiles, excluded);
    Assert.assertEquals(excluded.size(), 1);
    Assert.assertEquals((excluded.get(0)), excludeFile.toString());
    Assert.assertEquals(copyFiles.size(), 1);
    Assert.assertEquals(hardLinkFiles.size(), 0);
    excluded = new ArrayList<>();

    processFile(linkToExcludedFile, copyFiles, hardLinkFiles, toExcludeFiles,
        excluded);
    Assert.assertEquals(excluded.size(), 0);
    Assert.assertEquals(copyFiles.size(), 1);
    Assert.assertEquals(hardLinkFiles.size(), 1);
    Assert.assertEquals(hardLinkFiles.get(linkToExcludedFile), excludeFile);
    hardLinkFiles = new HashMap<>();

    processFile(linkToCopiedFile, copyFiles, hardLinkFiles, toExcludeFiles,
        excluded);
    Assert.assertEquals(excluded.size(), 0);
    Assert.assertEquals(copyFiles.size(), 1);
    Assert.assertEquals(hardLinkFiles.size(), 1);
    Assert.assertEquals(hardLinkFiles.get(linkToCopiedFile), copyFile);
    hardLinkFiles = new HashMap<>();

    processFile(addToCopiedFiles, copyFiles, hardLinkFiles, toExcludeFiles,
        excluded);
    Assert.assertEquals(excluded.size(), 0);
    Assert.assertEquals(copyFiles.size(), 2);
    Assert.assertTrue(copyFiles.contains(addToCopiedFiles));
    copyFiles = new HashSet<>(Collections.singletonList(copyFile));

    processFile(addNonSstToCopiedFiles, copyFiles, hardLinkFiles, toExcludeFiles,
        excluded);
    Assert.assertEquals(excluded.size(), 0);
    Assert.assertEquals(copyFiles.size(), 2);
    Assert.assertTrue(copyFiles.contains(addNonSstToCopiedFiles));
  }

  private SnapshotInfo createSnapshotInfo(
      String volumeName, String bucketName) {
    String snapshotName = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();
    return SnapshotInfo.newInstance(volumeName,
        bucketName,
        snapshotName,
        snapshotId,
        Time.now());
  }
}
