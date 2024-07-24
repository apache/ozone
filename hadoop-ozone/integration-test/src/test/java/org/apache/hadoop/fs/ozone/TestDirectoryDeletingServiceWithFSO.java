/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.ozone;

import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerDoubleBuffer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerStateMachine;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.service.DirectoryDeletingService;
import org.apache.hadoop.ozone.om.service.KeyDeletingService;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;

/**
 * Directory deletion service test cases.
 */
@Timeout(300)
public class TestDirectoryDeletingServiceWithFSO {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestDirectoryDeletingServiceWithFSO.class);

  private static boolean omRatisEnabled = true;

  private static MiniOzoneCluster cluster;
  private static FileSystem fs;
  private static String volumeName;
  private static String bucketName;
  private static OzoneClient client;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 2000);
    conf.setInt(OMConfigKeys.OZONE_PATH_DELETING_LIMIT_PER_TASK, 5);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, omRatisEnabled);
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    volumeName = bucket.getVolumeName();
    bucketName = bucket.getName();

    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName);

    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);

    fs = FileSystem.get(conf);
  }

  @AfterAll
  public static void teardown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  @AfterEach
  public void cleanup() {
    try {
      Path root = new Path("/");
      FileStatus[] fileStatuses = fs.listStatus(root);
      for (FileStatus fileStatus : fileStatuses) {
        fs.delete(fileStatus.getPath(), true);
      }
    } catch (IOException ex) {
      fail("Failed to cleanup files.");
    }
  }

  @Test
  public void testDeleteEmptyDirectory() throws Exception {
    Path root = new Path("/rootDir");
    Path appRoot = new Path(root, "appRoot");
    fs.mkdirs(appRoot);

    Table<String, OmKeyInfo> deletedDirTable =
        cluster.getOzoneManager().getMetadataManager().getDeletedDirTable();
    Table<String, OmDirectoryInfo> dirTable =
        cluster.getOzoneManager().getMetadataManager().getDirectoryTable();


    DirectoryDeletingService dirDeletingService =
        (DirectoryDeletingService) cluster.getOzoneManager().getKeyManager()
            .getDirDeletingService();
    // Before delete
    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(dirTable, 2);

    assertSubPathsCount(dirDeletingService::getDeletedDirsCount, 0);
    assertSubPathsCount(dirDeletingService::getMovedDirsCount, 0);
    assertSubPathsCount(dirDeletingService::getMovedFilesCount, 0);

    // Delete the appRoot, empty dir
    fs.delete(appRoot, true);

    // After Delete
    checkPath(appRoot);

    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(dirTable, 1);

    assertSubPathsCount(dirDeletingService::getDeletedDirsCount, 1);
    assertSubPathsCount(dirDeletingService::getMovedDirsCount, 0);
    assertSubPathsCount(dirDeletingService::getMovedFilesCount, 0);

    try (TableIterator<?, ? extends Table.KeyValue<?, OmDirectoryInfo>>
        iterator = dirTable.iterator()) {
      assertTrue(iterator.hasNext());
      assertEquals(root.getName(), iterator.next().getValue().getName());
    }

    assertTrue(dirDeletingService.getRunCount().get() > 1);
  }

  /**
   * Tests verifies that directories and files are getting purged in multiple
   * batches.
   */
  @Test
  public void testDeleteWithLargeSubPathsThanBatchSize() throws Exception {
    Path root = new Path("/rootDir");
    Path appRoot = new Path(root, "appRoot");
    // Creates 2 parent dirs from root.
    fs.mkdirs(appRoot);

    // create 2 more levels. In each level, creates 5 subdirs and 5 subfiles.
    // This will create total of 3 parentDirs + (3 * 5) childDirs and
    // Total of (3 * 5) childFiles
    for (int i = 1; i <= 3; i++) {
      Path childDir = new Path(appRoot, "parentDir" + i);
      for (int j = 1; j <= 5; j++) {
        // total 5 sub-dirs + 5 sub-files = 10 items in this level.
        Path childSubDir = new Path(childDir, "childDir" + j);
        Path childSubFile = new Path(childDir, "childFile" + j);
        ContractTestUtils.touch(fs, childSubFile); // create sub file
        fs.mkdirs(childSubDir); // create sub dir
      }
    }

    Table<String, OmKeyInfo> deletedDirTable =
        cluster.getOzoneManager().getMetadataManager().getDeletedDirTable();
    Table<String, OmKeyInfo> keyTable =
        cluster.getOzoneManager().getMetadataManager()
            .getKeyTable(getFSOBucketLayout());
    Table<String, OmDirectoryInfo> dirTable =
        cluster.getOzoneManager().getMetadataManager().getDirectoryTable();

    DirectoryDeletingService dirDeletingService =
        (DirectoryDeletingService) cluster.getOzoneManager().getKeyManager()
            .getDirDeletingService();

    // Before delete
    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(keyTable, 15);
    assertTableRowCount(dirTable, 20);

    assertSubPathsCount(dirDeletingService::getMovedFilesCount, 0);
    assertSubPathsCount(dirDeletingService::getMovedDirsCount, 0);
    assertSubPathsCount(dirDeletingService::getDeletedDirsCount, 0);

    long preRunCount = dirDeletingService.getRunCount().get();

    // Delete the appRoot
    fs.delete(appRoot, true);

    // After Delete
    checkPath(appRoot);

    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(keyTable, 0);
    assertTableRowCount(dirTable, 1);

    assertSubPathsCount(dirDeletingService::getMovedFilesCount, 15);
    // 15 subDir + 3 parentDir
    assertSubPathsCount(dirDeletingService::getMovedDirsCount, 18);
    assertSubPathsCount(dirDeletingService::getDeletedDirsCount, 19);

    long elapsedRunCount = dirDeletingService.getRunCount().get() - preRunCount;
    assertTrue(dirDeletingService.getRunCount().get() > 1);
    // Ensure dir deleting speed, here provide a backup value for safe CI
    assertTrue(elapsedRunCount >= 7);
  }

  @Test
  public void testDeleteWithMultiLevels() throws Exception {
    Path root = new Path("/rootDir");
    Path appRoot = new Path(root, "appRoot");

    for (int i = 1; i <= 3; i++) {
      Path parent = new Path(appRoot, "parentDir" + i);
      Path child = new Path(parent, "childFile");
      ContractTestUtils.touch(fs, child);
    }

    Table<String, OmKeyInfo> deletedDirTable =
        cluster.getOzoneManager().getMetadataManager().getDeletedDirTable();
    Table<String, OmKeyInfo> keyTable =
        cluster.getOzoneManager().getMetadataManager()
            .getKeyTable(getFSOBucketLayout());
    Table<String, OmDirectoryInfo> dirTable =
        cluster.getOzoneManager().getMetadataManager().getDirectoryTable();

    DirectoryDeletingService dirDeletingService =
        (DirectoryDeletingService) cluster.getOzoneManager().getKeyManager()
            .getDirDeletingService();

    // Before delete
    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(dirTable, 5);
    assertTableRowCount(keyTable, 3);

    assertSubPathsCount(dirDeletingService::getMovedFilesCount, 0);
    assertSubPathsCount(dirDeletingService::getMovedDirsCount, 0);
    assertSubPathsCount(dirDeletingService::getDeletedDirsCount, 0);

    // Delete the rootDir, which should delete all keys.
    fs.delete(root, true);

    // After Delete
    checkPath(root);

    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(keyTable, 0);
    assertTableRowCount(dirTable, 0);

    assertSubPathsCount(dirDeletingService::getMovedFilesCount, 3);
    assertSubPathsCount(dirDeletingService::getMovedDirsCount, 2);
    assertSubPathsCount(dirDeletingService::getDeletedDirsCount, 5);

    assertTrue(dirDeletingService.getRunCount().get() > 1);
  }

  @Test
  public void testDeleteWithMultiLevelsBlockDoubleBuffer() throws Exception {
    Path root = new Path("/rootDirdd");
    Path appRoot = new Path(root, "appRoot");

    for (int i = 1; i <= 3; i++) {
      Path parent = new Path(appRoot, "parentDir" + i);
      Path child = new Path(parent, "childFile");
      ContractTestUtils.touch(fs, child);
    }

    OMMetadataManager metadataManager = cluster.getOzoneManager().getMetadataManager();
    Table<String, OmKeyInfo> deletedDirTable = metadataManager.getDeletedDirTable();
    Table<String, OmKeyInfo> keyTable = metadataManager.getKeyTable(getFSOBucketLayout());
    Table<String, OmDirectoryInfo> dirTable = metadataManager.getDirectoryTable();

    DirectoryDeletingService dirDeletingService = (DirectoryDeletingService) cluster.getOzoneManager().getKeyManager()
            .getDirDeletingService();

    // Before delete
    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(dirTable, 5);
    assertTableRowCount(keyTable, 3);

    // stop daemon to simulate blocked double buffer
    OzoneManagerStateMachine omStateMachine = cluster.getOzoneManager().getOmRatisServer().getOmStateMachine();
    OzoneManagerDoubleBuffer omDoubleBuffer = omStateMachine.getOzoneManagerDoubleBuffer();
    omDoubleBuffer.awaitFlush();
    omDoubleBuffer.stopDaemon();

    OzoneVolume volume = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket(bucketName);    long volumeId = metadataManager.getVolumeId(volumeName);

    // manually delete dir and add to deleted table. namespace count occupied "1" as manual deletion do not reduce
    long bucketId = metadataManager.getBucketId(volumeName, bucketName);
    OzoneFileStatus keyStatus = OMFileRequest.getOMKeyInfoIfExists(
        metadataManager, volumeName, bucketName, "rootDirdd", 0,
        cluster.getOzoneManager().getDefaultReplicationConfig());
    assertNotNull(keyStatus);
    String ozoneDbKey = metadataManager.getOzonePathKey(volumeId, bucketId, keyStatus.getKeyInfo().getParentObjectID(),
        keyStatus.getKeyInfo().getFileName());
    deletedDirTable.put(ozoneDbKey, keyStatus.getKeyInfo());
    dirTable.delete(ozoneDbKey);
    assertTableRowCount(deletedDirTable, 1);
    assertTableRowCount(dirTable, 4);

    System.out.println("starting count: " + bucket.getUsedNamespace());

    // wait for running 2 more iteration
    long currentCount = dirDeletingService.getRunCount().get();
    GenericTestUtils.waitFor(() -> dirDeletingService.getRunCount().get() > currentCount + 2, 1000,
        10000);

    // verify bucket used namespace is not going -ve
    assertTrue(volume.getBucket(bucketName).getUsedNamespace() >= 0);

    // re-init double buffer in state machine to resume further processing
    omStateMachine.pause();
    omStateMachine.unpause(omStateMachine.getLastAppliedTermIndex().getIndex(),
        omStateMachine.getLatestSnapshot().getIndex());
    // flush previous pending transaction manually
    omDoubleBuffer.resume();
    CompletableFuture.supplyAsync(() -> {
      omDoubleBuffer.flushTransactions();
      return null;
    });
    omDoubleBuffer.awaitFlush();
    omDoubleBuffer.stop();
    // verify deletion progress completion
    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(keyTable, 0);
    assertTrue(volume.getBucket(bucketName).getUsedNamespace() >= 0);
    assertTrue(volume.getBucket(bucketName).getUsedBytes() == 0);
  }

  @Test
  public void testDeleteFilesAndSubFiles() throws Exception {

    Table<String, OmKeyInfo> deletedDirTable =
        cluster.getOzoneManager().getMetadataManager().getDeletedDirTable();
    Table<String, OmKeyInfo> keyTable =
        cluster.getOzoneManager().getMetadataManager()
            .getKeyTable(getFSOBucketLayout());
    Table<String, OmDirectoryInfo> dirTable =
        cluster.getOzoneManager().getMetadataManager().getDirectoryTable();
    Table<String, RepeatedOmKeyInfo> deletedKeyTable =
        cluster.getOzoneManager().getMetadataManager().getDeletedTable();

    Path root = new Path("/rootDir2");
    // Create  parent dir from root.
    fs.mkdirs(root);

    // Added 10 sub files inside root dir
    for (int i = 0; i < 5; i++) {
      Path path = new Path(root, "testKey" + i);
      try (FSDataOutputStream stream = fs.create(path)) {
        stream.write(1);
      }
    }

    KeyDeletingService keyDeletingService =
        (KeyDeletingService) cluster.getOzoneManager().getKeyManager()
            .getDeletingService();

    // Before delete
    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(keyTable, 5);
    assertTableRowCount(dirTable, 1);
    long prevDeletedKeyCount = keyDeletingService.getDeletedKeyCount().get();

    // Case-1) Delete 3 Files directly.
    for (int i = 0; i < 3; i++) {
      Path path = new Path(root, "testKey" + i);
      fs.delete(path, true);
    }

    DirectoryDeletingService dirDeletingService =
        (DirectoryDeletingService) cluster.getOzoneManager().getKeyManager()
            .getDirDeletingService();


    // After delete. 2 more files left out under the root dir
    assertTableRowCount(keyTable, 2);
    assertTableRowCount(dirTable, 1);

    // Eventually keys would get cleaned up from deletedTables too
    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(deletedKeyTable, 0);

    assertSubPathsCount(dirDeletingService::getMovedFilesCount, 0);
    assertSubPathsCount(dirDeletingService::getMovedDirsCount, 0);
    assertSubPathsCount(dirDeletingService::getDeletedDirsCount, 0);
    // verify whether KeyDeletingService has purged the keys
    long currentDeletedKeyCount = keyDeletingService.getDeletedKeyCount().get();
    assertEquals(prevDeletedKeyCount + 3, currentDeletedKeyCount);


    // Case-2) Delete dir, this will cleanup sub-files under the deleted dir.
    fs.delete(root, true);

    // After delete. 2 sub files to be deleted.
    assertTableRowCount(keyTable, 0);
    assertTableRowCount(dirTable, 0);

    // Eventually keys would get cleaned up from deletedTables too
    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(deletedKeyTable, 0);

    assertSubPathsCount(dirDeletingService::getMovedFilesCount, 2);
    assertSubPathsCount(dirDeletingService::getMovedDirsCount, 0);
    assertSubPathsCount(dirDeletingService::getDeletedDirsCount, 1);
    // verify whether KeyDeletingService has purged the keys
    currentDeletedKeyCount = keyDeletingService.getDeletedKeyCount().get();
    assertEquals(prevDeletedKeyCount + 5, currentDeletedKeyCount);
  }

  @Test
  public void testDirDeletedTableCleanUpForSnapshot() throws Exception {
    Table<String, OmKeyInfo> deletedDirTable =
        cluster.getOzoneManager().getMetadataManager().getDeletedDirTable();
    Table<String, OmKeyInfo> keyTable =
        cluster.getOzoneManager().getMetadataManager()
            .getKeyTable(getFSOBucketLayout());
    Table<String, OmDirectoryInfo> dirTable =
        cluster.getOzoneManager().getMetadataManager().getDirectoryTable();
    Table<String, RepeatedOmKeyInfo> deletedKeyTable =
        cluster.getOzoneManager().getMetadataManager().getDeletedTable();
    Table<String, SnapshotInfo> snapshotInfoTable =
        cluster.getOzoneManager().getMetadataManager().getSnapshotInfoTable();

    /*    DirTable                               KeyTable
    /v/b/snapDir                       /v/b/snapDir/testKey0 - testKey5
    /v/b/snapDir/appRoot/              /v/b/snapDir/appRoot/parentDir0/childFile
    /v/b/snapDir/appRoot/parentDir0/   /v/b/snapDir/appRoot/parentDir1/childFile
    /v/b/snapDir/appRoot/parentDir1/   /v/b/snapDir/appRoot/parentDir2/childFile
    /v/b/snapDir/appRoot/parentDir2/
     */

    Path root = new Path("/snapDir");
    Path appRoot = new Path(root, "appRoot");
    // Create  parent dir from root.
    fs.mkdirs(root);

    // Added 5 sub files inside root dir
    for (int i = 0; i < 5; i++) {
      Path path = new Path(root, "testKey" + i);
      try (FSDataOutputStream stream = fs.create(path)) {
        stream.write(1);
      }
    }

    // Add 3 more sub files in different level
    for (int i = 0; i < 3; i++) {
      Path parent = new Path(appRoot, "parentDir" + i);
      Path child = new Path(parent, "childFile");
      ContractTestUtils.touch(fs, child);
    }

    KeyDeletingService keyDeletingService =
        (KeyDeletingService) cluster.getOzoneManager().getKeyManager()
            .getDeletingService();

    // Before delete
    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(keyTable, 8);
    assertTableRowCount(dirTable, 5);

    // Create snapshot
    client.getObjectStore().createSnapshot(volumeName, bucketName, "snap1");
    assertTableRowCount(snapshotInfoTable, 1);

    // Case-1) Delete 3 Files directly.
    for (int i = 0; i < 3; i++) {
      Path path = new Path(root, "testKey" + i);
      fs.delete(path, true);
    }

    DirectoryDeletingService dirDeletingService =
        (DirectoryDeletingService) cluster.getOzoneManager().getKeyManager()
            .getDirDeletingService();

    // After delete. 5 more files left out under the root dir
    assertTableRowCount(keyTable, 5);
    assertTableRowCount(dirTable, 5);

    long prevKdsRunCount = keyDeletingService.getRunCount().get();

    // wait for at least 1 iteration of KeyDeletingService
    GenericTestUtils.waitFor(
        () -> (keyDeletingService.getRunCount().get() > prevKdsRunCount), 100,
        10000);

    // KeyDeletingService and DirectoryDeletingService will not
    // clean up because the paths are part of a snapshot.
    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(deletedKeyTable, 3);

    assertSubPathsCount(dirDeletingService::getMovedFilesCount, 0);
    assertSubPathsCount(dirDeletingService::getMovedDirsCount, 0);
    assertSubPathsCount(dirDeletingService::getDeletedDirsCount, 0);

    // Case-2) Delete dir
    fs.delete(root, true);

    // After delete. 5 sub files are still in keyTable.
    // 4 dirs in dirTable.
    assertTableRowCount(keyTable, 5);
    assertTableRowCount(dirTable, 4);

    // KeyDeletingService and DirectoryDeletingService will not
    // clean up because the paths are part of a snapshot.
    // As a result on 1 deleted dir and 3 deleted files will
    // remain in dirTable and keyTable respectively.
    long prevDDSRunCount = dirDeletingService.getRunCount().get();
    long prevKDSRunCount = keyDeletingService.getRunCount().get();
    assertTableRowCount(deletedDirTable, 1);
    assertTableRowCount(deletedKeyTable, 3);
    GenericTestUtils.waitFor(() -> dirDeletingService.getRunCount().get() >
        prevDDSRunCount, 100, 10000);
    GenericTestUtils.waitFor(() -> keyDeletingService.getRunCount().get() >
        prevKDSRunCount, 100, 10000);

    assertSubPathsCount(dirDeletingService::getMovedFilesCount, 0);
    assertSubPathsCount(dirDeletingService::getMovedDirsCount, 0);
    assertSubPathsCount(dirDeletingService::getDeletedDirsCount, 0);

    // Manual cleanup deletedDirTable for next tests
    cleanupTables();
  }

  private void cleanupTables() throws IOException {
    OMMetadataManager metadataManager =
        cluster.getOzoneManager().getMetadataManager();

    try (TableIterator<?, ?> it = metadataManager.getDeletedDirTable()
        .iterator()) {
      removeAllFromDB(it);
    }
    try (TableIterator<?, ?> it = metadataManager.getFileTable().iterator()) {
      removeAllFromDB(it);
    }
    try (TableIterator<?, ?> it = metadataManager.getDirectoryTable()
        .iterator()) {
      removeAllFromDB(it);
    }
  }

  private static void removeAllFromDB(TableIterator<?, ?> iterator)
      throws IOException {
    while (iterator.hasNext()) {
      iterator.next();
      iterator.removeFromDB();
    }
  }

  static void assertSubPathsCount(LongSupplier pathCount, long expectedCount)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> pathCount.getAsLong() >= expectedCount,
        1000, 120000);
  }

  private void assertTableRowCount(Table<String, ?> table, int count)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> assertTableRowCount(count, table), 1000,
        120000); // 2 minutes
  }

  private boolean assertTableRowCount(int expectedCount,
                                      Table<String, ?> table) {
    long count = 0L;
    try {
      count = cluster.getOzoneManager().getMetadataManager()
          .countRowsInTable(table);
      LOG.info("{} actual row count={}, expectedCount={}", table.getName(),
          count, expectedCount);
    } catch (IOException ex) {
      fail("testDoubleBuffer failed with: " + ex);
    }
    return count == expectedCount;
  }

  private void checkPath(Path path) {
    try {
      fs.getFileStatus(path);
      fail("testRecursiveDelete failed");
    } catch (IOException ex) {
      assertTrue(ex instanceof FileNotFoundException);
      assertTrue(ex.getMessage().contains("No such file or directory"));
    }
  }

  private static BucketLayout getFSOBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
