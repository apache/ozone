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

package org.apache.hadoop.ozone.om.service;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.DeletingServiceMetrics;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerDoubleBuffer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerStateMachine;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableDirFilter;
import org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableKeyFilter;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Directory deletion service test cases.
 */
public class TestDirectoryDeletingServiceWithFSO {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestDirectoryDeletingServiceWithFSO.class);

  private static MiniOzoneCluster cluster;
  private static FileSystem fs;
  private static String volumeName;
  private static String bucketName;
  private static OzoneClient client;
  private static DeletingServiceMetrics metrics;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 2000);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL, 1000, TimeUnit.MILLISECONDS);
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
    metrics = cluster.getOzoneManager().getDeletionMetrics();
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
  public void cleanup() throws InterruptedException, TimeoutException {
    assertDoesNotThrow(() -> {
      Path root = new Path("/");
      FileStatus[] fileStatuses = fs.listStatus(root);
      for (FileStatus fileStatus : fileStatuses) {
        fs.delete(fileStatus.getPath(), true);
      }
    });
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

    metrics.resetDirectoryMetrics();
    // Delete the appRoot, empty dir
    fs.delete(appRoot, true);

    // After Delete
    checkPath(appRoot);

    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(dirTable, 1);

    assertSubPathsCount(dirDeletingService::getDeletedDirsCount, 1);
    assertSubPathsCount(dirDeletingService::getMovedDirsCount, 0);
    assertSubPathsCount(dirDeletingService::getMovedFilesCount, 0);
    assertEquals(1, metrics.getNumDirsPurged());
    assertEquals(1, metrics.getNumDirsSentForPurge());

    try (Table.KeyValueIterator<?, OmDirectoryInfo>
        iterator = dirTable.iterator()) {
      assertTrue(iterator.hasNext());
      assertEquals(root.getName(), iterator.next().getValue().getName());
    }

    assertThat(dirDeletingService.getRunCount().get()).isGreaterThan(1);
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

    metrics.resetDirectoryMetrics();
    // Delete the appRoot
    fs.delete(appRoot, true);

    // After Delete
    checkPath(appRoot);

    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(keyTable, 0);
    assertTableRowCount(dirTable, 1);

    assertSubPathsCount(dirDeletingService::getMovedFilesCount, 15);
    assertSubPathsCount(dirDeletingService::getDeletedDirsCount, 19);

    assertEquals(15, metrics.getNumSubFilesSentForPurge());
    assertEquals(15, metrics.getNumSubFilesMovedToDeletedTable());
    assertEquals(19, metrics.getNumDirsPurged());
    assertEquals(19, metrics.getNumDirsSentForPurge());
    assertEquals(0, metrics.getNumSubDirsMovedToDeletedDirTable());
    assertEquals(18, metrics.getNumSubDirsSentForPurge());

    assertThat(dirDeletingService.getRunCount().get()).isGreaterThan(1);
    // Ensure dir deleting speed, here provide a backup value for safe CI
    GenericTestUtils.waitFor(() -> dirDeletingService.getRunCount().get() - preRunCount >= 7, 1000, 100000);
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

    metrics.resetDirectoryMetrics();
    // Delete the rootDir, which should delete all keys.
    fs.delete(root, true);

    // After Delete
    checkPath(root);

    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(keyTable, 0);
    assertTableRowCount(dirTable, 0);

    assertSubPathsCount(dirDeletingService::getMovedFilesCount, 3);
    assertSubPathsCount(dirDeletingService::getMovedDirsCount, 0);
    assertSubPathsCount(dirDeletingService::getDeletedDirsCount, 5);
    assertEquals(5, metrics.getNumDirsSentForPurge());
    assertEquals(5, metrics.getNumDirsPurged());
    assertEquals(0, metrics.getNumSubDirsMovedToDeletedDirTable());
    assertEquals(4, metrics.getNumSubDirsSentForPurge());
    assertEquals(3, metrics.getNumSubFilesSentForPurge());
    assertEquals(3, metrics.getNumSubFilesMovedToDeletedTable());

    assertThat(dirDeletingService.getRunCount().get()).isGreaterThan(1);
  }

  /**
   * Test to check the following scenario:
   * A subdir gets marked for move in DirectoryDeletingService and
   * marked for delete in AbstractKeyDeletingService#optimizeDirDeletesAndSubmitRequest.
   */
  @Test
  public void testDeleteWithLessDirsButMultipleLevels() throws Exception {
    Path root = new Path("/rootDir");
    Path appRoot = new Path(root, "appRoot");
    Path parent = new Path(appRoot, "parentDir");
    fs.mkdirs(parent);
    Path child = new Path(parent, "childFile");
    ContractTestUtils.touch(fs, child);

    Table<String, OmKeyInfo> deletedDirTable =
        cluster.getOzoneManager().getMetadataManager().getDeletedDirTable();
    Table<String, OmKeyInfo> keyTable =
        cluster.getOzoneManager().getMetadataManager().getKeyTable(getFSOBucketLayout());
    Table<String, OmDirectoryInfo> dirTable = cluster.getOzoneManager().getMetadataManager().getDirectoryTable();

    DirectoryDeletingService dirDeletingService =
        (DirectoryDeletingService) cluster.getOzoneManager().getKeyManager().getDirDeletingService();

    // Before delete
    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(dirTable, 3);
    assertTableRowCount(keyTable, 1);

    assertSubPathsCount(dirDeletingService::getMovedFilesCount, 0);
    assertSubPathsCount(dirDeletingService::getMovedDirsCount, 0);
    assertSubPathsCount(dirDeletingService::getDeletedDirsCount, 0);

    metrics.resetDirectoryMetrics();
    fs.delete(appRoot, true);

    // After delete
    checkPath(appRoot);
    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(keyTable, 0);
    assertTableRowCount(dirTable, 1);
    assertSubPathsCount(dirDeletingService::getMovedFilesCount, 1);
    assertSubPathsCount(dirDeletingService::getDeletedDirsCount, 2);
    assertSubPathsCount(dirDeletingService::getMovedDirsCount, 0);

    assertEquals(2, metrics.getNumDirsSentForPurge());
    assertEquals(2, metrics.getNumDirsPurged());
    assertEquals(0, metrics.getNumSubDirsMovedToDeletedDirTable());
    assertEquals(1, metrics.getNumSubDirsSentForPurge());
    assertEquals(1, metrics.getNumSubFilesSentForPurge());
    assertEquals(1, metrics.getNumSubFilesMovedToDeletedTable());
    assertThat(dirDeletingService.getRunCount().get()).isGreaterThan(1);
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
    OzoneBucket bucket = volume.getBucket(bucketName);
    long volumeId = metadataManager.getVolumeId(volumeName);

    // manually delete dir and add to deleted table. namespace count occupied "1" as manual deletion do not reduce
    long bucketId = metadataManager.getBucketId(volumeName, bucketName);
    OzoneFileStatus keyStatus = OMFileRequest.getOMKeyInfoIfExists(
        metadataManager, volumeName, bucketName, "rootDirdd", 0,
        cluster.getOzoneManager().getDefaultReplicationConfig());
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
    assertEquals(0, volume.getBucket(bucketName).getUsedBytes());
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

  private void createFileKey(OzoneBucket bucket, String key)
      throws Exception {
    byte[] value = RandomStringUtils.secure().nextAscii(10240).getBytes(UTF_8);
    OzoneOutputStream fileKey = bucket.createKey(key, value.length);
    fileKey.write(value);
    fileKey.close();
  }

  /*
   * Create key d1/k1
   * Create snap1
   * Rename dir1 to dir2
   * Delete dir2
   * Wait for KeyDeletingService to start processing deleted key k2
   * Create snap2 by making the KeyDeletingService thread wait till snap2 is flushed
   * Resume KeyDeletingService thread.
   * Read d1 from snap1.
   */
  @Test
  public void testAOSKeyDeletingWithSnapshotCreateParallelExecution()
      throws Exception {
    OMMetadataManager omMetadataManager = cluster.getOzoneManager().getMetadataManager();
    Table<String, SnapshotInfo> snapshotInfoTable = omMetadataManager.getSnapshotInfoTable();
    Table<String, OmKeyInfo> deletedDirTable = omMetadataManager.getDeletedDirTable();
    Table<String, String> renameTable = omMetadataManager.getSnapshotRenamedTable();
    cluster.getOzoneManager().getKeyManager().getSnapshotDeletingService().shutdown();
    DirectoryDeletingService dirDeletingService = cluster.getOzoneManager().getKeyManager().getDirDeletingService();
    // Suspend KeyDeletingService
    dirDeletingService.suspend();
    Random random = new Random();
    final String testVolumeName = "volume" + random.nextInt();
    final String testBucketName = "bucket" + random.nextInt();
    // Create Volume and Buckets
    ObjectStore store = client.getObjectStore();
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    volume.createBucket(testBucketName,
        BucketArgs.newBuilder().setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED).build());
    OzoneBucket bucket = volume.getBucket(testBucketName);

    OzoneManager ozoneManager = Mockito.spy(cluster.getOzoneManager());
    OmSnapshotManager omSnapshotManager = Mockito.spy(ozoneManager.getOmSnapshotManager());
    when(ozoneManager.getOmSnapshotManager()).thenAnswer(i -> omSnapshotManager);
    DirectoryDeletingService service = Mockito.spy(new DirectoryDeletingService(1000, TimeUnit.MILLISECONDS, 1000,
        ozoneManager, cluster.getConf(), 1, false));
    service.shutdown();
    final int initialSnapshotCount =
        (int) cluster.getOzoneManager().getMetadataManager().countRowsInTable(snapshotInfoTable);
    final int initialDeletedCount = (int) omMetadataManager.countRowsInTable(deletedDirTable);
    final int initialRenameCount = (int) omMetadataManager.countRowsInTable(renameTable);
    String snap1 = "snap1";
    String snap2 = "snap2";
    createFileKey(bucket, "dir1/key1");
    store.createSnapshot(testVolumeName, testBucketName, "snap1");
    bucket.renameKey("dir1", "dir2");
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(testVolumeName)
        .setBucketName(testBucketName)
        .setKeyName("dir2").build();
    long objectId = store.getClientProxy().getOzoneManagerClient().getKeyInfo(omKeyArgs, false)
        .getKeyInfo().getObjectID();
    long volumeId = omMetadataManager.getVolumeId(testVolumeName);
    long bucketId = omMetadataManager.getBucketId(testVolumeName, testBucketName);
    String deletePathKey = omMetadataManager.getOzoneDeletePathKey(objectId,
        omMetadataManager.getOzonePathKey(volumeId,
        bucketId, bucketId, "dir2"));
    bucket.deleteDirectory("dir2", true);


    assertTableRowCount(deletedDirTable, initialDeletedCount + 1);
    assertTableRowCount(renameTable, initialRenameCount + 1);
    Mockito.doAnswer(i -> {
      List<OzoneManagerProtocolProtos.PurgePathRequest> purgePathRequestList = i.getArgument(4);
      for (OzoneManagerProtocolProtos.PurgePathRequest purgeRequest : purgePathRequestList) {
        Assertions.assertNotEquals(deletePathKey, purgeRequest.getDeletedDir());
      }
      return null;
    }).when(service).optimizeDirDeletesAndSubmitRequest(anyLong(), anyLong(),
        anyLong(), anyList(), anyList(), eq(null), anyLong(), any(),
        any(ReclaimableDirFilter.class), any(ReclaimableKeyFilter.class), anyMap(), any(),
        anyLong(), any(AtomicInteger.class));

    Mockito.doAnswer(i -> {
      store.createSnapshot(testVolumeName, testBucketName, snap2);
      GenericTestUtils.waitFor(() -> {
        try {
          SnapshotInfo snapshotInfo = store.getClientProxy().getOzoneManagerClient()
              .getSnapshotInfo(testVolumeName, testBucketName, snap2);

          return OmSnapshotManager.areSnapshotChangesFlushedToDB(cluster.getOzoneManager().getMetadataManager(),
              snapshotInfo);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }, 1000, 100000);
      GenericTestUtils.waitFor(() -> {
        try {
          return renameTable.get(omMetadataManager.getRenameKey(testVolumeName, testBucketName, objectId)) == null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }, 1000, 10000);
      return i.callRealMethod();
    }).when(omSnapshotManager).getActiveSnapshot(ArgumentMatchers.eq(testVolumeName),
        ArgumentMatchers.eq(testBucketName), ArgumentMatchers.eq(snap1));
    assertTableRowCount(snapshotInfoTable, initialSnapshotCount + 1);
    service.runPeriodicalTaskNow();
    service.runPeriodicalTaskNow();
    assertTableRowCount(snapshotInfoTable, initialSnapshotCount + 2);
    store.deleteSnapshot(testVolumeName, testBucketName, snap2);
    service.runPeriodicalTaskNow();
    store.deleteSnapshot(testVolumeName, testBucketName, snap1);
    cluster.getOzoneManager().awaitDoubleBufferFlush();
    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();
    cluster.getOzoneManager().awaitDoubleBufferFlush();
    SnapshotDeletingService snapshotDeletingService =
        cluster.getOzoneManager().getKeyManager().getSnapshotDeletingService();
    GenericTestUtils.waitFor(() -> {
      try {
        snapshotDeletingService.runPeriodicalTaskNow();
        cluster.getOzoneManager().awaitDoubleBufferFlush();
        long currentSnapshotCount = cluster.getOzoneManager().getMetadataManager()
            .countRowsInTable(cluster.getOzoneManager().getMetadataManager().getSnapshotInfoTable());
        return currentSnapshotCount <= initialSnapshotCount;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 100, 10000);
    assertTableRowCount(cluster.getOzoneManager().getMetadataManager().getSnapshotInfoTable(), initialSnapshotCount);
    dirDeletingService.resume();
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
    // 0 dirs in dirTable.
    assertTableRowCount(keyTable, 5);
    assertTableRowCount(dirTable, 0);

    // KeyDeletingService and DirectoryDeletingService will not
    // clean up because the paths are part of a snapshot.
    // As a result on 5 deleted dir and 3 deleted files will
    // remain in dirTable and keyTable respectively.
    long prevDDSRunCount = dirDeletingService.getRunCount().get();
    long prevKDSRunCount = keyDeletingService.getRunCount().get();
    assertTableRowCount(deletedDirTable, 5);
    assertTableRowCount(deletedKeyTable, 3);
    GenericTestUtils.waitFor(() -> dirDeletingService.getRunCount().get() >
        prevDDSRunCount, 100, 10000);
    GenericTestUtils.waitFor(() -> keyDeletingService.getRunCount().get() >
        prevKDSRunCount, 100, 10000);

    assertSubPathsCount(dirDeletingService::getMovedFilesCount, 0);
    assertSubPathsCount(dirDeletingService::getMovedDirsCount, 4);
    assertSubPathsCount(dirDeletingService::getDeletedDirsCount, 0);

    // Manual cleanup deletedDirTable for next tests
    client.getObjectStore().deleteSnapshot(volumeName, bucketName, "snap1");
    cleanupTables();
  }

  private void cleanupTables() throws IOException {
    OMMetadataManager metadataManager =
        cluster.getOzoneManager().getMetadataManager();

    try (Table.KeyValueIterator<String, OmKeyInfo> it = metadataManager.getDeletedDirTable()
        .iterator()) {
      removeAllFromDB(it);
    }
    try (Table.KeyValueIterator<String, OmKeyInfo> it = metadataManager.getFileTable().iterator()) {
      removeAllFromDB(it);
    }
    try (Table.KeyValueIterator<String, OmDirectoryInfo> it = metadataManager.getDirectoryTable()
        .iterator()) {
      removeAllFromDB(it);
    }
  }

  private static void removeAllFromDB(Table.KeyValueIterator<?, ?> iterator)
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
    AtomicLong count = new AtomicLong(0L);
    assertDoesNotThrow(() -> {
      count.set(cluster.getOzoneManager().getMetadataManager().countRowsInTable(table));
      LOG.info("{} actual row count={}, expectedCount={}", table.getName(),
          count.get(), expectedCount);
    });

    return count.get() == expectedCount;
  }

  private void checkPath(Path path) {
    FileNotFoundException ex = assertThrows(FileNotFoundException.class, () -> fs.getFileStatus(path));
    assertThat(ex.getMessage()).contains("No such file or directory");
  }

  private static BucketLayout getFSOBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
