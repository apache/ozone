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

package org.apache.hadoop.ozone.om.snapshot;

import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.service.DirectoryDeletingService;
import org.apache.hadoop.ozone.om.service.KeyDeletingService;
import org.apache.hadoop.ozone.om.service.SnapshotDeletingService;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_DEEP_CLEANING_ENABLED;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

/**
 * Test Snapshot Deleting Service.
 */

@Timeout(300)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(OrderAnnotation.class)
public class TestSnapshotDeletingServiceIntegrationTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSnapshotDeletingServiceIntegrationTest.class);
  private static final ByteBuffer CONTENT =
      ByteBuffer.allocate(1024 * 1024 * 16);

  private MiniOzoneCluster cluster;
  private OzoneManager om;
  private OzoneBucket bucket1;
  private OzoneClient client;
  private static final String VOLUME_NAME = "vol1";
  private static final String BUCKET_NAME_ONE = "bucket1";
  private static final String BUCKET_NAME_TWO = "bucket2";
  private static final String BUCKET_NAME_FSO = "bucketfso";

  private boolean runIndividualTest = true;

  @BeforeAll
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE,
        4, StorageUnit.MB);
    conf.setStorageSize(OZONE_SCM_CHUNK_SIZE_KEY,
        1, StorageUnit.MB);
    conf.setTimeDuration(OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL,
        500, TimeUnit.MILLISECONDS);
    conf.setBoolean(OZONE_SNAPSHOT_DEEP_CLEANING_ENABLED, true);
    conf.setTimeDuration(OZONE_SNAPSHOT_DELETING_SERVICE_TIMEOUT,
        10000, TimeUnit.MILLISECONDS);
    conf.setInt(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 500);
    conf.setInt(OMConfigKeys.OZONE_PATH_DELETING_LIMIT_PER_TASK, 5);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 500,
        TimeUnit.MILLISECONDS);
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    // Enable filesystem snapshot feature for the test regardless of the default
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    om = cluster.getOzoneManager();
    bucket1 = TestDataUtil.createVolumeAndBucket(
        client, VOLUME_NAME, BUCKET_NAME_ONE, BucketLayout.DEFAULT);
  }

  @AfterAll
  public void teardown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  @Order(2)
  @Flaky("HDDS-11130")
  public void testSnapshotSplitAndMove() throws Exception {

    if (runIndividualTest) {
      SnapshotDeletingService snapshotDeletingService =
          om.getKeyManager().getSnapshotDeletingService();
      Table<String, SnapshotInfo> snapshotInfoTable =
          om.getMetadataManager().getSnapshotInfoTable();

      createSnapshotDataForBucket(bucket1);

      assertTableRowCount(snapshotInfoTable, 2);
      GenericTestUtils.waitFor(() -> snapshotDeletingService
          .getSuccessfulRunCount() >= 1, 1000, 10000);
    }
    OmSnapshot bucket1snap3 = om.getOmSnapshotManager()
        .getSnapshot(VOLUME_NAME, BUCKET_NAME_ONE, "bucket1snap3").get();

    // Check bucket1key1 added to next non deleted snapshot db.
    List<? extends Table.KeyValue<String, RepeatedOmKeyInfo>> omKeyInfos =
        bucket1snap3.getMetadataManager()
            .getDeletedTable().getRangeKVs(null, 100,
                "/vol1/bucket1/bucket1key1");
    assertEquals(1, omKeyInfos.size());
  }

  @Test
  @Order(1)
  public void testMultipleSnapshotKeyReclaim() throws Exception {

    Table<String, RepeatedOmKeyInfo> deletedTable =
        om.getMetadataManager().getDeletedTable();
    Table<String, SnapshotInfo> snapshotInfoTable =
        om.getMetadataManager().getSnapshotInfoTable();
    runIndividualTest = false;

    createSnapshotDataForBucket(bucket1);

    BucketArgs bucketArgs = new BucketArgs.Builder()
        .setBucketLayout(BucketLayout.LEGACY)
        .build();

    OzoneBucket bucket2 = TestDataUtil.createBucket(
        client, VOLUME_NAME, bucketArgs, BUCKET_NAME_TWO);
    // Create key1 and key2
    TestDataUtil.createKey(bucket2, "bucket2key1", ReplicationFactor.THREE,
        ReplicationType.RATIS, CONTENT);
    TestDataUtil.createKey(bucket2, "bucket2key2", ReplicationFactor.THREE,
        ReplicationType.RATIS, CONTENT);

    // Create Snapshot
    client.getObjectStore().createSnapshot(VOLUME_NAME, BUCKET_NAME_TWO,
        "bucket2snap1");
    assertTableRowCount(snapshotInfoTable, 3);

    // Both key 1 and key 2 can be reclaimed when Snapshot 1 is deleted.
    client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_TWO,
        "bucket2key1", false);
    client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_TWO,
        "bucket2key2", false);
    assertTableRowCount(deletedTable, 2);
    SnapshotInfo delSnapInfo = snapshotInfoTable
        .get("/vol1/bucket2/bucket2snap1");
    client.getObjectStore().deleteSnapshot(VOLUME_NAME, BUCKET_NAME_TWO,
        "bucket2snap1");
    assertTableRowCount(snapshotInfoTable, 2);
    // KeyDeletingService will clean up.
    assertTableRowCount(deletedTable, 0);

    verifySnapshotChain(delSnapInfo, null);

    // verify the cache of purged snapshot
    // /vol1/bucket2/bucket2snap1 has been cleaned up from cache map
    assertEquals(2, om.getOmSnapshotManager().getSnapshotCacheSize());

    // cleaning up the data
    client.getProxy().deleteSnapshot(VOLUME_NAME, BUCKET_NAME_ONE, "bucket1snap1");
    client.getProxy().deleteSnapshot(VOLUME_NAME, BUCKET_NAME_ONE, "bucket1snap3");
    client.getProxy().deleteBucket(VOLUME_NAME, BUCKET_NAME_TWO);
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  @Order(3)
  @Flaky("HDDS-11131")
  public void testSnapshotWithFSO() throws Exception {
    Table<String, OmDirectoryInfo> dirTable =
        om.getMetadataManager().getDirectoryTable();
    Table<String, SnapshotInfo> snapshotInfoTable =
        om.getMetadataManager().getSnapshotInfoTable();
    Table<String, OmKeyInfo> keyTable =
        om.getMetadataManager().getFileTable();
    Table<String, RepeatedOmKeyInfo> deletedTable =
        om.getMetadataManager().getDeletedTable();
    Table<String, OmKeyInfo>  deletedDirTable =
        om.getMetadataManager().getDeletedDirTable();
    Table<String, String> renamedTable =
        om.getMetadataManager().getSnapshotRenamedTable();

    BucketArgs bucketArgs = new BucketArgs.Builder()
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .build();

    OzoneBucket bucket2 = TestDataUtil.createBucket(
        client, VOLUME_NAME, bucketArgs, BUCKET_NAME_FSO);

    // Create 10 keys
    for (int i = 1; i <= 10; i++) {
      TestDataUtil.createKey(bucket2, "key" + i, ReplicationFactor.THREE,
          ReplicationType.RATIS, CONTENT);
    }

    // Create 5 keys to overwrite
    for (int i = 11; i <= 15; i++) {
      TestDataUtil.createKey(bucket2, "key" + i, ReplicationFactor.THREE,
          ReplicationType.RATIS, CONTENT);
    }

    // Create Directory and Sub
    for (int i = 1; i <= 3; i++) {
      String parent = "parent" + i;
      client.getProxy().createDirectory(VOLUME_NAME,
          BUCKET_NAME_FSO, parent);
      for (int j = 1; j <= 3; j++) {
        String childFile = "/childFile" + j;
        String childDir = "/childDir" + j;
        client.getProxy().createDirectory(VOLUME_NAME,
            BUCKET_NAME_FSO, parent + childDir);
        TestDataUtil.createKey(bucket2, parent + childFile,
            ReplicationFactor.THREE, ReplicationType.RATIS, CONTENT);
      }
    }

    // Total 12 dirs, 19 keys.
    assertTableRowCount(dirTable, 12);
    assertTableRowCount(keyTable, 24);
    assertTableRowCount(deletedDirTable, 0);

    // Create Snapshot1
    client.getObjectStore().createSnapshot(VOLUME_NAME, BUCKET_NAME_FSO,
        "snap1");
    assertTableRowCount(snapshotInfoTable, 1);

    // Overwrite 3 keys -> Moves previous version to deletedTable
    for (int i = 11; i <= 13; i++) {
      TestDataUtil.createKey(bucket2, "key" + i, ReplicationFactor.THREE,
          ReplicationType.RATIS, CONTENT);
    }
    assertTableRowCount(keyTable, 24);

    // Delete 5 Keys
    for (int i = 1; i <= 5; i++) {
      client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_FSO,
          "key" + i, false);
    }
    // Rename Keys 3 keys
    for (int i = 6; i <= 8; i++) {
      client.getProxy().renameKey(VOLUME_NAME, BUCKET_NAME_FSO, "key" + i,
          "renamedKey" + i);
    }

    // Rename 1 Dir
    for (int i = 1; i <= 1; i++) {
      client.getProxy().renameKey(VOLUME_NAME, BUCKET_NAME_FSO, "/parent" + i,
          "/renamedParent" + i);
    }

    // Delete 2 Dirs
    for (int i = 2; i <= 3; i++) {
      client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_FSO, "/parent" + i,
          true);
    }

    assertTableRowCount(renamedTable, 4);
    // Delete Renamed Keys
    for (int i = 6; i <= 8; i++) {
      client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_FSO,
          "renamedKey" + i, false);
    }

    // Delete Renamed Dir
    for (int i = 1; i <= 1; i++) {
      client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_FSO,
          "/renamedParent" + i, true);
    }

    assertTableRowCount(deletedTable, 11);
    assertTableRowCount(deletedDirTable, 3);
    assertTableRowCount(dirTable, 9);
    assertTableRowCount(renamedTable, 4);

    // Create Snapshot2
    client.getObjectStore().createSnapshot(VOLUME_NAME, BUCKET_NAME_FSO,
        "snap2");

    assertTableRowCount(snapshotInfoTable, 2);
    // Once snapshot is taken renamedTable, deletedTable, deletedDirTable
    // should be cleaned
    assertTableRowCount(renamedTable, 0);
    assertTableRowCount(deletedTable, 0);
    assertTableRowCount(deletedDirTable, 0);

    // Delete 3 overwritten keys
    for (int i = 11; i <= 13; i++) {
      client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_FSO,
          "key" + i, false);
    }

    // Overwrite 2 keys
    for (int i = 14; i <= 15; i++) {
      TestDataUtil.createKey(bucket2, "key" + i, ReplicationFactor.THREE,
          ReplicationType.RATIS, CONTENT);
    }

    // Delete 2 more keys
    for (int i = 9; i <= 10; i++) {
      client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_FSO,
          "key" + i, false);
    }

    assertTableRowCount(deletedTable, 7);

    // Create Snapshot3
    client.getObjectStore().createSnapshot(VOLUME_NAME, BUCKET_NAME_FSO,
        "snap3");
    assertTableRowCount(snapshotInfoTable, 3);

    assertTableRowCount(renamedTable, 0);
    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(deletedTable, 0);
    assertTableRowCount(keyTable, 11);
    SnapshotInfo deletedSnap = om.getMetadataManager()
        .getSnapshotInfoTable().get("/vol1/bucketfso/snap2");

    client.getObjectStore().deleteSnapshot(VOLUME_NAME, BUCKET_NAME_FSO,
        "snap2");
    assertTableRowCount(snapshotInfoTable, 2);

    // Delete 2 overwritten keys
    for (int i = 14; i <= 15; i++) {
      client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_FSO,
          "key" + i, false);
    }
    assertTableRowCount(deletedTable, 2);
    // Once all the tables are moved, the snapshot is deleted
    assertTableRowCount(om.getMetadataManager().getSnapshotInfoTable(), 2);

    verifySnapshotChain(deletedSnap, "/vol1/bucketfso/snap3");
    OmSnapshot snap3 = om.getOmSnapshotManager()
        .getSnapshot(VOLUME_NAME, BUCKET_NAME_FSO, "snap3").get();

    Table<String, OmKeyInfo> snapDeletedDirTable =
        snap3.getMetadataManager().getDeletedDirTable();
    Table<String, String> snapRenamedTable =
        snap3.getMetadataManager().getSnapshotRenamedTable();
    Table<String, RepeatedOmKeyInfo> snapDeletedTable =
        snap3.getMetadataManager().getDeletedTable();

    assertTableRowCount(snapRenamedTable, 4);
    assertTableRowCount(snapDeletedDirTable, 3);
    // All the keys deleted before snapshot2 is moved to snap3
    assertTableRowCount(snapDeletedTable, 15);

    // Before deleting the last snapshot
    assertTableRowCount(renamedTable, 0);
    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(deletedTable, 2);
    // Delete Snapshot3 and check entries moved to active DB
    client.getObjectStore().deleteSnapshot(VOLUME_NAME, BUCKET_NAME_FSO,
        "snap3");

    // Check entries moved to active DB
    assertTableRowCount(snapshotInfoTable, 1);
    assertTableRowCount(renamedTable, 4);
    assertTableRowCount(deletedDirTable, 3);

    ReferenceCounted<OmSnapshot> rcSnap1 =
        om.getOmSnapshotManager().getSnapshot(
            VOLUME_NAME, BUCKET_NAME_FSO, "snap1");
    OmSnapshot snap1 = rcSnap1.get();
    Table<String, OmKeyInfo> snap1KeyTable =
        snap1.getMetadataManager().getFileTable();
    try (TableIterator<String, ? extends Table.KeyValue<String,
        RepeatedOmKeyInfo>> iterator = deletedTable.iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, RepeatedOmKeyInfo> next = iterator.next();
        String activeDBDeletedKey = next.getKey();
        if (activeDBDeletedKey.matches(".*/key1/.*")) {
          RepeatedOmKeyInfo activeDBDeleted = next.getValue();
          OMMetadataManager metadataManager =
              cluster.getOzoneManager().getMetadataManager();
          assertEquals(1, activeDBDeleted.getOmKeyInfoList().size());
          OmKeyInfo activeDbDeletedKeyInfo =
              activeDBDeleted.getOmKeyInfoList().get(0);
          long volumeId = metadataManager
              .getVolumeId(activeDbDeletedKeyInfo.getVolumeName());
          long bucketId = metadataManager
              .getBucketId(activeDbDeletedKeyInfo.getVolumeName(),
                  activeDbDeletedKeyInfo.getBucketName());
          String keyForSnap =
              metadataManager.getOzonePathKey(volumeId, bucketId,
                  activeDbDeletedKeyInfo.getParentObjectID(),
                  activeDbDeletedKeyInfo.getKeyName());
          OmKeyInfo snap1keyInfo = snap1KeyTable.get(keyForSnap);
          assertEquals(activeDbDeletedKeyInfo.getLatestVersionLocations()
              .getLocationList(), snap1keyInfo.getLatestVersionLocations()
              .getLocationList());
        }
      }
    }
    assertTableRowCount(deletedTable, 15);

    snap1 = null;
    rcSnap1.close();
  }

  private DirectoryDeletingService getMockedDirectoryDeletingService(AtomicBoolean dirDeletionWaitStarted,
                                                                     AtomicBoolean dirDeletionStarted)
      throws InterruptedException, TimeoutException, IOException {
    OzoneManager ozoneManager = Mockito.spy(om);
    om.getKeyManager().getDirDeletingService().shutdown();
    GenericTestUtils.waitFor(() -> om.getKeyManager().getDirDeletingService().getThreadCount() == 0, 1000,
        100000);
    DirectoryDeletingService directoryDeletingService = Mockito.spy(new DirectoryDeletingService(10000,
        TimeUnit.MILLISECONDS, 100000, ozoneManager, cluster.getConf(), 1));
    directoryDeletingService.shutdown();
    GenericTestUtils.waitFor(() -> directoryDeletingService.getThreadCount() == 0, 1000,
        100000);
    doAnswer(i -> {
      // Wait for SDS to reach DDS wait block before processing any deleted directories.
      GenericTestUtils.waitFor(dirDeletionWaitStarted::get, 1000, 100000);
      dirDeletionStarted.set(true);
      return i.callRealMethod();
    }).when(directoryDeletingService).getPendingDeletedDirInfo();
    return directoryDeletingService;
  }

  private KeyDeletingService getMockedKeyDeletingService(AtomicBoolean keyDeletionWaitStarted,
                                                         AtomicBoolean keyDeletionStarted)
      throws InterruptedException, TimeoutException, IOException {
    OzoneManager ozoneManager = Mockito.spy(om);
    om.getKeyManager().getDeletingService().shutdown();
    GenericTestUtils.waitFor(() -> om.getKeyManager().getDeletingService().getThreadCount() == 0, 1000,
        100000);
    KeyManager keyManager = Mockito.spy(om.getKeyManager());
    when(ozoneManager.getKeyManager()).thenReturn(keyManager);
    KeyDeletingService keyDeletingService = Mockito.spy(new KeyDeletingService(ozoneManager,
        ozoneManager.getScmClient().getBlockClient(), keyManager, 10000,
        100000, cluster.getConf(), false));
    keyDeletingService.shutdown();
    GenericTestUtils.waitFor(() -> keyDeletingService.getThreadCount() == 0, 1000,
        100000);
    when(keyManager.getPendingDeletionKeys(anyInt())).thenAnswer(i -> {
      // wait for SDS to reach the KDS wait block before processing any key.
      GenericTestUtils.waitFor(keyDeletionWaitStarted::get, 1000, 100000);
      keyDeletionStarted.set(true);
      return i.callRealMethod();
    });
    return keyDeletingService;
  }

  @SuppressWarnings("checkstyle:parameternumber")
  private SnapshotDeletingService getMockedSnapshotDeletingService(KeyDeletingService keyDeletingService,
                                                                   DirectoryDeletingService directoryDeletingService,
                                                                   AtomicBoolean snapshotDeletionStarted,
                                                                   AtomicBoolean keyDeletionWaitStarted,
                                                                   AtomicBoolean dirDeletionWaitStarted,
                                                                   AtomicBoolean keyDeletionStarted,
                                                                   AtomicBoolean dirDeletionStarted,
                                                                   OzoneBucket testBucket)
      throws InterruptedException, TimeoutException, IOException {
    OzoneManager ozoneManager = Mockito.spy(om);
    om.getKeyManager().getSnapshotDeletingService().shutdown();
    GenericTestUtils.waitFor(() -> om.getKeyManager().getSnapshotDeletingService().getThreadCount() == 0, 1000,
        100000);
    KeyManager keyManager = Mockito.spy(om.getKeyManager());
    OmMetadataManagerImpl omMetadataManager = Mockito.spy((OmMetadataManagerImpl)om.getMetadataManager());
    SnapshotChainManager unMockedSnapshotChainManager =
        ((OmMetadataManagerImpl)om.getMetadataManager()).getSnapshotChainManager();
    SnapshotChainManager snapshotChainManager = Mockito.spy(unMockedSnapshotChainManager);
    OmSnapshotManager omSnapshotManager = Mockito.spy(om.getOmSnapshotManager());
    when(ozoneManager.getOmSnapshotManager()).thenReturn(omSnapshotManager);
    when(ozoneManager.getKeyManager()).thenReturn(keyManager);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(omMetadataManager.getSnapshotChainManager()).thenReturn(snapshotChainManager);
    when(keyManager.getDeletingService()).thenReturn(keyDeletingService);
    when(keyManager.getDirDeletingService()).thenReturn(directoryDeletingService);
    SnapshotDeletingService snapshotDeletingService = Mockito.spy(new SnapshotDeletingService(10000,
        100000, ozoneManager));
    snapshotDeletingService.shutdown();
    GenericTestUtils.waitFor(() -> snapshotDeletingService.getThreadCount() == 0, 1000,
        100000);
    when(snapshotChainManager.iterator(anyBoolean())).thenAnswer(i -> {
      Iterator<UUID> itr = (Iterator<UUID>) i.callRealMethod();
      return Lists.newArrayList(itr).stream().filter(uuid -> {
        try {
          SnapshotInfo snapshotInfo = SnapshotUtils.getSnapshotInfo(om, snapshotChainManager, uuid);
          return snapshotInfo.getBucketName().equals(testBucket.getName()) &&
              snapshotInfo.getVolumeName().equals(testBucket.getVolumeName());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }).iterator();
    });
    when(snapshotChainManager.getLatestGlobalSnapshotId())
        .thenAnswer(i -> unMockedSnapshotChainManager.getLatestGlobalSnapshotId());
    when(snapshotChainManager.getOldestGlobalSnapshotId())
        .thenAnswer(i -> unMockedSnapshotChainManager.getOldestGlobalSnapshotId());
    doAnswer(i -> {
      // KDS wait block reached in SDS.
      GenericTestUtils.waitFor(() -> {
        return keyDeletingService.isRunningOnAOS();
      }, 1000, 100000);
      keyDeletionWaitStarted.set(true);
      return i.callRealMethod();
    }).when(snapshotDeletingService).waitForKeyDeletingService();
    doAnswer(i -> {
      // DDS wait block reached in SDS.
      GenericTestUtils.waitFor(directoryDeletingService::isRunningOnAOS, 1000, 100000);
      dirDeletionWaitStarted.set(true);
      return i.callRealMethod();
    }).when(snapshotDeletingService).waitForDirDeletingService();
    doAnswer(i -> {
      // Assert KDS & DDS is not running when SDS starts moving entries & assert all wait block, KDS processing
      // AOS block & DDS AOS block have been executed.
      Assertions.assertTrue(keyDeletionWaitStarted.get());
      Assertions.assertTrue(dirDeletionWaitStarted.get());
      Assertions.assertTrue(keyDeletionStarted.get());
      Assertions.assertTrue(dirDeletionStarted.get());
      Assertions.assertFalse(keyDeletingService.isRunningOnAOS());
      Assertions.assertFalse(directoryDeletingService.isRunningOnAOS());
      snapshotDeletionStarted.set(true);
      return i.callRealMethod();
    }).when(omSnapshotManager).getSnapshot(anyString(), anyString(), anyString());
    return snapshotDeletingService;
  }

  @Test
  @Order(4)
  @Flaky("HDDS-11847")
  public void testParallelExcecutionOfKeyDeletionAndSnapshotDeletion() throws Exception {
    AtomicBoolean keyDeletionWaitStarted = new AtomicBoolean(false);
    AtomicBoolean dirDeletionWaitStarted = new AtomicBoolean(false);
    AtomicBoolean keyDeletionStarted = new AtomicBoolean(false);
    AtomicBoolean dirDeletionStarted = new AtomicBoolean(false);
    AtomicBoolean snapshotDeletionStarted = new AtomicBoolean(false);
    Random random = new Random();
    String bucketName = "bucket" + random.nextInt();
    BucketArgs bucketArgs = new BucketArgs.Builder()
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .build();
    OzoneBucket testBucket = TestDataUtil.createBucket(
        client, VOLUME_NAME, bucketArgs, bucketName);
    // mock keyDeletingService
    KeyDeletingService keyDeletingService = getMockedKeyDeletingService(keyDeletionWaitStarted, keyDeletionStarted);

    // mock dirDeletingService
    DirectoryDeletingService directoryDeletingService = getMockedDirectoryDeletingService(dirDeletionWaitStarted,
        dirDeletionStarted);

    // mock snapshotDeletingService.
    SnapshotDeletingService snapshotDeletingService = getMockedSnapshotDeletingService(keyDeletingService,
        directoryDeletingService, snapshotDeletionStarted, keyDeletionWaitStarted, dirDeletionWaitStarted,
        keyDeletionStarted, dirDeletionStarted, testBucket);
    createSnapshotFSODataForBucket(testBucket);
    List<Table.KeyValue<String, String>> renamesKeyEntries;
    List<Table.KeyValue<String, List<OmKeyInfo>>> deletedKeyEntries;
    List<Table.KeyValue<String, OmKeyInfo>> deletedDirEntries;
    try (ReferenceCounted<OmSnapshot> snapshot = om.getOmSnapshotManager().getSnapshot(testBucket.getVolumeName(),
        testBucket.getName(), testBucket.getName() + "snap2")) {
      renamesKeyEntries = snapshot.get().getKeyManager().getRenamesKeyEntries(testBucket.getVolumeName(),
          testBucket.getName(), "", 1000);
      deletedKeyEntries = snapshot.get().getKeyManager().getDeletedKeyEntries(testBucket.getVolumeName(),
          testBucket.getName(), "", 1000);
      deletedDirEntries = snapshot.get().getKeyManager().getDeletedDirEntries(testBucket.getVolumeName(),
          testBucket.getName(), 1000);
    }
    Thread keyDeletingThread = new Thread(() -> {
      try {
        keyDeletingService.runPeriodicalTaskNow();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    Thread directoryDeletingThread = new Thread(() -> {
      try {
        directoryDeletingService.runPeriodicalTaskNow();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    ExecutorService snapshotDeletingThread = Executors.newFixedThreadPool(1);
    Runnable snapshotDeletionRunnable = () -> {
      try {
        snapshotDeletingService.runPeriodicalTaskNow();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    keyDeletingThread.start();
    directoryDeletingThread.start();
    Future<?> future = snapshotDeletingThread.submit(snapshotDeletionRunnable);
    GenericTestUtils.waitFor(snapshotDeletionStarted::get, 1000, 30000);
    future.get();
    try (ReferenceCounted<OmSnapshot> snapshot = om.getOmSnapshotManager().getSnapshot(testBucket.getVolumeName(),
        testBucket.getName(), testBucket.getName() + "snap2")) {
      Assertions.assertEquals(Collections.emptyList(),
          snapshot.get().getKeyManager().getRenamesKeyEntries(testBucket.getVolumeName(),
          testBucket.getName(), "", 1000));
      Assertions.assertEquals(Collections.emptyList(),
          snapshot.get().getKeyManager().getDeletedKeyEntries(testBucket.getVolumeName(),
          testBucket.getName(), "", 1000));
      Assertions.assertEquals(Collections.emptyList(),
          snapshot.get().getKeyManager().getDeletedDirEntries(testBucket.getVolumeName(),
          testBucket.getName(), 1000));
    }
    List<Table.KeyValue<String, String>> aosRenamesKeyEntries =
        om.getKeyManager().getRenamesKeyEntries(testBucket.getVolumeName(),
            testBucket.getName(), "", 1000);
    List<Table.KeyValue<String, List<OmKeyInfo>>> aosDeletedKeyEntries =
        om.getKeyManager().getDeletedKeyEntries(testBucket.getVolumeName(),
            testBucket.getName(), "", 1000);
    List<Table.KeyValue<String, OmKeyInfo>> aosDeletedDirEntries =
        om.getKeyManager().getDeletedDirEntries(testBucket.getVolumeName(),
            testBucket.getName(), 1000);
    renamesKeyEntries.forEach(entry -> Assertions.assertTrue(aosRenamesKeyEntries.contains(entry)));
    deletedKeyEntries.forEach(entry -> Assertions.assertTrue(aosDeletedKeyEntries.contains(entry)));
    deletedDirEntries.forEach(entry -> Assertions.assertTrue(aosDeletedDirEntries.contains(entry)));
    Mockito.reset(snapshotDeletingService);
    SnapshotInfo snap2 = SnapshotUtils.getSnapshotInfo(om, testBucket.getVolumeName(),
        testBucket.getName(), testBucket.getName() + "snap2");
    Assertions.assertEquals(snap2.getSnapshotStatus(), SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED);
    future = snapshotDeletingThread.submit(snapshotDeletionRunnable);
    future.get();
    Assertions.assertThrows(IOException.class, () -> SnapshotUtils.getSnapshotInfo(om, testBucket.getVolumeName(),
        testBucket.getName(), testBucket.getName() + "snap2"));
    cluster.restartOzoneManager();
  }

  /*
      Flow
      ----
      create key0
      create key1
      create snapshot1
      create key0
      create key2
      delete key1
      delete key2
      delete key0
      create snapshot2
      create key3
      create key4
      delete key4
      create snapshot3
      delete snapshot2
   */
  private synchronized void createSnapshotDataForBucket(OzoneBucket bucket) throws Exception {
    Table<String, SnapshotInfo> snapshotInfoTable =
        om.getMetadataManager().getSnapshotInfoTable();
    Table<String, RepeatedOmKeyInfo> deletedTable =
        om.getMetadataManager().getDeletedTable();
    Table<String, OmKeyInfo> keyTable =
        om.getMetadataManager().getKeyTable(BucketLayout.DEFAULT);
    OmMetadataManagerImpl metadataManager = (OmMetadataManagerImpl)
        om.getMetadataManager();

    TestDataUtil.createKey(bucket, bucket.getName() + "key0", ReplicationFactor.THREE,
        ReplicationType.RATIS, CONTENT);
    TestDataUtil.createKey(bucket, bucket.getName() + "key1", ReplicationFactor.THREE,
        ReplicationType.RATIS, CONTENT);
    assertTableRowCount(keyTable, 2);

    // Create Snapshot 1.
    client.getProxy().createSnapshot(bucket.getVolumeName(), bucket.getName(),
        bucket.getName() + "snap1");
    assertTableRowCount(snapshotInfoTable, 1);

    // Overwrite bucket1key0, This is a newer version of the key which should
    // reclaimed as this is a different version of the key.
    TestDataUtil.createKey(bucket, bucket.getName() + "key0", ReplicationFactor.THREE,
        ReplicationType.RATIS, CONTENT);
    TestDataUtil.createKey(bucket, bucket.getName() + "key2", ReplicationFactor.THREE,
        ReplicationType.RATIS, CONTENT);

    // Key 1 cannot be reclaimed as it is still referenced by Snapshot 1.
    client.getProxy().deleteKey(bucket.getVolumeName(), bucket.getName(),
        bucket.getName() + "key1", false);
    // Key 2 is deleted here, which will be reclaimed here as
    // it is not being referenced by previous snapshot.
    client.getProxy().deleteKey(bucket.getVolumeName(), bucket.getName(),
        bucket.getName() + "key2", false);
    client.getProxy().deleteKey(bucket.getVolumeName(), bucket.getName(),
        bucket.getName() + "key0", false);
    assertTableRowCount(keyTable, 0);
    // one copy of bucket1key0 should also be reclaimed as it not same
    // but original deleted key created during overwrite should not be deleted
    assertTableRowCount(deletedTable, 2);

    // Create Snapshot 2.
    client.getProxy().createSnapshot(bucket.getVolumeName(), bucket.getName(),
        bucket.getName() + "snap2");
    assertTableRowCount(snapshotInfoTable, 2);
    // Key 2 is removed from the active Db's
    // deletedTable when Snapshot 2 is taken.
    assertTableRowCount(deletedTable, 0);

    TestDataUtil.createKey(bucket, bucket.getName() + "key3", ReplicationFactor.THREE,
        ReplicationType.RATIS, CONTENT);
    TestDataUtil.createKey(bucket, bucket.getName() + "key4", ReplicationFactor.THREE,
        ReplicationType.RATIS, CONTENT);
    client.getProxy().deleteKey(bucket.getVolumeName(), bucket.getName(),
        bucket.getName() + "key4", false);
    assertTableRowCount(keyTable, 1);
    assertTableRowCount(deletedTable, 0);

    // Create Snapshot 3.
    client.getProxy().createSnapshot(bucket.getVolumeName(), bucket.getName(),
        bucket.getName() + "snap3");
    assertTableRowCount(snapshotInfoTable, 3);

    SnapshotInfo snapshotInfo = metadataManager.getSnapshotInfoTable()
        .get(String.format("/%s/%s/%ssnap2", bucket.getVolumeName(), bucket.getName(), bucket.getName()));

    // Delete Snapshot 2.
    client.getProxy().deleteSnapshot(bucket.getVolumeName(), bucket.getName(),
        bucket.getName() + "snap2");
    assertTableRowCount(snapshotInfoTable, 2);
    verifySnapshotChain(snapshotInfo, String.format("/%s/%s/%ssnap3", bucket.getVolumeName(), bucket.getName(),
        bucket.getName()));
  }


  /*
      Flow
      ----
      create dir0/key0
      create dir1/key1
      overwrite dir0/key0
      create dir2/key2
      create snap1
      rename dir1/key1 -> dir1/key10
      delete dir1/key10
      delete dir2
      create snap2
      delete snap2
   */
  private synchronized void createSnapshotFSODataForBucket(OzoneBucket bucket) throws Exception {
    Table<String, SnapshotInfo> snapshotInfoTable =
        om.getMetadataManager().getSnapshotInfoTable();
    Table<String, RepeatedOmKeyInfo> deletedTable =
        om.getMetadataManager().getDeletedTable();
    Table<String, OmKeyInfo> deletedDirTable =
        om.getMetadataManager().getDeletedDirTable();
    Table<String, OmKeyInfo> keyTable =
        om.getMetadataManager().getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    Table<String, OmDirectoryInfo> dirTable =
        om.getMetadataManager().getDirectoryTable();
    Table<String, String> renameTable = om.getMetadataManager().getSnapshotRenamedTable();
    OmMetadataManagerImpl metadataManager = (OmMetadataManagerImpl)
        om.getMetadataManager();
    Map<String, Integer> countMap =
        metadataManager.listTables().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> {
              try {
                return (int)metadataManager.countRowsInTable(e.getValue());
              } catch (IOException ex) {
                throw new RuntimeException(ex);
              }
            }));
    TestDataUtil.createKey(bucket, "dir0/" + bucket.getName() + "key0", ReplicationFactor.THREE,
        ReplicationType.RATIS, CONTENT);
    TestDataUtil.createKey(bucket, "dir1/" + bucket.getName() + "key1", ReplicationFactor.THREE,
        ReplicationType.RATIS, CONTENT);
    assertTableRowCount(keyTable, countMap.get(keyTable.getName()) + 2);
    assertTableRowCount(dirTable, countMap.get(dirTable.getName()) + 2);

    // Overwrite bucket1key0, This is a newer version of the key which should
    // reclaimed as this is a different version of the key.
    TestDataUtil.createKey(bucket, "dir0/" + bucket.getName() + "key0", ReplicationFactor.THREE,
        ReplicationType.RATIS, CONTENT);
    TestDataUtil.createKey(bucket, "dir2/" + bucket.getName() + "key2", ReplicationFactor.THREE,
        ReplicationType.RATIS, CONTENT);
    assertTableRowCount(keyTable, countMap.get(keyTable.getName()) + 3);
    assertTableRowCount(dirTable, countMap.get(dirTable.getName()) + 3);
    assertTableRowCount(deletedTable, countMap.get(deletedTable.getName()) + 1);
    // create snap1
    client.getProxy().createSnapshot(bucket.getVolumeName(), bucket.getName(),
        bucket.getName() + "snap1");
    bucket.renameKey("dir1/" + bucket.getName() + "key1", "dir1/" + bucket.getName() + "key10");
    bucket.renameKey("dir1/", "dir10/");
    assertTableRowCount(renameTable, countMap.get(renameTable.getName()) + 2);
    client.getProxy().deleteKey(bucket.getVolumeName(), bucket.getName(),
        "dir10/" + bucket.getName() + "key10", false);
    assertTableRowCount(deletedTable, countMap.get(deletedTable.getName()) + 1);
    // Key 2 is deleted here, which will be reclaimed here as
    // it is not being referenced by previous snapshot.
    client.getProxy().deleteKey(bucket.getVolumeName(), bucket.getName(), "dir2", true);
    assertTableRowCount(deletedDirTable, countMap.get(deletedDirTable.getName()) + 1);
    client.getProxy().createSnapshot(bucket.getVolumeName(), bucket.getName(),
        bucket.getName() + "snap2");
    // Delete Snapshot 2.
    client.getProxy().deleteSnapshot(bucket.getVolumeName(), bucket.getName(),
        bucket.getName() + "snap2");
    assertTableRowCount(snapshotInfoTable, countMap.get(snapshotInfoTable.getName()) +  2);
  }


  private void verifySnapshotChain(SnapshotInfo deletedSnapshot,
                                   String nextSnapshot)
      throws Exception {
    OmMetadataManagerImpl metadataManager = (OmMetadataManagerImpl)
        om.getMetadataManager();
    UUID pathPreviousSnapshotId = deletedSnapshot.getPathPreviousSnapshotId();
    UUID globalPreviousSnapshotId =
        deletedSnapshot.getGlobalPreviousSnapshotId();

    GenericTestUtils.waitFor(() -> {
      try {
        SnapshotInfo snapshotInfo = metadataManager.getSnapshotInfoTable()
            .get(deletedSnapshot.getTableKey());
        return snapshotInfo == null;
      } catch (IOException e) {
        LOG.error("Error getting snapInfo.");
      }
      return false;
    }, 100, 10000);

    if (nextSnapshot != null) {
      SnapshotInfo nextSnapshotInfo = metadataManager
          .getSnapshotInfoTable().get(nextSnapshot);
      GenericTestUtils.waitFor(() -> Objects.equals(
          nextSnapshotInfo.getPathPreviousSnapshotId(), pathPreviousSnapshotId)
          && Objects.equals(nextSnapshotInfo.getGlobalPreviousSnapshotId(),
          globalPreviousSnapshotId), 100, 10000);
    }
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
}
