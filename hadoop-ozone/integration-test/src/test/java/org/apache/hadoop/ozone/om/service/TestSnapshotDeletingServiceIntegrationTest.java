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

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_DEEP_CLEANING_ENABLED;
import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.SNAPSHOT_GC_LOCK;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.snapshot.MultiSnapshotLocks;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableKeyFilter;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.apache.ozone.test.tag.Unhealthy;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedConstruction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Snapshot Deleting Service.
 */

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(OrderAnnotation.class)
@Unhealthy("HDDS-13303")
public class TestSnapshotDeletingServiceIntegrationTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSnapshotDeletingServiceIntegrationTest.class);
  private static final ByteBuffer CONTENT =
      ByteBuffer.allocate(1024 * 1024 * 16);

  private MiniOzoneCluster cluster;
  private OzoneManager om;
  private OzoneBucket bucket1;
  private OzoneClient client;
  private final Deque<UncheckedAutoCloseableSupplier<OmSnapshot>> rcSnaps = new ArrayDeque<>();
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
        500, TimeUnit.MILLISECONDS);
    conf.setInt(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 500);
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

  @AfterEach
  public void closeAllSnapshots() {
    while (!rcSnaps.isEmpty()) {
      rcSnaps.pop().close();
    }
    // Resume services
    om.getKeyManager().getDirDeletingService().resume();
    om.getKeyManager().getDeletingService().resume();
    om.getKeyManager().getSnapshotDeletingService().resume();
  }

  private UncheckedAutoCloseableSupplier<OmSnapshot> getOmSnapshot(String volume, String bucket, String snapshotName)
      throws IOException {
    rcSnaps.push(om.getOmSnapshotManager().getSnapshot(volume, bucket, snapshotName));
    return rcSnaps.peek();
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
    OmSnapshot bucket1snap3 = getOmSnapshot(VOLUME_NAME, BUCKET_NAME_ONE, "bucket1snap3").get();

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
    TestDataUtil.createKey(bucket2, "bucket2key1", CONTENT.array());
    TestDataUtil.createKey(bucket2, "bucket2key2", CONTENT.array());

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

    assertTableRowCount(snapshotInfoTable, 0);
    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(deletedTable, 0);

    om.getKeyManager().getDirDeletingService().suspend();
    om.getKeyManager().getDeletingService().suspend();
    // Create 10 keys
    for (int i = 1; i <= 10; i++) {
      TestDataUtil.createKey(bucket2, "key" + i, CONTENT.array());
    }

    // Create 5 keys to overwrite
    for (int i = 11; i <= 15; i++) {
      TestDataUtil.createKey(bucket2, "key" + i, CONTENT.array());
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
        TestDataUtil.createKey(bucket2, parent + childFile, CONTENT.array());
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
      TestDataUtil.createKey(bucket2, "key" + i, CONTENT.array());
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
      TestDataUtil.createKey(bucket2, "key" + i, CONTENT.array());
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

    om.getKeyManager().getDirDeletingService().resume();
    om.getKeyManager().getDeletingService().resume();
    for (int i = 1; i <= 3; i++) {
      String snapshotName = "snap" + i;
      GenericTestUtils.waitFor(() -> {
        try {
          SnapshotInfo snap = om.getMetadataManager().getSnapshotInfo(VOLUME_NAME, BUCKET_NAME_FSO, snapshotName);
          LOG.info("SnapshotInfo for {} is {}", snapshotName, snap.getSnapshotId());
          return snap.isDeepCleaned() && snap.isDeepCleanedDeletedDir();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }, 2000, 100000);
    }
    om.getKeyManager().getDirDeletingService().suspend();
    om.getKeyManager().getDeletingService().suspend();
    UncheckedAutoCloseableSupplier<OmSnapshot> rcSnap2 =
        getOmSnapshot(VOLUME_NAME, BUCKET_NAME_FSO, "snap2");
    OmSnapshot snap2 = rcSnap2.get();
    //Child directories should have moved to deleted Directory table to deleted directory table of snap2
    assertTableRowCount(dirTable, 0);
    assertTableRowCount(keyTable, 11);
    assertTableRowCount(snap2.getMetadataManager().getDeletedDirTable(), 12);
    assertTableRowCount(snap2.getMetadataManager().getDeletedTable(), 11);

    client.getObjectStore().deleteSnapshot(VOLUME_NAME, BUCKET_NAME_FSO,
        "snap2");
    rcSnap2.close();

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
    UncheckedAutoCloseableSupplier<OmSnapshot> rcSnap3 = getOmSnapshot(VOLUME_NAME, BUCKET_NAME_FSO, "snap3");
    OmSnapshot snap3 = rcSnap3.get();

    Table<String, OmKeyInfo> snapDeletedDirTable =
        snap3.getMetadataManager().getDeletedDirTable();
    Table<String, String> snapRenamedTable =
        snap3.getMetadataManager().getSnapshotRenamedTable();
    Table<String, RepeatedOmKeyInfo> snapDeletedTable =
        snap3.getMetadataManager().getDeletedTable();

    assertTableRowCount(snapRenamedTable, 4);
    assertTableRowCount(snapDeletedDirTable, 12);
    // All the keys deleted before snapshot2 is moved to snap3
    assertTableRowCount(snapDeletedTable, 18);

    om.getKeyManager().getDirDeletingService().resume();
    om.getKeyManager().getDeletingService().resume();
    for (int snapshotIndex : new int[] {1, 3}) {
      String snapshotName = "snap" + snapshotIndex;
      GenericTestUtils.waitFor(() -> {
        try {
          SnapshotInfo snap = om.getMetadataManager().getSnapshotInfo(VOLUME_NAME, BUCKET_NAME_FSO, snapshotName);
          return snap.isDeepCleaned() && snap.isDeepCleanedDeletedDir();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }, 2000, 100000);
    }
    om.getKeyManager().getDirDeletingService().suspend();
    om.getKeyManager().getDeletingService().suspend();

    assertTableRowCount(snapRenamedTable, 4);
    assertTableRowCount(snapDeletedDirTable, 12);
    // All the keys deleted before snapshot2 is moved to snap3
    assertTableRowCount(snapDeletedTable, 15);

    // Before deleting the last snapshot
    assertTableRowCount(renamedTable, 0);
    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(deletedTable, 2);
    // Delete Snapshot3 and check entries moved to active DB
    client.getObjectStore().deleteSnapshot(VOLUME_NAME, BUCKET_NAME_FSO,
        "snap3");
    rcSnap3.close();

    om.getKeyManager().getDirDeletingService().resume();
    om.getKeyManager().getDeletingService().resume();
    // Check entries moved to active DB
    assertTableRowCount(snapshotInfoTable, 1);
    assertTableRowCount(renamedTable, 4);
    assertTableRowCount(deletedDirTable, 12);
    assertTableRowCount(deletedTable, 15);

    UncheckedAutoCloseableSupplier<OmSnapshot> rcSnap1 = getOmSnapshot(VOLUME_NAME, BUCKET_NAME_FSO, "snap1");
    OmSnapshot snap1 = rcSnap1.get();
    Table<String, OmKeyInfo> snap1KeyTable =
        snap1.getMetadataManager().getFileTable();
    try (Table.KeyValueIterator<String, RepeatedOmKeyInfo> iterator = deletedTable.iterator()) {
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

    TestDataUtil.createKey(bucket, bucket.getName() + "key0", CONTENT.array());
    TestDataUtil.createKey(bucket, bucket.getName() + "key1", CONTENT.array());
    assertTableRowCount(keyTable, 2);

    // Create Snapshot 1.
    client.getProxy().createSnapshot(bucket.getVolumeName(), bucket.getName(),
        bucket.getName() + "snap1");
    assertTableRowCount(snapshotInfoTable, 1);

    // Overwrite bucket1key0, This is a newer version of the key which should
    // reclaimed as this is a different version of the key.
    TestDataUtil.createKey(bucket, bucket.getName() + "key0", CONTENT.array());
    TestDataUtil.createKey(bucket, bucket.getName() + "key2", CONTENT.array());

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

    TestDataUtil.createKey(bucket, bucket.getName() + "key3", CONTENT.array());
    TestDataUtil.createKey(bucket, bucket.getName() + "key4", CONTENT.array());
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

  private MockedConstruction<ReclaimableKeyFilter> getMockedReclaimableKeyFilter(String volume, String bucket,
      AtomicBoolean kdsWaitStarted, AtomicBoolean sdsLockWaitStarted,
      AtomicBoolean sdsLockAcquired, AtomicBoolean kdsFinished, ReclaimableKeyFilter keyFilter) throws IOException {

    return mockConstruction(ReclaimableKeyFilter.class,
        (mocked, context) -> {
          when(mocked.apply(any())).thenAnswer(i -> {
            Table.KeyValue<String, OmKeyInfo> keyInfo = i.getArgument(0);
            if (!keyInfo.getValue().getVolumeName().equals(volume) ||
                !keyInfo.getValue().getBucketName().equals(bucket)) {
              return keyFilter.apply(i.getArgument(0));
            }
            keyFilter.apply(i.getArgument(0));
            //Notify SDS that Kds has started for the bucket.
            kdsWaitStarted.set(true);
            GenericTestUtils.waitFor(sdsLockWaitStarted::get, 1000, 10000);
            // Wait for 1 more second so that the command moves to lock wait.
            Thread.sleep(1000);
            return keyFilter.apply(i.getArgument(0));
          });
          doAnswer(i -> {
            assertTrue(sdsLockWaitStarted.get());
            assertFalse(sdsLockAcquired.get());
            kdsFinished.set(true);
            keyFilter.close();
            return null;
          }).when(mocked).close();
          when(mocked.getExclusiveReplicatedSizeMap()).thenAnswer(i -> keyFilter.getExclusiveReplicatedSizeMap());
          when(mocked.getExclusiveSizeMap()).thenAnswer(i -> keyFilter.getExclusiveSizeMap());
        });
  }

  @ParameterizedTest
  @CsvSource({"true, 0", "true, 1", "false, 0", "false, 1", "false, 2"})
  @DisplayName("Tests Snapshot Deleting Service while KeyDeletingService is already running.")
  @Order(4)
  public void testSnapshotDeletingServiceWaitsForKeyDeletingService(boolean kdsRunningOnAOS,
      int snasphotDeleteIndex) throws Exception {
    SnapshotChainManager snapshotChainManager =
        ((OmMetadataManagerImpl)om.getMetadataManager()).getSnapshotChainManager();
    GenericTestUtils.waitFor(() -> {
      try {
        Iterator<UUID> itr = snapshotChainManager.iterator(false);
        while (itr.hasNext()) {
          SnapshotInfo snapshotInfo = SnapshotUtils.getSnapshotInfo(om, snapshotChainManager, itr.next());
          assertEquals(SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE, snapshotInfo.getSnapshotStatus());
        }
        return true;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 1000, 30000);
    om.awaitDoubleBufferFlush();
    // Suspend the services first
    om.getKeyManager().getDirDeletingService().suspend();
    om.getKeyManager().getDeletingService().suspend();
    om.getKeyManager().getSnapshotDeletingService().suspend();
    String volume = "vol" + RandomStringUtils.secure().nextNumeric(3),
        bucket = "bucket" + RandomStringUtils.secure().nextNumeric(3);
    client.getObjectStore().createVolume(volume);
    OzoneVolume ozoneVolume = client.getObjectStore().getVolume(volume);
    ozoneVolume.createBucket(bucket);
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucket);

    // Create snap0
    client.getObjectStore().createSnapshot(volume, bucket, "snap0");
    client.getObjectStore().getSnapshotInfo(volume, bucket, "snap0");
    UUID snap1Id = client.getObjectStore().getSnapshotInfo(volume, bucket, "snap0").getSnapshotId();

    // Create snap1
    TestDataUtil.createKey(ozoneBucket, "key", CONTENT.array());
    client.getObjectStore().createSnapshot(volume, bucket, "snap1");
    UUID snap2Id = client.getObjectStore().getSnapshotInfo(volume, bucket, "snap1").getSnapshotId();

    ozoneBucket.renameKey("key", "renamedKey");
    ozoneBucket.deleteKey("renamedKey");
    om.awaitDoubleBufferFlush();
    UUID snap3Id;
    ReclaimableKeyFilter keyFilter;
    SnapshotInfo snapInfo;
    UncheckedAutoCloseableSupplier<OmSnapshot> rcSnap2 = null;
    // Create snap3 to test snapshot 3 deep cleaning otherwise just run on AOS.
    if (kdsRunningOnAOS) {
      snap3Id = null;
      snapInfo = null;
      keyFilter = new ReclaimableKeyFilter(om, om.getOmSnapshotManager(),
          ((OmMetadataManagerImpl)om.getMetadataManager()).getSnapshotChainManager(),
          snapInfo, om.getKeyManager(), om.getMetadataManager().getLock());
    } else {

      client.getObjectStore().createSnapshot(volume, bucket, "snap2");
      snap3Id = client.getObjectStore().getSnapshotInfo(volume, bucket, "snap2").getSnapshotId();
      om.awaitDoubleBufferFlush();
      SnapshotInfo snap = om.getMetadataManager().getSnapshotInfo(volume, bucket, "snap2");
      snap.setDeepCleanedDeletedDir(true);
      om.getMetadataManager().getSnapshotInfoTable().put(snap.getTableKey(), snap);
      assertTrue(om.getMetadataManager().getSnapshotInfo(volume, bucket, "snap2")
          .isDeepCleanedDeletedDir());
      snapInfo = SnapshotUtils.getSnapshotInfo(om, volume, bucket, "snap2");
      rcSnap2 = getOmSnapshot(volume, bucket, "snap2");
      keyFilter = new ReclaimableKeyFilter(om, om.getOmSnapshotManager(),
          ((OmMetadataManagerImpl)om.getMetadataManager()).getSnapshotChainManager(),
          snapInfo, rcSnap2.get().getKeyManager(),
          om.getMetadataManager().getLock());
    }


    MultiSnapshotLocks sdsMultiLocks = new MultiSnapshotLocks(cluster.getOzoneManager().getMetadataManager().getLock(),
        SNAPSHOT_GC_LOCK, true);
    AtomicBoolean kdsWaitStarted = new AtomicBoolean(false);
    AtomicBoolean kdsFinished = new AtomicBoolean(false);
    AtomicBoolean sdsLockWaitStarted = new AtomicBoolean(false);
    AtomicBoolean sdsLockAcquired = new AtomicBoolean(false);

    try (MockedConstruction<MultiSnapshotLocks> mockedMultiSnapshotLock = mockConstruction(MultiSnapshotLocks.class,
        (mocked, context) -> {
          when(mocked.acquireLock(anyList())).thenAnswer(i -> {
            List<UUID> ids = i.getArgument(0);
            List<UUID> expectedIds = Arrays.asList(snap1Id, snap2Id, snap3Id).subList(snasphotDeleteIndex, Math.min(3,
                snasphotDeleteIndex + 2)).stream().filter(Objects::nonNull).collect(Collectors.toList());
            if (expectedIds.equals(ids) && !sdsLockWaitStarted.get() && !sdsLockAcquired.get()) {
              sdsLockWaitStarted.set(true);
              OMLockDetails lockDetails = sdsMultiLocks.acquireLock(ids);
              assertTrue(kdsFinished::get);
              sdsLockAcquired.set(true);
              return lockDetails;
            }
            return sdsMultiLocks.acquireLock(ids);
          });
          doAnswer(i -> {
            sdsMultiLocks.releaseLock();
            return null;
          }).when(mocked).releaseLock();
        })) {
      KeyDeletingService kds = new KeyDeletingService(om, om.getScmClient().getBlockClient(), 500, 10000,
          om.getConfiguration(), 1, true);
      kds.shutdown();
      KeyDeletingService.KeyDeletingTask task = kds.new KeyDeletingTask(snap3Id);

      CompletableFuture.supplyAsync(() -> {
        try (MockedConstruction<ReclaimableKeyFilter> mockedReclaimableFilter = getMockedReclaimableKeyFilter(
            volume, bucket, kdsWaitStarted, sdsLockWaitStarted, sdsLockAcquired, kdsFinished, keyFilter)) {
          return task.call();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });

      SnapshotDeletingService sds = new SnapshotDeletingService(500, 10000, om);
      sds.shutdown();
      GenericTestUtils.waitFor(kdsWaitStarted::get, 1000, 30000);
      client.getObjectStore().deleteSnapshot(volume, bucket, "snap" + snasphotDeleteIndex);
      CompletableFuture<Void> sdsFuture = new CompletableFuture<>();
      CompletableFuture.runAsync(() -> {
        try {
          sds.runPeriodicalTaskNow();
          sdsFuture.complete(null);
        } catch (Exception e) {
          sdsFuture.completeExceptionally(e);
        }
      });
      sdsFuture.get();
      om.awaitDoubleBufferFlush();
      if (snasphotDeleteIndex == 2) {
        CompletableFuture.runAsync(() -> {
          try {
            sds.runPeriodicalTaskNow();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }).get();
      }
      assertTrue(sdsLockWaitStarted.get());
      assertTrue(sdsLockAcquired.get());
      assertThrows(IOException.class, () -> SnapshotUtils.getSnapshotInfo(om, volume, bucket,
          "snap" + snasphotDeleteIndex));
    } finally {
      if (rcSnap2 != null) {
        rcSnap2.close();
      }
    }
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
