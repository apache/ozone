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
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.service.SnapshotDeletingService;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test Snapshot Deleting Service.
 */
public class TestSnapshotDeletingService {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSnapshotDeletingService.class);
  private static boolean omRatisEnabled = true;
  private static final ByteBuffer CONTENT =
      ByteBuffer.allocate(1024 * 1024 * 16);

  private MiniOzoneCluster cluster;
  private OzoneManager om;
  private OzoneBucket bucket1;
  private OzoneClient client;
  private static final String VOLUME_NAME = "vol1";
  private static final String BUCKET_NAME_ONE = "bucket1";
  private static final String BUCKET_NAME_TWO = "bucket2";

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE,
        4, StorageUnit.MB);
    conf.setStorageSize(OZONE_SCM_CHUNK_SIZE_KEY,
        1, StorageUnit.MB);
    conf.setTimeDuration(OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL,
        500, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SNAPSHOT_DELETING_SERVICE_TIMEOUT,
        10000, TimeUnit.MILLISECONDS);
    conf.setInt(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 500);
    conf.setInt(OMConfigKeys.OZONE_PATH_DELETING_LIMIT_PER_TASK, 5);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 500,
        TimeUnit.MILLISECONDS);
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, omRatisEnabled);
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

  @AfterEach
  public void teardown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testSnapshotSplitAndMove() throws Exception {
    SnapshotDeletingService snapshotDeletingService =
        om.getKeyManager().getSnapshotDeletingService();
    Table<String, SnapshotInfo> snapshotInfoTable =
        om.getMetadataManager().getSnapshotInfoTable();

    createSnapshotDataForBucket1();

    assertTableRowCount(snapshotInfoTable, 2);
    GenericTestUtils.waitFor(() -> snapshotDeletingService
            .getSuccessfulRunCount() >= 1, 1000, 10000);

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
  @Flaky("HDDS-9288")
  public void testMultipleSnapshotKeyReclaim() throws Exception {

    Table<String, RepeatedOmKeyInfo> deletedTable =
        om.getMetadataManager().getDeletedTable();
    Table<String, SnapshotInfo> snapshotInfoTable =
        om.getMetadataManager().getSnapshotInfoTable();

    createSnapshotDataForBucket1();

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
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Flaky("HDDS-9023")
  @Test
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
        client, VOLUME_NAME, bucketArgs, BUCKET_NAME_TWO);

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
          BUCKET_NAME_TWO, parent);
      for (int j = 1; j <= 3; j++) {
        String childFile = "/childFile" + j;
        String childDir = "/childDir" + j;
        client.getProxy().createDirectory(VOLUME_NAME,
            BUCKET_NAME_TWO, parent + childDir);
        TestDataUtil.createKey(bucket2, parent + childFile,
            ReplicationFactor.THREE, ReplicationType.RATIS, CONTENT);
      }
    }

    // Total 12 dirs, 19 keys.
    assertTableRowCount(dirTable, 12);
    assertTableRowCount(keyTable, 24);
    assertTableRowCount(deletedDirTable, 0);

    // Create Snapshot1
    client.getObjectStore().createSnapshot(VOLUME_NAME, BUCKET_NAME_TWO,
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
      client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_TWO,
          "key" + i, false);
    }
    // Rename Keys 3 keys
    for (int i = 6; i <= 8; i++) {
      client.getProxy().renameKey(VOLUME_NAME, BUCKET_NAME_TWO, "key" + i,
          "renamedKey" + i);
    }

    // Rename 1 Dir
    for (int i = 1; i <= 1; i++) {
      client.getProxy().renameKey(VOLUME_NAME, BUCKET_NAME_TWO, "/parent" + i,
          "/renamedParent" + i);
    }

    // Delete 2 Dirs
    for (int i = 2; i <= 3; i++) {
      client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_TWO, "/parent" + i,
          true);
    }

    assertTableRowCount(renamedTable, 4);
    // Delete Renamed Keys
    for (int i = 6; i <= 8; i++) {
      client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_TWO,
          "renamedKey" + i, false);
    }

    // Delete Renamed Dir
    for (int i = 1; i <= 1; i++) {
      client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_TWO,
          "/renamedParent" + i, true);
    }

    assertTableRowCount(deletedTable, 11);
    assertTableRowCount(deletedDirTable, 3);
    assertTableRowCount(dirTable, 9);
    assertTableRowCount(renamedTable, 4);

    // Create Snapshot2
    client.getObjectStore().createSnapshot(VOLUME_NAME, BUCKET_NAME_TWO,
        "snap2");

    assertTableRowCount(snapshotInfoTable, 2);
    // Once snapshot is taken renamedTable, deletedTable, deletedDirTable
    // should be cleaned
    assertTableRowCount(renamedTable, 0);
    assertTableRowCount(deletedTable, 0);
    assertTableRowCount(deletedDirTable, 0);

    // Delete 3 overwritten keys
    for (int i = 11; i <= 13; i++) {
      client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_TWO,
          "key" + i, false);
    }

    // Overwrite 2 keys
    for (int i = 14; i <= 15; i++) {
      TestDataUtil.createKey(bucket2, "key" + i, ReplicationFactor.THREE,
          ReplicationType.RATIS, CONTENT);
    }

    // Delete 2 more keys
    for (int i = 9; i <= 10; i++) {
      client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_TWO,
          "key" + i, false);
    }

    assertTableRowCount(deletedTable, 7);

    // Create Snapshot3
    client.getObjectStore().createSnapshot(VOLUME_NAME, BUCKET_NAME_TWO,
        "snap3");
    assertTableRowCount(snapshotInfoTable, 3);

    assertTableRowCount(renamedTable, 0);
    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(deletedTable, 0);
    assertTableRowCount(keyTable, 11);
    SnapshotInfo deletedSnap = om.getMetadataManager()
        .getSnapshotInfoTable().get("/vol1/bucket2/snap2");

    client.getObjectStore().deleteSnapshot(VOLUME_NAME, BUCKET_NAME_TWO,
        "snap2");
    assertTableRowCount(snapshotInfoTable, 2);

    // Delete 2 overwritten keys
    for (int i = 14; i <= 15; i++) {
      client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_TWO,
          "key" + i, false);
    }
    assertTableRowCount(deletedTable, 2);
    // Once all the tables are moved, the snapshot is deleted
    assertTableRowCount(om.getMetadataManager().getSnapshotInfoTable(), 2);

    verifySnapshotChain(deletedSnap, "/vol1/bucket2/snap3");
    OmSnapshot snap3 = om.getOmSnapshotManager()
        .getSnapshot(VOLUME_NAME, BUCKET_NAME_TWO, "snap3").get();

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
    client.getObjectStore().deleteSnapshot(VOLUME_NAME, BUCKET_NAME_TWO,
        "snap3");

    // Check entries moved to active DB
    assertTableRowCount(snapshotInfoTable, 1);
    assertTableRowCount(renamedTable, 4);
    assertTableRowCount(deletedDirTable, 3);

    ReferenceCounted<OmSnapshot> rcSnap1 =
        om.getOmSnapshotManager().getSnapshot(
            VOLUME_NAME, BUCKET_NAME_TWO, "snap1");
    OmSnapshot snap1 = rcSnap1.get();
    Table<String, OmKeyInfo> snap1KeyTable =
        snap1.getMetadataManager().getFileTable();
    try (TableIterator<String, ? extends Table.KeyValue<String,
        RepeatedOmKeyInfo>> iterator = deletedTable.iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, RepeatedOmKeyInfo> next = iterator.next();
        String activeDBDeletedKey = next.getKey();
        if (activeDBDeletedKey.matches(".*/key1.*")) {
          RepeatedOmKeyInfo activeDBDeleted = next.getValue();
          OMMetadataManager metadataManager =
              cluster.getOzoneManager().getMetadataManager();
          assertEquals(activeDBDeleted.getOmKeyInfoList().size(), 1);
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
  private void createSnapshotDataForBucket1() throws Exception {
    Table<String, SnapshotInfo> snapshotInfoTable =
        om.getMetadataManager().getSnapshotInfoTable();
    Table<String, RepeatedOmKeyInfo> deletedTable =
        om.getMetadataManager().getDeletedTable();
    Table<String, OmKeyInfo> keyTable =
        om.getMetadataManager().getKeyTable(BucketLayout.DEFAULT);
    OmMetadataManagerImpl metadataManager = (OmMetadataManagerImpl)
        om.getMetadataManager();

    TestDataUtil.createKey(bucket1, "bucket1key0", ReplicationFactor.THREE,
        ReplicationType.RATIS, CONTENT);
    TestDataUtil.createKey(bucket1, "bucket1key1", ReplicationFactor.THREE,
        ReplicationType.RATIS, CONTENT);
    assertTableRowCount(keyTable, 2);

    // Create Snapshot 1.
    client.getProxy().createSnapshot(VOLUME_NAME, BUCKET_NAME_ONE,
        "bucket1snap1");
    assertTableRowCount(snapshotInfoTable, 1);

    // Overwrite bucket1key0, This is a newer version of the key which should
    // reclaimed as this is a different version of the key.
    TestDataUtil.createKey(bucket1, "bucket1key0", ReplicationFactor.THREE,
        ReplicationType.RATIS, CONTENT);
    TestDataUtil.createKey(bucket1, "bucket1key2", ReplicationFactor.THREE,
        ReplicationType.RATIS, CONTENT);

    // Key 1 cannot be reclaimed as it is still referenced by Snapshot 1.
    client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_ONE,
        "bucket1key1", false);
    // Key 2 is deleted here, which will be reclaimed here as
    // it is not being referenced by previous snapshot.
    client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_ONE,
        "bucket1key2", false);
    client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_ONE,
        "bucket1key0", false);
    assertTableRowCount(keyTable, 0);
    // bucket1key0 should also be reclaimed as it not same
    assertTableRowCount(deletedTable, 1);

    // Create Snapshot 2.
    client.getProxy().createSnapshot(VOLUME_NAME, BUCKET_NAME_ONE,
        "bucket1snap2");
    assertTableRowCount(snapshotInfoTable, 2);
    // Key 2 is removed from the active Db's
    // deletedTable when Snapshot 2 is taken.
    assertTableRowCount(deletedTable, 0);

    TestDataUtil.createKey(bucket1, "bucket1key3", ReplicationFactor.THREE,
        ReplicationType.RATIS, CONTENT);
    TestDataUtil.createKey(bucket1, "bucket1key4", ReplicationFactor.THREE,
        ReplicationType.RATIS, CONTENT);
    client.getProxy().deleteKey(VOLUME_NAME, BUCKET_NAME_ONE,
        "bucket1key4", false);
    assertTableRowCount(keyTable, 1);
    assertTableRowCount(deletedTable, 0);

    // Create Snapshot 3.
    client.getProxy().createSnapshot(VOLUME_NAME, BUCKET_NAME_ONE,
        "bucket1snap3");
    assertTableRowCount(snapshotInfoTable, 3);

    SnapshotInfo snapshotInfo = metadataManager.getSnapshotInfoTable()
        .get("/vol1/bucket1/bucket1snap2");

    // Delete Snapshot 2.
    client.getProxy().deleteSnapshot(VOLUME_NAME, BUCKET_NAME_ONE,
        "bucket1snap2");
    assertTableRowCount(snapshotInfoTable, 2);
    verifySnapshotChain(snapshotInfo, "/vol1/bucket1/bucket1snap3");
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
}
