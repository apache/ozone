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

package org.apache.hadoop.ozone.om.snapshot;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.MULTIPART_INFO_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.VOLUME_TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.Table.KeyValueIterator;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * HDDS-13217: when an OM snapshot is created, the on-disk RocksDB checkpoint for that snapshot
 * should contain the same bucket-scoped metadata as the live OM for the tables exercised here.
 *
 * <p>Flow: write some keys, call createSnapshot, wait until the checkpoint is materialized, open
 * it read-only, then for each relevant table scan the key prefix for this volume/bucket on both
 * the live metadata manager and the checkpoint and assert the maps match.
 *
 * <p>Tables compared: volume and bucket rows; key table for OBJECT_STORE and LEGACY layouts;
 * file and directory tables for FILE_SYSTEM_OPTIMIZED; openKeyTable and multipartInfoTable (these
 * stay empty in these tests but we still compare to catch stray open writes).
 *
 * <p>Omitted on purpose: snapshotInfoTable, because the new snapshot row is part of the same DB
 * batch as the checkpoint step, so it appears on the live OM after the batch commits but not
 * inside the checkpoint taken mid-batch. deletedTable, deletedDirTable, snapshotRenamedTable:
 * the leader deletes those prefix ranges on the active DB immediately after the checkpoint, so
 * live and checkpoint intentionally differ. We also do not scan cluster-wide tables (tokens,
 * secrets, compaction log, other tenants/volumes).
 *
 * <p>Later tests worth adding: a second snapshot on the same bucket; writes after the snapshot to
 * prove the checkpoint slice does not change; multipart uploads with real state; linked buckets
 * resolving to a source bucket.
 *
 * <p>Executed as a nested class under {@link org.apache.ozone.test.TestOzoneIntegrationNonHA} so
 * one {@link MiniOzoneCluster} is shared with other safe tests (HDDS-12183). Tests only add
 * volumes, buckets, keys, and snapshots; they do not stop or restart the cluster.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestOmSnapshotCheckpointDbContent implements NonHATests.TestCase {

  private static final byte[] TEST_KEY_CONTENT = new byte[] {0x61, 0x62, 0x63};

  private OzoneConfiguration conf;
  private OzoneClient client;
  private ObjectStore store;

  @BeforeAll
  void init() throws IOException {
    conf = cluster().getConf();
    client = cluster().newClient();
    store = client.getObjectStore();
  }

  @AfterAll
  void cleanup() {
    IOUtils.closeQuietly(client);
  }

  /**
   * Object store layout: mixed key shapes (flat and path-like), then snapshot and compare tables
   * for this bucket.
   */
  @Test
  public void testSnapshotCheckpointMatchesLiveObsBucket()
      throws IOException, InterruptedException, TimeoutException {
    runLayoutScenario(BucketLayout.OBJECT_STORE,
        Arrays.asList("k1", "prefix/k2", "prefix/k3"));
  }

  /**
   * File-system-optimized layout: data lives in file/dir tables; includes keys under a short path and
   * one at the bucket root.
   */
  @Test
  public void testSnapshotCheckpointMatchesLiveFsoBucket()
      throws IOException, InterruptedException, TimeoutException {
    runLayoutScenario(BucketLayout.FILE_SYSTEM_OPTIMIZED,
        Arrays.asList("d/k1", "d/k2", "k-root"));
  }

  /**
   * Legacy layout: still uses the key table only; flat key names to match default path behavior on
   * this cluster.
   */
  @Test
  public void testSnapshotCheckpointMatchesLiveLegacyBucket()
      throws IOException, InterruptedException, TimeoutException {
    runLayoutScenario(BucketLayout.LEGACY,
        Arrays.asList("key-a", "key-b", "key-c"));
  }

  /** Create bucket, write keys, snapshot, wait for checkpoint files, open checkpoint DB, compare. */
  private void runLayoutScenario(BucketLayout layout, List<String> keyNames)
      throws IOException, InterruptedException, TimeoutException {
    OzoneBucket bucket =
        TestDataUtil.createVolumeAndBucket(client, layout);
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();

    for (String keyName : keyNames) {
      TestDataUtil.createKey(bucket, keyName, TEST_KEY_CONTENT);
    }

    String snapshotName = "snap-a";
    store.createSnapshot(volumeName, bucketName, snapshotName);

    OzoneManager om = cluster().getOzoneManager();
    OMMetadataManager liveMm = om.getMetadataManager();
    SnapshotInfo snapshotInfo = liveMm.getSnapshotInfoTable().get(
        SnapshotInfo.getTableKey(volumeName, bucketName, snapshotName));
    assertNotNull(snapshotInfo, "Snapshot row should exist after createSnapshot");
    assertEquals(snapshotName, snapshotInfo.getName());

    String currentPath =
        OmSnapshotManager.getSnapshotPath(conf, snapshotInfo, 0)
            + OM_KEY_PREFIX + "CURRENT";
    GenericTestUtils.waitFor(() -> new File(currentPath).exists(), 1000, 120_000);

    RocksDBCheckpoint checkpoint = new RocksDBCheckpoint(
        Paths.get(OmSnapshotManager.getSnapshotPath(conf, snapshotInfo, 0)));
    try (OmMetadataManagerImpl checkpointMm =
             OmMetadataManagerImpl.createCheckpointMetadataManager(conf, checkpoint)) {
      TablePrefixInfo prefixes = liveMm.getTableBucketPrefix(volumeName, bucketName);
      assertBucketPrefixTablesMatch(liveMm, checkpointMm, prefixes, layout);
    }
  }

  /** Load every row under the bucket prefix from live vs checkpoint for the tables this test cares about. */
  private void assertBucketPrefixTablesMatch(
      OMMetadataManager live,
      OMMetadataManager checkpoint,
      TablePrefixInfo prefixes,
      BucketLayout layout) throws IOException {

    assertPrefixEquals(VOLUME_TABLE, live.getVolumeTable(),
        checkpoint.getVolumeTable(), prefixes);
    assertPrefixEquals(BUCKET_TABLE, live.getBucketTable(),
        checkpoint.getBucketTable(), prefixes);

    if (layout.isFileSystemOptimized()) {
      assertPrefixEquals(FILE_TABLE, live.getFileTable(),
          checkpoint.getFileTable(), prefixes);
      assertPrefixEquals(DIRECTORY_TABLE, live.getDirectoryTable(),
          checkpoint.getDirectoryTable(), prefixes);
    } else {
      assertPrefixEquals(KEY_TABLE,
          live.getKeyTable(layout),
          checkpoint.getKeyTable(layout), prefixes);
    }

    assertPrefixEquals(OPEN_KEY_TABLE,
        live.getOpenKeyTable(layout),
        checkpoint.getOpenKeyTable(layout), prefixes);
    assertPrefixEquals(MULTIPART_INFO_TABLE, live.getMultipartInfoTable(),
        checkpoint.getMultipartInfoTable(), prefixes);
  }

  private static <V> void assertPrefixEquals(
      String tableName,
      Table<String, V> live,
      Table<String, V> checkpoint,
      TablePrefixInfo prefixes) throws IOException {
    String prefix = prefixes.getTablePrefix(tableName);
    assertEquals(readPrefix(live, prefix), readPrefix(checkpoint, prefix),
        tableName);
  }

  private static <V> SortedMap<String, V> readPrefix(
      Table<String, V> table, String prefix) throws IOException {
    SortedMap<String, V> map = new TreeMap<>();
    if (prefix == null || prefix.isEmpty()) {
      return map;
    }
    try (KeyValueIterator<String, V> it = table.iterator(prefix)) {
      while (it.hasNext()) {
        KeyValue<String, V> kv = it.next();
        map.put(kv.getKey(), kv.getValue());
      }
    }
    return map;
  }
}
