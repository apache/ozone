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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_DEFRAG_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DEFRAG_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.MULTIPART_INFO_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.VOLUME_TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.ManagedRawSSTFileReader;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.Table.KeyValueIterator;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.DataTestUtil;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * HDDS-13217: verify snapshot checkpoint DB content is preserved across defrag iterations.
 *
 * <p>After defrag compacts a snapshot checkpoint (S' = compact(S)), the on-disk metadata for that
 * bucket prefix in the defragged checkpoint must still match the original checkpoint taken at
 * snapshot creation time (version 0).
 *
 * <p>Tables compared per bucket: volume, bucket, key (object store layout), openKey, multipart.
 * Snapshot-info and deleted/renamed tables are omitted because they differ by design between live
 * OM and checkpoint or after purge.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOmSnapshotCheckpointDbContent {

  private static final byte[] TEST_KEY_CONTENT = new byte[] {0x61, 0x62, 0x63};
  private static final int DEFRAG_WAIT_MS = 120_000;

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static OzoneClient client;
  private static ObjectStore store;

  @BeforeAll
  void initCluster() throws Exception {
    assumeTrue(ManagedRawSSTFileReader.tryLoadLibrary(),
        "Snapshot defrag requires rocks-tools native library");

    conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);
    conf.setInt(OZONE_SNAPSHOT_DEFRAG_SERVICE_INTERVAL, 7200);
    conf.setInt(SNAPSHOT_DEFRAG_LIMIT_PER_TASK, 10);
    conf.setTimeDuration(OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL, 1, TimeUnit.SECONDS);

    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    store = client.getObjectStore();
  }

  @AfterAll
  void shutdownCluster() {
    IOUtils.closeQuietly(client);
    IOUtils.closeQuietly(cluster);
  }

  /**
   * Test#1 from HDDS-13217: create S1, S2, S3 on one bucket, run defrag, and verify each
   * defragged checkpoint still matches its version-0 baseline.
   */
  @Test
  public void testDefragPreservesSnapshotCheckpointContent()
      throws Exception {
    ThreeSnapshotSetup setup = createThreeSnapshotsOnNewBucket();
    triggerDefragUntilDone(setup.snapshots);
    assertCheckpointMatchesBaseline(setup.snapshots);
  }

  /**
   * Test#2 from HDDS-13217: after an initial defrag pass, delete the middle snapshot, run defrag
   * again, and verify the remaining youngest snapshot checkpoint still matches its baseline.
   */
  @Test
  public void testDefragAfterSnapshotDeletePreservesRemainingSnapshot()
      throws Exception {
    ThreeSnapshotSetup setup = createThreeSnapshotsOnNewBucket();
    triggerDefragUntilDone(setup.snapshots);

    SnapshotInfo s2 = setup.snapshots.get(1);
    SnapshotInfo s3 = setup.snapshots.get(2);
    store.deleteSnapshot(setup.volumeName, setup.bucketName, s2.getName());
    waitForSnapshotPurged(s2);

    triggerDefragUntilDone(Arrays.asList(s3));
    assertCheckpointMatchesBaseline(Arrays.asList(s3));
  }

  private ThreeSnapshotSetup createThreeSnapshotsOnNewBucket()
      throws IOException, InterruptedException, TimeoutException {
    OzoneBucket bucket =
        DataTestUtil.createVolumeAndBucket(client, BucketLayout.OBJECT_STORE);
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();

    DataTestUtil.createKey(bucket, "key-s1", TEST_KEY_CONTENT);
    store.createSnapshot(volumeName, bucketName, "snap-s1");

    DataTestUtil.createKey(bucket, "key-s2", TEST_KEY_CONTENT);
    store.createSnapshot(volumeName, bucketName, "snap-s2");

    DataTestUtil.createKey(bucket, "key-s3", TEST_KEY_CONTENT);
    store.createSnapshot(volumeName, bucketName, "snap-s3");

    List<SnapshotInfo> snapshots = Arrays.asList(
        loadSnapshotInfo(volumeName, bucketName, "snap-s1"),
        loadSnapshotInfo(volumeName, bucketName, "snap-s2"),
        loadSnapshotInfo(volumeName, bucketName, "snap-s3"));

    for (SnapshotInfo snapshotInfo : snapshots) {
      waitForCheckpointReady(snapshotInfo);
    }
    return new ThreeSnapshotSetup(volumeName, bucketName, snapshots);
  }

  private SnapshotInfo loadSnapshotInfo(String volumeName, String bucketName,
      String snapshotName) throws IOException {
    OzoneManager om = cluster.getOzoneManager();
    SnapshotInfo snapshotInfo = om.getMetadataManager().getSnapshotInfoTable().get(
        SnapshotInfo.getTableKey(volumeName, bucketName, snapshotName));
    assertNotNull(snapshotInfo, "Snapshot row should exist for " + snapshotName);
    assertEquals(snapshotName, snapshotInfo.getName());
    return snapshotInfo;
  }

  private void waitForCheckpointReady(SnapshotInfo snapshotInfo)
      throws TimeoutException, InterruptedException {
    String currentPath = OmSnapshotManager.getSnapshotPath(conf, snapshotInfo, 0)
        + OM_KEY_PREFIX + "CURRENT";
    GenericTestUtils.waitFor(() -> new File(currentPath).exists(), 1000, DEFRAG_WAIT_MS);
  }

  private void waitForSnapshotPurged(SnapshotInfo snapshotInfo)
      throws TimeoutException, InterruptedException {
    OzoneManager om = cluster.getOzoneManager();
    GenericTestUtils.waitFor(() -> {
      try {
        return om.getMetadataManager().getSnapshotInfoTable()
            .get(snapshotInfo.getTableKey()) == null;
      } catch (IOException e) {
        return false;
      }
    }, 1000, DEFRAG_WAIT_MS);
  }

  private void triggerDefragUntilDone(List<SnapshotInfo> snapshots)
      throws TimeoutException, InterruptedException {
    OzoneManager om = cluster.getOzoneManager();
    GenericTestUtils.waitFor(() -> {
      try {
        if (!om.triggerSnapshotDefrag(false)) {
          return false;
        }
      } catch (IOException e) {
        return false;
      }
      return snapshots.stream().allMatch(this::isSnapshotDefragComplete);
    }, 1000, DEFRAG_WAIT_MS);
  }

  private boolean isSnapshotDefragComplete(SnapshotInfo snapshotInfo) {
    try {
      OmSnapshotLocalDataManager localDataManager =
          cluster.getOzoneManager().getOmSnapshotManager().getSnapshotLocalDataManager();
      try (OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataProvider provider =
               localDataManager.getOmSnapshotLocalData(snapshotInfo)) {
        return provider.getVersion() > 0 && !provider.needsDefrag();
      }
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * For each snapshot, compare bucket-scoped tables in the current checkpoint against version 0.
   */
  private void assertCheckpointMatchesBaseline(List<SnapshotInfo> snapshots)
      throws IOException {
    OzoneManager om = cluster.getOzoneManager();
    OMMetadataManager liveMm = om.getMetadataManager();

    for (SnapshotInfo snapshotInfo : snapshots) {
      int currentVersion = readSnapshotVersion(snapshotInfo);
      TablePrefixInfo prefixes = liveMm.getTableBucketPrefix(
          snapshotInfo.getVolumeName(), snapshotInfo.getBucketName());

      try (OmMetadataManagerImpl baselineMm = openCheckpoint(snapshotInfo, 0);
           OmMetadataManagerImpl currentMm = openCheckpoint(snapshotInfo, currentVersion)) {
        assertBucketPrefixTablesMatch(baselineMm, currentMm, prefixes,
            BucketLayout.OBJECT_STORE);
      }
    }
  }

  private int readSnapshotVersion(SnapshotInfo snapshotInfo) throws IOException {
    OmSnapshotLocalDataManager localDataManager =
        cluster.getOzoneManager().getOmSnapshotManager().getSnapshotLocalDataManager();
    try (OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataProvider provider =
             localDataManager.getOmSnapshotLocalData(snapshotInfo)) {
      return (int) provider.getVersion();
    }
  }

  private OmMetadataManagerImpl openCheckpoint(SnapshotInfo snapshotInfo, int version)
      throws IOException {
    RocksDBCheckpoint checkpoint = new RocksDBCheckpoint(
        Paths.get(OmSnapshotManager.getSnapshotPath(conf, snapshotInfo, version)));
    return OmMetadataManagerImpl.createCheckpointMetadataManager(conf, checkpoint);
  }

  private void assertBucketPrefixTablesMatch(
      OMMetadataManager baseline,
      OMMetadataManager defragged,
      TablePrefixInfo prefixes,
      BucketLayout layout) throws IOException {

    assertPrefixEquals(VOLUME_TABLE, baseline.getVolumeTable(),
        defragged.getVolumeTable(), prefixes);
    assertPrefixEquals(BUCKET_TABLE, baseline.getBucketTable(),
        defragged.getBucketTable(), prefixes);
    assertPrefixEquals(KEY_TABLE,
        baseline.getKeyTable(layout),
        defragged.getKeyTable(layout), prefixes);
    assertPrefixEquals(OPEN_KEY_TABLE,
        baseline.getOpenKeyTable(layout),
        defragged.getOpenKeyTable(layout), prefixes);
    assertPrefixEquals(MULTIPART_INFO_TABLE, baseline.getMultipartInfoTable(),
        defragged.getMultipartInfoTable(), prefixes);
  }

  private static <V> void assertPrefixEquals(
      String tableName,
      Table<String, V> baseline,
      Table<String, V> defragged,
      TablePrefixInfo prefixes) throws IOException {
    String prefix = prefixes.getTablePrefix(tableName);
    assertTrue(prefix != null && !prefix.isEmpty(),
        "Expected non-empty prefix for " + tableName);
    assertEquals(readPrefix(baseline, prefix), readPrefix(defragged, prefix),
        tableName);
  }

  private static <V> SortedMap<String, V> readPrefix(
      Table<String, V> table, String prefix) throws IOException {
    SortedMap<String, V> map = new TreeMap<>();
    try (KeyValueIterator<String, V> it = table.iterator(prefix)) {
      while (it.hasNext()) {
        KeyValue<String, V> kv = it.next();
        if (!kv.getKey().startsWith(prefix)) {
          break;
        }
        map.put(kv.getKey(), kv.getValue());
      }
    }
    return map;
  }

  private static final class ThreeSnapshotSetup {
    private final String volumeName;
    private final String bucketName;
    private final List<SnapshotInfo> snapshots;

    private ThreeSnapshotSetup(String volumeName, String bucketName,
        List<SnapshotInfo> snapshots) {
      this.volumeName = volumeName;
      this.bucketName = bucketName;
      this.snapshots = new ArrayList<>(snapshots);
    }
  }
}
