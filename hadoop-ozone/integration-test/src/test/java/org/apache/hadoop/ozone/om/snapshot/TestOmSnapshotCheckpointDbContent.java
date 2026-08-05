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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
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
import org.apache.hadoop.ozone.DataTestUtil;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * HDDS-13217: verify snapshot checkpoint DB content is preserved across defrag iterations.
 *
 * <p>After defrag compacts a snapshot checkpoint (S' = compact(S)), the on-disk metadata for that
 * bucket prefix in the defragged checkpoint must still match the original checkpoint taken at
 * snapshot creation time (version 0).
 *
 * <p>Version-0 checkpoint directories are removed after defrag, so baselines are captured before
 * the first defrag iteration and compared against the active snapshot after defrag completes.
 */
public class TestOmSnapshotCheckpointDbContent {

  private static final byte[] TEST_KEY_CONTENT = new byte[] {0x61, 0x62, 0x63};
  private static final int CHECKPOINT_WAIT_MS = 120_000;
  private static final int DEFRAG_WAIT_MS = 600_000;

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private OzoneClient client;
  private ObjectStore store;

  @BeforeEach
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

  @AfterEach
  void shutdownCluster() {
    IOUtils.closeQuietly(client, cluster);
  }

  /**
   * HDDS-13217 scenarios:
   * <ol>
   *   <li>Create S1, S2, S3 on one bucket, run defrag, and verify each defragged checkpoint still
   *       matches its version-0 baseline.</li>
   *   <li>Delete the middle snapshot, run defrag again, and verify the remaining youngest snapshot
   *       checkpoint still matches its baseline.</li>
   * </ol>
   */
  @Test
  public void testSnapshotCheckpointContentPreservedAcrossDefragIterations()
      throws Exception {
    ThreeSnapshotSetup setup = createThreeSnapshotsOnNewBucket();
    triggerDefragUntilDone(setup.snapshots);
    assertCheckpointMatchesBaseline(setup.baselines, setup.snapshots);

    SnapshotInfo s2 = setup.snapshots.get(1);
    SnapshotInfo s3 = setup.snapshots.get(2);
    int s3VersionAfterFirstDefrag = readSnapshotVersion(s3);

    store.deleteSnapshot(setup.volumeName, setup.bucketName, s2.getName());
    waitForSnapshotPurged(s2);
    // Reload S3 so pathPreviousSnapshotId reflects the purged chain, not deleted S2.
    s3 = loadSnapshotInfo(setup.volumeName, setup.bucketName, s3.getName());

    triggerDefragUntilVersionIncreases(s3, s3VersionAfterFirstDefrag);

    assertCheckpointMatchesBaseline(
        Collections.singletonMap(s3.getSnapshotId(),
            setup.baselines.get(s3.getSnapshotId())),
        Arrays.asList(s3));
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

    OMMetadataManager liveMm = cluster.getOzoneManager().getMetadataManager();
    TablePrefixInfo prefixes = liveMm.getTableBucketPrefix(volumeName, bucketName);
    Map<UUID, SnapshotBaseline> baselines = new HashMap<>();
    for (SnapshotInfo snapshotInfo : snapshots) {
      baselines.put(snapshotInfo.getSnapshotId(), captureBaseline(snapshotInfo, prefixes));
    }
    return new ThreeSnapshotSetup(volumeName, bucketName, snapshots, baselines);
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
    GenericTestUtils.waitFor(() -> new File(currentPath).exists(), 1000, CHECKPOINT_WAIT_MS);
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
    }, 1000, CHECKPOINT_WAIT_MS);
  }

  /**
   * Wait for a follow-up defrag pass after snapshot-chain rewiring (e.g. middle snapshot purge).
   * Unlike {@link #triggerDefragUntilDone}, this requires the snapshot version to increase so we
   * do not treat an already-defragged checkpoint from an earlier pass as complete.
   */
  private void triggerDefragUntilVersionIncreases(SnapshotInfo snapshotInfo,
      int baselineVersion) throws TimeoutException, InterruptedException {
    OzoneManager om = cluster.getOzoneManager();
    GenericTestUtils.waitFor(() -> {
      try {
        if (readSnapshotVersion(snapshotInfo) > baselineVersion
            && isSnapshotDefragComplete(snapshotInfo)) {
          return true;
        }
        om.triggerSnapshotDefrag(false);
        return readSnapshotVersion(snapshotInfo) > baselineVersion
            && isSnapshotDefragComplete(snapshotInfo);
      } catch (IOException e) {
        return false;
      }
    }, 2000, DEFRAG_WAIT_MS);
  }

  private void triggerDefragUntilDone(List<SnapshotInfo> snapshots)
      throws TimeoutException, InterruptedException {
    OzoneManager om = cluster.getOzoneManager();
    for (SnapshotInfo snapshotInfo : snapshots) {
      GenericTestUtils.waitFor(() -> {
        if (isSnapshotDefragComplete(snapshotInfo)) {
          return true;
        }
        try {
          om.triggerSnapshotDefrag(false);
        } catch (IOException e) {
          return false;
        }
        return isSnapshotDefragComplete(snapshotInfo);
      }, 2000, DEFRAG_WAIT_MS);
    }
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

  private int readSnapshotVersion(SnapshotInfo snapshotInfo) throws IOException {
    OmSnapshotLocalDataManager localDataManager =
        cluster.getOzoneManager().getOmSnapshotManager().getSnapshotLocalDataManager();
    try (OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataProvider provider =
             localDataManager.getOmSnapshotLocalData(snapshotInfo)) {
      return (int) provider.getVersion();
    }
  }

  private SnapshotBaseline captureBaseline(SnapshotInfo snapshotInfo,
      TablePrefixInfo prefixes) throws IOException {
    try (OmMetadataManagerImpl checkpointMm = openCheckpoint(snapshotInfo, 0)) {
      return new SnapshotBaseline(
          readAllBucketPrefixTables(checkpointMm, prefixes, BucketLayout.OBJECT_STORE));
    }
  }

  private void assertCheckpointMatchesBaseline(
      Map<UUID, SnapshotBaseline> baselines, List<SnapshotInfo> snapshots)
      throws IOException {
    OzoneManager om = cluster.getOzoneManager();
    for (SnapshotInfo snapshotInfo : snapshots) {
      SnapshotBaseline baseline = baselines.get(snapshotInfo.getSnapshotId());
      assertNotNull(baseline, "Missing baseline for " + snapshotInfo.getName());
      TablePrefixInfo prefixes = om.getMetadataManager().getTableBucketPrefix(
          snapshotInfo.getVolumeName(), snapshotInfo.getBucketName());
      try (UncheckedAutoCloseableSupplier<OmSnapshot> activeSnapshot =
               om.getOmSnapshotManager().getActiveSnapshot(
                   snapshotInfo.getVolumeName(),
                   snapshotInfo.getBucketName(),
                   snapshotInfo.getName())) {
        OMMetadataManager currentMm = activeSnapshot.get().getMetadataManager();
        assertBucketPrefixTablesMatch(baseline.getTableData(), currentMm, prefixes,
            BucketLayout.OBJECT_STORE);
      }
    }
  }

  private OmMetadataManagerImpl openCheckpoint(SnapshotInfo snapshotInfo, int version)
      throws IOException {
    RocksDBCheckpoint checkpoint = new RocksDBCheckpoint(
        Paths.get(OmSnapshotManager.getSnapshotPath(conf, snapshotInfo, version)));
    return OmMetadataManagerImpl.createCheckpointMetadataManager(conf, checkpoint);
  }

  /**
   * Collects the snapshot OM metadata for the bucket-relevant tables and returns
   * a map from table name to sorted key-value entries scoped to the bucket prefix.
   */
  private static Map<String, SortedMap<String, ?>> readAllBucketPrefixTables(
      OMMetadataManager mm,
      TablePrefixInfo prefixes,
      BucketLayout layout) throws IOException {
    Map<String, SortedMap<String, ?>> tables = new HashMap<>();
    tables.put(VOLUME_TABLE, filterTableEntriesByKeyPrefix(mm.getVolumeTable(),
        prefixes.getTablePrefix(VOLUME_TABLE)));
    tables.put(BUCKET_TABLE, filterTableEntriesByKeyPrefix(mm.getBucketTable(),
        prefixes.getTablePrefix(BUCKET_TABLE)));
    tables.put(KEY_TABLE, filterTableEntriesByKeyPrefix(mm.getKeyTable(layout),
        prefixes.getTablePrefix(KEY_TABLE)));
    tables.put(OPEN_KEY_TABLE, filterTableEntriesByKeyPrefix(mm.getOpenKeyTable(layout),
        prefixes.getTablePrefix(OPEN_KEY_TABLE)));
    tables.put(MULTIPART_INFO_TABLE, filterTableEntriesByKeyPrefix(mm.getMultipartInfoTable(),
        prefixes.getTablePrefix(MULTIPART_INFO_TABLE)));
    return tables;
  }

  private static void assertBucketPrefixTablesMatch(
      Map<String, SortedMap<String, ?>> baseline,
      OMMetadataManager current,
      TablePrefixInfo prefixes,
      BucketLayout layout) throws IOException {

    assertPrefixEquals(VOLUME_TABLE, baseline.get(VOLUME_TABLE),
        current.getVolumeTable(), prefixes);
    assertPrefixEquals(BUCKET_TABLE, baseline.get(BUCKET_TABLE),
        current.getBucketTable(), prefixes);
    assertPrefixEquals(KEY_TABLE, baseline.get(KEY_TABLE),
        current.getKeyTable(layout), prefixes);
    assertPrefixEquals(OPEN_KEY_TABLE, baseline.get(OPEN_KEY_TABLE),
        current.getOpenKeyTable(layout), prefixes);
    assertPrefixEquals(MULTIPART_INFO_TABLE, baseline.get(MULTIPART_INFO_TABLE),
        current.getMultipartInfoTable(), prefixes);
  }

  private static <V> void assertPrefixEquals(
      String tableName,
      SortedMap<String, ?> expected,
      Table<String, V> current,
      TablePrefixInfo prefixes) throws IOException {
    String prefix = prefixes.getTablePrefix(tableName);
    assertTrue(prefix != null && !prefix.isEmpty(),
        "Expected non-empty prefix for " + tableName);
    assertEquals(expected, filterTableEntriesByKeyPrefix(current, prefix), tableName);
  }

  /**
   * Filters table entries whose keys start with {@code prefix} and returns them in a sorted map.
   */
  private static <V> SortedMap<String, V> filterTableEntriesByKeyPrefix(
      Table<String, V> table, String prefix) throws IOException {
    SortedMap<String, V> map = new TreeMap<>();
    if (prefix == null || prefix.isEmpty()) {
      return map;
    }
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

  private static final class SnapshotBaseline {
    private final Map<String, SortedMap<String, ?>> tableData;

    private SnapshotBaseline(Map<String, SortedMap<String, ?>> tableData) {
      this.tableData = tableData;
    }

    private Map<String, SortedMap<String, ?>> getTableData() {
      return tableData;
    }
  }

  private static final class ThreeSnapshotSetup {
    private final String volumeName;
    private final String bucketName;
    private final List<SnapshotInfo> snapshots;
    private final Map<UUID, SnapshotBaseline> baselines;

    private ThreeSnapshotSetup(String volumeName, String bucketName,
        List<SnapshotInfo> snapshots, Map<UUID, SnapshotBaseline> baselines) {
      this.volumeName = volumeName;
      this.bucketName = bucketName;
      this.snapshots = new ArrayList<>(snapshots);
      this.baselines = baselines;
    }
  }
}
