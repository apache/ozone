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

package org.apache.hadoop.ozone.freon;

import static java.util.stream.Collectors.toMap;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_VOLUME_NAME_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.DB_COMPACTION_LOG_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.DB_COMPACTION_SST_BACKUP_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIFF_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_DEFRAG_SERVICE_INTERVAL;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.COLUMN_FAMILIES_TO_TRACK_IN_DAG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.apache.ozone.rocksdiff.DifferSnapshotInfo;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.DifferSnapshotVersion;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import picocli.CommandLine;

/**
 * Tests Freon, with MiniOzoneCluster.
 */
public class TestOMSnapshotDAG {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestOMSnapshotDAG.class);

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static ObjectStore store;
  private static OzoneClient client;

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(3));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(3));
    conf.setFromObject(ratisServerConfig);

    RatisClientConfig.RaftConfig raftClientConfig =
        conf.getObject(RatisClientConfig.RaftConfig.class);
    raftClientConfig.setRpcRequestTimeout(Duration.ofSeconds(3));
    raftClientConfig.setRpcWatchRequestTimeout(Duration.ofSeconds(3));
    conf.setFromObject(raftClientConfig);
    // Enable filesystem snapshot feature for the test regardless of the default
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);
    conf.setInt(OZONE_SNAPSHOT_DEFRAG_SERVICE_INTERVAL, -1);

    // Set DB CF write buffer to a much lower value so that flush and compaction
    // happens much more frequently without having to create a lot of keys.
    conf.set(OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_CF_WRITE_BUFFER_SIZE,
        "256KB");

    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    store = client.getObjectStore();

    GenericTestUtils.setLogLevel(RaftLog.LOG, Level.INFO);
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.INFO);
  }

  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private String getDBCheckpointAbsolutePath(SnapshotInfo snapshotInfo) {
    return OmSnapshotManager.getSnapshotPath(conf, snapshotInfo, 0);
  }

  private static String getSnapshotDBKey(String volumeName, String bucketName,
      String snapshotName) {

    final String dbKeyPrefix = OM_KEY_PREFIX + volumeName +
        OM_KEY_PREFIX + bucketName;
    return dbKeyPrefix + OM_KEY_PREFIX + snapshotName;
  }

  private DifferSnapshotVersion getDifferSnapshotInfo(
      OMMetadataManager omMetadataManager, OmSnapshotLocalDataManager localDataManager,
      String volumeName, String bucketName, String snapshotName) throws IOException {

    final String dbKey = getSnapshotDBKey(volumeName, bucketName, snapshotName);
    final SnapshotInfo snapshotInfo =
        omMetadataManager.getSnapshotInfoTable().get(dbKey);
    String checkpointPath = getDBCheckpointAbsolutePath(snapshotInfo);

    // Use RocksDB transaction sequence number in SnapshotInfo, which is
    // persisted at the time of snapshot creation, as the snapshot generation
    try (OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataProvider snapshotLocalData =
             localDataManager.getOmSnapshotLocalData(snapshotInfo)) {
      NavigableMap<Integer, List<SstFileInfo>> versionSstFiles = snapshotLocalData.getSnapshotLocalData()
          .getVersionSstFileInfos().entrySet().stream()
          .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().getSstFiles(),
              (u, v) -> {
              throw new IllegalStateException(String.format("Duplicate key %s", u));
            }, TreeMap::new));
      DifferSnapshotInfo dsi = new DifferSnapshotInfo((version) -> Paths.get(checkpointPath),
          snapshotInfo.getSnapshotId(), snapshotLocalData.getSnapshotLocalData().getDbTxSequenceNumber(),
          versionSstFiles);
      return new DifferSnapshotVersion(dsi, 0, COLUMN_FAMILIES_TO_TRACK_IN_DAG);
    }
  }

  @Test
  public void testDAGReconstruction() throws IOException {
    // Generate keys
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster.getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "1",
        "--num-of-buckets", "1",
        "--num-of-keys", "500",
        "--num-of-threads", "1",
        "--key-size", "0",  // zero-byte keys since we don't test DNs here
        "--factor", "THREE",
        "--type", "RATIS",
        "--validate-writes"
    );

    assertEquals(500L, randomKeyGenerator.getNumberOfKeysAdded());
    assertEquals(500L, randomKeyGenerator.getSuccessfulValidationCount());

    List<OmVolumeArgs> volList = cluster.getOzoneManager()
        .listAllVolumes("", "", 2);
    LOG.debug("List of all volumes: {}", volList);
    final String volumeName = volList.stream().filter(e ->
        !e.getVolume().equals(OZONE_S3_VOLUME_NAME_DEFAULT))  // Ignore s3v vol
        .collect(Collectors.toList()).get(0).getVolume();
    List<OmBucketInfo> bucketList =
        cluster.getOzoneManager().listBuckets(volumeName, "", "", 10, false);
    LOG.debug("List of all buckets under the first volume: {}", bucketList);
    final String bucketName = bucketList.get(0).getBucketName();

    // Create snapshot
    String resp = store.createSnapshot(volumeName, bucketName, "snap1");
    LOG.debug("Snapshot created: {}", resp);

    final OzoneVolume volume = store.getVolume(volumeName);
    final OzoneBucket bucket = volume.getBucket(bucketName);

    for (int i = 0; i < 2000; i++) {
      bucket.createKey("b_" + i, 0).close();
    }

    // Create another snapshot
    resp = store.createSnapshot(volumeName, bucketName, "snap2");
    LOG.debug("Snapshot created: {}", resp);

    // Get snapshot SST diff list
    OzoneManager ozoneManager = cluster.getOzoneManager();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    TablePrefixInfo bucketPrefix = omMetadataManager.getTableBucketPrefix(volumeName, bucketName);
    OmSnapshotLocalDataManager localDataManager = ozoneManager.getOmSnapshotManager().getSnapshotLocalDataManager();
    RDBStore rdbStore = (RDBStore) omMetadataManager.getStore();
    RocksDBCheckpointDiffer differ = rdbStore.getRocksDBCheckpointDiffer();
    UncheckedAutoCloseableSupplier<OmSnapshot> snapDB1 = ozoneManager.getOmSnapshotManager()
        .getActiveSnapshot(volumeName, bucketName, "snap1");
    UncheckedAutoCloseableSupplier<OmSnapshot> snapDB2 = ozoneManager.getOmSnapshotManager()
        .getActiveSnapshot(volumeName, bucketName, "snap2");
    DifferSnapshotVersion snap1 = getDifferSnapshotInfo(omMetadataManager, localDataManager,
        volumeName, bucketName, "snap1");
    DifferSnapshotVersion snap2 = getDifferSnapshotInfo(omMetadataManager, localDataManager,
        volumeName, bucketName, "snap2");

      // RocksDB does checkpointing in a separate thread, wait for it
    List<SstFileInfo> sstDiffList21 = differ.getSSTDiffList(snap2, snap1, bucketPrefix,
            COLUMN_FAMILIES_TO_TRACK_IN_DAG, true).orElse(Collections.emptyList());
    LOG.debug("Got diff list: {}", sstDiffList21);

    // Delete 1000 keys, take a 3rd snapshot, and do another diff
    for (int i = 0; i < 1000; i++) {
      bucket.deleteKey("b_" + i);
    }

    resp = store.createSnapshot(volumeName, bucketName, "snap3");
    LOG.debug("Snapshot created: {}", resp);
    UncheckedAutoCloseableSupplier<OmSnapshot> snapDB3 = ozoneManager.getOmSnapshotManager()
        .getActiveSnapshot(volumeName, bucketName, "snap3");
    DifferSnapshotVersion snap3 = getDifferSnapshotInfo(omMetadataManager, localDataManager, volumeName, bucketName,
        "snap3");

    List<SstFileInfo> sstDiffList32 = differ.getSSTDiffList(snap3, snap2, bucketPrefix,
            COLUMN_FAMILIES_TO_TRACK_IN_DAG, true).orElse(Collections.emptyList());

    // snap3-snap1 diff result is a combination of snap3-snap2 and snap2-snap1
    List<SstFileInfo> sstDiffList31 = differ.getSSTDiffList(snap3, snap1, bucketPrefix,
            COLUMN_FAMILIES_TO_TRACK_IN_DAG, true).orElse(Collections.emptyList());

    // Same snapshot. Result should be empty list
    List<SstFileInfo> sstDiffList22 = differ.getSSTDiffList(snap2, snap2, bucketPrefix,
            COLUMN_FAMILIES_TO_TRACK_IN_DAG, true).orElse(Collections.emptyList());
    assertThat(sstDiffList22).isEmpty();
    snapDB1.close();
    snapDB2.close();
    snapDB3.close();
    // Test DAG reconstruction by restarting OM. Then do the same diffs again
    cluster.restartOzoneManager();
    ozoneManager = cluster.getOzoneManager();
    omMetadataManager = ozoneManager.getMetadataManager();
    localDataManager = ozoneManager.getOmSnapshotManager().getSnapshotLocalDataManager();
    snapDB1 = ozoneManager.getOmSnapshotManager()
        .getActiveSnapshot(volumeName, bucketName, "snap1");
    snapDB2 = ozoneManager.getOmSnapshotManager()
        .getActiveSnapshot(volumeName, bucketName, "snap2");
    snap1 = getDifferSnapshotInfo(omMetadataManager, localDataManager,
        volumeName, bucketName, "snap1");
    snap2 = getDifferSnapshotInfo(omMetadataManager, localDataManager,
        volumeName, bucketName, "snap2");
    snapDB3 = ozoneManager.getOmSnapshotManager()
        .getActiveSnapshot(volumeName, bucketName, "snap3");
    snap3 = getDifferSnapshotInfo(omMetadataManager, localDataManager,
        volumeName, bucketName, "snap3");
    List<SstFileInfo> sstDiffList21Run2 = differ.getSSTDiffList(snap2, snap1, bucketPrefix,
            COLUMN_FAMILIES_TO_TRACK_IN_DAG, true).orElse(Collections.emptyList());
    assertEquals(sstDiffList21, sstDiffList21Run2);

    List<SstFileInfo> sstDiffList32Run2 = differ.getSSTDiffList(snap3, snap2, bucketPrefix,
            COLUMN_FAMILIES_TO_TRACK_IN_DAG, true).orElse(Collections.emptyList());
    assertEquals(sstDiffList32, sstDiffList32Run2);

    List<SstFileInfo> sstDiffList31Run2 = differ.getSSTDiffList(snap3, snap1, bucketPrefix,
            COLUMN_FAMILIES_TO_TRACK_IN_DAG, true).orElse(Collections.emptyList());
    assertEquals(sstDiffList31, sstDiffList31Run2);
    snapDB1.close();
    snapDB2.close();
    snapDB3.close();
  }

  @Test
  public void testSkipTrackingWithZeroSnapshot() {
    // Verify that the listener correctly skips compaction tracking
    // when there is no snapshot in SnapshotInfoTable.

    // Generate keys
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster.getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    // 1000 keys are enough to trigger compaction with 256KB DB CF write buffer
    cmd.execute("--num-of-volumes", "1",
        "--num-of-buckets", "1",
        "--num-of-keys", "1000",
        "--num-of-threads", "1",
        "--key-size", "0",  // zero-byte keys since we don't test DNs here
        "--factor", "THREE",
        "--type", "RATIS",
        "--validate-writes"
    );

    assertEquals(1000L, randomKeyGenerator.getNumberOfKeysAdded());
    assertEquals(1000L, randomKeyGenerator.getSuccessfulValidationCount());

    String omMetadataDir =
        cluster.getOzoneManager().getConfiguration().get(OMConfigKeys.OZONE_OM_DB_DIRS);
    // Verify that no compaction log entry has been written
    Path logPath = Paths.get(omMetadataDir, OM_SNAPSHOT_DIFF_DIR,
        DB_COMPACTION_LOG_DIR);
    File[] fileList = logPath.toFile().listFiles();
    // fileList can be null when compaction log directory is not even created
    if (fileList != null) {
      for (File file : fileList) {
        if (file != null && file.isFile() && file.getName().endsWith(".log")) {
          assertEquals(0L, file.length());
        }
      }
    }
    // Verify that no SST has been backed up
    Path sstBackupPath = Paths.get(omMetadataDir, OM_SNAPSHOT_DIFF_DIR,
        DB_COMPACTION_SST_BACKUP_DIR);
    fileList = sstBackupPath.toFile().listFiles();
    assertNotNull(fileList);
    assertEquals(0L, fileList.length);
  }

}
