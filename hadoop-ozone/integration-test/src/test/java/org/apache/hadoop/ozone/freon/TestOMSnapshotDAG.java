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
package org.apache.hadoop.ozone.freon;

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
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
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.ozone.rocksdiff.DifferSnapshotInfo;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_VOLUME_NAME_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.DB_COMPACTION_LOG_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.DB_COMPACTION_SST_BACKUP_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIFF_DIR;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.getColumnFamilyToKeyPrefixMap;

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

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   */
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

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private String getDBCheckpointAbsolutePath(SnapshotInfo snapshotInfo) {
    return OmSnapshotManager.getSnapshotPath(conf, snapshotInfo);
  }

  private static String getSnapshotDBKey(String volumeName, String bucketName,
      String snapshotName) {

    final String dbKeyPrefix = OM_KEY_PREFIX + volumeName +
        OM_KEY_PREFIX + bucketName;
    return dbKeyPrefix + OM_KEY_PREFIX + snapshotName;
  }

  private DifferSnapshotInfo getDifferSnapshotInfo(
      OMMetadataManager omMetadataManager, String volumeName, String bucketName,
      String snapshotName, ManagedRocksDB snapshotDB) throws IOException {

    final String dbKey = getSnapshotDBKey(volumeName, bucketName, snapshotName);
    final SnapshotInfo snapshotInfo =
        omMetadataManager.getSnapshotInfoTable().get(dbKey);
    String checkpointPath = getDBCheckpointAbsolutePath(snapshotInfo);

    // Use RocksDB transaction sequence number in SnapshotInfo, which is
    // persisted at the time of snapshot creation, as the snapshot generation
    return new DifferSnapshotInfo(checkpointPath, snapshotInfo.getSnapshotId(),
        snapshotInfo.getDbTxSequenceNumber(),
        getColumnFamilyToKeyPrefixMap(omMetadataManager, volumeName,
            bucketName),
        snapshotDB);
  }

  @Test
  public void testDAGReconstruction()
      throws IOException, InterruptedException, TimeoutException {

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

    Assertions.assertEquals(500L, randomKeyGenerator.getNumberOfKeysAdded());
    Assertions.assertEquals(500L,
        randomKeyGenerator.getSuccessfulValidationCount());

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
    RDBStore rdbStore = (RDBStore) omMetadataManager.getStore();
    RocksDBCheckpointDiffer differ = rdbStore.getRocksDBCheckpointDiffer();
    ReferenceCounted<OmSnapshot> snapDB1 = ozoneManager.getOmSnapshotManager()
        .getActiveSnapshot(volumeName, bucketName, "snap1");
    ReferenceCounted<OmSnapshot> snapDB2 = ozoneManager.getOmSnapshotManager()
        .getActiveSnapshot(volumeName, bucketName, "snap2");
    DifferSnapshotInfo snap1 = getDifferSnapshotInfo(omMetadataManager,
        volumeName, bucketName, "snap1",
        ((RDBStore) snapDB1.get()
            .getMetadataManager().getStore()).getDb().getManagedRocksDb());
    DifferSnapshotInfo snap2 = getDifferSnapshotInfo(omMetadataManager,
        volumeName, bucketName, "snap2", ((RDBStore) snapDB2.get()
            .getMetadataManager().getStore()).getDb().getManagedRocksDb());

      // RocksDB does checkpointing in a separate thread, wait for it
    final File checkpointSnap1 = new File(snap1.getDbPath());
    GenericTestUtils.waitFor(checkpointSnap1::exists, 2000, 20000);
    final File checkpointSnap2 = new File(snap2.getDbPath());
    GenericTestUtils.waitFor(checkpointSnap2::exists, 2000, 20000);

    List<String> sstDiffList21 = differ.getSSTDiffList(snap2, snap1);
    LOG.debug("Got diff list: {}", sstDiffList21);

    // Delete 1000 keys, take a 3rd snapshot, and do another diff
    for (int i = 0; i < 1000; i++) {
      bucket.deleteKey("b_" + i);
    }

    resp = store.createSnapshot(volumeName, bucketName, "snap3");
    LOG.debug("Snapshot created: {}", resp);
    ReferenceCounted<OmSnapshot> snapDB3 = ozoneManager.getOmSnapshotManager()
        .getActiveSnapshot(volumeName, bucketName, "snap3");
    DifferSnapshotInfo snap3 = getDifferSnapshotInfo(omMetadataManager,
        volumeName, bucketName, "snap3",
        ((RDBStore) snapDB3.get()
            .getMetadataManager().getStore()).getDb().getManagedRocksDb());
    final File checkpointSnap3 = new File(snap3.getDbPath());
    GenericTestUtils.waitFor(checkpointSnap3::exists, 2000, 20000);

    List<String> sstDiffList32 = differ.getSSTDiffList(snap3, snap2);

    // snap3-snap1 diff result is a combination of snap3-snap2 and snap2-snap1
    List<String> sstDiffList31 = differ.getSSTDiffList(snap3, snap1);

    // Same snapshot. Result should be empty list
    List<String> sstDiffList22 = differ.getSSTDiffList(snap2, snap2);
    Assertions.assertTrue(sstDiffList22.isEmpty());
    snapDB1.close();
    snapDB2.close();
    snapDB3.close();
    // Test DAG reconstruction by restarting OM. Then do the same diffs again
    cluster.restartOzoneManager();
    ozoneManager = cluster.getOzoneManager();
    omMetadataManager = ozoneManager.getMetadataManager();
    snapDB1 = ozoneManager.getOmSnapshotManager()
        .getActiveSnapshot(volumeName, bucketName, "snap1");
    snapDB2 = ozoneManager.getOmSnapshotManager()
        .getActiveSnapshot(volumeName, bucketName, "snap2");
    snap1 = getDifferSnapshotInfo(omMetadataManager,
        volumeName, bucketName, "snap1",
        ((RDBStore) snapDB1.get()
            .getMetadataManager().getStore()).getDb().getManagedRocksDb());
    snap2 = getDifferSnapshotInfo(omMetadataManager,
        volumeName, bucketName, "snap2", ((RDBStore) snapDB2.get()
            .getMetadataManager().getStore()).getDb().getManagedRocksDb());
    snapDB3 = ozoneManager.getOmSnapshotManager()
        .getActiveSnapshot(volumeName, bucketName, "snap3");
    snap3 = getDifferSnapshotInfo(omMetadataManager,
        volumeName, bucketName, "snap3",
        ((RDBStore) snapDB3.get()
            .getMetadataManager().getStore()).getDb().getManagedRocksDb());
    List<String> sstDiffList21Run2 = differ.getSSTDiffList(snap2, snap1);
    Assertions.assertEquals(sstDiffList21, sstDiffList21Run2);

    List<String> sstDiffList32Run2 = differ.getSSTDiffList(snap3, snap2);
    Assertions.assertEquals(sstDiffList32, sstDiffList32Run2);

    List<String> sstDiffList31Run2 = differ.getSSTDiffList(snap3, snap1);
    Assertions.assertEquals(sstDiffList31, sstDiffList31Run2);
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

    Assertions.assertEquals(1000L, randomKeyGenerator.getNumberOfKeysAdded());
    Assertions.assertEquals(1000L,
        randomKeyGenerator.getSuccessfulValidationCount());

    String omMetadataDir =
        cluster.getOzoneManager().getConfiguration().get(OZONE_METADATA_DIRS);
    // Verify that no compaction log entry has been written
    Path logPath = Paths.get(omMetadataDir, OM_SNAPSHOT_DIFF_DIR,
        DB_COMPACTION_LOG_DIR);
    File[] fileList = logPath.toFile().listFiles();
    // fileList can be null when compaction log directory is not even created
    if (fileList != null) {
      for (File file : fileList) {
        if (file != null && file.isFile() && file.getName().endsWith(".log")) {
          Assertions.assertEquals(0L, file.length());
        }
      }
    }
    // Verify that no SST has been backed up
    Path sstBackupPath = Paths.get(omMetadataDir, OM_SNAPSHOT_DIFF_DIR,
        DB_COMPACTION_SST_BACKUP_DIR);
    fileList = sstBackupPath.toFile().listFiles();
    Assertions.assertNotNull(fileList);
    Assertions.assertEquals(0L, fileList.length);
  }

}
