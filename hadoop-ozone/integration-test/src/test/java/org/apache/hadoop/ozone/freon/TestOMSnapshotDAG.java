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

import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
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
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_VOLUME_NAME_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;

/**
 * Tests Freon, with MiniOzoneCluster.
 */
public class TestOMSnapshotDAG {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestOMSnapshotDAG.class);

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static ObjectStore store;
  private final File metaDir = OMStorage.getOmDbDir(conf);

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
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

    // Set DB CF write buffer to a much lower value so that flush and compaction
    // happens much more frequently without having to create a lot of keys.
    conf.set(OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_CF_WRITE_BUFFER_SIZE,
        "256KB");

    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();

    store = cluster.getClient().getObjectStore();

    GenericTestUtils.setLogLevel(RaftLog.LOG, Level.INFO);
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.INFO);
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private String getDBCheckpointAbsolutePath(SnapshotInfo snapshotInfo) {
    return metaDir + OM_KEY_PREFIX +
        OM_SNAPSHOT_DIR + OM_KEY_PREFIX +
        OM_DB_NAME + snapshotInfo.getCheckpointDirName();
  }

  private static String getSnapshotDBKey(String volumeName, String bucketName,
      String snapshotName) {

    final String dbKeyPrefix = OM_KEY_PREFIX + volumeName +
        OM_KEY_PREFIX + bucketName;
    return dbKeyPrefix + OM_KEY_PREFIX + snapshotName;
  }

  private DifferSnapshotInfo getDifferSnapshotInfo(
      OMMetadataManager omMetadataManager, String volumeName, String bucketName,
      String snapshotName) throws IOException {

    final String dbKey = getSnapshotDBKey(volumeName, bucketName, snapshotName);
    final SnapshotInfo snapshotInfo =
        omMetadataManager.getSnapshotInfoTable().get(dbKey);
    String checkpointPath = getDBCheckpointAbsolutePath(snapshotInfo);

    // Use RocksDB transaction sequence number in SnapshotInfo, which is
    // persisted at the time of snapshot creation, as the snapshot generation
    return new DifferSnapshotInfo(checkpointPath, snapshotInfo.getSnapshotID(),
        snapshotInfo.getDbTxSequenceNumber());
  }

  @Test
  void testZeroSizeKey()
      throws IOException, InterruptedException, TimeoutException {

    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster.getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "1",
        "--num-of-buckets", "1",
        "--num-of-keys", "500",
        "--num-of-threads", "1",
        "--key-size", "0",
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
        cluster.getOzoneManager().listBuckets(volumeName, "", "", 10);
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

    DifferSnapshotInfo snap1 = getDifferSnapshotInfo(omMetadataManager,
        volumeName, bucketName, "snap1");
    DifferSnapshotInfo snap2 = getDifferSnapshotInfo(omMetadataManager,
        volumeName, bucketName, "snap2");

    // RocksDB does checkpointing in a separate thread, wait for it
    final File checkpointSnap1 = new File(snap1.getDbPath());
    GenericTestUtils.waitFor(checkpointSnap1::exists, 2000, 20000);
    final File checkpointSnap2 = new File(snap2.getDbPath());
    GenericTestUtils.waitFor(checkpointSnap2::exists, 2000, 20000);

    List<String> actualDiffList21 = differ.getSSTDiffList(snap2, snap1);
    LOG.debug("Got diff list: {}", actualDiffList21);

    // Delete 1000 keys, take a 3rd snapshot, and do another diff
    for (int i = 0; i < 1000; i++) {
      bucket.deleteKey("b_" + i);
    }

    resp = store.createSnapshot(volumeName, bucketName, "snap3");
    LOG.debug("Snapshot created: {}", resp);

    DifferSnapshotInfo snap3 = getDifferSnapshotInfo(omMetadataManager,
        volumeName, bucketName, "snap3");
    final File checkpointSnap3 = new File(snap3.getDbPath());
    GenericTestUtils.waitFor(checkpointSnap3::exists, 2000, 20000);

    List<String> actualDiffList32 = differ.getSSTDiffList(snap3, snap2);

    // snap3-snap1 diff result is a combination of snap3-snap2 and snap2-snap1
    List<String> actualDiffList31 = differ.getSSTDiffList(snap3, snap1);

    // Restart OM, do the same diffs again. See if DAG reconstruction works
    cluster.restartOzoneManager();

    List<String> actualDiffList21Run2 = differ.getSSTDiffList(snap2, snap1);
    Assertions.assertEquals(actualDiffList21, actualDiffList21Run2);

    List<String> actualDiffList32Run2 = differ.getSSTDiffList(snap3, snap2);
    Assertions.assertEquals(actualDiffList32, actualDiffList32Run2);

    List<String> actualDiffList31Run2 = differ.getSSTDiffList(snap3, snap1);
    Assertions.assertEquals(actualDiffList31, actualDiffList31Run2);
  }

}