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
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.DifferSnapshotInfo;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5).build();
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
        snapshotInfo.getDbTxSequenceNumber(),
        getTablePrefixes(omMetadataManager, volumeName, bucketName));
  }

  private Map<String, String> getTablePrefixes(
      OMMetadataManager omMetadataManager, String volumeName, String bucketName)
      throws IOException {
    HashMap<String, String> tablePrefixes = new HashMap<>();
    String volumeId = String.valueOf(omMetadataManager.getVolumeId(volumeName));
    String bucketId =
        String.valueOf(omMetadataManager.getBucketId(volumeName, bucketName));
    tablePrefixes.put(OmMetadataManagerImpl.KEY_TABLE,
        OM_KEY_PREFIX + volumeName + OM_KEY_PREFIX + bucketName);
    tablePrefixes.put(OmMetadataManagerImpl.FILE_TABLE,
        OM_KEY_PREFIX + volumeId + OM_KEY_PREFIX + bucketId);
    tablePrefixes.put(OmMetadataManagerImpl.DIRECTORY_TABLE,
        OM_KEY_PREFIX + volumeId + OM_KEY_PREFIX + bucketId);
    return tablePrefixes;
  }

  @Test
  void testZeroSizeKey()
      throws IOException, InterruptedException, TimeoutException {

    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster.getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "1",
        "--num-of-buckets", "1",
        "--num-of-keys", "600",
        "--num-of-threads", "1",
        "--key-size", "0",
        "--factor", "THREE",
        "--type", "RATIS",
        "--validate-writes"
    );

    Assertions.assertEquals(600L, randomKeyGenerator.getNumberOfKeysAdded());
    Assertions.assertEquals(600L,
        randomKeyGenerator.getSuccessfulValidationCount());

    List<OmVolumeArgs> volList = cluster.getOzoneManager()
        .listAllVolumes("", "", 10);
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

    for (int i = 0; i < 6000; i++) {
      bucket.createKey("b_" + i, 0).close();
    }

    // Create another snapshot
    resp = store.createSnapshot(volumeName, bucketName, "snap3");
    LOG.debug("Snapshot created: {}", resp);

    // Get snapshot SST diff list
    OzoneManager ozoneManager = cluster.getOzoneManager();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    RDBStore rdbStore = (RDBStore) omMetadataManager.getStore();
    RocksDBCheckpointDiffer differ = rdbStore.getRocksDBCheckpointDiffer();

    DifferSnapshotInfo snap1 = getDifferSnapshotInfo(omMetadataManager,
        volumeName, bucketName, "snap1");
    DifferSnapshotInfo snap3 = getDifferSnapshotInfo(omMetadataManager,
        volumeName, bucketName, "snap3");

    // RocksDB does checkpointing in a separate thread, wait for it
    final File checkpointSnap1 = new File(snap1.getDbPath());
    GenericTestUtils.waitFor(checkpointSnap1::exists, 2000, 20000);
    final File checkpointSnap3 = new File(snap3.getDbPath());
    GenericTestUtils.waitFor(checkpointSnap3::exists, 2000, 20000);

    List<String> actualDiffList = differ.getSSTDiffList(snap3, snap1);
    LOG.debug("Got diff list: {}", actualDiffList);
    // Hard-coded expected output.
    // The result is deterministic. Retrieved from a successful run.
    final List<String> expectedDiffList = Collections.singletonList("000059");
    Assertions.assertEquals(expectedDiffList, actualDiffList);

    // TODO: Use smaller DB write buffer size (currently it is set to 128 MB
    //  in DBProfile), or generate enough keys (in the millions) to trigger
    //  RDB compaction. Take another snapshot and do the diff again.
    //  Then restart OM, do the same diff again to see if DAG reconstruction
    //  works.
  }

}