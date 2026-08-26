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

package org.apache.hadoop.ozone.om;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.utils.IOUtils.getINode;
import static org.apache.hadoop.ozone.DataTestUtil.readFully;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPath;
import static org.apache.hadoop.ozone.om.TestOzoneManagerHAWithStoppedNodes.createKey;
import static org.apache.ozone.test.OzoneTestBase.uniqueObjectName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.ExitManager;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBCheckpointUtils;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.conf.OMClientConfig;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServerConfig;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.server.protocol.TermIndex;
import org.assertj.core.api.Fail;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.event.Level;

/**
 * Tests the Ratis snapshots feature in OM. These tests do not depend on the
 * checkpoint transfer format and run once with the default (inode-based)
 * transfer; tests exercising the transfer path under both formats live in
 * {@link TestOMRatisSnapshotTransfer}. Bootstrap install snapshot coverage lives in
 * {@link TestOMInstallSnapshotDuringBootstrapping}.
 */
public class TestOMRatisSnapshots {
  private static final String OM_SERVICE_ID = "om-service-test1";
  private static final int NUM_OF_OMS = 3;

  private MiniOzoneHAClusterImpl cluster = null;
  private ObjectStore objectStore;
  private OzoneConfiguration conf;
  private OzoneBucket ozoneBucket;
  private String volumeName;
  private String bucketName;

  private static final long SNAPSHOT_THRESHOLD = 50;
  private static final int LOG_PURGE_GAP = 50;
  // This test depends on direct RocksDB checks that are easier done with OBS
  // buckets.
  private static final BucketLayout TEST_BUCKET_LAYOUT =
      BucketLayout.OBJECT_STORE;
  private OzoneClient client;

  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setInt(OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP, LOG_PURGE_GAP);
    conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_SEGMENT_SIZE_KEY, 16,
        StorageUnit.KB);
    conf.setStorageSize(OMConfigKeys.
        OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY, 16, StorageUnit.KB);
    conf.setLong(
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        SNAPSHOT_THRESHOLD);

    OzoneManagerRatisServerConfig omRatisConf =
        conf.getObject(OzoneManagerRatisServerConfig.class);
    omRatisConf.setLogAppenderWaitTimeMin(10);
    conf.setFromObject(omRatisConf);

    OMClientConfig clientConfig = conf.getObject(OMClientConfig.class);
    clientConfig.setRpcTimeOut(TimeUnit.SECONDS.toMillis(5));
    conf.setFromObject(clientConfig);

    MiniOzoneHAClusterImpl.Builder clusterBuilder =
        MiniOzoneCluster.newHABuilder(conf);
    clusterBuilder.setOMServiceId("om-service-test1")
        .setNumOfOzoneManagers(NUM_OF_OMS)
        .setNumOfActiveOMs(2)
        .setNumDatanodes(1);
    cluster = clusterBuilder.build();
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, conf);
    objectStore = client.getObjectStore();

    volumeName = uniqueObjectName("volume");
    bucketName = uniqueObjectName("bucket");

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner("user" + RandomStringUtils.secure().nextNumeric(5))
        .setAdmin("admin" + RandomStringUtils.secure().nextNumeric(5))
        .build();

    objectStore.createVolume(volumeName, createVolumeArgs);
    OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);

    retVolumeinfo.createBucket(bucketName,
        BucketArgs.newBuilder().setBucketLayout(TEST_BUCKET_LAYOUT).build());
    ozoneBucket = retVolumeinfo.getBucket(bucketName);
  }

  @AfterEach
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  static void checkSnapshot(String volumeName, String bucketName,
      OzoneManager leaderOM, OzoneManager followerOM,
                             String snapshotName,
                             List<String> keys, SnapshotInfo snapshotInfo)
      throws IOException, RocksDBException {
    // Read back data from snapshot.
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(".snapshot/" + snapshotName + "/" +
            keys.get(keys.size() - 1)).build();
    OmKeyInfo omKeyInfo;
    omKeyInfo = followerOM.lookupKey(omKeyArgs);
    assertNotNull(omKeyInfo);
    assertEquals(omKeyInfo.getKeyName(), omKeyArgs.getKeyName());

    // Confirm followers snapshot hard links are as expected
    File followerMetaDir = OMStorage.getOmDbDir(followerOM.getConfiguration());
    Path followerActiveDir = Paths.get(followerMetaDir.toString(), OM_DB_NAME);
    Path followerSnapshotDir =
        Paths.get(getSnapshotPath(followerOM.getConfiguration(), snapshotInfo, 0));
    File leaderMetaDir = OMStorage.getOmDbDir(leaderOM.getConfiguration());
    Path leaderActiveDir = Paths.get(leaderMetaDir.toString(), OM_DB_NAME);
    Path leaderSnapshotDir =
        Paths.get(getSnapshotPath(leaderOM.getConfiguration(), snapshotInfo, 0));

    // Get list of live files on the leader.
    RocksDB activeRocksDB = ((RDBStore) leaderOM.getMetadataManager().getStore())
        .getDb().getManagedRocksDb().get();
    // strip the leading "/".
    Set<String> liveSstFiles = activeRocksDB.getLiveFiles().files.stream()
        .map(s -> s.substring(1))
        .collect(Collectors.toSet());

    // Get the list of hardlinks from the leader.  Then confirm those links
    //  are on the follower
    int hardLinkCount = 0;
    try (Stream<Path> list = Files.list(leaderSnapshotDir)) {
      for (Path leaderSnapshotSST: list.collect(Collectors.toList())) {
        Path path = leaderSnapshotSST.getFileName();
        assertNotNull(path);
        String fileName = path.toString();
        if (fileName.toLowerCase().endsWith(".sst")) {

          Path leaderActiveSST =
              Paths.get(leaderActiveDir.toString(), fileName);
          // Skip if not hard link on the leader
          // First confirm it is live
          if (!liveSstFiles.contains(fileName)) {
            continue;
          }
          // If it is a hard link on the leader, it should be a hard
          // link on the follower
          if (getINode(leaderActiveSST).equals(getINode(leaderSnapshotSST))) {
            Path followerSnapshotSST =
                Paths.get(followerSnapshotDir.toString(), fileName);
            Path followerActiveSST =
                Paths.get(followerActiveDir.toString(), fileName);
            assertEquals(
                getINode(followerActiveSST),
                getINode(followerSnapshotSST),
                "Snapshot sst file is supposed to be a hard link");
            hardLinkCount++;
          }
        }
      }
    }
    assertThat(hardLinkCount).withFailMessage("No hard links were found")
        .isGreaterThan(0);
  }

  @Test
  public void testInstallSnapshotWithClientWrite() throws Exception {
    // Get the leader OM
    final String leaderOMNodeId = OmTestUtil.getCurrentOmProxyNodeId(objectStore);

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Do some transactions so that the log index increases
    List<String> keys = writeKeysToIncreaseLogIndex(leaderRatisServer, 200);

    // Get the latest db checkpoint from the leader OM.
    TransactionInfo transactionInfo =
        TransactionInfo.readTransactionInfo(leaderOM.getMetadataManager());
    TermIndex leaderOMTermIndex =
        TermIndex.valueOf(transactionInfo.getTerm(),
            transactionInfo.getTransactionIndex());
    long leaderOMSnapshotIndex = leaderOMTermIndex.getIndex();
    long leaderOMSnapshotTermIndex = leaderOMTermIndex.getTerm();

    // Start the inactive OM. Checkpoint installation will happen spontaneously.
    cluster.startInactiveOM(followerNodeId);
    LogCapturer logCapture = LogCapturer.captureLogs(OzoneManager.class);

    // Continuously create new keys
    ExecutorService executor = Executors.newFixedThreadPool(1);
    Future<List<String>> writeFuture = executor.submit(() -> {
      return writeKeys(200);
    });
    List<String> newKeys = writeFuture.get();

    // All newKeys writes have completed (writeFuture.get() above), so the
    // leader must already contain them.
    OMMetadataManager leaderOmMetaMgr = leaderOM.getMetadataManager();
    for (String key : newKeys) {
      assertNotNull(leaderOmMetaMgr.getKeyTable(
          TEST_BUCKET_LAYOUT)
          .get(leaderOmMetaMgr.getOzoneKey(volumeName, bucketName, key)));
    }

    // The recently started OM should be lagging behind the leader OM.
    // Wait & for follower to update transactions to leader snapshot index.
    // Timeout error if follower does not load update within 3s
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
          >= leaderOMSnapshotIndex - 1;
    }, 100, 30_000);

    // Verify checkpoint installation was happened.
    String msg = "Reloaded OM state";
    assertLogCapture(logCapture, msg);
    assertLogCapture(logCapture, "Install Checkpoint is finished");

    // Wait for the follower to apply everything the leader has committed; all
    // writes have completed on the leader, so after this no further snapshot
    // install (and DB reload) can occur and the follower DB reads below are
    // safe from "Rocks Database is closed" races.
    // The applied index only advances on double buffer flush, so it can lag
    // acked writes; the commit index covers them all.
    long leaderCommitted = leaderRatisServer.getServerDivision().getRaftLog()
        .getLastCommittedIndex();
    GenericTestUtils.waitFor(() -> followerOM.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex() >= leaderCommitted, 100, 30_000);

    long followerOMLastAppliedIndex =
        followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    assertThat(followerOMLastAppliedIndex).isGreaterThanOrEqualTo(leaderOMSnapshotIndex - 1);

    // After the new checkpoint is installed, the follower OM
    // lastAppliedIndex must >= the snapshot index of the checkpoint. It
    // could be great than snapshot index if there is any conf entry from ratis.
    followerOMLastAppliedIndex = followerOM.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex();
    assertThat(followerOMLastAppliedIndex).isGreaterThanOrEqualTo(leaderOMSnapshotIndex);
    assertThat(followerOM.getOmRatisServer().getLastAppliedTermIndex()
        .getTerm()).isGreaterThanOrEqualTo(leaderOMSnapshotTermIndex);

    // Verify that the follower OM's DB contains the transactions which were
    // made while it was inactive.
    OMMetadataManager followerOMMetaMgr = followerOM.getMetadataManager();
    assertNotNull(followerOMMetaMgr.getVolumeTable().get(
        followerOMMetaMgr.getVolumeKey(volumeName)));
    assertNotNull(followerOMMetaMgr.getBucketTable().get(
        followerOMMetaMgr.getBucketKey(volumeName, bucketName)));
    for (String key : keys) {
      assertNotNull(followerOMMetaMgr.getKeyTable(
          TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMgr.getOzoneKey(volumeName, bucketName, key)));
    }
    followerOMMetaMgr = followerOM.getMetadataManager();
    for (String key : newKeys) {
      assertNotNull(followerOMMetaMgr.getKeyTable(
          TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMgr.getOzoneKey(volumeName, bucketName, key)));
    }
    // Read newly created keys
    readKeys(newKeys);
    System.out.println("All data are replicated");
  }

  @Test
  public void testInstallSnapshotWithClientRead() throws Exception {
    // Get the leader OM
    final String leaderOMNodeId = OmTestUtil.getCurrentOmProxyNodeId(objectStore);

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Do some transactions so that the log index increases
    List<String> keys = writeKeysToIncreaseLogIndex(leaderRatisServer, 200);

    // Get transaction Index
    TransactionInfo transactionInfo =
        TransactionInfo.readTransactionInfo(leaderOM.getMetadataManager());
    TermIndex leaderOMTermIndex =
        TermIndex.valueOf(transactionInfo.getTerm(),
            transactionInfo.getTransactionIndex());
    long leaderOMSnapshotIndex = leaderOMTermIndex.getIndex();
    long leaderOMSnapshotTermIndex = leaderOMTermIndex.getTerm();

    // The snapshot index above only advances on double buffer flush, so it can
    // lag the keys written above; the commit index covers them all. Read after
    // the snapshot index, so it is never behind it.
    long leaderCommitted = leaderRatisServer.getServerDivision().getRaftLog()
        .getLastCommittedIndex();

    // Start the inactive OM. Checkpoint installation will happen spontaneously.
    OzoneManager.setTestInstallSnapshot(true);
    cluster.startInactiveOM(followerNodeId);
    LogCapturer logCapture = LogCapturer.captureLogs(OzoneManager.class);
    assertLogCapture(logCapture, "OzoneManager is not in running state");
    assertLogCapture(logCapture, "Abort install snapshot from Leader");
    GenericTestUtils.waitFor(followerOM::isRunning, 100, 30_000);
    OzoneManager.setTestInstallSnapshot(false);

    // Continuously read keys
    ExecutorService executor = Executors.newFixedThreadPool(1);
    Future<Void> readFuture = executor.submit(() -> {
      try {
        getKeys(keys, 10);
        readKeys(keys);
      } catch (IOException e) {
        assertTrue(Fail.fail("Read Key failed", e));
      }
      return null;
    });
    readFuture.get();

    // The recently started OM should be lagging behind the leader OM. Wait for
    // it to catch up, which covers the snapshot index and all the keys.
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
          >= leaderCommitted;
    }, 100, 30_000);

    long followerOMLastAppliedIndex =
        followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    assertThat(followerOMLastAppliedIndex).isGreaterThanOrEqualTo(leaderOMSnapshotIndex - 1);

    // After the new checkpoint is installed, the follower OM
    // lastAppliedIndex must >= the snapshot index of the checkpoint. It
    // could be great than snapshot index if there is any conf entry from ratis.
    followerOMLastAppliedIndex = followerOM.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex();
    assertThat(followerOMLastAppliedIndex).isGreaterThanOrEqualTo(leaderOMSnapshotIndex);
    assertThat(followerOM.getOmRatisServer().getLastAppliedTermIndex()
        .getTerm()).isGreaterThanOrEqualTo(leaderOMSnapshotTermIndex);

    // Verify that the follower OM's DB contains the transactions which were
    // made while it was inactive.
    OMMetadataManager followerOMMetaMngr = followerOM.getMetadataManager();
    assertNotNull(followerOMMetaMngr.getVolumeTable().get(
        followerOMMetaMngr.getVolumeKey(volumeName)));
    assertNotNull(followerOMMetaMngr.getBucketTable().get(
        followerOMMetaMngr.getBucketKey(volumeName, bucketName)));
    for (String key : keys) {
      assertNotNull(followerOMMetaMngr.getKeyTable(
          TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }

    // Verify checkpoint installation was happened.
    assertLogCapture(logCapture, "Reloaded OM state");
    assertLogCapture(logCapture, "Install Checkpoint is finished");
  }

  @Test
  public void testInstallOldCheckpointFailure() throws Exception {
    // Get the leader OM
    final String leaderOMNodeId = OmTestUtil.getCurrentOmProxyNodeId(objectStore);

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);

    // Find the inactive OM and start it
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    cluster.startInactiveOM(followerNodeId);
    GenericTestUtils.setLogLevel(OzoneManager.class, Level.INFO);
    LogCapturer logCapture = LogCapturer.captureLogs(OzoneManager.class);

    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);
    OzoneManagerRatisServer followerRatisServer = followerOM.getOmRatisServer();

    // Do some transactions so that the log index increases on follower OM
    writeKeysToIncreaseLogIndex(followerRatisServer, 100);

    TermIndex leaderCheckpointTermIndex = leaderOM.getOmRatisServer()
        .getLastAppliedTermIndex();
    DBCheckpoint leaderDbCheckpoint = leaderOM.getMetadataManager().getStore()
        .getCheckpoint(false);

    // Do some more transactions to increase the log index further on
    // follower OM such that it is more than the checkpoint index taken on
    // leader OM.
    writeKeysToIncreaseLogIndex(followerOM.getOmRatisServer(),
        leaderCheckpointTermIndex.getIndex() + 100);

    // Wait for the follower to finish applying in-flight transactions, so
    // that the TermIndex read below matches what installCheckpoint observes.
    // The applied index only advances on double buffer flush, so it can lag
    // acked writes; the commit index covers them all.
    long leaderCommitted = leaderOM.getOmRatisServer().getServerDivision()
        .getRaftLog().getLastCommittedIndex();
    GenericTestUtils.waitFor(() -> followerRatisServer
        .getLastAppliedTermIndex().getIndex() >= leaderCommitted, 100, 10_000);

    // Install the old checkpoint on the follower OM. This should fail as the
    // followerOM is already ahead of that transactionLogIndex and the OM
    // state should be reloaded.
    TermIndex followerTermIndex = followerRatisServer.getLastAppliedTermIndex();
    Path leaderCheckpointLocation = leaderDbCheckpoint.getCheckpointLocation();
    assertNotNull(leaderCheckpointLocation);
    Path omDbDir = leaderCheckpointLocation.resolve(OM_DB_NAME);
    assertTrue(omDbDir.toFile().mkdir());
    moveCheckpointContentsToOmDbDir(leaderCheckpointLocation, omDbDir);

    TermIndex newTermIndex = followerOM.installCheckpoint(
        leaderOMNodeId, leaderCheckpointLocation);

    String errorMsg = "Cannot proceed with InstallSnapshot as OM is at " +
        "TermIndex " + followerTermIndex + " and checkpoint has lower " +
        "TermIndex";
    assertLogCapture(logCapture, errorMsg);
    assertNull(newTermIndex,
        "OM installed checkpoint even though checkpoint " +
            "logIndex is less than it's lastAppliedIndex");
    assertEquals(followerTermIndex,
        followerRatisServer.getLastAppliedTermIndex());
    String msg = "OM DB is not stopped. Started services with Term: " +
        followerTermIndex.getTerm() + " and Index: " +
        followerTermIndex.getIndex();
    assertLogCapture(logCapture, msg);
  }

  @Test
  public void testInstallCorruptedCheckpointFailure() throws Exception {
    // Get the leader OM
    final String leaderOMNodeId = OmTestUtil.getCurrentOmProxyNodeId(objectStore);

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Do some transactions so that the log index increases
    writeKeysToIncreaseLogIndex(leaderRatisServer, 100);

    DBCheckpoint leaderDbCheckpoint = leaderOM.getMetadataManager().getStore()
        .getCheckpoint(false);
    Path leaderCheckpointLocation = leaderDbCheckpoint.getCheckpointLocation();
    assertNotNull(leaderCheckpointLocation);
    Path omDbDir = leaderCheckpointLocation.resolve(OM_DB_NAME);
    assertTrue(omDbDir.toFile().mkdir());
    moveCheckpointContentsToOmDbDir(leaderCheckpointLocation, omDbDir);

    TransactionInfo leaderCheckpointTrxnInfo = OzoneManagerRatisUtils
        .getTrxnInfoFromCheckpoint(conf, omDbDir);

    // Corrupt the leader checkpoint and install that on the OM. The
    // operation should fail and OM should shutdown.
    boolean delete = true;
    File[] files = omDbDir.toFile().listFiles();
    assertNotNull(files);
    for (File file : files) {
      if (file.getName().contains(".sst")) {
        if (delete) {
          FileUtils.deleteQuietly(file);
          delete = false;
        } else {
          delete = true;
        }
      }
    }

    GenericTestUtils.setLogLevel(OzoneManager.class, Level.INFO);
    LogCapturer logCapture = LogCapturer.captureLogs(OzoneManager.class);
    followerOM.setExitManagerForTesting(new DummyExitManager());
    // Install corrupted checkpoint
    followerOM.installCheckpoint(leaderOMNodeId, leaderCheckpointLocation,
        leaderCheckpointTrxnInfo);

    // Wait checkpoint installation to be finished.
    assertLogCapture(logCapture, "System Exit: " +
        "Failed to reload OM state and instantiate services.");
    String msg = "RPC server is stopped";
    assertLogCapture(logCapture, msg);
  }

  /**
   * When the pre-install backup loop in replaceOMDBWithCheckpoint fails part way
   * through, every item it already relocated into om.db.backup.* must be put back.
   * Today the loop has no catch, so the items stay in the backup directory: the
   * one that was moved first is lost, and if that item is om.db then reloadOMState
   * silently re-creates an empty one.
   */
  @Test
  public void testInstallSnapshotFailedBackupRestoresDbDir() throws Exception {
    final String leaderOMNodeId = OmTestUtil.getCurrentOmProxyNodeId(objectStore);
    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM, so the checkpoint index is ahead of its applied index
    // and canProceed lets the replacement start.
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    writeKeysToIncreaseLogIndex(leaderRatisServer, 100);

    // Build a checkpoint whose top level holds two entries, so the backup loop
    // performs two moves and can fail on the second.
    DBCheckpoint leaderDbCheckpoint =
        leaderOM.getMetadataManager().getStore().getCheckpoint(false);
    Path leaderCheckpointLocation = leaderDbCheckpoint.getCheckpointLocation();
    assertNotNull(leaderCheckpointLocation);
    Path omDbDir = leaderCheckpointLocation.resolve(OM_DB_NAME);
    Files.createDirectory(omDbDir);
    moveCheckpointContentsToOmDbDir(leaderCheckpointLocation, omDbDir);
    Files.createDirectories(leaderCheckpointLocation.resolve(OM_SNAPSHOT_DIR));

    TransactionInfo leaderCheckpointTrxnInfo =
        OzoneManagerRatisUtils.getTrxnInfoFromCheckpoint(conf, omDbDir);

    // Give the follower a matching second entry with a sentinel inside it, so the
    // loop finds two items in the follower's metadata dir to relocate.
    File followerMetaDir = OMStorage.getOmDbDir(followerOM.getConfiguration());
    Path followerDbDir = Paths.get(followerMetaDir.toString(), OM_DB_NAME);
    Path followerSnapshotDir = Paths.get(followerMetaDir.toString(), OM_SNAPSHOT_DIR);
    Files.createDirectories(followerSnapshotDir);
    Path sentinel = followerSnapshotDir.resolve("sentinel");
    Files.write(sentinel, "keep-me".getBytes(UTF_8));

    Set<String> namesBefore = topLevelNames(followerMetaDir);
    assertThat(namesBefore).contains(OM_DB_NAME, OM_SNAPSHOT_DIR);
    Object dbInodeBefore = getINode(followerDbDir);

    // Fail the second of the two backup moves. The first has already succeeded,
    // so the DB directory is now missing whichever item was relocated first.
    followerOM.setCheckpointBackupInjector(new ThrowOnNthPauseFaultInjector(2,
        "Simulated backup move failure for test"));
    followerOM.setExitManagerForTesting(new DummyExitManager());
    try {
      TermIndex termIndex = followerOM.installCheckpoint(
          leaderOMNodeId, leaderCheckpointLocation, leaderCheckpointTrxnInfo);
      assertNull(termIndex, "Install should have been reported as failed");

      // Everything present before the aborted install must still be present.
      assertThat(topLevelNames(followerMetaDir)).containsAll(namesBefore);
      assertTrue(Files.exists(sentinel),
          "Sentinel under " + OM_SNAPSHOT_DIR + " was relocated and never restored");
      assertEquals("keep-me", new String(Files.readAllBytes(sentinel), UTF_8));
      // The original om.db must be the one still in place, not a fresh empty DB.
      assertEquals(dbInodeBefore, getINode(followerDbDir),
          OM_DB_NAME + " was replaced rather than restored");
    } finally {
      followerOM.setCheckpointBackupInjector(null);
      cluster.setupExitManagerForTesting();
    }
  }

  /** Top-level entries of the OM metadata dir, ignoring backup dirs the install creates. */
  private static Set<String> topLevelNames(File metaDir) throws IOException {
    try (Stream<Path> list = Files.list(metaDir.toPath())) {
      return list.map(p -> p.getFileName().toString())
          .filter(n -> !n.startsWith(OzoneConsts.OM_DB_BACKUP_PREFIX))
          .collect(Collectors.toSet());
    }
  }

  /**
   * After a successful install the in-memory transaction info must describe the
   * position the state machine was unpaused at, not the follower's pre-install
   * index. Asserted immediately after the call: a later takeSnapshot recomputes
   * the value from the applied index and would mask a regression here.
   *
   * This pins an Ozone-internal invariant, not the Ratis contract. Calling
   * installCheckpoint directly queues no Ratis reload, so the pre-fix value seen
   * here is starker than a real install leaves behind.
   */
  @Test
  public void testInstallCheckpointPublishesNewTransactionInfo() throws Exception {
    final String leaderOMNodeId = OmTestUtil.getCurrentOmProxyNodeId(objectStore);
    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    writeKeysToIncreaseLogIndex(leaderRatisServer, 100);

    DBCheckpoint leaderDbCheckpoint =
        leaderOM.getMetadataManager().getStore().getCheckpoint(false);
    Path leaderCheckpointLocation = leaderDbCheckpoint.getCheckpointLocation();
    assertNotNull(leaderCheckpointLocation);
    Path omDbDir = leaderCheckpointLocation.resolve(OM_DB_NAME);
    assertTrue(omDbDir.toFile().mkdir());
    moveCheckpointContentsToOmDbDir(leaderCheckpointLocation, omDbDir);
    TransactionInfo leaderCheckpointTrxnInfo =
        OzoneManagerRatisUtils.getTrxnInfoFromCheckpoint(conf, omDbDir);

    // The follower was never started, so restarting its RPC server at the end of
    // installCheckpoint fails. That happens after the transaction info is published
    // and is not what this test is about, so swallow the exit.
    followerOM.setExitManagerForTesting(new DummyExitManager());

    TermIndex installed = followerOM.installCheckpoint(
        leaderOMNodeId, leaderCheckpointLocation, leaderCheckpointTrxnInfo);
    assertNotNull(installed, "Install should have succeeded");
    assertEquals(leaderCheckpointTrxnInfo.getTransactionIndex(), installed.getIndex());

    assertEquals(followerOM.getOmRatisServer().getLastAppliedTermIndex(),
        followerOM.getTransactionInfo().getTermIndex(),
        "In-memory transaction info must match the position the state machine was unpaused at");
  }

  @Test
  public void testInstallSnapshotFromLeaderFailedDownloadCleanupSucceeds()
      throws Exception {
    final String leaderOMNodeId = OmTestUtil.getCurrentOmProxyNodeId(objectStore);
    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);
    File candidateDir = followerOM.getOmSnapshotProvider().getCandidateDir();
    assertTrue(candidateDir.exists(),
        "Candidate dir should exist before download attempt");

    // Inject fault: throw on first pause (after first download part, before untar)
    FaultInjector faultInjector =
        new ThrowOnPauseFaultInjector("Simulated download failure for test");
    followerOM.getOmSnapshotProvider().setInjector(faultInjector);

    // Advance leader so follower will need install snapshot when started
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();
    writeKeysToIncreaseLogIndex(leaderRatisServer, 100);

    // Start follower - Ratis will trigger install snapshot
    cluster.startInactiveOM(followerNodeId);

    // Wait for install snapshot attempt to complete (download fails, cleanup runs)
    GenericTestUtils.waitFor(() -> {
      return !candidateDir.exists() || (candidateDir.list() != null && candidateDir.list().length == 0);
    }, 500, 10_000);

    // Verify cleanup succeeded: candidate dir is empty
    String[] filesInCandidate = candidateDir.exists() ? candidateDir.list() : new String[0];
    assertNotNull(filesInCandidate);
    assertEquals(0, filesInCandidate.length,
        "Candidate dir should be cleaned after failed download");
    // Clear injector
    followerOM.getOmSnapshotProvider().setInjector(null);
  }

  /**
   * Moves all contents from the checkpoint location into the omDbDir.
   * This reorganizes the checkpoint structure so that all checkpoint files
   * are contained within the om.db directory.
   *
   * @param checkpointLocation the source checkpoint location containing files/directories
   * @param omDbDir the target directory (om.db) where contents should be moved
   * @throws IOException if file operations fail
   */
  private void moveCheckpointContentsToOmDbDir(Path checkpointLocation, Path omDbDir)
      throws IOException {
    File checkpointLocationFile = checkpointLocation.toFile();
    File omDbDirFile = omDbDir.toFile();

    // Ensure omDbDir exists
    if (!omDbDirFile.exists()) {
      if (!omDbDirFile.mkdirs()) {
        throw new IOException("Failed to create directory: " + omDbDir);
      }
    }

    if (!checkpointLocationFile.exists() || !checkpointLocationFile.isDirectory()) {
      throw new IOException("Checkpoint location does not exist or is not a directory: " + checkpointLocation);
    }

    // Move all contents from checkpointLocation to omDbDir
    File[] contents = checkpointLocationFile.listFiles();
    if (contents != null) {
      for (File item : contents) {
        String name = item != null ? item.getName() : null;
        Path fileName = omDbDir.getFileName();
        // Skip the target directory itself if it already exists in source
        assertNotNull(name);
        assertNotNull(fileName);
        if (name.equals(fileName.toString())) {
          continue;
        }

        Path targetPath = omDbDir.resolve(item.getName());

        // Delete target if it exists
        if (Files.exists(targetPath)) {
          if (Files.isDirectory(targetPath)) {
            FileUtils.deleteDirectory(targetPath.toFile());
          } else {
            Files.delete(targetPath);
          }
        }

        // Move item to target - Files.move handles both files and directories recursively
        Files.move(item.toPath(), targetPath);
      }
    }
  }

  static SnapshotInfo createOzoneSnapshot(ObjectStore objectStore, String volumeName, String bucketName,
      OzoneManager leaderOM, String name)
      throws IOException {
    objectStore.createSnapshot(volumeName, bucketName, name);

    String tableKey = SnapshotInfo.getTableKey(volumeName,
                                               bucketName,
                                               name);
    SnapshotInfo snapshotInfo = leaderOM.getMetadataManager()
        .getSnapshotInfoTable()
        .get(tableKey);
    // Allow the snapshot to be written to disk
    String fileName =
        getSnapshotPath(leaderOM.getConfiguration(), snapshotInfo, 0);
    File snapshotDir = new File(fileName);
    if (!RDBCheckpointUtils
        .waitForCheckpointDirectoryExist(snapshotDir)) {
      throw new IOException("snapshot directory doesn't exist");
    }
    return snapshotInfo;
  }

  private List<String> writeKeysToIncreaseLogIndex(
      OzoneManagerRatisServer omRatisServer, long targetLogIndex)
      throws IOException, InterruptedException {
    List<String> keys = new ArrayList<>();
    long logIndex = omRatisServer.getLastAppliedTermIndex().getIndex();
    while (logIndex < targetLogIndex) {
      keys.add(createKey(ozoneBucket));
      logIndex = omRatisServer.getLastAppliedTermIndex().getIndex();
    }
    return keys;
  }

  private List<String> writeKeys(long keyCount) throws IOException {
    return writeKeys(ozoneBucket, keyCount);
  }

  static List<String> writeKeys(OzoneBucket ozoneBucket, long keyCount) throws IOException {
    List<String> keys = new ArrayList<>();
    long index = 0;
    while (index < keyCount) {
      keys.add(createKey(ozoneBucket));
      index++;
    }
    return keys;
  }

  private void getKeys(List<String> keys, int round) throws IOException {
    while (round > 0) {
      for (String keyName : keys) {
        OzoneKeyDetails key = ozoneBucket.getKey(keyName);
        assertEquals(keyName, key.getName());
      }
      round--;
    }
  }

  private void readKeys(List<String> keys) throws IOException {
    for (String keyName : keys) {
      readFully(ozoneBucket, keyName);
    }
  }

  private void assertLogCapture(LogCapturer logCapture,
                              String msg)
      throws InterruptedException, TimeoutException {
    GenericTestUtils.waitFor(() -> {
      return logCapture.getOutput().contains(msg);
    }, 100, 30_000);
  }

  private static class DummyExitManager extends ExitManager {
    @Override
    public void exitSystem(int status, String message, Throwable throwable,
        Logger log) {
      log.error("System Exit: " + message, throwable);
    }
  }

  /**
   * FaultInjector that throws IOException on pause(), simulating a download failure
   * after the first part completes. Used to test cleanup on failed download.
   */
  private static class ThrowOnPauseFaultInjector extends FaultInjector {
    private final IOException toThrow;

    ThrowOnPauseFaultInjector(String message) {
      this.toThrow = new IOException(message);
    }

    @Override
    public void pause() throws IOException {
      throw toThrow;
    }
  }

  /**
   * FaultInjector that throws IOException on the nth pause() call and lets the
   * earlier ones through, so a loop can be failed part way rather than up front.
   */
  private static class ThrowOnNthPauseFaultInjector extends FaultInjector {
    private final int failOnCall;
    private final String message;
    private int calls;

    ThrowOnNthPauseFaultInjector(int failOnCall, String message) {
      this.failOnCall = failOnCall;
      this.message = message;
    }

    @Override
    public void pause() throws IOException {
      if (++calls == failOnCall) {
        throw new IOException(message);
      }
    }
  }
}
