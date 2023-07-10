/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.ExitManager;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.DBCheckpointMetrics;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBCheckpointUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.apache.ratis.server.protocol.TermIndex;
import org.assertj.core.api.Fail;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.FILTERED_SNAPSHOTS;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.OM_HARDLINK_FILE;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPath;
import static org.apache.hadoop.ozone.om.TestOzoneManagerHAWithData.createKey;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests the Ratis snapshots feature in OM.
 */
@Timeout(5000)
public class TestOMRatisSnapshots {

  private MiniOzoneHAClusterImpl cluster = null;
  private ObjectStore objectStore;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omServiceId;
  private int numOfOMs = 3;
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

  /**
   * Create a MiniOzoneCluster for testing. The cluster initially has one
   * inactive OM. So at the start of the cluster, there will be 2 active and 1
   * inactive OM.
   *
   * @throws IOException
   */
  @BeforeEach
  public void init(TestInfo testInfo) throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "om-service-test1";
    conf.setInt(OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP, LOG_PURGE_GAP);
    conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_SEGMENT_SIZE_KEY, 16,
        StorageUnit.KB);
    conf.setStorageSize(OMConfigKeys.
        OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY, 16, StorageUnit.KB);
    if (testInfo.getDisplayName().equals("testSnapshotBackgroundServices")) {
      conf.setTimeDuration(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL,
          5, TimeUnit.SECONDS);
      conf.setTimeDuration(OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL, 5,
          TimeUnit.SECONDS);
      conf.setTimeDuration(OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED,
          5, TimeUnit.MILLISECONDS);
    }
    long snapshotThreshold = SNAPSHOT_THRESHOLD;
    // TODO: refactor tests to run under a new class with different configs.
    if (testInfo.getTestMethod().isPresent() &&
        testInfo.getTestMethod().get().getName()
            .equals("testInstallSnapshot")) {
      snapshotThreshold = SNAPSHOT_THRESHOLD * 10;
    }
    conf.setLong(
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        snapshotThreshold);
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId("om-service-test1")
        .setNumOfOzoneManagers(numOfOMs)
        .setNumOfActiveOMs(2)
        .build();
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient(omServiceId, conf);
    objectStore = client.getObjectStore();

    volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    bucketName = "bucket" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner("user" + RandomStringUtils.randomNumeric(5))
        .setAdmin("admin" + RandomStringUtils.randomNumeric(5))
        .build();

    objectStore.createVolume(volumeName, createVolumeArgs);
    OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);

    retVolumeinfo.createBucket(bucketName,
        BucketArgs.newBuilder().setBucketLayout(TEST_BUCKET_LAYOUT).build());
    ozoneBucket = retVolumeinfo.getBucket(bucketName);
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterEach
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {100})
  // tried up to 1000 snapshots and this test works, but some of the
  //  timeouts have to be increased.
  public void testInstallSnapshot(int numSnapshotsToCreate) throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Create some snapshots, each with new keys
    int keyIncrement = 10;
    String snapshotNamePrefix = "snapshot";
    String snapshotName = "";
    List<String> keys = new ArrayList<>();
    SnapshotInfo snapshotInfo = null;
    for (int snapshotCount = 0; snapshotCount < numSnapshotsToCreate;
        snapshotCount++) {
      snapshotName = snapshotNamePrefix + snapshotCount;
      keys = writeKeys(keyIncrement);
      snapshotInfo = createOzoneSnapshot(leaderOM, snapshotName);
    }


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
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.LOG);

    // The recently started OM should be lagging behind the leader OM.
    // Wait & for follower to update transactions to leader snapshot index.
    // Timeout error if follower does not load update within 10s
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
          >= leaderOMSnapshotIndex - 1;
    }, 100, 10000);

    long followerOMLastAppliedIndex =
        followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    assertTrue(
        followerOMLastAppliedIndex >= leaderOMSnapshotIndex - 1);

    // After the new checkpoint is installed, the follower OM
    // lastAppliedIndex must >= the snapshot index of the checkpoint. It
    // could be great than snapshot index if there is any conf entry from ratis.
    followerOMLastAppliedIndex = followerOM.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex();
    assertTrue(followerOMLastAppliedIndex >= leaderOMSnapshotIndex);
    assertTrue(followerOM.getOmRatisServer().getLastAppliedTermIndex()
        .getTerm() >= leaderOMSnapshotTermIndex);

    // Verify checkpoint installation was happened.
    String msg = "Reloaded OM state";
    assertLogCapture(logCapture, msg);

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

    // Verify RPC server is running
    GenericTestUtils.waitFor(() -> {
      return followerOM.isOmRpcServerRunning();
    }, 100, 5000);

    assertLogCapture(logCapture,
        "Install Checkpoint is finished");

    // Read & Write after snapshot installed.
    List<String> newKeys = writeKeys(1);
    readKeys(newKeys);
    // TODO: Enable this part after RATIS-1481 used
    /*
    Assert.assertNotNull(followerOMMetaMngr.getKeyTable(
        TEST_BUCKET_LAYOUT).get(followerOMMetaMngr.getOzoneKey(
        volumeName, bucketName, newKeys.get(0))));
     */

    checkSnapshot(leaderOM, followerOM, snapshotName, keys, snapshotInfo);
  }

  private void checkSnapshot(OzoneManager leaderOM, OzoneManager followerOM,
                             String snapshotName,
                             List<String> keys, SnapshotInfo snapshotInfo)
      throws IOException {
    // Read back data from snapshot.
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(".snapshot/" + snapshotName + "/" +
            keys.get(keys.size() - 1)).build();
    OmKeyInfo omKeyInfo;
    omKeyInfo = followerOM.lookupKey(omKeyArgs);
    Assertions.assertNotNull(omKeyInfo);
    Assertions.assertEquals(omKeyInfo.getKeyName(), omKeyArgs.getKeyName());

    // Confirm followers snapshot hard links are as expected
    File followerMetaDir = OMStorage.getOmDbDir(followerOM.getConfiguration());
    Path followerActiveDir = Paths.get(followerMetaDir.toString(), OM_DB_NAME);
    Path followerSnapshotDir =
        Paths.get(getSnapshotPath(followerOM.getConfiguration(), snapshotInfo));
    File leaderMetaDir = OMStorage.getOmDbDir(leaderOM.getConfiguration());
    Path leaderActiveDir = Paths.get(leaderMetaDir.toString(), OM_DB_NAME);
    Path leaderSnapshotDir =
        Paths.get(getSnapshotPath(leaderOM.getConfiguration(), snapshotInfo));
    // Get the list of hardlinks from the leader.  Then confirm those links
    //  are on the follower
    int hardLinkCount = 0;
    try (Stream<Path>list = Files.list(leaderSnapshotDir)) {
      for (Path leaderSnapshotSST: list.collect(Collectors.toList())) {
        String fileName = leaderSnapshotSST.getFileName().toString();
        if (fileName.toLowerCase().endsWith(".sst")) {

          Path leaderActiveSST =
              Paths.get(leaderActiveDir.toString(), fileName);
          // Skip if not hard link on the leader
          if (!leaderActiveSST.toFile().exists()) {
            continue;
          }
          // If it is a hard link on the leader, it should be a hard
          // link on the follower
          if (OmSnapshotUtils.getINode(leaderActiveSST)
              .equals(OmSnapshotUtils.getINode(leaderSnapshotSST))) {
            Path followerSnapshotSST =
                Paths.get(followerSnapshotDir.toString(), fileName);
            Path followerActiveSST =
                Paths.get(followerActiveDir.toString(), fileName);
            Assertions.assertEquals(
                OmSnapshotUtils.getINode(followerActiveSST),
                OmSnapshotUtils.getINode(followerSnapshotSST),
                "Snapshot sst file is supposed to be a hard link");
            hardLinkCount++;
          }
        }
      }
    }
    Assertions.assertTrue(hardLinkCount > 0, "No hard links were found");
  }

  @Test
  @Timeout(300)
  public void testInstallIncrementalSnapshot(@TempDir Path tempDir)
      throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Set fault injector to pause before install
    FaultInjector faultInjector = new SnapshotPauseInjector();
    followerOM.getOmSnapshotProvider().setInjector(faultInjector);

    // Do some transactions so that the log index increases
    List<String> firstKeys = writeKeysToIncreaseLogIndex(leaderRatisServer,
        80);

    SnapshotInfo snapshotInfo2 = createOzoneSnapshot(leaderOM, "snap80");

    // Start the inactive OM. Checkpoint installation will happen spontaneously.
    cluster.startInactiveOM(followerNodeId);

    // Wait the follower download the snapshot,but get stuck by injector
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmSnapshotProvider().getNumDownloaded() == 1;
    }, 1000, 10000);

    // Get two incremental tarballs, adding new keys/snapshot for each.
    IncrementData firstIncrement = getNextIncrementalTarball(160, 2, leaderOM,
        leaderRatisServer, faultInjector, followerOM, tempDir);
    IncrementData secondIncrement = getNextIncrementalTarball(240, 3, leaderOM,
        leaderRatisServer, faultInjector, followerOM, tempDir);

    // Resume the follower thread, it would download the incremental snapshot.
    faultInjector.resume();

    // Get the latest db checkpoint from the leader OM.
    TransactionInfo transactionInfo =
        TransactionInfo.readTransactionInfo(leaderOM.getMetadataManager());
    TermIndex leaderOMTermIndex =
        TermIndex.valueOf(transactionInfo.getTerm(),
            transactionInfo.getTransactionIndex());
    long leaderOMSnapshotIndex = leaderOMTermIndex.getIndex();

    // The recently started OM should be lagging behind the leader OM.
    // Wait & for follower to update transactions to leader snapshot index.
    // Timeout error if follower does not load update within 30s
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
          >= leaderOMSnapshotIndex - 1;
    }, 1000, 30000);

    assertEquals(3, followerOM.getOmSnapshotProvider().getNumDownloaded());

    // Verify that the follower OM's DB contains the transactions which were
    // made while it was inactive.
    OMMetadataManager followerOMMetaMngr = followerOM.getMetadataManager();
    assertNotNull(followerOMMetaMngr.getVolumeTable().get(
        followerOMMetaMngr.getVolumeKey(volumeName)));
    assertNotNull(followerOMMetaMngr.getBucketTable().get(
        followerOMMetaMngr.getBucketKey(volumeName, bucketName)));

    for (String key : firstKeys) {
      assertNotNull(followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }
    for (String key : firstIncrement.getKeys()) {
      assertNotNull(followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }

    for (String key : secondIncrement.getKeys()) {
      assertNotNull(followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }

    // Verify the metrics recording the incremental checkpoint at leader side
    DBCheckpointMetrics dbMetrics = leaderOM.getMetrics().
        getDBCheckpointMetrics();
    Assertions.assertTrue(
        dbMetrics.getLastCheckpointStreamingNumSSTExcluded() > 0);
    assertEquals(2, dbMetrics.getNumIncrementalCheckpoints());

    // Verify RPC server is running
    GenericTestUtils.waitFor(() -> {
      return followerOM.isOmRpcServerRunning();
    }, 100, 5000);

    // Read & Write after snapshot installed.
    List<String> newKeys = writeKeys(1);
    readKeys(newKeys);
    GenericTestUtils.waitFor(() -> {
      try {
        return followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
            .get(followerOMMetaMngr.getOzoneKey(
                volumeName, bucketName, newKeys.get(0))) != null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 100, 10000);

    // Verify follower candidate directory get cleaned
    String[] filesInCandidate = followerOM.getOmSnapshotProvider().
        getCandidateDir().list();
    assertNotNull(filesInCandidate);
    assertEquals(0, filesInCandidate.length);

    checkSnapshot(leaderOM, followerOM, "snap80", firstKeys, snapshotInfo2);
    checkSnapshot(leaderOM, followerOM, "snap160", firstIncrement.getKeys(),
        firstIncrement.getSnapshotInfo());
    checkSnapshot(leaderOM, followerOM, "snap240", secondIncrement.getKeys(),
        secondIncrement.getSnapshotInfo());
    Assertions.assertEquals(
        followerOM.getOmSnapshotProvider().getInitCount(), 2,
        "Only initialized twice");
  }

  static class IncrementData {
    private List<String> keys;
    private SnapshotInfo snapshotInfo;
    public List<String> getKeys() {
      return keys;
    }
    public SnapshotInfo getSnapshotInfo() {
      return snapshotInfo;
    }
  }

  private IncrementData getNextIncrementalTarball(
      int numKeys, int expectedNumDownloads,
      OzoneManager leaderOM, OzoneManagerRatisServer leaderRatisServer,
      FaultInjector faultInjector, OzoneManager followerOM, Path tempDir)
      throws IOException, InterruptedException, TimeoutException {
    IncrementData id = new IncrementData();

    // Get the latest db checkpoint from the leader OM.
    TransactionInfo transactionInfo =
        TransactionInfo.readTransactionInfo(leaderOM.getMetadataManager());
    TermIndex leaderOMTermIndex =
        TermIndex.valueOf(transactionInfo.getTerm(),
            transactionInfo.getTransactionIndex());
    long leaderOMSnapshotIndex = leaderOMTermIndex.getIndex();
    // Do some transactions, let leader OM take a new snapshot and purge the
    // old logs, so that follower must download the new increment.
    id.keys = writeKeysToIncreaseLogIndex(leaderRatisServer,
        numKeys);

    id.snapshotInfo = createOzoneSnapshot(leaderOM, "snap" + numKeys);
    // Resume the follower thread, it would download the incremental snapshot.
    faultInjector.resume();

    // Pause the follower thread again to block the next install
    faultInjector.reset();

    // Wait the follower download the incremental snapshot, but get stuck
    // by injector
    GenericTestUtils.waitFor(() ->
        followerOM.getOmSnapshotProvider().getNumDownloaded() ==
        expectedNumDownloads, 1000, 10000);

    assertTrue(followerOM.getOmRatisServer().
        getLastAppliedTermIndex().getIndex()
        >= leaderOMSnapshotIndex - 1);

    // Now confirm tarball is just incremental and contains no unexpected
    //  files/links.
    Path increment = Paths.get(tempDir.toString(), "increment" + numKeys);
    assertTrue(increment.toFile().mkdirs());
    unTarLatestTarBall(followerOM, increment);
    List<String> sstFiles = HAUtils.getExistingSstFiles(increment.toFile());
    Path followerCandidatePath = followerOM.getOmSnapshotProvider().
        getCandidateDir().toPath();

    // Confirm that none of the files in the tarball match one in the
    // candidate dir.
    assertTrue(sstFiles.size() > 0);
    for (String s: sstFiles) {
      File sstFile = Paths.get(followerCandidatePath.toString(), s).toFile();
      assertFalse(sstFile.exists(),
          sstFile + " should not duplicate existing files");
    }

    // Confirm that none of the links in the tarballs hardLinkFile
    //  match the existing files
    Path hardLinkFile = Paths.get(increment.toString(), OM_HARDLINK_FILE);
    try (Stream<String> lines = Files.lines(hardLinkFile)) {
      int lineCount = 0;
      for (String line: lines.collect(Collectors.toList())) {
        lineCount++;
        String link = line.split("\t")[0];
        File linkFile = Paths.get(
            followerCandidatePath.toString(), link).toFile();
        assertFalse(linkFile.exists(),
            "Incremental checkpoint should not " +
                "duplicate existing links");
      }
      assertTrue(lineCount > 0);
    }
    return id;
  }

  @Test
  @Timeout(300)
  @Flaky("HDDS-8876")
  public void testInstallIncrementalSnapshotWithFailure() throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Set fault injector to pause before install
    FaultInjector faultInjector = new SnapshotPauseInjector();
    followerOM.getOmSnapshotProvider().setInjector(faultInjector);

    // Do some transactions so that the log index increases
    List<String> firstKeys = writeKeysToIncreaseLogIndex(leaderRatisServer,
        80);

    // Start the inactive OM. Checkpoint installation will happen spontaneously.
    cluster.startInactiveOM(followerNodeId);

    // Wait the follower download the snapshot,but get stuck by injector
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmSnapshotProvider().getNumDownloaded() == 1;
    }, 1000, 10000);

    // Do some transactions, let leader OM take a new snapshot and purge the
    // old logs, so that follower must download the new snapshot again.
    List<String> secondKeys = writeKeysToIncreaseLogIndex(leaderRatisServer,
        160);

    // Resume the follower thread, it would download the incremental snapshot.
    faultInjector.resume();

    // Pause the follower thread again to block the tarball install
    faultInjector.reset();

    // Wait the follower download the incremental snapshot, but get stuck
    // by injector
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmSnapshotProvider().getNumDownloaded() == 2;
    }, 1000, 10000);

    // Corrupt the mixed checkpoint in the candidate DB dir
    File followerCandidateDir = followerOM.getOmSnapshotProvider().
        getCandidateDir();
    List<String> sstList = HAUtils.getExistingSstFiles(followerCandidateDir);
    Assertions.assertTrue(sstList.size() > 0);
    Collections.shuffle(sstList);
    List<String> victimSstList = sstList.subList(0, sstList.size() / 3);
    for (String sst: victimSstList) {
      File victimSst = new File(followerCandidateDir, sst);
      Assertions.assertTrue(victimSst.delete());
    }

    // Resume the follower thread, it would download the full snapshot again
    // as the installation will fail for the corruption detected.
    faultInjector.resume();

    // Get the latest db checkpoint from the leader OM.
    TransactionInfo transactionInfo =
        TransactionInfo.readTransactionInfo(leaderOM.getMetadataManager());
    TermIndex leaderOMTermIndex =
        TermIndex.valueOf(transactionInfo.getTerm(),
            transactionInfo.getTransactionIndex());
    long leaderOMSnapshotIndex = leaderOMTermIndex.getIndex();

    // Wait & for follower to update transactions to leader snapshot index.
    // Timeout error if follower does not load update within 10s
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
          >= leaderOMSnapshotIndex - 1;
    }, 1000, 10000);

    // Verify that the follower OM's DB contains the transactions which were
    // made while it was inactive.
    OMMetadataManager followerOMMetaMngr = followerOM.getMetadataManager();
    assertNotNull(followerOMMetaMngr.getVolumeTable().get(
        followerOMMetaMngr.getVolumeKey(volumeName)));
    assertNotNull(followerOMMetaMngr.getBucketTable().get(
        followerOMMetaMngr.getBucketKey(volumeName, bucketName)));

    // Verify that the follower OM's DB contains the transactions which were
    // made while it was inactive.
    for (String key : firstKeys) {
      assertNotNull(followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }
    for (String key : secondKeys) {
      assertNotNull(followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }

    // There is a chance we end up checking the DBCheckpointMetrics
    // before the follower sends another request to the leader
    // to generate a checkpoint.
    // TODO: Add wait check here, to avoid flakiness.

    // Verify the metrics
    DBCheckpointMetrics dbMetrics = leaderOM.getMetrics().
        getDBCheckpointMetrics();
    assertEquals(0, dbMetrics.getLastCheckpointStreamingNumSSTExcluded());
    assertTrue(dbMetrics.getNumIncrementalCheckpoints() >= 1);
    assertTrue(dbMetrics.getNumCheckpoints() >= 3);

    // Verify RPC server is running
    GenericTestUtils.waitFor(() -> {
      return followerOM.isOmRpcServerRunning();
    }, 100, 5000);

    // Read & Write after snapshot installed.
    List<String> newKeys = writeKeys(1);
    readKeys(newKeys);
    GenericTestUtils.waitFor(() -> {
      try {
        return followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
            .get(followerOMMetaMngr.getOzoneKey(
                volumeName, bucketName, newKeys.get(0))) != null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 100, 10000);

    // Verify follower candidate directory get cleaned
    String[] filesInCandidate = followerOM.getOmSnapshotProvider().
        getCandidateDir().list();
    assertNotNull(filesInCandidate);
    assertEquals(0, filesInCandidate.length);
  }

  @Test
  public void testInstallSnapshotWithClientWrite() throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

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
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.LOG);

    // Continuously create new keys
    ExecutorService executor = Executors.newFixedThreadPool(1);
    Future<List<String>> writeFuture = executor.submit(() -> {
      return writeKeys(200);
    });
    List<String> newKeys = writeFuture.get();

    // Wait checkpoint installation to finish
    Thread.sleep(5000);

    // The recently started OM should be lagging behind the leader OM.
    // Wait & for follower to update transactions to leader snapshot index.
    // Timeout error if follower does not load update within 3s
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
          >= leaderOMSnapshotIndex - 1;
    }, 100, 3000);

    // Verify checkpoint installation was happened.
    String msg = "Reloaded OM state";
    assertLogCapture(logCapture, msg);
    assertLogCapture(logCapture, "Install Checkpoint is finished");

    long followerOMLastAppliedIndex =
        followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    assertTrue(
        followerOMLastAppliedIndex >= leaderOMSnapshotIndex - 1);

    // After the new checkpoint is installed, the follower OM
    // lastAppliedIndex must >= the snapshot index of the checkpoint. It
    // could be great than snapshot index if there is any conf entry from ratis.
    followerOMLastAppliedIndex = followerOM.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex();
    assertTrue(followerOMLastAppliedIndex >= leaderOMSnapshotIndex);
    assertTrue(followerOM.getOmRatisServer().getLastAppliedTermIndex()
        .getTerm() >= leaderOMSnapshotTermIndex);

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
    OMMetadataManager leaderOmMetaMgr = leaderOM.getMetadataManager();
    for (String key : newKeys) {
      assertNotNull(leaderOmMetaMgr.getKeyTable(
          TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMgr.getOzoneKey(volumeName, bucketName, key)));
    }
    Thread.sleep(5000);
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
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

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

    // Start the inactive OM. Checkpoint installation will happen spontaneously.
    cluster.startInactiveOM(followerNodeId);
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.LOG);

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

    // The recently started OM should be lagging behind the leader OM.
    // Wait & for follower to update transactions to leader snapshot index.
    // Timeout error if follower does not load update within 3s
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
          >= leaderOMSnapshotIndex - 1;
    }, 100, 3000);

    long followerOMLastAppliedIndex =
        followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    assertTrue(
        followerOMLastAppliedIndex >= leaderOMSnapshotIndex - 1);

    // After the new checkpoint is installed, the follower OM
    // lastAppliedIndex must >= the snapshot index of the checkpoint. It
    // could be great than snapshot index if there is any conf entry from ratis.
    followerOMLastAppliedIndex = followerOM.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex();
    assertTrue(followerOMLastAppliedIndex >= leaderOMSnapshotIndex);
    assertTrue(followerOM.getOmRatisServer().getLastAppliedTermIndex()
        .getTerm() >= leaderOMSnapshotTermIndex);

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

    // Wait installation finish
    Thread.sleep(5000);
    // Verify checkpoint installation was happened.
    assertLogCapture(logCapture, "Reloaded OM state");
    assertLogCapture(logCapture, "Install Checkpoint is finished");
  }

  @Test
  public void testInstallOldCheckpointFailure() throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);

    // Find the inactive OM and start it
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    cluster.startInactiveOM(followerNodeId);
    GenericTestUtils.setLogLevel(OzoneManager.LOG, Level.INFO);
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.LOG);

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

    // Install the old checkpoint on the follower OM. This should fail as the
    // followerOM is already ahead of that transactionLogIndex and the OM
    // state should be reloaded.
    TermIndex followerTermIndex = followerRatisServer.getLastAppliedTermIndex();
    TermIndex newTermIndex = followerOM.installCheckpoint(
        leaderOMNodeId, leaderDbCheckpoint);

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
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

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
    TransactionInfo leaderCheckpointTrxnInfo = OzoneManagerRatisUtils
        .getTrxnInfoFromCheckpoint(conf, leaderCheckpointLocation);

    // Corrupt the leader checkpoint and install that on the OM. The
    // operation should fail and OM should shutdown.
    boolean delete = true;
    for (File file : leaderCheckpointLocation.toFile()
        .listFiles()) {
      if (file.getName().contains(".sst")) {
        if (delete) {
          file.delete();
          delete = false;
        } else {
          delete = true;
        }
      }
    }

    GenericTestUtils.setLogLevel(OzoneManager.LOG, Level.INFO);
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.LOG);
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

  @Test
  @DisplayName("testSnapshotBackgroundServices")
  @SuppressWarnings("methodlength")
  public void testSnapshotBackgroundServices()
      throws IOException, InterruptedException, TimeoutException {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();
    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Create some snapshots, each with new keys
    int keyIncrement = 10;
    String snapshotNamePrefix = "snapshot";
    String snapshotName = "";
    List<String> keys = new ArrayList<>();
    SnapshotInfo snapshotInfo = null;
    for (int snapshotCount = 0; snapshotCount < 10;
         snapshotCount++) {
      snapshotName = snapshotNamePrefix + snapshotCount;
      keys = writeKeys(keyIncrement);
      snapshotInfo = createOzoneSnapshot(leaderOM, snapshotName);
    }

    // Get the latest db checkpoint from the leader OM.
    TransactionInfo transactionInfo =
        TransactionInfo.readTransactionInfo(leaderOM.getMetadataManager());
    TermIndex leaderOMTermIndex =
        TermIndex.valueOf(transactionInfo.getTerm(),
            transactionInfo.getTransactionIndex());
    long leaderOMSnapshotIndex = leaderOMTermIndex.getIndex();

    // Start the inactive OM. Checkpoint installation will happen spontaneously.
    cluster.startInactiveOM(followerNodeId);
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.LOG);

    // The recently started OM should be lagging behind the leader OM.
    // Wait & for follower to update transactions to leader snapshot index.
    // Timeout error if follower does not load update within 10s
    GenericTestUtils.waitFor(() ->
        followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
            >= leaderOMSnapshotIndex - 1, 100, 10000);


    // Verify RPC server is running
    GenericTestUtils.waitFor(followerOM::isOmRpcServerRunning, 100, 5000);

    // Read & Write after snapshot installed.
    List<String> newKeys = writeKeys(1);
    readKeys(newKeys);

    checkSnapshot(leaderOM, followerOM, snapshotName, keys, snapshotInfo);

    // verify that the bootstrap Follower OM can become leader again
    leaderOM.transferLeadership(followerNodeId);

    GenericTestUtils.waitFor(() -> {
      try {
        followerOM.checkLeaderStatus();
        return true;
      } catch (OMNotLeaderException | OMLeaderNotReadyException e) {
        return false;
      }
    }, 1000, 10000);
    OzoneManager newLeaderOM = cluster.getOMLeader();
    Assertions.assertEquals(followerOM, newLeaderOM);
    OzoneManager newFollowerOM =
        cluster.getOzoneManager(leaderOM.getOMNodeId());
    Assertions.assertEquals(leaderOM, newFollowerOM);

    checkSnapshot(newLeaderOM, newFollowerOM, snapshotName, keys, snapshotInfo);
    readKeys(newKeys);

    // Prepare baseline data for compaction logs
    String currentCompactionLogPath = newLeaderOM
        .getMetadataManager()
        .getStore()
        .getRocksDBCheckpointDiffer()
        .getCurrentCompactionLogPath();
    int lastIndex = currentCompactionLogPath.lastIndexOf(OM_KEY_PREFIX);
    String compactionLogsPath = currentCompactionLogPath
        .substring(0, lastIndex);
    int numberOfLogFiles = 0;
    long contentLength;
    Path compactionLogPath = Paths.get(compactionLogsPath);
    Path currentCompactionLog = Paths.get(currentCompactionLogPath);
    try (BufferedReader bufferedReader =
             Files.newBufferedReader(currentCompactionLog);
         DirectoryStream<Path> files =
             Files.newDirectoryStream(compactionLogPath)) {
      contentLength = bufferedReader.lines()
          .mapToLong(String::length)
          .reduce(0L, Long::sum);
      for (Path ignored : files) {
        numberOfLogFiles++;
      }
    }

    // Check whether newly created snapshot gets processed by SFS
    newKeys = writeKeys(1);
    SnapshotInfo newSnapshot = createOzoneSnapshot(newLeaderOM,
        snapshotName + RandomStringUtils.randomNumeric(5));
    Assertions.assertNotNull(newSnapshot);
    File omMetadataDir =
        OMStorage.getOmDbDir(newLeaderOM.getConfiguration());
    String snapshotDir = omMetadataDir + OM_KEY_PREFIX + OM_SNAPSHOT_DIR;
    Path filePath =
        Paths.get(snapshotDir + OM_KEY_PREFIX + FILTERED_SNAPSHOTS);
    Assertions.assertTrue(Files.exists(filePath));
    GenericTestUtils.waitFor(() -> {
      List<String> processedSnapshotIds;
      try {
        processedSnapshotIds = Files.readAllLines(filePath);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      return processedSnapshotIds.contains(newSnapshot.getSnapshotId()
          .toString());
    }, 1000, 30000);

    // Check whether newly created snapshot data is reclaimed
    ozoneBucket.deleteKeys(newKeys);
    client.getObjectStore()
        .deleteSnapshot(volumeName, bucketName, newSnapshot.getName());
    GenericTestUtils.waitFor(() -> {
      Table<String, SnapshotInfo> snapshotInfoTable =
          newLeaderOM.getMetadataManager().getSnapshotInfoTable();
      try {
        return null == snapshotInfoTable.get(newSnapshot.getTableKey());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 1000, 10000);

    // Check whether newly created keys data is reclaimed
    Table<String, OmKeyInfo> omKeyInfoTableBeforeDeletion = newLeaderOM
        .getMetadataManager()
        .getKeyTable(ozoneBucket.getBucketLayout());
    String newKey = OM_KEY_PREFIX + ozoneBucket.getVolumeName() +
        OM_KEY_PREFIX + ozoneBucket.getName() +
        OM_KEY_PREFIX + newKeys.get(0);
    Assertions.assertNull(omKeyInfoTableBeforeDeletion.get(newKey));
    GenericTestUtils.waitFor(() -> {
      Table<String, OmKeyInfo> omKeyInfoTableAfterDeletion = newLeaderOM
          .getMetadataManager()
          .getKeyTable(ozoneBucket.getBucketLayout());
      try {
        return Objects.isNull(omKeyInfoTableAfterDeletion.get(newKey));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 1000, 10000);

    // Check whether compaction logs get appeneded to
    // Force compaction
    newLeaderOM.getMetadataManager()
        .getStore()
        .compactDB();
    int newNumberOfLogFiles = 0;
    long newContentLength;
    try (BufferedReader bufferedReader =
             Files.newBufferedReader(currentCompactionLog);
         DirectoryStream<Path> files =
             Files.newDirectoryStream(compactionLogPath)) {
      newContentLength = bufferedReader.lines()
          .mapToLong(String::length)
          .reduce(0L, Long::sum);
      for (Path ignored : files) {
        newNumberOfLogFiles++;
      }
    }
    Assertions.assertTrue(numberOfLogFiles != newNumberOfLogFiles
        || contentLength != newContentLength);
  }

  private SnapshotInfo createOzoneSnapshot(OzoneManager leaderOM, String name)
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
        getSnapshotPath(leaderOM.getConfiguration(), snapshotInfo);
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
      Thread.sleep(100);
      logIndex = omRatisServer.getLastAppliedTermIndex().getIndex();
    }
    return keys;
  }

  private List<String> writeKeys(long keyCount) throws IOException {
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
      OzoneInputStream inputStream = ozoneBucket.readKey(keyName);
      byte[] data = new byte[100];
      inputStream.read(data, 0, 100);
      inputStream.close();
    }
  }

  private void assertLogCapture(GenericTestUtils.LogCapturer logCapture,
                              String msg)
      throws InterruptedException, TimeoutException {
    GenericTestUtils.waitFor(() -> {
      return logCapture.getOutput().contains(msg);
    }, 100, 5000);
  }

  // Returns temp dir where tarball was untarred.
  private void unTarLatestTarBall(OzoneManager followerOm, Path tempDir)
      throws IOException {
    File snapshotDir = followerOm.getOmSnapshotProvider().getSnapshotDir();
    // Find the latest tarball.
    String tarBall = Arrays.stream(Objects.requireNonNull(snapshotDir.list())).
        filter(s -> s.toLowerCase().endsWith(".tar")).
        reduce("", (s1, s2) -> s1.compareToIgnoreCase(s2) > 0 ? s1 : s2);
    FileUtil.unTar(new File(snapshotDir, tarBall), tempDir.toFile());
  }

  private static class DummyExitManager extends ExitManager {
    @Override
    public void exitSystem(int status, String message, Throwable throwable,
        Logger log) {
      log.error("System Exit: " + message, throwable);
    }
  }

  private static class SnapshotPauseInjector extends FaultInjector {
    private CountDownLatch ready;
    private CountDownLatch wait;

    SnapshotPauseInjector() {
      init();
    }

    @Override
    public void init() {
      this.ready = new CountDownLatch(1);
      this.wait = new CountDownLatch(1);
    }

    @Override
    public void pause() throws IOException {
      ready.countDown();
      try {
        wait.await();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void resume() throws IOException {
      // Make sure injector pauses before resuming.
      try {
        ready.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
        assertTrue(Fail.fail("resume interrupted"));
      }
      wait.countDown();
    }

    @Override
    public void reset() throws IOException {
      init();
    }
  }
}
