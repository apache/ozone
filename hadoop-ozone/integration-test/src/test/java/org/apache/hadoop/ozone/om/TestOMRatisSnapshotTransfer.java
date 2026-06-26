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

import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.TestDataUtil.readFully;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.OM_HARDLINK_FILE;
import static org.apache.hadoop.ozone.om.TestOzoneManagerHAWithStoppedNodes.createKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.DBCheckpointMetrics;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.InodeMetadataRocksDBCheckpoint;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.audit.AuditLogTestUtils;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.conf.OMClientConfig;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServerConfig;
import org.apache.hadoop.utils.FaultInjectorImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ozone.test.tag.Unhealthy;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ValueSource;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests OM Ratis snapshot installs that exercise the checkpoint transfer
 * (tarball download) path, parameterized over both checkpoint formats
 * (v1 and inode-based v2). Transfer-independent install tests live in
 * {@link TestOMRatisSnapshots}.
 */
@ParameterizedClass
@ValueSource(booleans = {false, true})
public class TestOMRatisSnapshotTransfer {
  // tried up to 1000 snapshots and this test works, but some of the
  //  timeouts have to be increased.
  private static final int SNAPSHOTS_TO_CREATE = 100;
  private static final String OM_SERVICE_ID = "om-service-test1";
  private static final int NUM_OF_OMS = 3;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOMRatisSnapshotTransfer.class);

  private MiniOzoneHAClusterImpl cluster = null;
  private ObjectStore objectStore;
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
  @Parameter
  private boolean useInodeBasedCheckpoint;

  @BeforeEach
  public void init(TestInfo testInfo) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP, LOG_PURGE_GAP);
    conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_SEGMENT_SIZE_KEY, 16,
        StorageUnit.KB);
    conf.setStorageSize(OMConfigKeys.
        OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY, 16, StorageUnit.KB);
    conf.setBoolean(OMConfigKeys.OZONE_OM_DB_CHECKPOINT_USE_INODE_BASED_KEY, useInodeBasedCheckpoint);
    long snapshotThreshold = SNAPSHOT_THRESHOLD;
    // TODO: refactor tests to run under a new class with different configs.
    if (testInfo.getTestMethod().isPresent() &&
        testInfo.getTestMethod().get().getName()
            .equals("testInstallSnapshot")) {
      snapshotThreshold = SNAPSHOT_THRESHOLD * 10;
      AuditLogTestUtils.enableAuditLog();
    }
    conf.setLong(
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        snapshotThreshold);

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

    volumeName = "volume" + RandomStringUtils.secure().nextNumeric(5);
    bucketName = "bucket" + RandomStringUtils.secure().nextNumeric(5);

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

  @Test
  public void testInstallSnapshot(@TempDir Path tempDir) throws Exception {
    // Get the leader OM
    final String leaderOMNodeId = OmTestUtil.getCurrentOmProxyNodeId(objectStore);

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    List<Set<String>> sstSetList = new ArrayList<>();
    FaultInjector faultInjector =
        new SnapshotMaxSizeInjector(leaderOM,
            followerOM.getOmSnapshotProvider().getSnapshotDir(), sstSetList,
            tempDir, useInodeBasedCheckpoint);
    followerOM.getOmSnapshotProvider().setInjector(faultInjector);

    // Create some snapshots, each with new keys
    int keyIncrement = 10;
    String snapshotNamePrefix = "snapshot";
    String snapshotName = "";
    List<String> keys = new ArrayList<>();
    SnapshotInfo snapshotInfo = null;
    for (int snapshotCount = 0; snapshotCount < SNAPSHOTS_TO_CREATE; snapshotCount++) {
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
    LogCapturer logCapture = LogCapturer.captureLogs(OzoneManager.class);

    // The recently started OM should be lagging behind the leader OM.
    // Wait & for follower to update transactions to leader snapshot index.
    // Timeout error if follower does not load update within 10s
    GenericTestUtils.waitFor(() -> {
      long index = followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex();
      return index >= leaderOMSnapshotIndex - 1;
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
    }, 100, 30_000);

    assertLogCapture(logCapture,
        "Install Checkpoint is finished");
    String toMatch = String.format(
        "op=DB_CHECKPOINT_INSTALL {\"leaderId\":\"%s\",\"term\":\"%d\",\"lastAppliedIndex\":\"%d\"}",
        leaderOMNodeId, leaderOMSnapshotTermIndex, followerOMLastAppliedIndex);
    assertTrue(AuditLogTestUtils.auditLogContains(toMatch));

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
    int sstFileCount = 0;
    Set<String> sstFileUnion = new HashSet<>();
    for (Set<String> sstFiles : sstSetList) {
      sstFileCount += sstFiles.size();
      sstFileUnion.addAll(sstFiles);
    }
    // Confirm that there were multiple tarballs.
    assertThat(sstSetList.size()).isGreaterThan(1);
    // Confirm that there was no overlap of sst files
    // between the individual tarballs.
    assertEquals(sstFileUnion.size(), sstFileCount);
  }

  private void checkSnapshot(OzoneManager leaderOM, OzoneManager followerOM,
      String snapshotName,
      List<String> keys, SnapshotInfo snapshotInfo) throws RocksDBException, IOException {
    TestOMRatisSnapshots.checkSnapshot(volumeName, bucketName, leaderOM,
        followerOM, snapshotName, keys, snapshotInfo);
  }

  @Test
  @Unhealthy("HDDS-13300")
  public void testInstallIncrementalSnapshot(@TempDir Path tempDir)
      throws Exception {
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

    // Set fault injector to pause before install
    FaultInjector faultInjector = new FaultInjectorImpl();
    followerOM.getOmSnapshotProvider().setInjector(faultInjector);

    // Do some transactions so that the log index increases
    List<String> firstKeys = writeKeysToIncreaseLogIndex(leaderRatisServer,
        100);

    SnapshotInfo snapshotInfo2 = createOzoneSnapshot(leaderOM, "snap100");
    followerOM.getConfiguration().setInt(
        OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL,
        -1);
    // Start the inactive OM. Checkpoint installation will happen spontaneously.
    cluster.startInactiveOM(followerNodeId);

    // Wait the follower download the snapshot,but get stuck by injector
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmSnapshotProvider().getNumDownloaded() == 1;
    }, 1000, 30_000);

    // Get two incremental tarballs, adding new keys/snapshot for each.
    IncrementData firstIncrement = getNextIncrementalTarball(200, 2, leaderOM,
        leaderRatisServer, faultInjector, followerOM, tempDir);
    IncrementData secondIncrement = getNextIncrementalTarball(300, 3, leaderOM,
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
    }, 1000, 30_000);

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
    assertThat(dbMetrics.getLastCheckpointStreamingNumSSTExcluded()).isGreaterThan(0);
    assertEquals(2, dbMetrics.getNumIncrementalCheckpoints());

    // Verify RPC server is running
    GenericTestUtils.waitFor(() -> {
      return followerOM.isOmRpcServerRunning();
    }, 100, 30_000);

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
    }, 100, 30_000);

    // Verify follower candidate directory get cleaned
    String[] filesInCandidate = followerOM.getOmSnapshotProvider().
        getCandidateDir().list();
    assertNotNull(filesInCandidate);
    assertEquals(0, filesInCandidate.length);

    checkSnapshot(leaderOM, followerOM, "snap100", firstKeys, snapshotInfo2);
    checkSnapshot(leaderOM, followerOM, "snap200", firstIncrement.getKeys(),
        firstIncrement.getSnapshotInfo());
    checkSnapshot(leaderOM, followerOM, "snap300", secondIncrement.getKeys(),
        secondIncrement.getSnapshotInfo());
    assertEquals(
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
        expectedNumDownloads, 1000, 30_000);

    assertThat(followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex())
        .isGreaterThanOrEqualTo(leaderOMSnapshotIndex - 1);

    // Now confirm tarball is just incremental and contains no unexpected
    //  files/links.
    Path increment = Paths.get(tempDir.toString(), "increment" + numKeys);
    assertTrue(increment.toFile().mkdirs());
    unTarLatestTarBall(followerOM, increment);
    List<String> sstFiles = HAUtils.getExistingFiles(increment.toFile());
    Path followerCandidatePath = followerOM.getOmSnapshotProvider().
        getCandidateDir().toPath();

    // Confirm that none of the files in the tarball match one in the
    // candidate dir.
    assertThat(sstFiles.size()).isGreaterThan(0);
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
      assertThat(lineCount).isGreaterThan(0);
    }
    return id;
  }

  @Test
  @Unhealthy("HDDS-13300")
  public void testInstallIncrementalSnapshotWithFailure() throws Exception {
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

    // Set fault injector to pause before install
    FaultInjector faultInjector = new FaultInjectorImpl();
    followerOM.getOmSnapshotProvider().setInjector(faultInjector);

    // Do some transactions so that the log index increases
    List<String> firstKeys = writeKeysToIncreaseLogIndex(leaderRatisServer,
        100);

    // Start the inactive OM. Checkpoint installation will happen spontaneously.
    cluster.startInactiveOM(followerNodeId);

    // Wait the follower download the snapshot,but get stuck by injector
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmSnapshotProvider().getNumDownloaded() == 1;
    }, 1000, 30_000);

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
    }, 1000, 30_000);

    // Corrupt the mixed checkpoint in the candidate DB dir
    File followerCandidateDir = followerOM.getOmSnapshotProvider().
        getCandidateDir();
    List<String> sstList = HAUtils.getExistingFiles(followerCandidateDir);
    assertThat(sstList.size()).isGreaterThan(0);
    for (int i = 0; i < sstList.size(); i += 2) {
      File victimSst = new File(followerCandidateDir, sstList.get(i));
      assertTrue(victimSst.delete());
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
    }, 1000, 30_000);

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

    // Verify the metrics
    GenericTestUtils.waitFor(() -> {
      DBCheckpointMetrics dbMetrics =
          leaderOM.getMetrics().getDBCheckpointMetrics();
      return dbMetrics.getLastCheckpointStreamingNumSSTExcluded() == 0;
    }, 100, 30_000);

    GenericTestUtils.waitFor(() -> {
      DBCheckpointMetrics dbMetrics =
          leaderOM.getMetrics().getDBCheckpointMetrics();
      return dbMetrics.getNumIncrementalCheckpoints() >= 1;
    }, 100, 30_000);

    GenericTestUtils.waitFor(() -> {
      DBCheckpointMetrics dbMetrics =
          leaderOM.getMetrics().getDBCheckpointMetrics();
      return dbMetrics.getNumCheckpoints() >= 3;
    }, 100, 30_000);

    // Verify RPC server is running
    GenericTestUtils.waitFor(() -> {
      return followerOM.isOmRpcServerRunning();
    }, 100, 30_000);

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
    }, 100, 30_000);

    // Verify follower candidate directory get cleaned
    String[] filesInCandidate = followerOM.getOmSnapshotProvider().
        getCandidateDir().list();
    assertNotNull(filesInCandidate);
    assertEquals(0, filesInCandidate.length);
  }

  private SnapshotInfo createOzoneSnapshot(OzoneManager leaderOM, String name)
      throws IOException {
    return TestOMRatisSnapshots.createOzoneSnapshot(objectStore, volumeName,
        bucketName, leaderOM, name);
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
    return TestOMRatisSnapshots.writeKeys(ozoneBucket, keyCount);
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

  // Returns temp dir where tarball was untarred.
  private void unTarLatestTarBall(OzoneManager followerOm, Path tempDir)
      throws IOException {
    File snapshotDir = followerOm.getOmSnapshotProvider().getSnapshotDir();
    // Find the latest tarball.
    String[] list = snapshotDir.list();
    assertNotNull(list);
    String tarBall = Arrays.stream(list).
        filter(s -> s.toLowerCase().endsWith(".tar")).
        reduce("", (s1, s2) -> s1.compareToIgnoreCase(s2) > 0 ? s1 : s2);
    FileUtil.unTar(new File(snapshotDir, tarBall), tempDir.toFile());
  }

  // Interrupts the tarball download process to test creation of
  // multiple tarballs as needed when the tarball size exceeds the
  // max.
  private static class SnapshotMaxSizeInjector extends FaultInjector {
    private final OzoneManager om;
    private int count;
    private final File snapshotDir;
    private final List<Set<String>> sstSetList;
    private final Path tempDir;
    private boolean useInodeBasedCheckpoint;

    SnapshotMaxSizeInjector(OzoneManager om, File snapshotDir,
                            List<Set<String>> sstSetList, Path tempDir,
        boolean useInodeBasedCheckpoint) {
      this.om = om;
      this.snapshotDir = snapshotDir;
      this.sstSetList = sstSetList;
      this.tempDir = tempDir;
      this.useInodeBasedCheckpoint = useInodeBasedCheckpoint;
      init();
    }

    @Override
    public void init() {
    }

    @Override
    // Pause each time a tarball is received, to process it.
    public void pause() throws IOException {
      count++;
      File tarball = getTarball(snapshotDir);
      // First time through, get total size of sst files and reduce
      // max size config.  That way next time through, we get multiple
      // tarballs.
      if (count == 1) {
        long sstSize = getSizeOfSstFiles(tarball);
        LOG.info("Setting ozone.om.ratis.snapshot.max.total.sst.size to {}", sstSize);
        om.getConfiguration().setLong(
            OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY, sstSize / 2);
        // Now empty the tarball to restart the download
        // process from the beginning.
        createEmptyTarball(tarball);
      } else {
        // Each time we get a new tarball add a set of
        // its sst file to the list, (i.e. one per tarball.)
        sstSetList.add(getFilenames(tarball));
      }
    }

    // Get Size of sstfiles in tarball.
    private long getSizeOfSstFiles(File tarball) throws IOException {
      FileUtil.unTar(tarball, tempDir.toFile());
      InodeMetadataRocksDBCheckpoint obtainedCheckpoint =
          new InodeMetadataRocksDBCheckpoint(tempDir, useInodeBasedCheckpoint);
      assertNotNull(obtainedCheckpoint);
      Path omDbDir = Paths.get(obtainedCheckpoint.getCheckpointLocation().toString(), OM_DB_NAME);
      assertNotNull(omDbDir);
      List<Path> sstPaths = Files.list(omDbDir).collect(Collectors.toList());
      long totalFileSize = 0;
      int numFiles = 0;
      for (Path sstPath : sstPaths) {
        File file = sstPath.toFile();
        if (file.isFile() && file.getName().endsWith(".sst")) {
          totalFileSize += Files.size(sstPath);
          numFiles++;
        }
      }
      LOG.info("Total num files {}",  numFiles);
      return totalFileSize;
    }

    private void createEmptyTarball(File dummyTarFile)
        throws IOException {
      OutputStream fileOutputStream = Files.newOutputStream(dummyTarFile.toPath());
      TarArchiveOutputStream archiveOutputStream =
          new TarArchiveOutputStream(fileOutputStream);
      archiveOutputStream.close();
    }

    // Return a list of files in tarball.
    private Set<String> getFilenames(File tarball)
        throws IOException {
      Set<String> fileNames = new HashSet<>();
      try (TarArchiveInputStream tarInput =
           new TarArchiveInputStream(Files.newInputStream(tarball.toPath()))) {
        TarArchiveEntry entry;
        while ((entry = tarInput.getNextTarEntry()) != null) {
          fileNames.add(entry.getName());
        }
      }
      return fileNames;
    }

    // Find the tarball in the dir.
    private File getTarball(File dir) {
      File[] fileList = dir.listFiles();
      assertNotNull(fileList);
      for (File f : fileList) {
        if (f.getName().toLowerCase().endsWith(".tar")) {
          return f;
        }
      }
      return null;
    }

    @Override
    public void resume() throws IOException {
    }

    @Override
    public void reset() throws IOException {
      init();
    }
  }
}
