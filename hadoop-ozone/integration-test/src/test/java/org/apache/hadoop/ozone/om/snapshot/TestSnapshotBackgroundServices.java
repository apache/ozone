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

import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_PRUNE_DAEMON_RUN_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.TestDataUtil.readFully;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPath;
import static org.apache.hadoop.ozone.om.TestOzoneManagerHAWithStoppedNodes.createKey;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.DONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.RDBCheckpointUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.conf.OMClientConfig;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmFailoverProxyUtil;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SstFilteringService;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServerConfig;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.ozone.compaction.log.CompactionLogEntry;
import org.apache.ozone.rocksdiff.CompactionNode;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests snapshot background services.
 */
public class TestSnapshotBackgroundServices {
  private MiniOzoneHAClusterImpl cluster;
  private ObjectStore objectStore;
  private OzoneBucket ozoneBucket;
  private String volumeName;
  private String bucketName;

  private static final long SNAPSHOT_THRESHOLD = 50;
  private static final int LOG_PURGE_GAP = 50;
  // This test depends on direct RocksDB checks that are easier done with OBS buckets.
  private static final BucketLayout TEST_BUCKET_LAYOUT = BucketLayout.OBJECT_STORE;
  private static final String SNAPSHOT_NAME_PREFIX = "snapshot-";
  private static final String KEY_NAME_PREFIX = "key-";
  private OzoneClient client;
  private final AtomicInteger counter = new AtomicInteger();

  @BeforeEach
  public void init(TestInfo testInfo) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    String omServiceId = "om-service-test1";
    OzoneManagerRatisServerConfig omRatisConf = conf.getObject(OzoneManagerRatisServerConfig.class);
    omRatisConf.setLogAppenderWaitTimeMin(10);
    conf.setFromObject(omRatisConf);

    OMClientConfig clientConfig = conf.getObject(OMClientConfig.class);
    clientConfig.setRpcTimeOut(TimeUnit.SECONDS.toMillis(5));
    conf.setFromObject(clientConfig);

    conf.setInt(OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP, LOG_PURGE_GAP);
    conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_SEGMENT_SIZE_KEY, 16, StorageUnit.KB);
    conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY, 16, StorageUnit.KB);
    if ("testSSTFilteringBackgroundService".equals(testInfo.getDisplayName())) {
      conf.setTimeDuration(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL, 1,
          TimeUnit.SECONDS);
    }
    if ("testCompactionLogBackgroundService"
        .equals(testInfo.getDisplayName())) {
      conf.setTimeDuration(OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED, 1,
          TimeUnit.MILLISECONDS);
    }
    if ("testBackupCompactionFilesPruningBackgroundService"
        .equals(testInfo.getDisplayName())) {
      conf.setTimeDuration(OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED, 1,
          TimeUnit.MILLISECONDS);
      conf.setTimeDuration(
          OZONE_OM_SNAPSHOT_COMPACTION_DAG_PRUNE_DAEMON_RUN_INTERVAL, 1,
          TimeUnit.SECONDS);
    }
    if ("testSnapshotAndKeyDeletionBackgroundServices"
        .equals(testInfo.getDisplayName())) {
      conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 1,
          TimeUnit.SECONDS);
      conf.setTimeDuration(OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL, 1,
          TimeUnit.SECONDS);
      conf.setTimeDuration(OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED, 1,
          TimeUnit.MILLISECONDS);
      conf.setTimeDuration(
          OZONE_OM_SNAPSHOT_COMPACTION_DAG_PRUNE_DAEMON_RUN_INTERVAL, 3,
          TimeUnit.SECONDS);
      conf.setTimeDuration(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL, 3,
          TimeUnit.SECONDS);
    }
    conf.setLong(
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        SNAPSHOT_THRESHOLD);
    int numOfOMs = 3;
    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId("om-service-test1")
        .setNumOfOzoneManagers(numOfOMs)
        .setNumOfActiveOMs(2)
        .build();
    if ("testBackupCompactionFilesPruningBackgroundService"
        .equals(testInfo.getDisplayName())) {
      cluster.
          getOzoneManagersList()
          .forEach(
              TestSnapshotBackgroundServices
                  ::suspendBackupCompactionFilesPruning);
    }
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient(omServiceId, conf);
    objectStore = client.getObjectStore();

    volumeName = "volume" + counter.incrementAndGet();
    bucketName = "bucket" + counter.incrementAndGet();

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner("user" + counter.incrementAndGet())
        .setAdmin("admin" + counter.incrementAndGet())
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
  @DisplayName("testSnapshotAndKeyDeletionBackgroundServices")
  @SuppressWarnings("methodlength")
  public void testSnapshotAndKeyDeletionBackgroundServices()
      throws Exception {
    OzoneManager leaderOM = getLeaderOM();
    OzoneManager followerOM = getInactiveFollowerOM(leaderOM);

    createSnapshotsEachWithNewKeys(leaderOM);

    startInactiveFollower(leaderOM, followerOM, () -> {
    });

    // Read & Write after snapshot installed.
    List<String> newKeys = writeKeys(1);
    readKeys(newKeys);

    OzoneManager newLeaderOM =
        getNewLeader(leaderOM, followerOM.getOMNodeId(), followerOM);
    OzoneManager newFollowerOM =
        cluster.getOzoneManager(leaderOM.getOMNodeId());
    assertEquals(leaderOM, newFollowerOM);

    SnapshotInfo newSnapshot = createOzoneSnapshot(newLeaderOM, SNAPSHOT_NAME_PREFIX + counter.incrementAndGet());

    /*
      Check whether newly created key data is reclaimed
      create key a
      create snapshot b
      delete key a
      create snapshot c
      assert that a is in c's deleted table
      create snapshot d
      delete snapshot c
      wait until key a appears in deleted table of d.
    */
    // create key a
    String keyNameA = writeKeys(1).get(0);
    String keyA = OM_KEY_PREFIX + ozoneBucket.getVolumeName() +
        OM_KEY_PREFIX + ozoneBucket.getName() +
        OM_KEY_PREFIX + keyNameA;
    Table<String, OmKeyInfo> omKeyInfoTable = newLeaderOM
        .getMetadataManager()
        .getKeyTable(ozoneBucket.getBucketLayout());
    OmKeyInfo keyInfoA = omKeyInfoTable.get(keyA);
    assertNotNull(keyInfoA);

    // create snapshot b
    SnapshotInfo snapshotInfoB = createOzoneSnapshot(newLeaderOM, SNAPSHOT_NAME_PREFIX + counter.incrementAndGet());
    assertNotNull(snapshotInfoB);

    // delete key a
    ozoneBucket.deleteKey(keyNameA);

    LambdaTestUtils.await(10000, 1000,
        () -> !isKeyInTable(keyA, omKeyInfoTable));

    // create snapshot c
    SnapshotInfo snapshotInfoC = createOzoneSnapshot(newLeaderOM, SNAPSHOT_NAME_PREFIX + counter.incrementAndGet());

    // get snapshot c
    OmSnapshot snapC;
    try (UncheckedAutoCloseableSupplier<OmSnapshot> rcC = newLeaderOM
        .getOmSnapshotManager()
        .getSnapshot(volumeName, bucketName, snapshotInfoC.getName())) {
      assertNotNull(rcC);
      snapC = rcC.get();
    }

    // assert that key a is in snapshot c's deleted table
    LambdaTestUtils.await(10000, 1000,
        () -> isKeyInTable(keyA, snapC.getMetadataManager().getDeletedTable()));

    // create snapshot d
    SnapshotInfo snapshotInfoD = createOzoneSnapshot(newLeaderOM, SNAPSHOT_NAME_PREFIX + counter.incrementAndGet());

    // delete snapshot c
    client.getObjectStore()
        .deleteSnapshot(volumeName, bucketName, snapshotInfoC.getName());

    LambdaTestUtils.await(30000, 1000, () ->
        !isKeyInTable(snapshotInfoC.getTableKey(),
            newLeaderOM.getMetadataManager().getSnapshotInfoTable()));

    // get snapshot d
    OmSnapshot snapD;
    try (UncheckedAutoCloseableSupplier<OmSnapshot> rcD = newLeaderOM
        .getOmSnapshotManager()
        .getSnapshot(volumeName, bucketName, snapshotInfoD.getName())) {
      assertNotNull(rcD);
      snapD = rcD.get();
    }

    // wait until key a appears in deleted table of snapshot d
    LambdaTestUtils.await(10000, 1000,
        () -> isKeyInTable(keyA, snapD.getMetadataManager().getDeletedTable()));

    // Confirm entry for deleted snapshot removed from info table
    client.getObjectStore()
        .deleteSnapshot(volumeName, bucketName, newSnapshot.getName());
    LambdaTestUtils.await(10000, 1000,
        () -> isKeyInTable(newSnapshot.getTableKey(),
            newLeaderOM.getMetadataManager().getSnapshotInfoTable()));

    confirmSnapDiffForTwoSnapshotsDifferingBySingleKey(
        newLeaderOM);
  }

  private static <V> boolean isKeyInTable(String key, Table<String, V> table) {
    try (Table.KeyValueIterator<String, V> iterator
             = table.iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, V> next = iterator.next();
        if (next.getKey().contains(key)) {
          return true;
        }
      }
      return false;
    } catch (IOException e) {
      return false;
    }
  }

  private void startInactiveFollower(OzoneManager leaderOM,
                                     OzoneManager followerOM,
                                     Runnable actionAfterStarting)
      throws IOException, TimeoutException, InterruptedException {
    // Get the latest db checkpoint from the leader OM.
    TransactionInfo transactionInfo =
        TransactionInfo.readTransactionInfo(leaderOM.getMetadataManager());
    TermIndex leaderOMTermIndex =
        TermIndex.valueOf(transactionInfo.getTerm(),
            transactionInfo.getTransactionIndex());
    long leaderOMSnapshotIndex = leaderOMTermIndex.getIndex();

    // Start the inactive OM. Checkpoint installation will happen spontaneously.
    cluster.startInactiveOM(followerOM.getOMNodeId());
    actionAfterStarting.run();

    // The recently started OM should be lagging behind the leader OM.
    // Wait & for follower to update transactions to leader snapshot index.
    // Timeout error if follower does not load update within 10s
    GenericTestUtils.waitFor(() ->
        followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
            >= leaderOMSnapshotIndex - 1, 100, 10000);

    // Verify RPC server is running
    GenericTestUtils.waitFor(followerOM::isOmRpcServerRunning, 100, 5000);
  }

  private void createSnapshotsEachWithNewKeys(OzoneManager ozoneManager)
      throws IOException {
    int keyIncrement = 10;
    for (int snapshotCount = 0; snapshotCount < 10;
         snapshotCount++) {
      String snapshotName = SNAPSHOT_NAME_PREFIX + snapshotCount;
      writeKeys(keyIncrement);
      createOzoneSnapshot(ozoneManager, snapshotName);
    }
  }

  private OzoneManager getInactiveFollowerOM(OzoneManager leaderOM) {
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    return cluster.getOzoneManager(followerNodeId);
  }

  private OzoneManager getLeaderOM() {
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();
    return cluster.getOzoneManager(leaderOMNodeId);
  }

  @Test
  @DisplayName("testCompactionLogBackgroundService")
  @Flaky("HDDS-11672")
  public void testCompactionLogBackgroundService()
      throws IOException, InterruptedException, TimeoutException {
    OzoneManager leaderOM = getLeaderOM();
    OzoneManager followerOM = getInactiveFollowerOM(leaderOM);

    createSnapshotsEachWithNewKeys(leaderOM);

    startInactiveFollower(leaderOM, followerOM,
        () -> suspendBackupCompactionFilesPruning(followerOM));

    // Read & Write after snapshot installed.
    List<String> newKeys = writeKeys(1);
    readKeys(newKeys);

    OzoneManager newLeaderOM =
        getNewLeader(leaderOM, followerOM.getOMNodeId(), followerOM);
    OzoneManager newFollowerOM =
        cluster.getOzoneManager(leaderOM.getOMNodeId());
    assertEquals(leaderOM, newFollowerOM);

    List<CompactionLogEntry> compactionLogEntriesOnPreviousLeader =
        getCompactionLogEntries(leaderOM);

    List<CompactionLogEntry> compactionLogEntriesOnNewLeader =
        getCompactionLogEntries(newLeaderOM);
    assertEquals(compactionLogEntriesOnPreviousLeader,
        compactionLogEntriesOnNewLeader);

    assertEquals(leaderOM.getMetadataManager().getStore()
            .getRocksDBCheckpointDiffer().getForwardCompactionDAG().nodes()
            .stream().map(CompactionNode::getFileName).collect(toSet()),
        newLeaderOM.getMetadataManager().getStore()
            .getRocksDBCheckpointDiffer().getForwardCompactionDAG().nodes()
            .stream().map(CompactionNode::getFileName).collect(toSet()));
    assertEquals(leaderOM.getMetadataManager().getStore()
            .getRocksDBCheckpointDiffer().getForwardCompactionDAG().edges()
            .stream().map(edge ->
                edge.source().getFileName() + "-" + edge.target().getFileName())
            .collect(toSet()),
        newLeaderOM.getMetadataManager().getStore()
            .getRocksDBCheckpointDiffer().getForwardCompactionDAG().edges()
            .stream().map(edge ->
                edge.source().getFileName() + "-" + edge.target().getFileName())
            .collect(toSet()));

    confirmSnapDiffForTwoSnapshotsDifferingBySingleKey(newLeaderOM);
  }

  private List<CompactionLogEntry> getCompactionLogEntries(OzoneManager om)
      throws IOException {
    List<CompactionLogEntry> compactionLogEntries = new ArrayList<>();
    try (Table.KeyValueIterator<String, CompactionLogEntry>
             iterator = om.getMetadataManager().getCompactionLogTable()
        .iterator()) {
      iterator.seekToFirst();

      while (iterator.hasNext()) {
        compactionLogEntries.add(iterator.next().getValue());
      }
    }
    return compactionLogEntries;
  }

  @Test
  @DisplayName("testBackupCompactionFilesPruningBackgroundService")
  public void testBackupCompactionFilesPruningBackgroundService()
      throws IOException, InterruptedException, TimeoutException {
    OzoneManager leaderOM = getLeaderOM();
    OzoneManager followerOM = getInactiveFollowerOM(leaderOM);

    startInactiveFollower(leaderOM, followerOM,
        () -> suspendBackupCompactionFilesPruning(followerOM));

    // Read & Write after snapshot installed.
    List<String> newKeys = writeKeys(1);
    readKeys(newKeys);

    OzoneManager newLeaderOM =
        getNewLeader(leaderOM, followerOM.getOMNodeId(), followerOM);
    OzoneManager newFollowerOM =
        cluster.getOzoneManager(leaderOM.getOMNodeId());
    assertEquals(leaderOM, newFollowerOM);

    createSnapshotsEachWithNewKeys(newLeaderOM);

    File sstBackupDir = getSstBackupDir(newLeaderOM);
    File[] files = sstBackupDir.listFiles();
    assertNotNull(files);
    int numberOfSstFiles = files.length;

    resumeBackupCompactionFilesPruning(newLeaderOM);

    checkIfCompactionBackupFilesWerePruned(sstBackupDir,
        numberOfSstFiles);

    confirmSnapDiffForTwoSnapshotsDifferingBySingleKey(newLeaderOM);
  }

  private static void resumeBackupCompactionFilesPruning(
      OzoneManager ozoneManager) {
    ozoneManager
        .getMetadataManager()
        .getStore()
        .getRocksDBCheckpointDiffer()
        .resume();
  }

  private static void suspendBackupCompactionFilesPruning(
      OzoneManager ozoneManager) {
    ozoneManager
        .getMetadataManager()
        .getStore()
        .getRocksDBCheckpointDiffer()
        .suspend();
  }

  @Test
  @DisplayName("testSSTFilteringBackgroundService")
  public void testSSTFilteringBackgroundService()
      throws IOException, InterruptedException, TimeoutException {
    OzoneManager leaderOM = getLeaderOM();
    OzoneManager followerOM = getInactiveFollowerOM(leaderOM);

    createSnapshotsEachWithNewKeys(leaderOM);

    startInactiveFollower(leaderOM, followerOM, () -> {
    });

    // Read & Write after snapshot installed.
    List<String> newKeys = writeKeys(1);
    readKeys(newKeys);

    OzoneManager newLeaderOM =
        getNewLeader(leaderOM, followerOM.getOMNodeId(), followerOM);
    OzoneManager newFollowerOM =
        cluster.getOzoneManager(leaderOM.getOMNodeId());
    assertEquals(leaderOM, newFollowerOM);

    checkIfSnapshotGetsProcessedBySFS(newLeaderOM);

    confirmSnapDiffForTwoSnapshotsDifferingBySingleKey(
        newLeaderOM);
  }

  private void confirmSnapDiffForTwoSnapshotsDifferingBySingleKey(
      OzoneManager ozoneManager)
      throws IOException, InterruptedException, TimeoutException {
    String firstSnapshot = createOzoneSnapshot(ozoneManager, SNAPSHOT_NAME_PREFIX + counter.incrementAndGet())
        .getName();
    String diffKey = writeKeys(1).get(0);
    String secondSnapshot = createOzoneSnapshot(ozoneManager, SNAPSHOT_NAME_PREFIX + counter.incrementAndGet())
        .getName();
    SnapshotDiffReportOzone diff = getSnapDiffReport(volumeName, bucketName, firstSnapshot, secondSnapshot);
    assertEquals(Collections.singletonList(
            SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.CREATE, diffKey, null)),
        diff.getDiffList());
  }

  private static void checkIfCompactionBackupFilesWerePruned(
      File sstBackupDir,
      int numberOfSstFiles
  ) throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      int newNumberOfSstFiles = Objects.requireNonNull(
          sstBackupDir.listFiles()).length;
      return numberOfSstFiles > newNumberOfSstFiles;
    }, 1000, 10000);
  }

  private static File getSstBackupDir(OzoneManager ozoneManager) {
    String sstBackupDirPath = ozoneManager
        .getMetadataManager()
        .getStore()
        .getRocksDBCheckpointDiffer()
        .getSSTBackupDir();
    assertNotNull(sstBackupDirPath);
    File sstBackupDir = new File(sstBackupDirPath);
    assertNotNull(sstBackupDir);
    return sstBackupDir;
  }

  private void checkIfSnapshotGetsProcessedBySFS(OzoneManager ozoneManager)
      throws IOException, TimeoutException, InterruptedException {
    writeKeys(1);
    SnapshotInfo newSnapshot = createOzoneSnapshot(ozoneManager, SNAPSHOT_NAME_PREFIX + counter.incrementAndGet());
    assertNotNull(newSnapshot);
    Table<String, SnapshotInfo> snapshotInfoTable =
        ozoneManager.getMetadataManager().getSnapshotInfoTable();
    GenericTestUtils.waitFor(() -> {
      SnapshotInfo snapshotInfo = null;
      try {
        snapshotInfo = snapshotInfoTable.get(newSnapshot.getTableKey());
      } catch (IOException e) {
        fail();
      }
      return SstFilteringService.isSstFiltered(ozoneManager.getConfiguration(), snapshotInfo);
    }, 1000, 10000);
  }

  private OzoneManager getNewLeader(OzoneManager leaderOM,
                                    String followerNodeId,
                                    OzoneManager followerOM)
      throws IOException, TimeoutException, InterruptedException {
    verifyLeadershipTransfer(leaderOM, followerNodeId, followerOM);
    OzoneManager newLeaderOM = cluster.getOMLeader();
    assertEquals(followerOM, newLeaderOM);
    return newLeaderOM;
  }

  private static void verifyLeadershipTransfer(OzoneManager leaderOM,
                                               String followerNodeId,
                                               OzoneManager followerOM)
      throws IOException, TimeoutException, InterruptedException {
    leaderOM.transferLeadership(followerNodeId);

    GenericTestUtils.waitFor(() -> {
      try {
        followerOM.checkLeaderStatus();
        return true;
      } catch (OMNotLeaderException | OMLeaderNotReadyException e) {
        return false;
      }
    }, 100, 10000);
  }

  private SnapshotDiffReportOzone getSnapDiffReport(String volume,
                                                    String bucket,
                                                    String fromSnapshot,
                                                    String toSnapshot)
      throws InterruptedException, TimeoutException {
    AtomicReference<SnapshotDiffResponse> response = new AtomicReference<>();
    AtomicLong responseInMillis = new AtomicLong(100L);
    GenericTestUtils.waitFor(() -> {
      try {
        response.set(client.getObjectStore()
            .snapshotDiff(
                volume, bucket, fromSnapshot, toSnapshot, null, 0, false,
                false));
        responseInMillis.set(response.get().getWaitTimeInMs());
        return response.get().getJobStatus() == DONE;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, responseInMillis.intValue(), 10000);

    return response.get().getSnapshotDiffReport();
  }

  private SnapshotInfo createOzoneSnapshot(OzoneManager leaderOM, String name) throws IOException {
    objectStore.createSnapshot(volumeName, bucketName, name);

    String tableKey = SnapshotInfo.getTableKey(volumeName, bucketName, name);
    SnapshotInfo snapshotInfo = leaderOM.getMetadataManager()
        .getSnapshotInfoTable()
        .get(tableKey);
    // Allow the snapshot to be written to disk
    String fileName = getSnapshotPath(leaderOM.getConfiguration(), snapshotInfo, 0);
    File snapshotDir = new File(fileName);
    if (!RDBCheckpointUtils.waitForCheckpointDirectoryExist(snapshotDir)) {
      throw new IOException("snapshot directory doesn't exist");
    }
    return snapshotInfo;
  }

  private List<String> writeKeys(long keyCount) throws IOException {
    List<String> keys = new ArrayList<>();
    long index = 0;
    while (index < keyCount) {
      String key = KEY_NAME_PREFIX + counter.incrementAndGet();
      createKey(ozoneBucket, key);
      keys.add(key);
      index++;
    }
    return keys;
  }

  private void readKeys(List<String> keys) throws IOException {
    for (String keyName : keys) {
      readFully(ozoneBucket, keyName);
    }
  }
}
