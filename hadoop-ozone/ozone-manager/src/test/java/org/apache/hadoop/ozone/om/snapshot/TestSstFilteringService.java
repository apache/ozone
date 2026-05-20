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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_GRPC_PORT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_PORT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_DEFRAG_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.ozone.test.LambdaTestUtils.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SstFilteringService;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.LiveFileMetaData;

/**
 * Test SST Filtering Service.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestSstFilteringService {
  private static final String SST_FILE_EXTENSION = ".sst";
  private OzoneManagerProtocol writeClient;
  private OzoneManager om;
  private OzoneConfiguration conf;
  private KeyManager keyManager;
  private short countTotalSnapshots = 0;

  @BeforeAll
  void setup(@TempDir Path folder) throws Exception {
    ExitUtils.disableSystemExit();
    conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS, folder.toString());
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setEnum(HDDS_DB_PROFILE, DBProfile.TEST);
    conf.setQuietMode(false);

    OmTestManagers omTestManagers = new OmTestManagers(conf);
    keyManager = omTestManagers.getKeyManager();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();
  }

  @AfterAll
  public void cleanup() throws Exception {
    if (keyManager != null) {
      keyManager.stop();
    }
    if (writeClient != null) {
      writeClient.close();
    }
    if (om != null) {
      om.stop();
    }
  }

  /**
   * Test checks whether for existing snapshots
   * the checkpoint should not have any sst files that do not correspond to
   * the bucket on which create snapshot command was issued.
   * <p>
   * The SSTFiltering service deletes only the last level of
   * sst file (rocksdb behaviour).
   * <p>
   * 1. Create Keys for vol1/buck1 (L0 ssts will be created for vol1/buck1)
   * 2. compact the db (new level SSTS will be created for vol1/buck1)
   * 3. Create keys for vol1/buck2 (L0 ssts will be created for vol1/buck2)
   * 4. Take snapshot on vol1/buck2.
   * 5. The snapshot will contain compacted sst files pertaining to vol1/buck1
   * Wait till the BG service deletes these.
   *
   * @throws IOException - on Failure.
   */
  @Test
  public void testIrrelevantSstFileDeletion()
      throws Exception {
    RDBStore activeDbStore = (RDBStore) om.getMetadataManager().getStore();
    SstFilteringService filteringService =
        keyManager.getSnapshotSstFilteringService();

    final int keyCount = 100;
    String volumeName = "volz";
    String bucketName1 = "buck1";
    createVolume(volumeName);
    addBucketToVolume(volumeName, bucketName1);

    long countExistingSnapshots = filteringService.getSnapshotFilteredCount().get();
    List<LiveFileMetaData> previousFiles = activeDbStore.getDb().getSstFileList();
    List<String> listPreviousFiles = new ArrayList<String>();
    int level0FilesCountDiff = 0;
    int totalFileCountDiff = 0;
    for (LiveFileMetaData fileMetaData : previousFiles) {
      totalFileCountDiff++;
      listPreviousFiles.add(fileMetaData.fileName());
      if (fileMetaData.level() == 0) {
        level0FilesCountDiff++;
      }
    }

    createKeys(volumeName, bucketName1, keyCount / 2);
    activeDbStore.getDb().flush(KEY_TABLE);

    createKeys(volumeName, bucketName1, keyCount / 2);
    activeDbStore.getDb().flush(KEY_TABLE);

    int level0FilesCount = 0;
    int totalFileCount = 0;

    List<LiveFileMetaData> initialsstFileList = activeDbStore.getDb().getSstFileList();
    for (LiveFileMetaData fileMetaData : initialsstFileList) {
      totalFileCount++;
      if (fileMetaData.level() == 0) {
        level0FilesCount++;
      }
    }

    assertEquals(totalFileCount - totalFileCountDiff, level0FilesCount - level0FilesCountDiff);

    activeDbStore.getDb().compactRange(KEY_TABLE);

    int nonLevel0FilesCountAfterCompact = 0;

    List<String> nonLevelOFiles = new ArrayList<>();
    for (LiveFileMetaData fileMetaData : activeDbStore.getDb()
        .getSstFileList()) {
      if (fileMetaData.level() != 0) {
        nonLevel0FilesCountAfterCompact++;
        nonLevelOFiles.add(fileMetaData.fileName());
      }
    }

    assertThat(nonLevel0FilesCountAfterCompact).isGreaterThan(0);

    String bucketName2 = "buck2";
    addBucketToVolume(volumeName, bucketName2);
    createKeys(volumeName, bucketName2, keyCount);

    activeDbStore.getDb().flush(KEY_TABLE);
    List<LiveFileMetaData> allFiles = activeDbStore.getDb().getSstFileList();
    String snapshotName1 = "snapshot1";
    createSnapshot(volumeName, bucketName2, snapshotName1);
    SnapshotInfo snapshotInfo = om.getMetadataManager().getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(volumeName, bucketName2, snapshotName1));
    assertFalse(SstFilteringService.isSstFiltered(om.getConfiguration(), snapshotInfo));
    waitForSnapshotsAtLeast(filteringService, countExistingSnapshots + 1);
    assertEquals(countExistingSnapshots + 1, filteringService.getSnapshotFilteredCount().get());

    Set<String> keysFromActiveDb = getKeysFromDb(om.getMetadataManager(),
        volumeName, bucketName2);
    Set<String> keysFromSnapshot =
        getKeysFromSnapshot(volumeName, bucketName2, snapshotName1);
    assertEquals(keysFromActiveDb, keysFromSnapshot);

    snapshotInfo = om.getMetadataManager().getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(volumeName, bucketName2, snapshotName1));

    String snapshotDirName =
        OmSnapshotManager.getSnapshotPath(conf, snapshotInfo, 0);

    for (LiveFileMetaData file : allFiles) {
      //Skipping the previous files from this check even those also works.
      if (listPreviousFiles.contains(file.fileName())) {
        continue;
      }
      File sstFile =
          new File(snapshotDirName + OM_KEY_PREFIX + file.fileName());
      if (nonLevelOFiles.contains(file.fileName())) {
        assertFalse(sstFile.exists());
      } else {
        assertTrue(sstFile.exists());
      }
    }

    // Need to read the sstFiltered flag which is set in background process and
    // hence snapshotInfo.isSstFiltered() may not work sometimes.
    assertTrue(SstFilteringService.isSstFiltered(om.getConfiguration(),
        om.getMetadataManager().getSnapshotInfoTable().get(SnapshotInfo
            .getTableKey(volumeName, bucketName2, snapshotName1))));

    String snapshotName2 = "snapshot2";
    final long count;
    try (UncheckedAutoCloseable lock = filteringService.getBootstrapStateLock().acquireWriteLock()) {
      count = filteringService.getSnapshotFilteredCount().get();
      createSnapshot(volumeName, bucketName2, snapshotName2);

      assertThrows(TimeoutException.class,
          () -> waitForSnapshotsAtLeast(filteringService, count + 1 + countExistingSnapshots));
      assertEquals(count, filteringService.getSnapshotFilteredCount().get());
    }

    waitForSnapshotsAtLeast(filteringService, count + 1);

    Set<String> keysFromActiveDb2 = getKeysFromDb(om.getMetadataManager(),
        volumeName, bucketName2);
    Set<String> keysFromSnapshot2 =
        getKeysFromSnapshot(volumeName, bucketName2, snapshotName2);
    assertEquals(keysFromActiveDb2, keysFromSnapshot2);
  }

  @Test
  public void testActiveAndDeletedSnapshotCleanup() throws Exception {
    RDBStore activeDbStore = (RDBStore) om.getMetadataManager().getStore();
    String volumeName = "volume1";
    List<String> bucketNames = Arrays.asList("bucket1", "bucket2");

    createVolume(volumeName);
    // Create 2 Buckets
    for (String bucketName : bucketNames) {
      addBucketToVolume(volumeName, bucketName);
    }
    // Write 25 keys in each bucket, 2 sst files would be generated each for
    // keys in a single bucket
    int keyCount = 25;
    for (String bucketName : bucketNames) {
      for (int i = 1; i <= keyCount; i++) {
        createKey(writeClient, volumeName, bucketName, "key" + i);
      }
      activeDbStore.getDb().flush(KEY_TABLE);
      activeDbStore.getDb().compactRange(KEY_TABLE);
    }

    SstFilteringService sstFilteringService =
        keyManager.getSnapshotSstFilteringService();
    sstFilteringService.pause();

    createSnapshot(volumeName, bucketNames.get(0), "snap1");
    createSnapshot(volumeName, bucketNames.get(0), "snap2");

    SnapshotInfo snapshot1Info = om.getMetadataManager().getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(volumeName, bucketNames.get(0), "snap1"));
    File snapshot1Dir =
        new File(OmSnapshotManager.getSnapshotPath(conf, snapshot1Info, 0));
    SnapshotInfo snapshot2Info = om.getMetadataManager().getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(volumeName, bucketNames.get(0), "snap2"));
    File snapshot2Dir =
        new File(OmSnapshotManager.getSnapshotPath(conf, snapshot2Info, 0));

    File snap1Current = new File(snapshot1Dir, "CURRENT");
    File snap2Current = new File(snapshot2Dir, "CURRENT");

    // wait till both checkpoints are created
    await(10_000, 1_000, () -> snap1Current.exists() && snap2Current.exists());

    long snap1SstFileCountBeforeFilter = Arrays.stream(snapshot1Dir.listFiles())
        .filter(f -> f.getName().endsWith(SST_FILE_EXTENSION)).count();
    long snap2SstFileCountBeforeFilter = Arrays.stream(snapshot2Dir.listFiles())
        .filter(f -> f.getName().endsWith(SST_FILE_EXTENSION)).count();

    // delete snap1
    deleteSnapshot(volumeName, bucketNames.get(0), "snap1");
    sstFilteringService.resume();
    // Filtering service will only act on snap2 as it is an active snaphot
    waitForSnapshotsAtLeast(sstFilteringService, countTotalSnapshots);
    long snap1SstFileCountAfterFilter = Arrays.stream(snapshot1Dir.listFiles())
        .filter(f -> f.getName().endsWith(SST_FILE_EXTENSION)).count();
    long snap2SstFileCountAfterFilter = Arrays.stream(snapshot2Dir.listFiles())
        .filter(f -> f.getName().endsWith(SST_FILE_EXTENSION)).count();
    // one sst will be filtered in both active but not in  deleted snapshot
    // as sstFiltering svc won't run on already deleted snapshots but will mark
    // it as filtered.
    assertEquals(countTotalSnapshots, sstFilteringService.getSnapshotFilteredCount().get());
    assertEquals(snap1SstFileCountBeforeFilter, snap1SstFileCountAfterFilter);
    // If method with order 1 is run .sst file from /vol1/buck1 and /vol1/buck2 will be deleted.
    // As part of this method .sst file from /volume1/bucket2/ will be deleted.
    // sstFiltering won't run on deleted snapshots in /volume1/bucket1.
    assertThat(snap2SstFileCountBeforeFilter).isGreaterThan(snap2SstFileCountAfterFilter);
  }

  private void createKeys(String volumeName,
                          String bucketName,
                          int keyCount)
      throws IOException {
    for (int x = 0; x < keyCount; x++) {
      String keyName = "key-" + RandomStringUtils.secure().nextAlphanumeric(5);
      createKey(writeClient, volumeName, bucketName, keyName);
    }
  }

  private void createVolume(String volumeName)
      throws IOException {
    OMRequestTestUtils.addVolumeToOM(keyManager.getMetadataManager(),
        OmVolumeArgs.newBuilder()
            .setOwnerName("o")
            .setAdminName("a")
            .setVolume(volumeName)
            .build());
  }

  private void addBucketToVolume(String volumeName, String bucketName)
      throws IOException {
    OMRequestTestUtils.addBucketToOM(keyManager.getMetadataManager(),
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setIsVersionEnabled(false)
            .build());
  }

  private void createKey(OzoneManagerProtocol managerProtocol,
                         String volumeName,
                         String bucketName,
                         String keyName)
      throws IOException {

    OmKeyArgs keyArg =
        new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setAcls(Collections.emptyList())
            .setReplicationConfig(StandaloneReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.ONE))
            .setLocationInfoList(new ArrayList<>())
            .setOwnerName(
                UserGroupInformation.getCurrentUser().getShortUserName())
            .build();
    //Open and Commit the Key in the Key Manager.
    OpenKeySession session = managerProtocol.openKey(keyArg);
    keyArg.addLocationInfo(managerProtocol.allocateBlock(keyArg,
        session.getId(), new ExcludeList()));
    managerProtocol.commitKey(keyArg, session.getId());
  }

  /**
   * Test to verify the data integrity after SST filtering service runs.
   * This test creates 150 keys randomly in one of the three buckets. It also
   * forces flush and compaction after every 50 keys written.
   * Once key creation finishes, we create one snapshot per bucket. After that,
   * it waits for SSTFilteringService to run for all three snapshots. Once run
   * finishes, it validates that keys in active DB buckets are same as in
   * snapshot bucket.
   */
  @Test
  public void testSstFilteringService() throws Exception {
    RDBStore activeDbStore = (RDBStore) om.getMetadataManager().getStore();
    String volumeName = "volume";
    List<String> bucketNames = Arrays.asList("bucket", "bucket1", "bucket2");

    createVolume(volumeName);
    for (String bucketName : bucketNames) {
      addBucketToVolume(volumeName, bucketName);
    }

    int keyCount = 150;
    Set<String> keyInBucket = new HashSet<>();
    Set<String> keyInBucket1 = new HashSet<>();
    Set<String> keyInBucket2 = new HashSet<>();

    Random random = new Random();
    for (int i = 0; i < keyCount; i++) {
      String keyName = "key-" + i;
      String bucketName;
      switch (random.nextInt(1000) % 3) {
      case 0:
        bucketName = bucketNames.get(0);
        keyInBucket.add(keyName);
        break;
      case 1:
        bucketName = bucketNames.get(1);
        keyInBucket1.add(keyName);
        break;
      default:
        bucketName = bucketNames.get(2);
        keyInBucket2.add(keyName);
      }
      createKey(writeClient, volumeName, bucketName, keyName);
      if (i % 50 == 0) {
        activeDbStore.getDb().flush(KEY_TABLE);
        activeDbStore.getDb().compactRange(KEY_TABLE);
      }
    }

    List<String> snapshotNames = Arrays.asList("snap", "snap-1", "snap-2");

    for (int i = 0; i < 3; i++) {
      createSnapshot(volumeName, bucketNames.get(i), snapshotNames.get(i));
    }

    SstFilteringService sstFilteringService =
        keyManager.getSnapshotSstFilteringService();

    waitForSnapshotsAtLeast(sstFilteringService, countTotalSnapshots);
    assertEquals(countTotalSnapshots, sstFilteringService.getSnapshotFilteredCount().get());

    Set<String> keyInBucketAfterFilteringRun =
        getKeysFromSnapshot(volumeName, bucketNames.get(0),
            snapshotNames.get(0));
    Set<String> keyInBucket1AfterFilteringRun =
        getKeysFromSnapshot(volumeName, bucketNames.get(1),
            snapshotNames.get(1));
    Set<String> keyInBucket2AfterFilteringRun =
        getKeysFromSnapshot(volumeName, bucketNames.get(2),
            snapshotNames.get(2));
    assertEquals(keyInBucket, keyInBucketAfterFilteringRun);
    assertEquals(keyInBucket1, keyInBucket1AfterFilteringRun);
    assertEquals(keyInBucket2, keyInBucket2AfterFilteringRun);
  }

  private static void waitForSnapshotsAtLeast(SstFilteringService filteringService, long n)
      throws Exception {
    await(10_000, 1_000, () -> filteringService.getSnapshotFilteredCount().get() >= n);
  }

  private Set<String> getKeysFromDb(OMMetadataManager omMetadataReader,
                                    String volume,
                                    String bucket) throws IOException {
    Set<String> allKeys = new HashSet<>();

    String startKey = null;
    while (true) {
      List<OmKeyInfo> omKeyInfoList = omMetadataReader.listKeys(volume, bucket,
          startKey, null, 1000).getKeys();
      if (omKeyInfoList.isEmpty()) {
        break;
      }
      for (OmKeyInfo omKeyInfo : omKeyInfoList) {
        allKeys.add(omKeyInfo.getKeyName());
      }
      startKey = omKeyInfoList.get(omKeyInfoList.size() - 1).getKeyName();
    }
    return allKeys;
  }

  private Set<String> getKeysFromSnapshot(String volume,
                                          String bucket,
                                          String snapshot) throws IOException {
    SnapshotInfo snapshotInfo = om.getMetadataManager().getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(volume, bucket, snapshot));
    try (UncheckedAutoCloseableSupplier<OmSnapshot> snapshotMetadataReader =
             om.getOmSnapshotManager().getActiveSnapshot(
                 snapshotInfo.getVolumeName(),
                 snapshotInfo.getBucketName(),
                 snapshotInfo.getName())) {
      OmSnapshot omSnapshot = snapshotMetadataReader.get();
      return getKeysFromDb(omSnapshot.getMetadataManager(), volume, bucket);
    }
  }

  private void createSnapshot(String volumeName, String bucketName, String snapshotName) throws IOException {
    writeClient.createSnapshot(volumeName, bucketName, snapshotName);
    countTotalSnapshots++;
  }

  private void deleteSnapshot(String volumeName, String bucketName, String snapshotName) throws IOException {
    writeClient.deleteSnapshot(volumeName, bucketName, snapshotName);
    countTotalSnapshots--;
  }

  /**
   * Test to verify that SSTFilteringService is disabled when defrag service is enabled.
   * This test creates a new OzoneManager instance with defrag service enabled and verifies
   * that the SST filtering service is not started.
   */
  @Test
  public void testSstFilteringDisabledWhenDefragEnabled(@TempDir Path folder) throws Exception {
    OzoneConfiguration testConf = new OzoneConfiguration();
    testConf.set(OZONE_METADATA_DIRS, folder.toString());
    testConf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200, TimeUnit.MILLISECONDS);
    // Enable SST filtering service
    testConf.setTimeDuration(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);
    // Enable defrag service
    testConf.setTimeDuration(OZONE_SNAPSHOT_DEFRAG_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);
    testConf.setEnum(HDDS_DB_PROFILE, DBProfile.TEST);
    testConf.setQuietMode(false);
    // Configure dynamic ports to avoid conflicts with the OzoneManager instance from @BeforeAll
    testConf.set(OZONE_OM_ADDRESS_KEY, "127.0.0.1:0");
    testConf.set(OZONE_OM_HTTP_ADDRESS_KEY, "127.0.0.1:0");
    testConf.set(OZONE_OM_HTTPS_ADDRESS_KEY, "127.0.0.1:0");
    testConf.setInt(OZONE_OM_RATIS_PORT_KEY, 0);
    testConf.setInt(OZONE_OM_GRPC_PORT_KEY, 0);

    OmTestManagers testManagers = new OmTestManagers(testConf);
    KeyManager testKeyManager = testManagers.getKeyManager();
    OzoneManager testOm = testManagers.getOzoneManager();

    try {
      // Verify that SST filtering service is not started when defrag is enabled
      SstFilteringService sstFilteringService = testKeyManager.getSnapshotSstFilteringService();
      assertThat(sstFilteringService).as("SstFilteringService should be null when defrag is enabled").isNull();
    } finally {
      if (testKeyManager != null) {
        testKeyManager.stop();
      }
      if (testManagers.getWriteClient() != null) {
        testManagers.getWriteClient().close();
      }
      if (testOm != null) {
        testOm.stop();
      }
    }
  }
}
