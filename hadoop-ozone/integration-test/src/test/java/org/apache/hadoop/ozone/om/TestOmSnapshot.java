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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.rocksdb.LiveFileMetaData;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.ozone.admin.scm.FinalizeUpgradeCommandUtil.isDone;
import static org.apache.hadoop.ozone.admin.scm.FinalizeUpgradeCommandUtil.isStarting;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.CONTAINS_SNAPSHOT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.FILE_SYSTEM_OPTIMIZED;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.OBJECT_STORE;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.DONE;
import static org.awaitility.Awaitility.with;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.IN_PROGRESS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertThrows;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Test OmSnapshot bucket interface.
 */
@RunWith(Parameterized.class)
@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
public class TestOmSnapshot {

  static {
    Logger.getLogger(ManagedRocksObjectUtils.class).setLevel(Level.DEBUG);
  }

  private static MiniOzoneCluster cluster = null;
  private static OzoneClient client;
  private static String volumeName;
  private static String bucketName;
  private static OzoneManagerProtocol writeClient;
  private static BucketLayout bucketLayout = BucketLayout.LEGACY;
  private static boolean enabledFileSystemPaths;
  private static boolean forceFullSnapshotDiff;
  private static boolean forceNonNativeSnapshotDiff;
  private static ObjectStore store;
  private static OzoneManager ozoneManager;
  private static RDBStore rdbStore;
  private static OzoneBucket ozoneBucket;
  private static final Duration POLL_INTERVAL_DURATION = Duration.ofMillis(500);
  private static final Duration POLL_MAX_DURATION = Duration.ofSeconds(10);

  private static AtomicInteger counter;

  @Rule
  public Timeout timeout = new Timeout(180, TimeUnit.SECONDS);

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[]{OBJECT_STORE, false, false, false},
        new Object[]{FILE_SYSTEM_OPTIMIZED, false, false, false},
        new Object[]{FILE_SYSTEM_OPTIMIZED, false, false, true},
        new Object[]{BucketLayout.LEGACY, true, true, false});
  }

  public TestOmSnapshot(BucketLayout newBucketLayout,
      boolean newEnableFileSystemPaths, boolean forceFullSnapDiff,
      boolean forceNonNativeSnapDiff)
      throws Exception {
    // Checking whether 'newBucketLayout' and
    // 'newEnableFileSystemPaths' flags represents next parameter
    // index values. This is to ensure that initialize init() function
    // will be invoked only at the beginning of every new set of
    // Parameterized.Parameters.
    if (TestOmSnapshot.enabledFileSystemPaths != newEnableFileSystemPaths ||
            TestOmSnapshot.bucketLayout != newBucketLayout ||
            TestOmSnapshot.forceFullSnapshotDiff != forceFullSnapDiff ||
            TestOmSnapshot.forceNonNativeSnapshotDiff
                != forceNonNativeSnapDiff) {
      setConfig(newBucketLayout, newEnableFileSystemPaths,
          forceFullSnapDiff, forceNonNativeSnapDiff);
      tearDown();
      init();
    }
  }

  private static void setConfig(BucketLayout newBucketLayout,
      boolean newEnableFileSystemPaths, boolean forceFullSnapDiff,
      boolean forceNonNativeSnapDiff) {
    TestOmSnapshot.enabledFileSystemPaths = newEnableFileSystemPaths;
    TestOmSnapshot.bucketLayout = newBucketLayout;
    TestOmSnapshot.forceFullSnapshotDiff = forceFullSnapDiff;
    TestOmSnapshot.forceNonNativeSnapshotDiff = forceNonNativeSnapDiff;
  }

  /**
   * Create a MiniDFSCluster for testing.
   */
  private void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
        enabledFileSystemPaths);
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        bucketLayout.name());
    conf.setBoolean(OMConfigKeys.OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF,
        forceFullSnapshotDiff);
    conf.setBoolean(OMConfigKeys.OZONE_OM_SNAPSHOT_FORCE_NON_NATIVE_DIFF,
        forceNonNativeSnapshotDiff);
    String omId = UUID.randomUUID().toString();
    conf.setBoolean(OZONE_OM_ENABLE_FILESYSTEM_PATHS, enabledFileSystemPaths);
    conf.set(OZONE_DEFAULT_BUCKET_LAYOUT, bucketLayout.name());
    conf.setBoolean(OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF, forceFullSnapshotDiff);
    conf.setEnum(HDDS_DB_PROFILE, DBProfile.TEST);
    // Enable filesystem snapshot feature for the test regardless of the default
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setNumOfOzoneManagers(3)
        .setOmLayoutVersion(OMLayoutFeature.
          BUCKET_LAYOUT_SUPPORT.layoutVersion())
        .setOmId(omId)
        .build();

    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    // create a volume and a bucket to be used by OzoneFileSystem
    ozoneBucket = TestDataUtil
        .createVolumeAndBucket(client, bucketLayout);
    volumeName = ozoneBucket.getVolumeName();
    bucketName = ozoneBucket.getName();
    ozoneManager = cluster.getOzoneManager();
    rdbStore = (RDBStore) ozoneManager.getMetadataManager().getStore();

    store = client.getObjectStore();
    writeClient = store.getClientProxy().getOzoneManagerClient();

    KeyManagerImpl keyManager = (KeyManagerImpl) HddsWhiteboxTestUtils
        .getInternalState(ozoneManager, "keyManager");
    counter = new AtomicInteger(0);
    // stop the deletion services so that keys can still be read
    keyManager.stop();
    preFinalizationChecks();
    finalizeOMUpgrade();
    counter = new AtomicInteger();
  }

  private static void expectFailurePreFinalization(LambdaTestUtils.
      VoidCallable eval) throws Exception {
    OMException ex  = Assert.assertThrows(OMException.class,
            () -> eval.call());
    Assert.assertEquals(ex.getResult(),
            NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION);
    Assert.assertTrue(ex.getMessage().contains(
            "cannot be invoked before finalization."));
  }

  private static void preFinalizationChecks() throws Exception {
    // None of the snapshot APIs is usable before the upgrade finalization step
    expectFailurePreFinalization(() ->
        store.createSnapshot(volumeName, bucketName,
            UUID.randomUUID().toString()));
    expectFailurePreFinalization(() ->
        store.listSnapshot(volumeName, bucketName, null, null));
    expectFailurePreFinalization(() ->
        store.snapshotDiff(volumeName, bucketName,
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
          "", 1000, false, forceNonNativeSnapshotDiff));
    expectFailurePreFinalization(() ->
        store.deleteSnapshot(volumeName, bucketName,
            UUID.randomUUID().toString()));
  }

  /**
   * Trigger OM upgrade finalization from the client and block until completion
   * (status FINALIZATION_DONE).
   */
  private static void finalizeOMUpgrade() throws IOException {

    // Trigger OM upgrade finalization. Ref: FinalizeUpgradeSubCommand#call
    final OzoneManagerProtocol omclient =
        client.getObjectStore()
        .getClientProxy().getOzoneManagerClient();
    final String upgradeClientID = "Test-Upgrade-Client-" + UUID.randomUUID();
    UpgradeFinalizer.StatusAndMessages finalizationResponse =
        omclient.finalizeUpgrade(upgradeClientID);

    // The status should transition as soon as the client call above returns
    Assert.assertTrue(isStarting(finalizationResponse.status()));
    // Wait for the finalization to be marked as done.
    // 10s timeout should be plenty.
    try {
      with().atMost(POLL_MAX_DURATION)
          .pollInterval(POLL_INTERVAL_DURATION)
          .await()
          .until(() -> {
            final UpgradeFinalizer.StatusAndMessages progress =
                omclient.queryUpgradeFinalizationProgress(
                    upgradeClientID, false, false);
            return isDone(progress.status());
          });
    } catch (Exception e) {
      Assert.fail("Unexpected exception while waiting for "
          + "the OM upgrade to finalize: " + e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  // based on TestOzoneRpcClientAbstract:testListKey
  public void testListKey() throws Exception {
    String volumeA = "vol-a-" + counter.incrementAndGet();
    String volumeB = "vol-b-" + counter.incrementAndGet();
    String bucketA = "buc-a-" + counter.incrementAndGet();
    String bucketB = "buc-b-" + counter.incrementAndGet();
    store.createVolume(volumeA);
    store.createVolume(volumeB);
    OzoneVolume volA = store.getVolume(volumeA);
    OzoneVolume volB = store.getVolume(volumeB);
    volA.createBucket(bucketA);
    volA.createBucket(bucketB);
    volB.createBucket(bucketA);
    volB.createBucket(bucketB);
    OzoneBucket volAbucketA = volA.getBucket(bucketA);
    OzoneBucket volAbucketB = volA.getBucket(bucketB);
    OzoneBucket volBbucketA = volB.getBucket(bucketA);
    OzoneBucket volBbucketB = volB.getBucket(bucketB);

    /*
    Create 10 keys in  vol-a-<random>/buc-a-<random>,
    vol-a-<random>/buc-b-<random>, vol-b-<random>/buc-a-<random> and
    vol-b-<random>/buc-b-<random>
     */
    String keyBaseA = "key-a-";
    for (int i = 0; i < 10; i++) {
      createFileKeyWithPrefix(volAbucketA, keyBaseA + i + "-");
      createFileKeyWithPrefix(volAbucketB, keyBaseA + i + "-");
      createFileKeyWithPrefix(volBbucketA, keyBaseA + i + "-");
      createFileKeyWithPrefix(volBbucketB, keyBaseA + i + "-");
    }
    /*
    Create 10 keys in  vol-a-<random>/buc-a-<random>,
    vol-a-<random>/buc-b-<random>, vol-b-<random>/buc-a-<random> and
    vol-b-<random>/buc-b-<random>
     */
    String keyBaseB = "key-b-";
    for (int i = 0; i < 10; i++) {
      createFileKeyWithPrefix(volAbucketA, keyBaseB + i + "-");
      createFileKeyWithPrefix(volAbucketB, keyBaseB + i + "-");
      createFileKeyWithPrefix(volBbucketA, keyBaseB + i + "-");
      createFileKeyWithPrefix(volBbucketB, keyBaseB + i + "-");
    }

    String snapshotKeyPrefix = createSnapshot(volumeA, bucketA);

    int volABucketAKeyCount = keyCount(volAbucketA,
        snapshotKeyPrefix + "key-");
    Assert.assertEquals(20, volABucketAKeyCount);

    snapshotKeyPrefix = createSnapshot(volumeA, bucketB);
    deleteKeys(volAbucketB);

    int volABucketBKeyCount = keyCount(volAbucketB,
        snapshotKeyPrefix + "key-");
    Assert.assertEquals(20, volABucketBKeyCount);

    snapshotKeyPrefix = createSnapshot(volumeB, bucketA);
    deleteKeys(volBbucketA);

    int volBBucketAKeyCount = keyCount(volBbucketA,
        snapshotKeyPrefix + "key-");
    Assert.assertEquals(20, volBBucketAKeyCount);

    snapshotKeyPrefix = createSnapshot(volumeB, bucketB);
    deleteKeys(volBbucketB);

    int volBBucketBKeyCount = keyCount(volBbucketB,
        snapshotKeyPrefix + "key-");
    Assert.assertEquals(20, volBBucketBKeyCount);

    snapshotKeyPrefix = createSnapshot(volumeA, bucketA);
    deleteKeys(volAbucketA);

    int volABucketAKeyACount = keyCount(volAbucketA,
        snapshotKeyPrefix + "key-a-");
    Assert.assertEquals(10, volABucketAKeyACount);


    int volABucketAKeyBCount = keyCount(volAbucketA,
        snapshotKeyPrefix + "key-b-");
    Assert.assertEquals(10, volABucketAKeyBCount);
  }

  @Test
  // based on TestOzoneRpcClientAbstract:testListKeyOnEmptyBucket
  public void testListKeyOnEmptyBucket()
      throws IOException, InterruptedException, TimeoutException {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buc-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    vol.createBucket(bucket);
    String snapshotKeyPrefix = createSnapshot(volume, bucket);
    OzoneBucket buc = vol.getBucket(bucket);
    Iterator<? extends OzoneKey> keys = buc.listKeys(snapshotKeyPrefix);
    while (keys.hasNext()) {
      fail();
    }
  }

  private OmKeyArgs genKeyArgs(String keyName) {
    return new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setAcls(Collections.emptyList())
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE))
        .setLocationInfoList(new ArrayList<>())
        .build();
  }

  @Test
  @Ignore("HDDS-8089")
  public void checkKey() throws Exception {
    String s = "testData";
    String dir1 = "dir1";
    String key1 = dir1 + "/key1";

    // create key1
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(key1,
        s.length());
    byte[] input = s.getBytes(StandardCharsets.UTF_8);
    ozoneOutputStream.write(input);
    ozoneOutputStream.close();


    String snapshotKeyPrefix = createSnapshot(volumeName, bucketName);
    ozoneBucket.deleteKey(key1);
    try {
      ozoneBucket.deleteKey(dir1);
    } catch (OMException e) {
      // OBJECT_STORE won't have directory entry so ignore KEY_NOT_FOUND
      if (e.getResult() != KEY_NOT_FOUND) {
        fail("got exception on cleanup: " + e.getMessage());
      }
    }
    OmKeyArgs keyArgs = genKeyArgs(snapshotKeyPrefix + key1);

    OmKeyInfo omKeyInfo = writeClient.lookupKey(keyArgs);
    assertEquals(omKeyInfo.getKeyName(), snapshotKeyPrefix + key1);

    OmKeyInfo fileInfo = writeClient.lookupFile(keyArgs);
    assertEquals(fileInfo.getKeyName(), snapshotKeyPrefix + key1);

    OzoneFileStatus ozoneFileStatus = writeClient.getFileStatus(keyArgs);
    assertEquals(ozoneFileStatus.getKeyInfo().getKeyName(),
        snapshotKeyPrefix + key1);
  }

  @Test
  public void testListDeleteKey() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buc-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    vol.createBucket(bucket);
    OzoneBucket volBucket = vol.getBucket(bucket);

    String key = "key-";
    createFileKeyWithPrefix(volBucket, key);
    String snapshotKeyPrefix = createSnapshot(volume, bucket);
    deleteKeys(volBucket);

    int volBucketKeyCount = keyCount(volBucket, snapshotKeyPrefix + "key-");
    Assert.assertEquals(1, volBucketKeyCount);

    snapshotKeyPrefix = createSnapshot(volume, bucket);
    Iterator<? extends OzoneKey> volBucketIter2 =
            volBucket.listKeys(snapshotKeyPrefix);
    while (volBucketIter2.hasNext()) {
      fail("The last snapshot should not have any keys in it!");
    }
  }

  @Test
  public void testListAddNewKey() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buc-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    vol.createBucket(bucket);
    OzoneBucket bucket1 = vol.getBucket(bucket);

    String key1 = "key-1-";
    createFileKeyWithPrefix(bucket1, key1);
    String snapshotKeyPrefix1 = createSnapshot(volume, bucket);

    String key2 = "key-2-";
    createFileKeyWithPrefix(bucket1, key2);
    String snapshotKeyPrefix2 = createSnapshot(volume, bucket);

    int volBucketKeyCount = keyCount(bucket1, snapshotKeyPrefix1 + "key-");
    Assert.assertEquals(1, volBucketKeyCount);


    int volBucketKeyCount2 = keyCount(bucket1, snapshotKeyPrefix2 + "key-");
    Assert.assertEquals(2, volBucketKeyCount2);

    deleteKeys(bucket1);
  }

  private int keyCount(OzoneBucket bucket, String keyPrefix)
      throws IOException {
    Iterator<? extends OzoneKey> iterator = bucket.listKeys(keyPrefix);
    int keyCount = 0;
    while (iterator.hasNext()) {
      iterator.next();
      keyCount++;
    }
    return keyCount;
  }

  @Test
  public void testNonExistentBucket() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buc-" + counter.incrementAndGet();
    //create volume but not bucket
    store.createVolume(volume);

    LambdaTestUtils.intercept(OMException.class,
            "Bucket not found",
            () -> createSnapshot(volume, bucket));
  }

  @Test
  public void testCreateSnapshotMissingMandatoryParams() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    volume1.createBucket(bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    createFileKey(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap1);

    String nullstr = "";
    // Bucket is empty
    assertThrows(IllegalArgumentException.class,
            () -> createSnapshot(volume, nullstr));
    // Volume is empty
    assertThrows(IllegalArgumentException.class,
            () -> createSnapshot(nullstr, bucket));
  }

  private Set<OmKeyInfo> getDeletedKeysFromRocksDb(
      OMMetadataManager metadataManager) throws IOException {
    Set<OmKeyInfo> deletedKeys = Sets.newHashSet();
    try (TableIterator<String,
        ? extends Table.KeyValue<String, RepeatedOmKeyInfo>>
             deletedTableIterator = metadataManager.getDeletedTable()
            .iterator()) {
      while (deletedTableIterator.hasNext()) {
        Table.KeyValue<String, RepeatedOmKeyInfo> val =
            deletedTableIterator.next();
        deletedKeys.addAll(val.getValue().getOmKeyInfoList());
      }
    }

    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
             deletedDirTableIterator = metadataManager.getDeletedDirTable()
            .iterator()) {
      while (deletedDirTableIterator.hasNext()) {
        deletedKeys.add(deletedDirTableIterator.next().getValue());
      }
    }
    return deletedKeys;
  }

  private OmKeyInfo getOmKeyInfo(String volume, String bucket,
                                 String key) throws IOException {
    return cluster.getOzoneManager().getKeyManager()
            .getKeyInfo(new OmKeyArgs.Builder().setVolumeName(volume)
            .setBucketName(bucket).setKeyName(key).build(), null);
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Snapshot snap2 is created
   * 4) Key k1 is deleted.
   * 5) Snapdiff b/w snap3 & snap2 taken to assert difference of 1 key
   */
  @Test
  public void testSnapDiffHandlingReclaimWithLatestUse() throws Exception {
    String testVolumeName = "vol" + RandomStringUtils.randomNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    volume.createBucket(testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    OmKeyInfo keyInfo = getOmKeyInfo(testVolumeName, testBucketName, key1);
    bucket.deleteKey(key1);
    String snap3 = "snap3";
    String activeSnapshotPrefix =
            createSnapshot(testVolumeName, testBucketName, snap3);
    SnapshotDiffReportOzone diff =
        getSnapDiffReport(testVolumeName, testBucketName, snap1, snap2);
    Assert.assertEquals(diff.getDiffList().size(), 0);
    diff = getSnapDiffReport(testVolumeName, testBucketName, snap2, snap3);
    Assert.assertEquals(diff.getDiffList().size(), 1);
    Assert.assertEquals(diff.getDiffList(), Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1)));
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Key k1 is deleted.
   * 4) Snapshot snap2 is created.
   * 5) Snapshot snap3 is created.
   * 6) Snapdiff b/w snap3 & snap1 taken to assert difference of 1 key.
   * 7) Snapdiff b/w snap3 & snap2 taken to assert difference of 0 key.
   */
  @Test
  public void testSnapDiffHandlingReclaimWithPreviousUse() throws Exception {
    String testVolumeName = "vol" + RandomStringUtils.randomNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    volume.createBucket(testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    OmKeyInfo keyInfo = getOmKeyInfo(testVolumeName, testBucketName, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    bucket.deleteKey(key1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    String snap3 = "snap3";
    String activeSnapshotPrefix = createSnapshot(testVolumeName, testBucketName,
            snap3);
    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap3);
    Assert.assertEquals(diff.getDiffList().size(), 1);
    Assert.assertEquals(diff.getDiffList(), Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1)));
    diff = getSnapDiffReport(testVolumeName, testBucketName,
            snap2, snap3);
    Assert.assertEquals(diff.getDiffList().size(), 0);
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Key k1 is deleted.
   * 4) Key k1 is recreated.
   * 5) Snapshot snap2 is created.
   * 6) Snapdiff b/w Active FS & snap1 taken to assert difference of 2 keys.
   * 7) Snapdiff b/w Active FS & snap2 taken to assert difference of 0 key.
   * 8) Checking rocks db to ensure the object created shouldn't be reclaimed
   *    as it is used by snapshot.
   * 9) Key k1 is deleted.
   * 10) Snapdiff b/w Active FS & snap1 taken to assert difference of 1 key.
   * 11) Snapdiff b/w Active FS & snap2 taken to assert difference of 1 key.
   */
  @Test
  public void testSnapDiffReclaimWithKeyRecreation() throws Exception {
    String testVolumeName = "vol" + RandomStringUtils.randomNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    volume.createBucket(testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    OmKeyInfo keyInfo1 = getOmKeyInfo(testVolumeName, testBucketName, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    bucket.deleteKey(key1);
    key1 = createFileKey(bucket, key1);
    OmKeyInfo keyInfo2 = getOmKeyInfo(testVolumeName, testBucketName, key1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    String snap3 = "snap3";
    String activeSnapshotPrefix = createSnapshot(testVolumeName, testBucketName,
        snap3);
    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap3);
    Assert.assertEquals(diff.getDiffList().size(), 2);
    Assert.assertEquals(diff.getDiffList(), Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1),
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.CREATE, key1)));
    diff = getSnapDiffReport(testVolumeName, testBucketName,
            snap2, snap3);
    Assert.assertEquals(diff.getDiffList().size(), 0);
    bucket.deleteKey(key1);
    String snap4 = "snap4";
    activeSnapshotPrefix = createSnapshot(testVolumeName, testBucketName,
        snap4);
    diff = getSnapDiffReport(testVolumeName, testBucketName,
            snap1, snap4);
    Assert.assertEquals(diff.getDiffList().size(), 1);
    Assert.assertEquals(diff.getDiffList(), Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1)));
    diff = getSnapDiffReport(testVolumeName, testBucketName,
            snap2, snap4);
    Assert.assertEquals(diff.getDiffList().size(), 1);
    Assert.assertEquals(diff.getDiffList(), Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1)));
  }


  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Key k1 is renamed to renamed-k1.
   * 4) Key renamed-k1 is deleted.
   * 5) Snapshot snap2 created.
   * 4) Snapdiff b/w snap2 & snap1 taken to assert difference of 1 key.
   */
  @Test
  public void testSnapDiffReclaimWithKeyRename() throws Exception {
    String testVolumeName = "vol" + RandomStringUtils.randomNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    volume.createBucket(testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    String renamedKey = "renamed-" + key1;
    bucket.renameKey(key1, renamedKey);
    GenericTestUtils.waitFor(
            () -> {
              try {
                getOmKeyInfo(testVolumeName, testBucketName, renamedKey);
              } catch (IOException e) {
                return false;
              }
              return true;
            }, 1000, 10000);
    OmKeyInfo renamedKeyInfo = getOmKeyInfo(testVolumeName, testBucketName,
            renamedKey);
    bucket.deleteKey(renamedKey);
    String snap2 = "snap2";
    String activeSnapshotPrefix = createSnapshot(testVolumeName, testBucketName,
            snap2);
    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap2);
    Assert.assertEquals(diff.getDiffList().size(), 1);
    Assert.assertEquals(diff.getDiffList(), Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1)
    ));
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Key k1 is renamed to renamed-k1.
   * 4) Key renamed-k1 is renamed to renamed-renamed-k1.
   * 5) Key renamed-renamed-k1 is deleted.
   * 6) Snapshot snap2 is created.
   * 7) Snapdiff b/w Active FS & snap1 taken to assert difference of 1 key.
   */
  @Test
  public void testSnapDiffWith2RenamesAndDelete() throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket" + counter.incrementAndGet();
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    volume.createBucket(testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    String renamedKey = "renamed-" + key1;
    bucket.renameKey(key1, renamedKey);
    GenericTestUtils.waitFor(
            () -> {
              try {
                getOmKeyInfo(testVolumeName, testBucketName, renamedKey);
              } catch (IOException e) {
                return false;
              }
              return true;
            }, 1000, 120000);
    String renamedRenamedKey = "renamed-" + renamedKey;
    bucket.renameKey(renamedKey, renamedRenamedKey);
    GenericTestUtils.waitFor(
            () -> {
              try {
                getOmKeyInfo(testVolumeName, testBucketName, renamedRenamedKey);
              } catch (IOException e) {
                return false;
              }
              return true;
            }, 1000, 120000);
    OmKeyInfo renamedRenamedKeyInfo = getOmKeyInfo(testVolumeName,
            testBucketName, renamedRenamedKey);
    bucket.deleteKey(renamedRenamedKey);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    String snap3 = "snap3";
    createSnapshot(testVolumeName, testBucketName,
            snap3);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
            snap1, snap3);
    Assert.assertEquals(diff.getDiffList().size(), 1);
    Assert.assertEquals(diff.getDiffList(), Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1)
    ));
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Key k1 is renamed to renamed-k1.
   * 4) Key k1 is recreated.
   * 5) Key k1 is deleted.
   * 6) Snapdiff b/w Active FS & snap1 taken to assert difference of 1 key.
   */
  @Test
  public void testSnapDiffWithKeyRenamesRecreationAndDelete()
          throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    volume.createBucket(testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    String renamedKey = "renamed-" + key1;
    bucket.renameKey(key1, renamedKey);
    key1 =  createFileKey(bucket, key1);
    OmKeyInfo recreatedKeyInfo = getOmKeyInfo(testVolumeName, testBucketName,
            key1);
    String snap3 = "snap3";
    createSnapshot(testVolumeName, testBucketName, snap3);
    getSnapDiffReport(testVolumeName, testBucketName,
            snap1, snap3);
    bucket.deleteKey(key1);
    String activeSnap = "activefs";
    createSnapshot(testVolumeName, testBucketName, activeSnap);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
            snap1, activeSnap);
    Assert.assertEquals(diff.getDiffList().size(), 1);
    Assert.assertEquals(diff.getDiffList(), Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.RENAME, key1, renamedKey)
    ));
  }

  /**
   * Testing scenario:
   * 1) Snapshot snap1 created.
   * 2) Key k1 is created.
   * 3) Key k1 is deleted.
   * 4) Snapshot s2 is created instantly before the key k1 is deleted.
   * 5) Snapdiff b/w Active FS & snap1 taken to assert difference of 0 keys.
   * 6) Snapdiff b/w Active FS & snap2 taken to assert difference of 0 keys.
   */
  @Test
  public void testSnapDiffReclaimWithDeferredKeyDeletion() throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    volume.createBucket(testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    OmKeyInfo keyInfo = getOmKeyInfo(testVolumeName, testBucketName, key1);
    bucket.deleteKey(key1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    String activeSnap = "activefs";
    String activeSnapshotPrefix = createSnapshot(testVolumeName, testBucketName,
            activeSnap);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
            snap1, activeSnap);
    Assert.assertEquals(diff.getDiffList().size(), 0);
    diff = getSnapDiffReport(testVolumeName, testBucketName,
            snap2, activeSnap);
    Assert.assertEquals(diff.getDiffList().size(), 0);
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Key k1 is renamed to key k1_renamed
   * 4) Key k1_renamed is renamed to key k1
   * 5) Snapdiff b/w Active FS & snap1 taken to assert difference of 1 key
   *    with 1 Modified entry.
   */
  @Test
  public void testSnapDiffWithNoEffectiveRename() throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    volume.createBucket(testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String snap1 = "snap1";
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    createSnapshot(testVolumeName, testBucketName, snap1);
    getOmKeyInfo(bucket.getVolumeName(), bucket.getName(),
        key1);
    String key1Renamed = key1 + "_renamed";

    bucket.renameKey(key1, key1Renamed);
    bucket.renameKey(key1Renamed, key1);


    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
        snap1, snap2);
    getOmKeyInfo(bucket.getVolumeName(), bucket.getName(),
        key1);
    Assert.assertEquals(diff.getDiffList(), Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.MODIFY, key1)));
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Dir dir1/dir2 is created.
   * 4) Key k1 is renamed to key dir1/dir2/k1_renamed
   * 5) Snapdiff b/w Active FS & snap1 taken to assert difference of 3 key
   *    with 1 rename entry & 2 dirs create entry.
   */
  @Test
  public void testSnapDiffWithDirectory() throws Exception {

    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    volume.createBucket(testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String snap1 = "snap1";
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    createSnapshot(testVolumeName, testBucketName, snap1);
    getOmKeyInfo(bucket.getVolumeName(), bucket.getName(),
        key1);
    bucket.createDirectory("dir1/dir2");
    String key1Renamed = "dir1/dir2/" + key1 + "_renamed";
    bucket.renameKey(key1, key1Renamed);

    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
        snap1, snap2);

    List<SnapshotDiffReport.DiffReportEntry> diffEntries;
    if (bucketLayout.isFileSystemOptimized()) {
      diffEntries = Arrays.asList(SnapshotDiffReportOzone.getDiffReportEntry(
          SnapshotDiffReport.DiffType.RENAME, key1, key1 + "_renamed"),
          SnapshotDiffReportOzone.getDiffReportEntry(
              SnapshotDiffReport.DiffType.CREATE, "dir1"),
          SnapshotDiffReportOzone.getDiffReportEntry(
              SnapshotDiffReport.DiffType.CREATE, "dir2"));
    } else {
      diffEntries = Arrays.asList(SnapshotDiffReportOzone.getDiffReportEntry(
              SnapshotDiffReport.DiffType.RENAME, key1, key1 + "_renamed"),
          SnapshotDiffReportOzone.getDiffReportEntry(
              SnapshotDiffReport.DiffType.CREATE, "dir2"),
          SnapshotDiffReportOzone.getDiffReportEntry(
              SnapshotDiffReport.DiffType.CREATE, "dir1"));
    }
    Assert.assertEquals(diff.getDiffList(), diffEntries);
  }

  @Test
  public void testSnapdiffWithFilesystemCreate()
      throws IOException, URISyntaxException, InterruptedException,
      TimeoutException {

    Assume.assumeTrue(!bucketLayout.isObjectStore(enabledFileSystemPaths));
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket" + counter.incrementAndGet();
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    volume.createBucket(testBucketName);
    String rootPath = String.format("%s://%s.%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, testBucketName, testVolumeName,
        "om-service-test1");
    try (FileSystem fs = FileSystem.get(new URI(rootPath), cluster.getConf())) {
      String snap1 = "snap1";
      String key = "/dir1/dir2/key1";
      createSnapshot(testVolumeName, testBucketName, snap1);
      createFileKey(fs, key);
      String snap2 = "snap2";
      createSnapshot(testVolumeName, testBucketName, snap2);
      SnapshotDiffReport diff = getSnapDiffReport(testVolumeName,
          testBucketName, snap1, snap2);
      int idx = bucketLayout.isFileSystemOptimized() ? 0 :
          diff.getDiffList().size() - 1;
      Path p = new Path("/");
      while (true) {
        FileStatus[] fileStatuses = fs.listStatus(p);
        Assertions.assertEquals(fileStatuses[0].isDirectory(),
            bucketLayout.isFileSystemOptimized() &&
                idx < diff.getDiffList().size() - 1 ||
                !bucketLayout.isFileSystemOptimized() && idx > 0);
        p = fileStatuses[0].getPath();
        Assertions.assertEquals(diff.getDiffList().get(idx),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.CREATE, p.getName()));
        if (fileStatuses[0].isFile()) {
          break;
        }
        idx += bucketLayout.isFileSystemOptimized() ? 1 : -1;
      }
    }
  }

  @Test
  public void testSnapdiffWithFilesystemDirectoryRenameOperation()
      throws IOException, URISyntaxException, InterruptedException,
      TimeoutException {
    Assume.assumeTrue(!bucketLayout.isObjectStore(enabledFileSystemPaths));
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket" + counter.incrementAndGet();
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    volume.createBucket(testBucketName);
    String rootPath = String.format("%s://%s.%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, testBucketName, testVolumeName,
        "om-service-test1");
    try (FileSystem fs = FileSystem.get(new URI(rootPath), cluster.getConf())) {
      String snap1 = "snap1";
      String key = "/dir1/dir2/key1";
      createFileKey(fs, key);
      createSnapshot(testVolumeName, testBucketName, snap1);
      String snap2 = "snap2";
      fs.rename(new Path("/dir1/dir2"), new Path("/dir1/dir3"));
      createSnapshot(testVolumeName, testBucketName, snap2);
      SnapshotDiffReport diff = getSnapDiffReport(testVolumeName,
          testBucketName, snap1, snap2);
      if (bucketLayout.isFileSystemOptimized()) {
        Assertions.assertEquals(diff.getDiffList(), Arrays.asList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.RENAME, "dir2", "dir3"),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.MODIFY, "dir1")));
      } else {
        Assertions.assertEquals(diff.getDiffList(), Arrays.asList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.RENAME, "dir2", "dir3"),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.MODIFY, "key1")));
      }

    }
  }

  @Test
  public void testSnapdiffWithFilesystemDirectoryMoveOperation()
      throws IOException, URISyntaxException, InterruptedException,
      TimeoutException {
    Assume.assumeTrue(!bucketLayout.isObjectStore(enabledFileSystemPaths));
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket" + counter.incrementAndGet();
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    volume.createBucket(testBucketName);
    String rootPath = String.format("%s://%s.%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, testBucketName, testVolumeName,
        "om-service-test1");
    try (FileSystem fs = FileSystem.get(new URI(rootPath), cluster.getConf())) {
      String snap1 = "snap1";
      String key = "/dir1/dir2/key1";
      createFileKey(fs, key);
      fs.mkdirs(new Path("/dir3"));
      createSnapshot(testVolumeName, testBucketName, snap1);
      String snap2 = "snap2";
      fs.rename(new Path("/dir1/dir2"), new Path("/dir3/dir2"));
      createSnapshot(testVolumeName, testBucketName, snap2);
      SnapshotDiffReport diff = getSnapDiffReport(testVolumeName,
          testBucketName, snap1, snap2);
      if (bucketLayout.isFileSystemOptimized()) {
        Assertions.assertEquals(diff.getDiffList(), Arrays.asList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.MODIFY, "dir1"),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.MODIFY, "dir2"),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.MODIFY, "dir3")));
      } else {
        Assertions.assertEquals(diff.getDiffList(), Arrays.asList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.MODIFY, "key1"),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.MODIFY, "dir2")));
      }
    }
  }

  @Test
  public void testBucketDeleteIfSnapshotExists() throws Exception {
    String volume1 = "vol-" + counter.incrementAndGet();
    String bucket1 = "buc-" + counter.incrementAndGet();
    String bucket2 = "buc-" + counter.incrementAndGet();
    store.createVolume(volume1);
    OzoneVolume volume = store.getVolume(volume1);
    volume.createBucket(bucket1);
    volume.createBucket(bucket2);
    OzoneBucket bucketWithSnapshot = volume.getBucket(bucket1);
    OzoneBucket bucketWithoutSnapshot = volume.getBucket(bucket2);
    String key = "key-";
    createFileKey(bucketWithSnapshot, key);
    createFileKey(bucketWithoutSnapshot, key);
    createSnapshot(volume1, bucket1);
    deleteKeys(bucketWithSnapshot);
    deleteKeys(bucketWithoutSnapshot);
    OMException omException = Assertions.assertThrows(OMException.class,
        () -> volume.deleteBucket(bucket1));
    Assertions.assertEquals(CONTAINS_SNAPSHOT, omException.getResult());
    // TODO: Delete snapshot then delete bucket1 when deletion is implemented
    // no exception for bucket without snapshot
    volume.deleteBucket(bucket2);
  }

  @Test
  public void testSnapDiff() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    volume1.createBucket(bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    key1 = createFileKeyWithPrefix(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap1);
    // Do nothing, take another snapshot
    String snap2 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap2);

    SnapshotDiffReportOzone
        diff1 = getSnapDiffReport(volume, bucket, snap1, snap2);
    Assert.assertTrue(diff1.getDiffList().isEmpty());
    // Create Key2 and delete Key1, take snapshot
    String key2 = "key-2-";
    key2 = createFileKeyWithPrefix(bucket1, key2);
    bucket1.deleteKey(key1);
    String snap3 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap3);

    // Diff should have 2 entries
    SnapshotDiffReportOzone
        diff2 = getSnapDiffReport(volume, bucket, snap2, snap3);
    Assert.assertEquals(2, diff2.getDiffList().size());
    Assert.assertTrue(diff2.getDiffList().contains(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReportOzone.DiffType.CREATE, key2)));
    Assert.assertTrue(diff2.getDiffList().contains(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReportOzone.DiffType.DELETE, key1)));

    // Rename Key2
    String key2Renamed = key2 + "_renamed";
    bucket1.renameKey(key2, key2Renamed);
    String snap4 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap4);

    SnapshotDiffReportOzone
        diff3 = getSnapDiffReport(volume, bucket, snap3, snap4);
    Assert.assertEquals(1, diff3.getDiffList().size());
    Assert.assertTrue(diff3.getDiffList().contains(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReportOzone.DiffType.RENAME, key2, key2Renamed)));


    // Create a directory
    String dir1 = "dir-1" +  counter.incrementAndGet();
    bucket1.createDirectory(dir1);
    String snap5 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap5);
    SnapshotDiffReportOzone
        diff4 = getSnapDiffReport(volume, bucket, snap4, snap5);
    Assert.assertEquals(1, diff4.getDiffList().size());
    // for non-fso, directories are a special type of key with "/" appended
    // at the end.
    if (!bucket1.getBucketLayout().isFileSystemOptimized()) {
      dir1 = dir1 + OM_KEY_PREFIX;
    }
    Assert.assertTrue(diff4.getDiffList().contains(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReportOzone.DiffType.CREATE, dir1)));

  }

  private SnapshotDiffReportOzone getSnapDiffReport(String volume,
                                                    String bucket,
                                                    String fromSnapshot,
                                                    String toSnapshot)
      throws InterruptedException, IOException {
    SnapshotDiffResponse response;
    do {
      response = store.snapshotDiff(volume, bucket, fromSnapshot,
          toSnapshot, null, 0,
          forceFullSnapshotDiff, forceNonNativeSnapshotDiff);
      Thread.sleep(response.getWaitTimeInMs());
    } while (response.getJobStatus() != DONE);

    return response.getSnapshotDiffReport();
  }

  @Test
  public void testSnapDiffNoSnapshot() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    volume1.createBucket(bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    createFileKey(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap1);
    String snap2 = "snap" + counter.incrementAndGet();
    // Destination snapshot is invalid
    LambdaTestUtils.intercept(OMException.class,
            "KEY_NOT_FOUND",
            () -> store.snapshotDiff(volume, bucket, snap1, snap2,
                null, 0, false, forceNonNativeSnapshotDiff));
    // From snapshot is invalid
    LambdaTestUtils.intercept(OMException.class,
            "KEY_NOT_FOUND",
            () -> store.snapshotDiff(volume, bucket, snap2, snap1,
                null, 0, false, forceNonNativeSnapshotDiff));
  }

  @Test
  public void testSnapDiffNonExistentUrl() throws Exception {
    // Valid volume bucket
    String volumea = "vol-" + counter.incrementAndGet();
    String bucketa = "buck-" + counter.incrementAndGet();
    // Dummy volume bucket
    String volumeb = "vol-" + counter.incrementAndGet();
    String bucketb = "buck-" + counter.incrementAndGet();
    store.createVolume(volumea);
    OzoneVolume volume1 = store.getVolume(volumea);
    volume1.createBucket(bucketa);
    OzoneBucket bucket1 = volume1.getBucket(bucketa);
    // Create Key1 and take 2 snapshots
    String key1 = "key-1-";
    createFileKey(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volumea, bucketa, snap1);
    String snap2 = "snap" + counter.incrementAndGet();
    createSnapshot(volumea, bucketa, snap2);
    // Bucket is nonexistent
    LambdaTestUtils.intercept(OMException.class,
            "KEY_NOT_FOUND",
            () -> store.snapshotDiff(volumea, bucketb, snap1, snap2,
                null, 0, forceFullSnapshotDiff, forceNonNativeSnapshotDiff));
    // Volume is nonexistent
    LambdaTestUtils.intercept(OMException.class,
            "KEY_NOT_FOUND",
            () -> store.snapshotDiff(volumeb, bucketa, snap2, snap1,
                null, 0, forceFullSnapshotDiff, forceNonNativeSnapshotDiff));
    // Both volume and bucket are nonexistent
    LambdaTestUtils.intercept(OMException.class,
            "KEY_NOT_FOUND",
            () -> store.snapshotDiff(volumeb, bucketb, snap2, snap1,
                null, 0, forceFullSnapshotDiff, forceNonNativeSnapshotDiff));
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Key k1 is recreated.
   * 4) Snapshot s2 is created.
   * 5) Snapdiff b/w Snap1 & Snap2 taken to assert difference of 1 key.
   */
  @Test
  public void testSnapDiffWithKeyOverwrite() throws Exception {
    String testVolumeName = "vol" + RandomStringUtils.randomNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    volume.createBucket(testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    OmKeyInfo keyInfo = getOmKeyInfo(testVolumeName, testBucketName, key1);
    createFileKey(bucket, key1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap2);
    keyInfo = getOmKeyInfo(testVolumeName, testBucketName, key1);
    Assert.assertEquals(diff.getDiffList().size(), 1);
    Assert.assertEquals(diff.getDiffList(), Lists.newArrayList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.MODIFY, key1)));
  }

  @Test
  public void testSnapDiffMissingMandatoryParams() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    volume1.createBucket(bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    createFileKey(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap1);
    String snap2 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap2);
    String nullstr = "";
    // Destination snapshot is empty
    LambdaTestUtils.intercept(OMException.class,
            "KEY_NOT_FOUND",
            () -> store.snapshotDiff(volume, bucket, snap1, nullstr,
                null, 0, forceFullSnapshotDiff, forceNonNativeSnapshotDiff));
    // From snapshot is empty
    LambdaTestUtils.intercept(OMException.class,
            "KEY_NOT_FOUND",
            () -> store.snapshotDiff(volume, bucket, nullstr, snap1,
                null, 0, forceFullSnapshotDiff, forceNonNativeSnapshotDiff));
    // Bucket is empty
    assertThrows(IllegalArgumentException.class,
            () -> store.snapshotDiff(volume, nullstr, snap1, snap2,
                null, 0, forceFullSnapshotDiff, forceNonNativeSnapshotDiff));
    // Volume is empty
    assertThrows(IllegalArgumentException.class,
            () -> store.snapshotDiff(nullstr, bucket, snap1, snap2,
                null, 0, forceFullSnapshotDiff, forceNonNativeSnapshotDiff));
  }

  @Test
  public void testSnapDiffMultipleBuckets() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucketName1 = "buck-" + counter.incrementAndGet();
    String bucketName2 = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    volume1.createBucket(bucketName1);
    volume1.createBucket(bucketName2);
    OzoneBucket bucket1 = volume1.getBucket(bucketName1);
    OzoneBucket bucket2 = volume1.getBucket(bucketName2);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    key1 = createFileKey(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucketName1, snap1);
    // Create key in bucket2 and bucket1 and calculate diff
    // Diff should not contain bucket2's key
    createFileKey(bucket1, key1);
    createFileKey(bucket2, key1);
    String snap2 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucketName1, snap2);
    SnapshotDiffReportOzone diff1 =
        getSnapDiffReport(volume, bucketName1, snap1, snap2);
    Assert.assertEquals(1, diff1.getDiffList().size());
  }


  /**
   * Tests snapdiff when there are multiple sst files in the from & to
   * snapshots pertaining to different buckets. This will test the
   * sst filtering code path.
   */
  @Ignore //TODO - Fix in HDDS-8005
  @Test
  public void testSnapDiffWithMultipleSSTs()
      throws Exception {
    // Create a volume and 2 buckets
    String volumeName1 = "vol-" + counter.incrementAndGet();
    String bucketName1 = "buck1";
    String bucketName2 = "buck2";
    store.createVolume(volumeName1);
    OzoneVolume volume1 = store.getVolume(volumeName1);
    volume1.createBucket(bucketName1);
    volume1.createBucket(bucketName2);
    OzoneBucket bucket1 = volume1.getBucket(bucketName1);
    OzoneBucket bucket2 = volume1.getBucket(bucketName2);
    String keyPrefix = "key-";
    // add file to bucket1 and take snapshot
    createFileKeyWithPrefix(bucket1, keyPrefix);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volumeName1, bucketName1, snap1); // 1.sst
    Assert.assertEquals(1, getKeyTableSstFiles().size());
    // add files to bucket2 and flush twice to create 2 sst files
    for (int i = 0; i < 5; i++) {
      createFileKeyWithPrefix(bucket2, keyPrefix);
    }
    flushKeyTable(); // 1.sst 2.sst
    Assert.assertEquals(2, getKeyTableSstFiles().size());
    for (int i = 0; i < 5; i++) {
      createFileKeyWithPrefix(bucket2, keyPrefix);
    }
    flushKeyTable(); // 1.sst 2.sst 3.sst
    Assert.assertEquals(3, getKeyTableSstFiles().size());
    // add a file to bucket1 and take second snapshot
    createFileKeyWithPrefix(bucket1, keyPrefix);
    String snap2 = "snap" + counter.incrementAndGet();
    createSnapshot(volumeName1, bucketName1, snap2); // 1.sst 2.sst 3.sst 4.sst
    Assert.assertEquals(4, getKeyTableSstFiles().size());
    SnapshotDiffReportOzone diff1 =
        store.snapshotDiff(volumeName1, bucketName1, snap1, snap2,
                null, 0, forceFullSnapshotDiff, forceNonNativeSnapshotDiff)
            .getSnapshotDiffReport();
    Assert.assertEquals(1, diff1.getDiffList().size());
  }

  @Test
  public void testDeleteSnapshotTwice() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    volume1.createBucket(bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    createFileKey(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap1);
    store.deleteSnapshot(volume, bucket, snap1);

    LambdaTestUtils.intercept(OMException.class,
            "FILE_NOT_FOUND",
            () -> store.deleteSnapshot(volume, bucket, snap1));

  }

  @Test
  public void testDeleteSnapshotFailure() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    volume1.createBucket(bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    createFileKey(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap1);

    // Delete non-existent snapshot
    LambdaTestUtils.intercept(OMException.class,
            "FILE_NOT_FOUND",
            () -> store.deleteSnapshot(volume, bucket, "snapnonexistent"));

    // Delete snapshot with non-existent url
    LambdaTestUtils.intercept(OMException.class,
            "BUCKET_NOT_FOUND",
            () -> store.deleteSnapshot(volume, "nonexistentbucket", snap1));
  }

  @Test
  public void testDeleteSnapshotMissingMandatoryParams() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    volume1.createBucket(bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    createFileKey(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap1);
    String nullstr = "";
    // Snapshot is empty
    assertThrows(IllegalArgumentException.class,
            () -> store.deleteSnapshot(volume, bucket, nullstr));
    // Bucket is empty
    assertThrows(IllegalArgumentException.class,
            () -> store.deleteSnapshot(volume, nullstr, snap1));
    // Volume is empty
    assertThrows(IllegalArgumentException.class,
            () -> store.deleteSnapshot(nullstr, bucket, snap1));
  }

  @NotNull
  private static List<LiveFileMetaData> getKeyTableSstFiles() {
    if (!bucketLayout.isFileSystemOptimized()) {
      return rdbStore.getDb().getSstFileList().stream().filter(
          x -> new String(x.columnFamilyName(), UTF_8).equals(
              OmMetadataManagerImpl.KEY_TABLE)).collect(Collectors.toList());
    }
    return rdbStore.getDb().getSstFileList().stream().filter(
        x -> new String(x.columnFamilyName(), UTF_8).equals(
            OmMetadataManagerImpl.FILE_TABLE)).collect(Collectors.toList());
  }

  private static void flushKeyTable() throws IOException {
    if (!bucketLayout.isFileSystemOptimized()) {
      rdbStore.getDb().flush(OmMetadataManagerImpl.KEY_TABLE);
    } else {
      rdbStore.getDb().flush(OmMetadataManagerImpl.FILE_TABLE);
    }
  }

  private String createSnapshot(String volName, String buckName)
      throws IOException, InterruptedException, TimeoutException {
    return createSnapshot(volName, buckName, UUID.randomUUID().toString());
  }

  private String createSnapshot(String volName, String buckName,
      String snapshotName)
      throws IOException, InterruptedException, TimeoutException {
    store.createSnapshot(volName, buckName, snapshotName);
    String snapshotKeyPrefix =
        OmSnapshotManager.getSnapshotPrefix(snapshotName);
    SnapshotInfo snapshotInfo = ozoneManager.getMetadataManager()
        .getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(volName, buckName, snapshotName));
    String snapshotDirName =
        OmSnapshotManager.getSnapshotPath(ozoneManager.getConfiguration(),
            snapshotInfo) + OM_KEY_PREFIX + "CURRENT";
    GenericTestUtils
        .waitFor(() -> new File(snapshotDirName).exists(), 1000, 120000);
    return snapshotKeyPrefix;
  }

  private void deleteKeys(OzoneBucket bucket) throws IOException {
    Iterator<? extends OzoneKey> bucketIterator = bucket.listKeys(null);
    while (bucketIterator.hasNext()) {
      OzoneKey key = bucketIterator.next();
      bucket.deleteKey(key.getName());
    }
  }

  private String createFileKeyWithPrefix(OzoneBucket bucket, String keyPrefix)
      throws Exception {
    String key = keyPrefix + counter.incrementAndGet();
    return createFileKey(bucket, key);
  }

  private String createFileKey(OzoneBucket bucket, String key)
          throws Exception {
    byte[] value = RandomStringUtils.randomAscii(10240).getBytes(UTF_8);
    OzoneOutputStream fileKey = bucket.createKey(key, value.length);
    fileKey.write(value);
    fileKey.close();
    GenericTestUtils.waitFor(() -> {
      try {
        getOmKeyInfo(bucket.getVolumeName(), bucket.getName(), key);
      } catch (IOException e) {
        return false;
      }
      return true;
    }, 1000, 10000);
    return key;
  }

  private String createFileKey(FileSystem fs,
                               String path)
      throws IOException, InterruptedException, TimeoutException {
    byte[] value = RandomStringUtils.randomAscii(10240).getBytes(UTF_8);
    Path pathVal = new Path(path);
    FSDataOutputStream fileKey = fs.create(pathVal);
    fileKey.write(value);
    fileKey.close();
    GenericTestUtils.waitFor(() -> {
      try {
        fs.getFileStatus(pathVal);
      } catch (IOException e) {
        return false;
      }
      return true;
    }, 1000, 30000);
    return path;
  }

  @Test
  public void testUniqueSnapshotId() throws Exception {
    createFileKeyWithPrefix(ozoneBucket, "key");

    String snapshotName = UUID.randomUUID().toString();
    store.createSnapshot(volumeName, bucketName, snapshotName);
    List<OzoneManager> ozoneManagers = ((MiniOzoneHAClusterImpl) cluster)
        .getOzoneManagersList();
    List<String> snapshotIds = new ArrayList<>();

    for (OzoneManager om : ozoneManagers) {
      GenericTestUtils.waitFor(
          () -> {
            SnapshotInfo snapshotInfo;
            try {
              snapshotInfo = om.getMetadataManager()
                  .getSnapshotInfoTable()
                  .get(
                      SnapshotInfo.getTableKey(volumeName,
                          bucketName,
                          snapshotName)
                  );
            } catch (IOException e) {
              throw new RuntimeException(e);
            }

            if (snapshotInfo != null) {
              snapshotIds.add(snapshotInfo.getSnapshotId().toString());
            }
            return snapshotInfo != null;
          },
          1000,
          120000);
    }

    assertEquals(1, snapshotIds.stream().distinct().count());
  }

  @Test
  public void testSnapshotOpensWithDisabledAutoCompaction() throws Exception {
    String snapPrefix = createSnapshot(volumeName, bucketName);
    RDBStore snapshotDBStore = (RDBStore)
        ((OmSnapshot)cluster.getOzoneManager().getOmSnapshotManager()
            .checkForSnapshot(volumeName, bucketName, snapPrefix, false))
            .getMetadataManager().getStore();

    for (String table : snapshotDBStore.getTableNames().values()) {
      Assertions.assertTrue(snapshotDBStore.getDb().getColumnFamily(table)
          .getHandle().getDescriptor()
          .getOptions().disableAutoCompactions());
    }
  }

  // Test snapshot diff when OM restarts in non-HA OM env and diff job is
  // in_progress when it restarts.
  @Test
  public void testSnapshotDiffWhenOmRestart() throws Exception {
    String snapshot1 = "snap-" + RandomStringUtils.randomNumeric(5);
    String snapshot2 = "snap-" + RandomStringUtils.randomNumeric(5);
    createSnapshots(snapshot1, snapshot2);

    SnapshotDiffResponse response = store.snapshotDiff(volumeName, bucketName,
        snapshot1, snapshot2, null, 0, forceFullSnapshotDiff,
        forceNonNativeSnapshotDiff);

    assertEquals(IN_PROGRESS, response.getJobStatus());

    // Restart the OM and wait for sometime to make sure that previous snapDiff
    // job finishes.
    cluster.restartOzoneManager();
    await().atMost(Duration.ofSeconds(120)).
        until(() -> cluster.getOzoneManager().isRunning());

    response = store.snapshotDiff(volumeName, bucketName,
        snapshot1, snapshot2, null, 0, forceFullSnapshotDiff,
        forceNonNativeSnapshotDiff);

    // If job was IN_PROGRESS or DONE state when OM restarted, it should be
    // DONE by this time.
    // If job FAILED during crash (which mostly happens in the test because
    // of active snapshot checks), it would be removed by clean up service on
    // startup, and request after clean up will be considered a new request
    // and would return IN_PROGRESS. No other state is expected other than
    // IN_PROGRESS and DONE.
    if (response.getJobStatus() == DONE) {
      assertEquals(100, response.getSnapshotDiffReport().getDiffList().size());
    } else if (response.getJobStatus() == IN_PROGRESS) {
      SnapshotDiffReportOzone diffReport =
          fetchReportPage(snapshot1, snapshot2, null, 0);
      assertEquals(100, diffReport.getDiffList().size());
    } else {
      fail("Unexpected job status for the test.");
    }
  }

  // Test snapshot diff when OM restarts in non-HA OM env and report is
  // partially received.
  @Test
  public void testSnapshotDiffWhenOmRestartAndReportIsPartiallyFetched()
      throws Exception {
    int pageSize = 10;
    String snapshot1 = "snap-" + RandomStringUtils.randomNumeric(5);
    String snapshot2 = "snap-" + RandomStringUtils.randomNumeric(5);
    createSnapshots(snapshot1, snapshot2);

    SnapshotDiffReportOzone diffReport = fetchReportPage(snapshot1, snapshot2,
        null, pageSize);

    List<DiffReportEntry> diffReportEntries = diffReport.getDiffList();
    String nextToken = diffReport.getToken();

    // Restart the OM and no need to wait because snapDiff job finished before
    // the restart.
    cluster.restartOzoneManager();
    await().atMost(Duration.ofSeconds(120)).
        until(() -> cluster.getOzoneManager().isRunning());

    while (nextToken == null || StringUtils.isNotEmpty(nextToken)) {
      diffReport = fetchReportPage(snapshot1, snapshot2, nextToken, pageSize);
      diffReportEntries.addAll(diffReport.getDiffList());
      nextToken = diffReport.getToken();
    }
    assertEquals(100, diffReportEntries.size());
  }

  private SnapshotDiffReportOzone fetchReportPage(String fromSnapshot,
                                                  String toSnapshot,
                                                  String token,
                                                  int pageSize)
      throws IOException, InterruptedException {

    while (true) {
      SnapshotDiffResponse response = store.snapshotDiff(volumeName, bucketName,
          fromSnapshot, toSnapshot, token, pageSize, forceFullSnapshotDiff,
          forceNonNativeSnapshotDiff);
      if (response.getJobStatus() == IN_PROGRESS) {
        Thread.sleep(response.getWaitTimeInMs());
      } else if (response.getJobStatus() == DONE) {
        return response.getSnapshotDiffReport();
      } else {
        fail("Unexpected job status for the test.");
      }
    }
  }

  private void createSnapshots(String snapshot1,
                               String snapshot2) throws Exception {
    createFileKeyWithPrefix(ozoneBucket, "key");
    store.createSnapshot(volumeName, bucketName, snapshot1);

    for (int i = 0; i < 100; i++) {
      createFileKeyWithPrefix(ozoneBucket, "key-" + i);
    }

    store.createSnapshot(volumeName, bucketName, snapshot2);
  }

  @Test
  public void testCompactionDagDisableForSnapshotMetadata() throws Exception {
    String snapshotName = createSnapshot(volumeName, bucketName);

    RDBStore activeDbStore =
        (RDBStore) cluster.getOzoneManager().getMetadataManager().getStore();
    // RocksDBCheckpointDiffer should be not null for active DB store.
    assertNotNull(activeDbStore.getRocksDBCheckpointDiffer());
    assertEquals(2,  activeDbStore.getDbOptions().listeners().size());

    OmSnapshot omSnapshot = (OmSnapshot) cluster.getOzoneManager()
        .getOmSnapshotManager()
        .checkForSnapshot(volumeName, bucketName, snapshotName, false);

    RDBStore snapshotDbStore =
        (RDBStore) omSnapshot.getMetadataManager().getStore();
    // RocksDBCheckpointDiffer should be null for snapshot DB store.
    assertNull(snapshotDbStore.getRocksDBCheckpointDiffer());
    assertEquals(0, snapshotDbStore.getDbOptions().listeners().size());
  }
}
