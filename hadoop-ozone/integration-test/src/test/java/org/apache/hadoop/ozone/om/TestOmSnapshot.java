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
import java.time.Duration;
import java.util.List;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.service.SnapshotDiffCleanupService;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.admin.scm.FinalizeUpgradeCommandUtil.isDone;
import static org.apache.hadoop.ozone.admin.scm.FinalizeUpgradeCommandUtil.isStarting;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.DELIMITER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.CONTAINS_SNAPSHOT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.FILE_SYSTEM_OPTIMIZED;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.OBJECT_STORE;
import static org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse.CancelMessage.CANCEL_ALREADY_CANCELLED_JOB;
import static org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse.CancelMessage.CANCEL_JOB_NOT_EXIST;
import static org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse.CancelMessage.CANCEL_SUCCEEDED;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.CANCELLED;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.DONE;
import static org.awaitility.Awaitility.with;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.IN_PROGRESS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
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
  private static ObjectStore store;
  private static OzoneManager ozoneManager;
  private static RDBStore rdbStore;
  private static OzoneBucket ozoneBucket;
  private static final Duration POLL_INTERVAL_DURATION = Duration.ofMillis(500);
  private static final Duration POLL_MAX_DURATION = Duration.ofSeconds(10);

  private static AtomicInteger counter;
  private static final String SNAPSHOT_DAY_PREFIX = "snap-day-";
  private static final String SNAPSHOT_WEEK_PREFIX = "snap-week-";
  private static final String SNAPSHOT_MONTH_PREFIX = "snap-month-";
  private static final String KEY_PREFIX = "key-";
  private static final String SNAPSHOT_KEY_PATTERN_STRING = "(.+)/(.+)/(.+)";
  private static Pattern snapshotKeyPattern =
      Pattern.compile(SNAPSHOT_KEY_PATTERN_STRING);

  @Rule
  public Timeout timeout = new Timeout(300, TimeUnit.SECONDS);

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[]{OBJECT_STORE, false, false},
        new Object[]{FILE_SYSTEM_OPTIMIZED, false, false},
        new Object[]{BucketLayout.LEGACY, true, true});
  }

  public TestOmSnapshot(BucketLayout newBucketLayout,
      boolean newEnableFileSystemPaths, boolean forceFullSnapDiff)
      throws Exception {
    // Checking whether 'newBucketLayout' and
    // 'newEnableFileSystemPaths' flags represents next parameter
    // index values. This is to ensure that initialize init() function
    // will be invoked only at the beginning of every new set of
    // Parameterized.Parameters.
    if (TestOmSnapshot.enabledFileSystemPaths != newEnableFileSystemPaths ||
            TestOmSnapshot.bucketLayout != newBucketLayout ||
            TestOmSnapshot.forceFullSnapshotDiff != forceFullSnapDiff) {
      setConfig(newBucketLayout, newEnableFileSystemPaths,
          forceFullSnapDiff);
      tearDown();
      init();
    }
  }

  private static void setConfig(BucketLayout newBucketLayout,
      boolean newEnableFileSystemPaths, boolean forceFullSnapDiff) {
    TestOmSnapshot.enabledFileSystemPaths = newEnableFileSystemPaths;
    TestOmSnapshot.bucketLayout = newBucketLayout;
    TestOmSnapshot.forceFullSnapshotDiff = forceFullSnapDiff;
  }

  /**
   * Create a MiniDFSCluster for testing.
   */
  private void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
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

    // stop the deletion services so that keys can still be read
    keyManager.stop();
    preFinalizationChecks();
    finalizeOMUpgrade();
    counter = new AtomicInteger();
  }

  private static void expectFailurePreFinalization(LambdaTestUtils.
      VoidCallable eval) throws Exception {
    OMException ex  = assertThrows(OMException.class,
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
          "", 1000, false));
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
  public void testListKey()
      throws IOException, InterruptedException, TimeoutException {
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

    GenericTestUtils.waitFor(() -> {
      try {
        int keyCount = keyCount(ozoneBucket, key1);
        if (keyCount == 0) {
          return true;
        }

        ozoneBucket.deleteKey(key1);

        return false;
      } catch (Exception e) {
        return  false;
      }
    }, 1000, 10000);

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
  public void testListDeleteKey()
          throws IOException, InterruptedException, TimeoutException {
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
  public void testListAddNewKey()
          throws IOException, InterruptedException, TimeoutException {
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
    createFileKeyWithPrefix(bucket1, key1);
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
    createFileKeyWithPrefix(bucketWithSnapshot, key);
    createFileKeyWithPrefix(bucketWithoutSnapshot, key);
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
  public void testSnapDiffWithDirRename() throws Exception {
    Assume.assumeTrue(bucketLayout.isFileSystemOptimized());
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    volume1.createBucket(bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    bucket1.createDirectory("dir1");
    String snap1 = "snap1";
    createSnapshot(volume, bucket, snap1);
    bucket1.renameKey("dir1", "dir1_rename");
    String snap2 = "snap2";
    createSnapshot(volume, bucket, snap2);
    SnapshotDiffReportOzone diff = getSnapDiffReport(volume, bucket,
        snap1, snap2);
    Assertions.assertEquals(Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.RENAME, "/dir1", "/dir1_rename")),
        diff.getDiffList());
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

    // When from and to snapshots are same, it returns empty response.
    SnapshotDiffReportOzone
        diff0 = getSnapDiffReport(volume, bucket, snap1, snap1);
    assertTrue(diff0.getDiffList().isEmpty());

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
    Assert.assertEquals(
        Arrays.asList(SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE,
                OZONE_URI_DELIMITER + key1),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.CREATE,
                OZONE_URI_DELIMITER + key2)),
        diff2.getDiffList());

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
            SnapshotDiffReportOzone.DiffType.RENAME, OZONE_URI_DELIMITER + key2,
            OZONE_URI_DELIMITER + key2Renamed)));


    // Create a directory
    String dir1 = "dir-1" +  counter.incrementAndGet();
    bucket1.createDirectory(dir1);
    String snap5 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap5);
    SnapshotDiffReportOzone
        diff4 = getSnapDiffReport(volume, bucket, snap4, snap5);
    Assert.assertEquals(1, diff4.getDiffList().size());
    Assert.assertTrue(diff4.getDiffList().contains(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReportOzone.DiffType.CREATE,
            OM_KEY_PREFIX + dir1)));

    String key3 = createFileKeyWithPrefix(bucket1, "key-3-");
    String snap6 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap6);
    createFileKey(bucket1, key3);
    String renamedKey3 = key3 + "_renamed";
    bucket1.renameKey(key3, renamedKey3);

    String snap7 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap7);
    SnapshotDiffReportOzone
        diff5 = getSnapDiffReport(volume, bucket, snap6, snap7);
    List<SnapshotDiffReport.DiffReportEntry> expectedDiffList =
        Arrays.asList(SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.RENAME, OZONE_URI_DELIMITER + key3,
            OZONE_URI_DELIMITER + renamedKey3),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.MODIFY, OZONE_URI_DELIMITER + key3)
        );
    assertEquals(expectedDiffList, diff5.getDiffList());
  }

  @Test
  public void testSnapDiffCancel() throws Exception {
    // Create key1 and take snapshot.
    String key1 = "key-1-" + counter.incrementAndGet();
    createFileKey(ozoneBucket, key1);
    String fromSnapName = "snap-1-" + counter.incrementAndGet();
    createSnapshot(volumeName, bucketName, fromSnapName);

    // Create key2 and take snapshot.
    String key2 = "key-2-" + counter.incrementAndGet();
    createFileKey(ozoneBucket, key2);
    String toSnapName = "snap-2-" + counter.incrementAndGet();
    createSnapshot(volumeName, bucketName, toSnapName);

    SnapshotDiffResponse response = store.snapshotDiff(volumeName, bucketName,
        fromSnapName, toSnapName, null, 0, false);

    assertEquals(IN_PROGRESS, response.getJobStatus());

    CancelSnapshotDiffResponse cancelResponse =
        store.cancelSnapshotDiff(volumeName,
            bucketName, fromSnapName, toSnapName);

    assertEquals(CANCEL_SUCCEEDED.getMessage(), cancelResponse.getMessage());
    response = store.snapshotDiff(volumeName, bucketName, fromSnapName,
        toSnapName, null, 0, false);

    // Job status should be updated to CANCELLED.
    assertEquals(CANCELLED, response.getJobStatus());

    // Executing the command again should return the
    // CANCEL_ALREADY_CANCELLED_JOB message.
    cancelResponse = store.cancelSnapshotDiff(volumeName, bucketName,
        fromSnapName, toSnapName);

    assertEquals(CANCEL_ALREADY_CANCELLED_JOB.getMessage(),
        cancelResponse.getMessage());

    response = store.snapshotDiff(volumeName,
        bucketName, fromSnapName, toSnapName,
        null, 0, false);
    assertEquals(CANCELLED, response.getJobStatus());

    String fromSnapshotTableKey =
        SnapshotInfo.getTableKey(volumeName, bucketName, fromSnapName);
    String toSnapshotTableKey =
        SnapshotInfo.getTableKey(volumeName, bucketName, toSnapName);

    UUID fromSnapshotID = ozoneManager.getOmSnapshotManager()
        .getSnapshotInfo(fromSnapshotTableKey).getSnapshotId();
    UUID toSnapshotID = ozoneManager.getOmSnapshotManager()
        .getSnapshotInfo(toSnapshotTableKey).getSnapshotId();

    // Construct SnapshotDiffJob table key.
    String snapDiffJobKey = fromSnapshotID + DELIMITER + toSnapshotID;

    // Get the job from the SnapDiffJobTable, in order to get its ID.
    String jobID = ozoneManager.getOmSnapshotManager()
        .getSnapshotDiffManager().getSnapDiffJobTable()
        .get(snapDiffJobKey).getJobId();

    SnapshotDiffCleanupService snapshotDiffCleanupService =
        ozoneManager.getOmSnapshotManager().getSnapshotDiffCleanupService();

    // Run SnapshotDiffCleanupService.
    snapshotDiffCleanupService.run();
    // Verify that after running the cleanup service,
    // job exists in the purged job table.
    assertNotNull(snapshotDiffCleanupService.getEntryFromPurgedJobTable(jobID));
  }

  @Test
  public void testSnapDiffCancelFailureResponses() throws Exception {
    // Create key1 and take snapshot.
    String key1 = "key-1-" + counter.incrementAndGet();
    createFileKey(ozoneBucket, key1);
    String fromSnapName = "snap-1-" + counter.incrementAndGet();
    createSnapshot(volumeName, bucketName, fromSnapName);

    // Create key2 and take snapshot.
    String key2 = "key-2-" + counter.incrementAndGet();
    createFileKey(ozoneBucket, key2);
    String toSnapName = "snap-2-" + counter.incrementAndGet();
    createSnapshot(volumeName, bucketName, toSnapName);

    // New job that doesn't exist, cancel fails.
    CancelSnapshotDiffResponse cancelResponse =
        store.cancelSnapshotDiff(volumeName, bucketName, fromSnapName,
            toSnapName);

    assertEquals(CANCEL_JOB_NOT_EXIST.getMessage(),
        cancelResponse.getMessage());

    SnapshotDiffResponse response = store.snapshotDiff(
        volumeName, bucketName, fromSnapName, toSnapName,
        null, 0, false);

    Assert.assertEquals(IN_PROGRESS, response.getJobStatus());

    // Cancel success.
    cancelResponse = store.cancelSnapshotDiff(volumeName,
        bucketName, fromSnapName, toSnapName);

    Assert.assertEquals(CANCEL_SUCCEEDED.getMessage(),
        cancelResponse.getMessage());

    response = store.snapshotDiff(
        volumeName, bucketName, fromSnapName, toSnapName,
        null, 0, false);

    Assert.assertEquals(CANCELLED, response.getJobStatus());

    // Job already cancelled.
    cancelResponse = store.cancelSnapshotDiff(volumeName,
        bucketName, fromSnapName, toSnapName);
    Assert.assertEquals(CANCEL_ALREADY_CANCELLED_JOB.getMessage(),
        cancelResponse.getMessage());
  }

  private SnapshotDiffReportOzone getSnapDiffReport(String volume,
                                                    String bucket,
                                                    String fromSnapshot,
                                                    String toSnapshot)
      throws InterruptedException, IOException {
    SnapshotDiffResponse response;
    do {
      response = store.snapshotDiff(volume, bucket, fromSnapshot,
          toSnapshot, null, 0, false);
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
    createFileKeyWithPrefix(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap1);
    String snap2 = "snap" + counter.incrementAndGet();

    // Destination snapshot is invalid
    OMException omException = assertThrows(OMException.class,
        () -> store.snapshotDiff(volume, bucket, snap1, snap2,
            null, 0, false));
    assertEquals(KEY_NOT_FOUND, omException.getResult());
    // From snapshot is invalid
    omException = assertThrows(OMException.class,
        () -> store.snapshotDiff(volume, bucket, snap2, snap1,
            null, 0, false));

    assertEquals(KEY_NOT_FOUND, omException.getResult());

    createSnapshot(volume, bucket, snap2);

    omException = assertThrows(OMException.class, () ->
        store.snapshotDiff(volume, bucket, snap2, snap1, null, 0, false)
    );

    assertEquals(INTERNAL_ERROR, omException.getResult());
    assertEquals("fromSnapshot:" + snap2 + " should be older than to " +
        "toSnapshot:" + snap1, omException.getMessage());
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
    createFileKeyWithPrefix(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volumea, bucketa, snap1);
    String snap2 = "snap" + counter.incrementAndGet();
    createSnapshot(volumea, bucketa, snap2);
    // Bucket is nonexistent
    LambdaTestUtils.intercept(OMException.class,
            "KEY_NOT_FOUND",
            () -> store.snapshotDiff(volumea, bucketb, snap1, snap2,
                null, 0, false));
    // Volume is nonexistent
    LambdaTestUtils.intercept(OMException.class,
            "KEY_NOT_FOUND",
            () -> store.snapshotDiff(volumeb, bucketa, snap2, snap1,
                null, 0, false));
    // Both volume and bucket are nonexistent
    LambdaTestUtils.intercept(OMException.class,
            "KEY_NOT_FOUND",
            () -> store.snapshotDiff(volumeb, bucketb, snap2, snap1,
                null, 0, false));
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
    createFileKeyWithPrefix(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap1);
    String snap2 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap2);
    String nullstr = "";
    // Destination snapshot is empty
    LambdaTestUtils.intercept(OMException.class,
            "KEY_NOT_FOUND",
            () -> store.snapshotDiff(volume, bucket, snap1, nullstr,
                null, 0, false));
    // From snapshot is empty
    LambdaTestUtils.intercept(OMException.class,
            "KEY_NOT_FOUND",
            () -> store.snapshotDiff(volume, bucket, nullstr, snap1,
                null, 0, false));
    // Bucket is empty
    assertThrows(IllegalArgumentException.class,
            () -> store.snapshotDiff(volume, nullstr, snap1, snap2,
                null, 0, false));
    // Volume is empty
    assertThrows(IllegalArgumentException.class,
            () -> store.snapshotDiff(nullstr, bucket, snap1, snap2,
                null, 0, false));
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
    key1 = createFileKeyWithPrefix(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucketName1, snap1);
    // Create key in bucket2 and bucket1 and calculate diff
    // Diff should not contain bucket2's key
    createFileKeyWithPrefix(bucket1, key1);
    createFileKeyWithPrefix(bucket2, key1);
    String snap2 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucketName1, snap2);
    SnapshotDiffReportOzone diff1 =
        getSnapDiffReport(volume, bucketName1, snap1, snap2);
    Assert.assertEquals(1, diff1.getDiffList().size());
  }

  @Test
  public void testListSnapshotDiffWithInvalidParameters()
      throws Exception {
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket = "buck-" + RandomStringUtils.randomNumeric(5);

    String volBucketErrorMessage = "Provided volume name " + volume +
        " or bucket name " + bucket + " doesn't exist";

    Exception volBucketEx = Assertions.assertThrows(OMException.class,
        () -> store.listSnapshotDiffJobs(volume, bucket,
            "", true));
    Assertions.assertEquals(volBucketErrorMessage,
        volBucketEx.getMessage());

    // Create the volume and the bucket.
    store.createVolume(volume);
    OzoneVolume ozVolume = store.getVolume(volume);
    ozVolume.createBucket(bucket);

    Assertions.assertDoesNotThrow(() ->
        store.listSnapshotDiffJobs(volume, bucket, "", true));

    // There are no snapshots, response should be empty.
    Assertions.assertTrue(store
        .listSnapshotDiffJobs(volume, bucket,
            "", true).isEmpty());

    OzoneBucket ozBucket = ozVolume.getBucket(bucket);
    // Create keys and take snapshots.
    String key1 = "key-1-" + RandomStringUtils.randomNumeric(5);
    createFileKey(ozBucket, key1);
    String snap1 = "snap-1-" + RandomStringUtils.randomNumeric(5);
    createSnapshot(volume, bucket, snap1);

    String key2 = "key-2-" + RandomStringUtils.randomNumeric(5);
    createFileKey(ozBucket, key2);
    String snap2 = "snap-2-" + RandomStringUtils.randomNumeric(5);
    createSnapshot(volume, bucket, snap2);

    store.snapshotDiff(volume, bucket, snap1, snap2, null, 0, true);

    String invalidStatus = "invalid";
    String statusErrorMessage = "Invalid job status: " + invalidStatus;

    Exception statusEx = Assertions.assertThrows(OMException.class,
        () -> store.listSnapshotDiffJobs(volume, bucket,
            invalidStatus, false));
    Assertions.assertEquals(statusErrorMessage,
        statusEx.getMessage());
  }

  /**
   * Tests snapdiff when there are multiple sst files in the from & to
   * snapshots pertaining to different buckets. This will test the
   * sst filtering code path.
   */
  @Ignore //TODO - Fix in HDDS-8005
  @Test
  public void testSnapDiffWithMultipleSSTs()
      throws IOException, InterruptedException, TimeoutException {
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
                null, 0, false)
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
    createFileKeyWithPrefix(bucket1, key1);
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
    createFileKeyWithPrefix(bucket1, key1);
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
    createFileKeyWithPrefix(bucket1, key1);
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
      throws IOException {
    String key = keyPrefix + counter.incrementAndGet();
    createFileKey(bucket, key);
    return key;
  }

  private void createFileKey(OzoneBucket bucket, String key)
      throws IOException {
    byte[] value = RandomStringUtils.randomAscii(10240).getBytes(UTF_8);
    OzoneOutputStream fileKey = bucket.createKey(key, value.length);
    fileKey.write(value);
    fileKey.close();
  }

  @Test
  public void testSnapshotOpensWithDisabledAutoCompaction() throws Exception {
    String snapPrefix = createSnapshot(volumeName, bucketName);
    RDBStore snapshotDBStore = (RDBStore)
        ((OmSnapshot) cluster.getOzoneManager().getOmSnapshotManager()
            .checkForSnapshot(volumeName, bucketName, snapPrefix, false).get())
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
  public void testSnapshotDiffWhenOmRestart()
      throws IOException, InterruptedException {
    String snapshot1 = "snap-" + RandomStringUtils.randomNumeric(5);
    String snapshot2 = "snap-" + RandomStringUtils.randomNumeric(5);
    createSnapshots(snapshot1, snapshot2);

    SnapshotDiffResponse response = store.snapshotDiff(volumeName, bucketName,
        snapshot1, snapshot2, null, 0, false);

    assertEquals(IN_PROGRESS, response.getJobStatus());

    // Restart the OM and wait for sometime to make sure that previous snapDiff
    // job finishes.
    cluster.restartOzoneManager();
    await().atMost(Duration.ofSeconds(120)).
        until(() -> cluster.getOzoneManager().isRunning());

    response = store.snapshotDiff(volumeName, bucketName,
        snapshot1, snapshot2, null, 0, false);

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
      throws IOException, InterruptedException {
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
          fromSnapshot, toSnapshot, token, pageSize, false);
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
                               String snapshot2) throws IOException {
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
        .checkForSnapshot(volumeName, bucketName, snapshotName, false).get();

    RDBStore snapshotDbStore =
        (RDBStore) omSnapshot.getMetadataManager().getStore();
    // RocksDBCheckpointDiffer should be null for snapshot DB store.
    assertNull(snapshotDbStore.getRocksDBCheckpointDiffer());
    assertEquals(0, snapshotDbStore.getDbOptions().listeners().size());
  }

  @Test
  public void testDayWeekMonthSnapshotCreationAndExpiration() throws Exception {
    String volumeA = "vol-a-" + RandomStringUtils.randomNumeric(5);
    String bucketA = "buc-a-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volumeA);
    OzoneVolume volA = store.getVolume(volumeA);
    volA.createBucket(bucketA);
    OzoneBucket volAbucketA = volA.getBucket(bucketA);

    int latestDayIndex = 0;
    int latestWeekIndex = 0;
    int latestMonthIndex = 0;
    int oldestDayIndex = latestDayIndex;
    int oldestWeekIndex = latestWeekIndex;
    int oldestMonthIndex = latestMonthIndex;
    int daySnapshotRetentionPeriodDays = 7;
    int weekSnapshotRetentionPeriodWeek = 1;
    int monthSnapshotRetentionPeriodMonth = 1;
    int[] updatedDayIndexArr;
    int[] updatedWeekIndexArr;
    int[] updatedMonthIndexArr;
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 4; j++) {
        for (int k = 0; k < 7; k++) {
          // If there are seven day's snapshots in cluster already,
          // remove the oldest day snapshot then create the latest day snapshot
          updatedDayIndexArr = checkSnapshotExpirationThenCreateLatest(
              SNAPSHOT_DAY_PREFIX, oldestDayIndex, latestDayIndex,
              oldestWeekIndex, latestWeekIndex,
              oldestMonthIndex, latestMonthIndex,
              daySnapshotRetentionPeriodDays, volumeA, bucketA, volAbucketA);
          oldestDayIndex = updatedDayIndexArr[0];
          latestDayIndex = updatedDayIndexArr[1];
        }
        // If there is one week's snapshot in cluster already,
        // remove the oldest week snapshot then create the latest week snapshot
        updatedWeekIndexArr = checkSnapshotExpirationThenCreateLatest(
            SNAPSHOT_WEEK_PREFIX, oldestDayIndex, latestDayIndex,
            oldestWeekIndex, latestWeekIndex,
            oldestMonthIndex, latestMonthIndex,
            weekSnapshotRetentionPeriodWeek, volumeA, bucketA, volAbucketA);
        oldestWeekIndex = updatedWeekIndexArr[0];
        latestWeekIndex = updatedWeekIndexArr[1];
      }
      // If there is one month's snapshot in cluster already,
      // remove the oldest month snapshot then create the latest month snapshot
      updatedMonthIndexArr = checkSnapshotExpirationThenCreateLatest(
          SNAPSHOT_MONTH_PREFIX, oldestDayIndex, latestDayIndex,
          oldestWeekIndex, latestWeekIndex, oldestMonthIndex, latestMonthIndex,
          monthSnapshotRetentionPeriodMonth, volumeA, bucketA, volAbucketA);
      oldestMonthIndex = updatedMonthIndexArr[0];
      latestMonthIndex = updatedMonthIndexArr[1];
    }
  }

  @SuppressWarnings("parameternumber")
  private int[] checkSnapshotExpirationThenCreateLatest(String snapshotPrefix,
      int oldestDayIndex, int latestDayIndex,
      int oldestWeekIndex, int latestWeekIndex,
      int oldestMonthIndex, int latestMonthIndex,
      int snapshotRetentionPeriod,
      String volumeNameStr, String bucketNameStr, OzoneBucket ozoneBucketClient)
      throws Exception {
    int targetOldestIndex = 0;
    int targetLatestIndex = 0;
    if (snapshotPrefix.equals(SNAPSHOT_DAY_PREFIX)) {
      targetOldestIndex = oldestDayIndex;
      targetLatestIndex = latestDayIndex;
    } else if (snapshotPrefix.equals(SNAPSHOT_WEEK_PREFIX)) {
      targetOldestIndex = oldestWeekIndex;
      targetLatestIndex = latestWeekIndex;
    } else if (snapshotPrefix.equals(SNAPSHOT_MONTH_PREFIX)) {
      targetOldestIndex = oldestMonthIndex;
      targetLatestIndex = latestMonthIndex;
    }
    targetOldestIndex = deleteOldestSnapshot(targetOldestIndex,
        targetLatestIndex, snapshotPrefix, snapshotRetentionPeriod,
        oldestDayIndex, latestDayIndex, oldestWeekIndex, latestWeekIndex,
        oldestMonthIndex, latestMonthIndex, volumeNameStr, bucketNameStr,
        snapshotPrefix + targetOldestIndex, ozoneBucketClient);

    // When it's day, create a new key.
    // Week/month period will only create new snapshot
    if (snapshotPrefix.equals(SNAPSHOT_DAY_PREFIX)) {
      createFileKeyWithData(ozoneBucketClient, KEY_PREFIX + latestDayIndex);
    }
    createSnapshot(volumeNameStr, bucketNameStr,
        snapshotPrefix + targetLatestIndex);
    targetLatestIndex++;
    return new int[]{targetOldestIndex, targetLatestIndex};
  }

  @SuppressWarnings("parameternumber")
  private int deleteOldestSnapshot(int targetOldestIndex, int targetLatestIndex,
      String snapshotPrefix, int snapshotRetentionPeriod, int oldestDayIndex,
      int latestDayIndex, int oldestWeekIndex, int latestWeekIndex,
      int oldestMonthIndex, int latestMonthIndex, String volumeNameStr,
      String bucketNameStr, String snapshotName, OzoneBucket ozoneBucketClient)
        throws Exception {
    if (targetLatestIndex - targetOldestIndex >= snapshotRetentionPeriod) {
      store.deleteSnapshot(volumeNameStr, bucketNameStr, snapshotName);
      targetOldestIndex++;

      if (snapshotPrefix.equals(SNAPSHOT_DAY_PREFIX)) {
        oldestDayIndex = targetOldestIndex;
      } else if (snapshotPrefix.equals(SNAPSHOT_WEEK_PREFIX)) {
        oldestWeekIndex = targetOldestIndex;
      } else if (snapshotPrefix.equals(SNAPSHOT_MONTH_PREFIX)) {
        oldestMonthIndex = targetOldestIndex;
      }

      checkDayWeekMonthSnapshotData(ozoneBucketClient,
          oldestDayIndex, latestDayIndex,
          oldestWeekIndex, latestWeekIndex,
          oldestMonthIndex, latestMonthIndex);
    }
    return targetOldestIndex;
  }

  private void checkDayWeekMonthSnapshotData(OzoneBucket ozoneBucketClient,
      int oldestDayIndex, int latestDayIndex,
      int oldestWeekIndex, int latestWeekIndex,
      int oldestMonthIndex, int latestMonthIndex) throws Exception {
    for (int i = 0; i < latestDayIndex; i++) {
      String keyName = KEY_PREFIX + i;
      // Validate keys metadata in active Ozone namespace
      OzoneKeyDetails ozoneKeyDetails = ozoneBucketClient.getKey(keyName);
      Assert.assertEquals(keyName, ozoneKeyDetails.getName());
      Assert.assertEquals(ozoneBucketClient.getName(),
          ozoneKeyDetails.getBucketName());
      Assert.assertEquals(ozoneBucketClient.getVolumeName(),
          ozoneKeyDetails.getVolumeName());

      // Validate keys data in active Ozone namespace
      try (OzoneInputStream ozoneInputStream =
               ozoneBucketClient.readKey(keyName)) {
        byte[] fileContent = new byte[keyName.length()];
        ozoneInputStream.read(fileContent);
        Assert.assertEquals(keyName, new String(fileContent, UTF_8));
      }
    }
    // Validate history day snapshot data integrity
    validateSnapshotDataIntegrity(SNAPSHOT_DAY_PREFIX, oldestDayIndex,
        latestDayIndex, ozoneBucketClient);
    // Validate history week snapshot data integrity
    validateSnapshotDataIntegrity(SNAPSHOT_WEEK_PREFIX, oldestWeekIndex,
        latestWeekIndex, ozoneBucketClient);
    // Validate history month snapshot data integrity
    validateSnapshotDataIntegrity(SNAPSHOT_MONTH_PREFIX, oldestMonthIndex,
        latestMonthIndex, ozoneBucketClient);
  }

  private void validateSnapshotDataIntegrity(String snapshotPrefix,
      int oldestIndex, int latestIndex, OzoneBucket ozoneBucketClient)
      throws Exception {
    if (latestIndex == 0) {
      return;
    }
    // Verify key exists from the oldest day/week/month snapshot
    // to latest snapshot
    for (int i = oldestIndex; i < latestIndex; i++) {
      Iterator<? extends OzoneKey> iterator =
          ozoneBucketClient.listKeys(
              OmSnapshotManager.getSnapshotPrefix(snapshotPrefix + i));
      while (iterator.hasNext()) {
        String keyName = iterator.next().getName();
        try (OzoneInputStream ozoneInputStream =
                 ozoneBucketClient.readKey(keyName)) {
          // We previously created key content as only "key-{index}"
          // Thus need to remove snapshot related prefix from key name by regex
          // before use key name to compare key content
          // e.g.,".snapshot/snap-day-1/key-0" -> "key-0"
          Matcher snapshotKeyNameMatcher = snapshotKeyPattern.matcher(keyName);
          if (snapshotKeyNameMatcher.matches()) {
            String truncatedSnapshotKeyName = snapshotKeyNameMatcher.group(3);
            byte[] fileContent = new byte[truncatedSnapshotKeyName.length()];
            ozoneInputStream.read(fileContent);
            Assert.assertEquals(truncatedSnapshotKeyName,
                new String(fileContent, UTF_8));
          }
        }
      }
    }
  }

  private void createFileKeyWithData(OzoneBucket bucket, String keyName)
      throws IOException {
    OzoneOutputStream fileKey = bucket.createKey(keyName,
        keyName.length());
    // Use key name as key content
    fileKey.write(keyName.getBytes(UTF_8));
    fileKey.close();
  }

}
