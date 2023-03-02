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
import java.util.List;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
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
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReport;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.AfterClass;
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
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.CONTAINS_SNAPSHOT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.FILE_SYSTEM_OPTIMIZED;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.OBJECT_STORE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertThrows;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Test OmSnapshot bucket interface.
 */
@RunWith(Parameterized.class)
@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
public class TestOmSnapshot {
  private static MiniOzoneCluster cluster = null;
  private static String volumeName;
  private static String bucketName;
  private static OzoneManagerProtocol writeClient;
  private static BucketLayout bucketLayout = BucketLayout.LEGACY;
  private static boolean enabledFileSystemPaths;
  private static ObjectStore store;
  private static File metaDir;
  private static OzoneManager leaderOzoneManager;

  private static RDBStore rdbStore;

  private static OzoneBucket ozoneBucket;

  @Rule
  public Timeout timeout = new Timeout(180, TimeUnit.SECONDS);

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
                         new Object[]{OBJECT_STORE, false},
                         new Object[]{FILE_SYSTEM_OPTIMIZED, false},
                         new Object[]{BucketLayout.LEGACY, true});
  }

  public TestOmSnapshot(BucketLayout newBucketLayout,
      boolean newEnableFileSystemPaths) throws Exception {
    // Checking whether 'newBucketLayout' and
    // 'newEnableFileSystemPaths' flags represents next parameter
    // index values. This is to ensure that initialize init() function
    // will be invoked only at the beginning of every new set of
    // Parameterized.Parameters.
    if (TestOmSnapshot.enabledFileSystemPaths != newEnableFileSystemPaths ||
            TestOmSnapshot.bucketLayout != newBucketLayout) {
      setConfig(newBucketLayout, newEnableFileSystemPaths);
      tearDown();
      init();
    }
  }

  private static void setConfig(BucketLayout newBucketLayout,
      boolean newEnableFileSystemPaths) {
    TestOmSnapshot.enabledFileSystemPaths = newEnableFileSystemPaths;
    TestOmSnapshot.bucketLayout = newBucketLayout;
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
    conf.setEnum(HDDS_DB_PROFILE, DBProfile.TEST);

    cluster = MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId("om-service-test1")
        .setNumOfOzoneManagers(3)
        .build();
    cluster.waitForClusterToBeReady();
    // create a volume and a bucket to be used by OzoneFileSystem
    ozoneBucket = TestDataUtil
        .createVolumeAndBucket(cluster, bucketLayout);
    volumeName = ozoneBucket.getVolumeName();
    bucketName = ozoneBucket.getName();

    leaderOzoneManager = ((MiniOzoneHAClusterImpl) cluster).getOMLeader();
    rdbStore =
        (RDBStore) leaderOzoneManager.getMetadataManager().getStore();
    OzoneConfiguration leaderConfig = leaderOzoneManager.getConfiguration();
    cluster.setConf(leaderConfig);

    OzoneClient client = cluster.getClient();
    store = client.getObjectStore();
    writeClient = store.getClientProxy().getOzoneManagerClient();

    KeyManagerImpl keyManager = (KeyManagerImpl) HddsWhiteboxTestUtils
        .getInternalState(leaderOzoneManager, "keyManager");

    // stop the deletion services so that keys can still be read
    keyManager.stop();
    metaDir = OMStorage.getOmDbDir(leaderConfig);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  // based on TestOzoneRpcClientAbstract:testListKey
  public void testListKey()
      throws IOException, InterruptedException, TimeoutException {
    String volumeA = "vol-a-" + RandomStringUtils.randomNumeric(5);
    String volumeB = "vol-b-" + RandomStringUtils.randomNumeric(5);
    String bucketA = "buc-a-" + RandomStringUtils.randomNumeric(5);
    String bucketB = "buc-b-" + RandomStringUtils.randomNumeric(5);
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
      createFileKey(volAbucketA, keyBaseA + i + "-");
      createFileKey(volAbucketB, keyBaseA + i + "-");
      createFileKey(volBbucketA, keyBaseA + i + "-");
      createFileKey(volBbucketB, keyBaseA + i + "-");
    }
    /*
    Create 10 keys in  vol-a-<random>/buc-a-<random>,
    vol-a-<random>/buc-b-<random>, vol-b-<random>/buc-a-<random> and
    vol-b-<random>/buc-b-<random>
     */
    String keyBaseB = "key-b-";
    for (int i = 0; i < 10; i++) {
      createFileKey(volAbucketA, keyBaseB + i + "-");
      createFileKey(volAbucketB, keyBaseB + i + "-");
      createFileKey(volBbucketA, keyBaseB + i + "-");
      createFileKey(volBbucketB, keyBaseB + i + "-");
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
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket = "buc-" + RandomStringUtils.randomNumeric(5);
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
  public void testListDeleteKey()
          throws IOException, InterruptedException, TimeoutException {
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket = "buc-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    vol.createBucket(bucket);
    OzoneBucket volBucket = vol.getBucket(bucket);

    String key = "key-";
    createFileKey(volBucket, key);
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
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket = "buc-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    vol.createBucket(bucket);
    OzoneBucket bucket1 = vol.getBucket(bucket);

    String key1 = "key-1-";
    createFileKey(bucket1, key1);
    String snapshotKeyPrefix1 = createSnapshot(volume, bucket);

    String key2 = "key-2-";
    createFileKey(bucket1, key2);
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
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket = "buc-" + RandomStringUtils.randomNumeric(5);
    //create volume but not bucket
    store.createVolume(volume);

    LambdaTestUtils.intercept(OMException.class,
            "Bucket not found",
            () -> createSnapshot(volume, bucket));
  }

  @Test
  public void testCreateSnapshotMissingMandatoryParams() throws Exception {
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket = "buck-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    volume1.createBucket(bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    createFileKey(bucket1, key1);
    String snap1 = "snap" + RandomStringUtils.randomNumeric(5);
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
    String volume1 = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket1 = "buc-" + RandomStringUtils.randomNumeric(5);
    String bucket2 = "buc-" + RandomStringUtils.randomNumeric(5);
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
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket = "buck-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    volume1.createBucket(bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    key1 = createFileKey(bucket1, key1);
    String snap1 = "snap" + RandomStringUtils.randomNumeric(5);
    createSnapshot(volume, bucket, snap1);
    // Do nothing, take another snapshot
    String snap2 = "snap" + RandomStringUtils.randomNumeric(5);
    createSnapshot(volume, bucket, snap2);
    SnapshotDiffReport diff1 = store.snapshotDiff(volume, bucket, snap1, snap2);
    Assert.assertTrue(diff1.getDiffList().isEmpty());
    // Create Key2 and delete Key1, take snapshot
    String key2 = "key-2-";
    key2 = createFileKey(bucket1, key2);
    bucket1.deleteKey(key1);
    String snap3 = "snap" + RandomStringUtils.randomNumeric(5);
    createSnapshot(volume, bucket, snap3);
    // Diff should have 2 entries
    SnapshotDiffReport diff2 = store.snapshotDiff(volume, bucket, snap2, snap3);
    Assert.assertEquals(2, diff2.getDiffList().size());
    Assert.assertTrue(diff2.getDiffList().contains(
        SnapshotDiffReport.DiffReportEntry
            .of(SnapshotDiffReport.DiffType.CREATE, key2)));
    Assert.assertTrue(diff2.getDiffList().contains(
        SnapshotDiffReport.DiffReportEntry
            .of(SnapshotDiffReport.DiffType.DELETE, key1)));

    // Rename Key2
    String key2Renamed = key2 + "_renamed";
    bucket1.renameKey(key2, key2Renamed);
    String snap4 = "snap" + RandomStringUtils.randomNumeric(5);
    createSnapshot(volume, bucket, snap4);
    SnapshotDiffReport diff3 = store.snapshotDiff(volume, bucket, snap3, snap4);
    Assert.assertEquals(1, diff3.getDiffList().size());
    Assert.assertTrue(diff3.getDiffList().contains(
        SnapshotDiffReport.DiffReportEntry
            .of(SnapshotDiffReport.DiffType.RENAME, key2, key2Renamed)));


    // Create a directory
    String dir1 = "dir-1" +  RandomStringUtils.randomNumeric(5);
    bucket1.createDirectory(dir1);
    String snap5 = "snap" + RandomStringUtils.randomNumeric(5);
    createSnapshot(volume, bucket, snap5);
    SnapshotDiffReport diff4 = store.snapshotDiff(volume, bucket, snap4, snap5);
    Assert.assertEquals(1, diff4.getDiffList().size());
    // for non-fso, directories are a special type of key with "/" appended
    // at the end.
    if (!bucket1.getBucketLayout().isFileSystemOptimized()) {
      dir1 = dir1 + OM_KEY_PREFIX;
    }
    Assert.assertTrue(diff4.getDiffList().contains(
        SnapshotDiffReport.DiffReportEntry
            .of(SnapshotDiffReport.DiffType.CREATE, dir1)));

  }

  @Test
  public void testSnapDiffNoSnapshot() throws Exception {
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket = "buck-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    volume1.createBucket(bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    createFileKey(bucket1, key1);
    String snap1 = "snap" + RandomStringUtils.randomNumeric(5);
    createSnapshot(volume, bucket, snap1);
    String snap2 = "snap" + RandomStringUtils.randomNumeric(5);
    // Destination snapshot is invalid
    LambdaTestUtils.intercept(OMException.class,
            "KEY_NOT_FOUND",
            () -> store.snapshotDiff(volume, bucket, snap1, snap2));
    // From snapshot is invalid
    LambdaTestUtils.intercept(OMException.class,
            "KEY_NOT_FOUND",
            () -> store.snapshotDiff(volume, bucket, snap2, snap1));
  }

  @Test
  public void testSnapDiffNonExistentUrl() throws Exception {
    // Valid volume bucket
    String volumea = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucketa = "buck-" + RandomStringUtils.randomNumeric(5);
    // Dummy volume bucket
    String volumeb = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucketb = "buck-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volumea);
    OzoneVolume volume1 = store.getVolume(volumea);
    volume1.createBucket(bucketa);
    OzoneBucket bucket1 = volume1.getBucket(bucketa);
    // Create Key1 and take 2 snapshots
    String key1 = "key-1-";
    createFileKey(bucket1, key1);
    String snap1 = "snap" + RandomStringUtils.randomNumeric(5);
    createSnapshot(volumea, bucketa, snap1);
    String snap2 = "snap" + RandomStringUtils.randomNumeric(5);
    createSnapshot(volumea, bucketa, snap2);
    // Bucket is nonexistent
    LambdaTestUtils.intercept(OMException.class,
            "KEY_NOT_FOUND",
            () -> store.snapshotDiff(volumea, bucketb, snap1, snap2));
    // Volume is nonexistent
    LambdaTestUtils.intercept(OMException.class,
            "KEY_NOT_FOUND",
            () -> store.snapshotDiff(volumeb, bucketa, snap2, snap1));
    // Both volume and bucket are nonexistent
    LambdaTestUtils.intercept(OMException.class,
            "KEY_NOT_FOUND",
            () -> store.snapshotDiff(volumeb, bucketb, snap2, snap1));
  }

  @Test
  public void testSnapDiffMissingMandatoryParams() throws Exception {
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket = "buck-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    volume1.createBucket(bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    createFileKey(bucket1, key1);
    String snap1 = "snap" + RandomStringUtils.randomNumeric(5);
    createSnapshot(volume, bucket, snap1);
    String snap2 = "snap" + RandomStringUtils.randomNumeric(5);
    createSnapshot(volume, bucket, snap2);
    String nullstr = "";
    // Destination snapshot is empty
    LambdaTestUtils.intercept(OMException.class,
            "KEY_NOT_FOUND",
            () -> store.snapshotDiff(volume, bucket, snap1, nullstr));
    // From snapshot is empty
    LambdaTestUtils.intercept(OMException.class,
            "KEY_NOT_FOUND",
            () -> store.snapshotDiff(volume, bucket, nullstr, snap1));
    // Bucket is empty
    assertThrows(IllegalArgumentException.class,
            () -> store.snapshotDiff(volume, nullstr, snap1, snap2));
    // Volume is empty
    assertThrows(IllegalArgumentException.class,
            () -> store.snapshotDiff(nullstr, bucket, snap1, snap2));
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
    String volumeName1 = "vol-" + RandomStringUtils.randomNumeric(5);
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
    createFileKey(bucket1, keyPrefix);
    String snap1 = "snap" + RandomStringUtils.randomNumeric(5);
    createSnapshot(volumeName1, bucketName1, snap1); // 1.sst
    Assert.assertEquals(1, getKeyTableSstFiles().size());
    // add files to bucket2 and flush twice to create 2 sst files
    for (int i = 0; i < 5; i++) {
      createFileKey(bucket2, keyPrefix);
    }
    flushKeyTable(); // 1.sst 2.sst
    Assert.assertEquals(2, getKeyTableSstFiles().size());
    for (int i = 0; i < 5; i++) {
      createFileKey(bucket2, keyPrefix);
    }
    flushKeyTable(); // 1.sst 2.sst 3.sst
    Assert.assertEquals(3, getKeyTableSstFiles().size());
    // add a file to bucket1 and take second snapshot
    createFileKey(bucket1, keyPrefix);
    String snap2 = "snap" + RandomStringUtils.randomNumeric(5);
    createSnapshot(volumeName1, bucketName1, snap2); // 1.sst 2.sst 3.sst 4.sst
    Assert.assertEquals(4, getKeyTableSstFiles().size());
    SnapshotDiffReport diff1 =
        store.snapshotDiff(volumeName1, bucketName1, snap1, snap2);
    Assert.assertEquals(1, diff1.getDiffList().size());
  }

  @Test
  public void testDeleteSnapshotTwice() throws Exception {
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket = "buck-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    volume1.createBucket(bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    createFileKey(bucket1, key1);
    String snap1 = "snap" + RandomStringUtils.randomNumeric(5);
    createSnapshot(volume, bucket, snap1);
    store.deleteSnapshot(volume, bucket, snap1);

    LambdaTestUtils.intercept(OMException.class,
            "FILE_NOT_FOUND",
            () -> store.deleteSnapshot(volume, bucket, snap1));

  }

  @Test
  public void testDeleteSnapshotFailure() throws Exception {
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket = "buck-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    volume1.createBucket(bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    createFileKey(bucket1, key1);
    String snap1 = "snap" + RandomStringUtils.randomNumeric(5);
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
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket = "buck-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    volume1.createBucket(bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    createFileKey(bucket1, key1);
    String snap1 = "snap" + RandomStringUtils.randomNumeric(5);
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
    SnapshotInfo snapshotInfo =
        leaderOzoneManager.getMetadataManager().getSnapshotInfoTable()
            .get(SnapshotInfo.getTableKey(volName, buckName, snapshotName));
    String snapshotDirName =
        metaDir + OM_KEY_PREFIX + OM_SNAPSHOT_DIR + OM_KEY_PREFIX + OM_DB_NAME
            + snapshotInfo.getCheckpointDirName() + OM_KEY_PREFIX + "CURRENT";
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

  private String createFileKey(OzoneBucket bucket, String keyPrefix)
      throws IOException {
    byte[] value = RandomStringUtils.randomAscii(10240).getBytes(UTF_8);
    String key = keyPrefix + RandomStringUtils.randomNumeric(5);
    OzoneOutputStream fileKey = bucket.createKey(key, value.length);
    fileKey.write(value);
    fileKey.close();
    return key;
  }

  @Test
  public void testUniqueSnapshotId()
      throws IOException, InterruptedException, TimeoutException {
    createFileKey(ozoneBucket, "key");

    String snapshotName = UUID.randomUUID().toString();
    store.createSnapshot(volumeName, bucketName, snapshotName);

    List<OzoneManager> ozoneManagers = ((MiniOzoneHAClusterImpl) cluster)
        .getOzoneManagersList();
    List<String> snapshotIds = new ArrayList<>();

    for (OzoneManager ozoneManager : ozoneManagers) {
      GenericTestUtils.waitFor(
          () -> {
            SnapshotInfo snapshotInfo;
            try {
              snapshotInfo = ozoneManager.getMetadataManager()
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
              snapshotIds.add(snapshotInfo.getSnapshotID());
            }
            return snapshotInfo != null;
          },
          1000,
          120000);
    }

    assertEquals(1, snapshotIds.stream().distinct().count());
  }
}
