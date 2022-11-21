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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
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
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.FILE_SYSTEM_OPTIMIZED;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.OBJECT_STORE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Test OmSnapshot bucket interface.
 */
@RunWith(Parameterized.class)
public class TestOmSnapshot {
  private static MiniOzoneCluster cluster = null;
  private static String volumeName;
  private static String bucketName;
  private static OzoneManagerProtocol writeClient;
  private static BucketLayout bucketLayout = BucketLayout.LEGACY;
  private static boolean enabledFileSystemPaths;
  private static ObjectStore store;
  private static File metaDir;
  private static OzoneManager ozoneManager;

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
   * <p>
   *
   */
  private void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    String omId = UUID.randomUUID().toString();
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
        enabledFileSystemPaths);
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        bucketLayout.name());
    cluster = MiniOzoneCluster.newBuilder(conf).setClusterId(clusterId)
      .setScmId(scmId).setOmId(omId).build();
    cluster.waitForClusterToBeReady();
    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket bucket = TestDataUtil
        .createVolumeAndBucket(cluster, bucketLayout);
    volumeName = bucket.getVolumeName();
    bucketName = bucket.getName();

    OzoneClient client = cluster.getClient();
    store = client.getObjectStore();
    writeClient = store.getClientProxy().getOzoneManagerClient();
    ozoneManager = cluster.getOzoneManager();
    KeyManagerImpl keyManager = (KeyManagerImpl) HddsWhiteboxTestUtils
        .getInternalState(ozoneManager, "keyManager");

    // stop the deletion services so that keys can still be read
    keyManager.stop();
    metaDir = OMStorage.getOmDbDir(conf);

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
    Iterator<? extends OzoneKey> volABucketAIter =
        volAbucketA.listKeys(snapshotKeyPrefix + "key-");
    int volABucketAKeyCount = 0;
    while (volABucketAIter.hasNext()) {
      volABucketAIter.next();
      volABucketAKeyCount++;
    }
    Assert.assertEquals(20, volABucketAKeyCount);

    snapshotKeyPrefix = createSnapshot(volumeA, bucketB);
    deleteKeys(volAbucketB);
    Iterator<? extends OzoneKey> volABucketBIter =
        volAbucketB.listKeys(snapshotKeyPrefix + "key-");
    int volABucketBKeyCount = 0;
    while (volABucketBIter.hasNext()) {
      volABucketBIter.next();
      volABucketBKeyCount++;
    }
    Assert.assertEquals(20, volABucketBKeyCount);

    snapshotKeyPrefix = createSnapshot(volumeB, bucketA);
    deleteKeys(volBbucketA);
    Iterator<? extends OzoneKey> volBBucketAIter =
        volBbucketA.listKeys(snapshotKeyPrefix + "key-");
    int volBBucketAKeyCount = 0;
    while (volBBucketAIter.hasNext()) {
      volBBucketAIter.next();
      volBBucketAKeyCount++;
    }
    Assert.assertEquals(20, volBBucketAKeyCount);

    snapshotKeyPrefix = createSnapshot(volumeB, bucketB);
    deleteKeys(volBbucketB);
    Iterator<? extends OzoneKey> volBBucketBIter =
        volBbucketB.listKeys(snapshotKeyPrefix + "key-");
    int volBBucketBKeyCount = 0;
    while (volBBucketBIter.hasNext()) {
      volBBucketBIter.next();
      volBBucketBKeyCount++;
    }
    Assert.assertEquals(20, volBBucketBKeyCount);

    snapshotKeyPrefix = createSnapshot(volumeA, bucketA);
    deleteKeys(volAbucketA);
    Iterator<? extends OzoneKey> volABucketAKeyAIter =
        volAbucketA.listKeys(snapshotKeyPrefix + "key-a-");
    int volABucketAKeyACount = 0;
    while (volABucketAKeyAIter.hasNext()) {
      volABucketAKeyAIter.next();
      volABucketAKeyACount++;
    }
    Assert.assertEquals(10, volABucketAKeyACount);
    Iterator<? extends OzoneKey> volABucketAKeyBIter =
        volAbucketA.listKeys(snapshotKeyPrefix + "key-b-");
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(volABucketAKeyBIter.next().getName()
          .startsWith(snapshotKeyPrefix + "key-b-" + i + "-"));
    }
    Assert.assertFalse(volABucketBIter.hasNext());
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
    OzoneVolume ozoneVolume = store.getVolume(volumeName);
    assertTrue(ozoneVolume.getName().equals(volumeName));
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    assertTrue(ozoneBucket.getName().equals(bucketName));

    String s = "testData";
    String dir1 = "dir1";
    String key1 = dir1 + "/key1";

    // create key1
    OzoneOutputStream ozoneOutputStream =
            ozoneBucket.createKey(key1, s.length());
    byte[] input = s.getBytes(StandardCharsets.UTF_8);
    ozoneOutputStream.write(input);
    ozoneOutputStream.close();

    String snapshotKeyPrefix = createSnapshot();
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
    OzoneBucket volbucket = vol.getBucket(bucket);

    String key = "key-";
    createFileKey(volbucket, key);
    String snapshotKeyPrefix = createSnapshot(volume, bucket);
    deleteKeys(volbucket);

    Iterator<? extends OzoneKey> volBucketIter =
            volbucket.listKeys(snapshotKeyPrefix + "key-");
    int volBucketKeyCount = 0;
    while (volBucketIter.hasNext()) {
      volBucketIter.next();
      volBucketKeyCount++;
    }
    Assert.assertEquals(1, volBucketKeyCount);

    snapshotKeyPrefix = createSnapshot(volume, bucket);
    Iterator<? extends OzoneKey> volBucketIter2 =
            volbucket.listKeys(snapshotKeyPrefix);
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
    OzoneBucket volbucket = vol.getBucket(bucket);

    String key1 = "key-1-";
    createFileKey(volbucket, key1);
    String snapshotKeyPrefix1 = createSnapshot(volume, bucket);

    String key2 = "key-2-";
    createFileKey(volbucket, key2);
    String snapshotKeyPrefix2 = createSnapshot(volume, bucket);

    Iterator<? extends OzoneKey> volBucketIter =
            volbucket.listKeys(snapshotKeyPrefix1 + "key-");
    int volBucketKeyCount = 0;
    while (volBucketIter.hasNext()) {
      volBucketIter.next();
      volBucketKeyCount++;
    }
    Assert.assertEquals(1, volBucketKeyCount);

    Iterator<? extends OzoneKey> volBucketIter2 =
            volbucket.listKeys(snapshotKeyPrefix2 + "key-");
    int volBucketKeyCount2 = 0;
    while (volBucketIter2.hasNext()) {
      volBucketIter2.next();
      volBucketKeyCount2++;
    }
    Assert.assertEquals(2, volBucketKeyCount2);

    deleteKeys(volbucket);

  }

  @Test
  public void testNonExistentBucket()
          throws Exception {
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket = "buc-" + RandomStringUtils.randomNumeric(5);
    //create volume but not bucket
    store.createVolume(volume);

    LambdaTestUtils.intercept(OMException.class,
            "Bucket not found",
            () -> createSnapshot(volume, bucket));
  }

  private String createSnapshot()
      throws IOException, InterruptedException, TimeoutException {
    return createSnapshot(volumeName, bucketName);
  }
  private String createSnapshot(String vname, String bname)
      throws IOException, InterruptedException, TimeoutException {
    String snapshotName = UUID.randomUUID().toString();
    writeClient = store.getClientProxy().getOzoneManagerClient();
    writeClient.createSnapshot(vname, bname, snapshotName);
    String snapshotKeyPrefix = OmSnapshotManager
        .getSnapshotPrefix(snapshotName);
    SnapshotInfo snapshotInfo = ozoneManager
        .getMetadataManager().getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(vname, bname, snapshotName));
    String snapshotDirName = metaDir + OM_KEY_PREFIX +
        OM_SNAPSHOT_DIR + OM_KEY_PREFIX + OM_DB_NAME +
        snapshotInfo.getCheckpointDirName() + OM_KEY_PREFIX + "CURRENT";
    GenericTestUtils.waitFor(() -> new File(snapshotDirName).exists(),
        1000, 120000);

    return snapshotKeyPrefix;

  }


  private void deleteKeys(OzoneBucket bucket) throws IOException {
    Iterator<? extends OzoneKey> bucketIter =
        bucket.listKeys(null);
    while (bucketIter.hasNext()) {
      OzoneKey key = bucketIter.next();
      bucket.deleteKey(key.getName());
    }
  }

  private void createFileKey(OzoneBucket bucket, String keyprefix)
          throws IOException {
    byte[] value = RandomStringUtils.randomAscii(10240).getBytes(UTF_8);
    OzoneOutputStream fileKey = bucket.createKey(
            keyprefix + RandomStringUtils.randomNumeric(5),
            value.length, RATIS, ONE,
            new HashMap<>());
    fileKey.write(value);
    fileKey.close();
  }

}

