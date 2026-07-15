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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.ozone.OzoneFsShell;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.util.ToolRunner;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests Snapshot Restore function.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOzoneSnapshotRestore {
  private static final String OM_SERVICE_ID = "om-service-test-1";
  private MiniOzoneHAClusterImpl cluster;
  private ObjectStore store;
  private OzoneManager leaderOzoneManager;
  private OzoneConfiguration clientConf;
  private OzoneClient client;
  private static AtomicInteger counter;

  private static Stream<Arguments> bucketTypes() {
    return Stream.of(
            arguments(BucketLayout.FILE_SYSTEM_OPTIMIZED),
            arguments(BucketLayout.LEGACY)
    );
  }

  private static Stream<Arguments> bucketTypesCombinations() {
    return Stream.of(
            arguments(BucketLayout.FILE_SYSTEM_OPTIMIZED, BucketLayout.LEGACY),
            arguments(BucketLayout.LEGACY, BucketLayout.FILE_SYSTEM_OPTIMIZED)
    );
  }

  static {
    counter = new AtomicInteger();
  }

  @BeforeAll
  public void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    // Enable filesystem snapshot feature for the test regardless of the default
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);

    String serviceID = OM_SERVICE_ID + RandomStringUtils.secure().nextNumeric(5);

    cluster = MiniOzoneCluster.newHABuilder(conf)
            .setOMServiceId(serviceID)
            .setNumOfOzoneManagers(3)
            .build();
    cluster.waitForClusterToBeReady();

    leaderOzoneManager = cluster.getOMLeader();
    OzoneConfiguration leaderConfig = leaderOzoneManager.getConfiguration();

    String hostPrefix = OZONE_OFS_URI_SCHEME + "://" + serviceID;
    clientConf = new OzoneConfiguration(leaderConfig);
    clientConf.set(FS_DEFAULT_NAME_KEY, hostPrefix);

    client = cluster.newClient();
    store = client.getObjectStore();

    KeyManagerImpl keyManager = (KeyManagerImpl) HddsWhiteboxTestUtils
            .getInternalState(leaderOzoneManager, "keyManager");
    // stop the deletion services so that keys can still be read
    keyManager.stop();
    OMStorage.getOmDbDir(leaderConfig);

  }

  @AfterAll
  public void tearDown() throws Exception {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void createFileKey(OzoneBucket bucket, String key)
          throws IOException {
    byte[] value = RandomStringUtils.secure().nextAscii(10240).getBytes(UTF_8);
    OzoneOutputStream fileKey = bucket.createKey(key, value.length);
    fileKey.write(value);
    fileKey.close();
  }

  private void deleteKeys(OzoneBucket bucket) throws IOException {
    Iterator<? extends OzoneKey> bucketIterator = bucket.listKeys(null);
    while (bucketIterator.hasNext()) {
      OzoneKey key = bucketIterator.next();
      bucket.deleteKey(key.getName());
    }
  }

  private String createSnapshot(String volName, String buckName,
                                String snapshotName)
          throws IOException, InterruptedException, TimeoutException {
    store.createSnapshot(volName, buckName, snapshotName);
    String snapshotKeyPrefix = OmSnapshotManager
            .getSnapshotPrefix(snapshotName);
    SnapshotInfo snapshotInfo = leaderOzoneManager
            .getMetadataManager()
            .getSnapshotInfoTable()
            .get(SnapshotInfo.getTableKey(volName, buckName, snapshotName));
    String snapshotDirName = OmSnapshotManager
        .getSnapshotPath(clientConf, snapshotInfo, 0) + OM_KEY_PREFIX + "CURRENT";
    GenericTestUtils.waitFor(() -> new File(snapshotDirName).exists(),
            1000, 120000);
    return snapshotKeyPrefix;

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

  private void keyCopy(String sourcePath, String destPath)
          throws Exception {
    OzoneFsShell shell = new OzoneFsShell(clientConf);

    try {
      // Copy key from source to destination path
      int res = ToolRunner.run(shell,
              new String[]{"-cp", sourcePath, destPath});
      assertEquals(0, res);
    } finally {
      shell.close();
    }
  }

  @ParameterizedTest
  @MethodSource("bucketTypes")
  public void testRestoreSnapshot(BucketLayout bucketLayoutTest)
          throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buc-" + counter.incrementAndGet();
    String snapshotName = "snap-" + counter.incrementAndGet();
    String keyPrefix = "key-";

    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    BucketArgs bucketArgs = BucketArgs.newBuilder()
            .setBucketLayout(bucketLayoutTest).build();
    vol.createBucket(bucket, bucketArgs);
    OzoneBucket buck = vol.getBucket(bucket);

    for (int i = 0; i < 5; i++) {
      createFileKey(buck, keyPrefix + i);
    }

    String snapshotKeyPrefix = createSnapshot(volume, bucket, snapshotName);

    assertDoesNotThrow(() -> waitForKeyCount(buck,
        snapshotKeyPrefix + keyPrefix, 5));

    deleteKeys(buck);
    assertDoesNotThrow(() -> waitForKeyCount(buck, keyPrefix, 0));

    String sourcePath = OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket
        + OM_KEY_PREFIX + snapshotKeyPrefix;
    String destPath = OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket
        + OM_KEY_PREFIX;

    for (int i = 0; i < 5; i++) {
      keyCopy(sourcePath + keyPrefix + i, destPath);
    }

    assertDoesNotThrow(() -> waitForKeyCount(buck, keyPrefix, 5));
  }

  @ParameterizedTest
  @MethodSource("bucketTypes")
  public void testRestoreSnapshotDifferentBucket(BucketLayout bucketLayoutTest)
          throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buc-" + counter.incrementAndGet();
    String bucket2 = "buc-" + counter.incrementAndGet();
    String keyPrefix = "key-";
    String snapshotName = "snap-" + counter.incrementAndGet();

    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    BucketArgs bucketArgs = BucketArgs.newBuilder()
            .setBucketLayout(bucketLayoutTest).build();
    vol.createBucket(bucket, bucketArgs);
    OzoneBucket buck = vol.getBucket(bucket);
    vol.createBucket(bucket2, bucketArgs);
    OzoneBucket buck2 = vol.getBucket(bucket2);

    for (int i = 0; i < 5; i++) {
      createFileKey(buck, keyPrefix + i);
    }

    String snapshotKeyPrefix = createSnapshot(volume, bucket, snapshotName);

    assertDoesNotThrow(() -> waitForKeyCount(buck,
        snapshotKeyPrefix + keyPrefix, 5));


    // Delete keys from the source bucket.
    // This is temporary fix to make sure that test passes all the time.
    // If we don't delete keys from the source bucket, copy command
    // will fail. In copy command, there is a check to make sure that key
    // doesn't exist in the destination bucket.
    // Problem is that RocksDB's seek operation doesn't 100% guarantee that
    // item key is available. Tho we believe that it is in
    // `createFakeDirIfShould` function. When we seek if there is a directory
    // for the key name, sometime it returns non-null response but in reality
    // item doesn't exist.
    // In the case when seek returns non-null response, "key doesn't exist in
    // the destination bucket" check fails because getFileStatus returns
    // dir with key name exists.
    deleteKeys(buck);

    String sourcePath = OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket
        + OM_KEY_PREFIX + snapshotKeyPrefix;
    String destPath = OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket2
        + OM_KEY_PREFIX;

    for (int i = 0; i < 5; i++) {
      keyCopy(sourcePath + keyPrefix + i, destPath);
    }

    assertDoesNotThrow(() -> waitForKeyCount(buck2, keyPrefix, 5));
  }

  @ParameterizedTest
  @MethodSource("bucketTypesCombinations")
  public void testRestoreSnapshotDifferentBucketLayout(
          BucketLayout bucketLayoutSource, BucketLayout bucketLayoutDest)
          throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buc-" + counter.incrementAndGet();
    String bucket2 = "buc-" + counter.incrementAndGet();
    String keyPrefix = "key-";
    String snapshotName = "snap-" + counter.incrementAndGet();

    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    BucketArgs bucketArgs = BucketArgs.newBuilder()
            .setBucketLayout(bucketLayoutSource).build();
    vol.createBucket(bucket, bucketArgs);
    OzoneBucket buck = vol.getBucket(bucket);

    bucketArgs = BucketArgs.newBuilder()
            .setBucketLayout(bucketLayoutDest).build();
    vol.createBucket(bucket2, bucketArgs);
    OzoneBucket buck2 = vol.getBucket(bucket2);

    for (int i = 0; i < 5; i++) {
      createFileKey(buck, keyPrefix + i);
    }

    String snapshotKeyPrefix = createSnapshot(volume, bucket, snapshotName);

    assertDoesNotThrow(() -> waitForKeyCount(buck,
        snapshotKeyPrefix + keyPrefix, 5));

    String sourcePath = OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket
        + OM_KEY_PREFIX + snapshotKeyPrefix;
    String destPath = OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket2
        + OM_KEY_PREFIX;

    for (int i = 0; i < 5; i++) {
      keyCopy(sourcePath + keyPrefix + i, destPath);
    }

    assertDoesNotThrow(() -> waitForKeyCount(buck2, keyPrefix, 5));
  }

  @ParameterizedTest
  @MethodSource("bucketTypes")
  public void testUnorderedDeletion(BucketLayout bucketLayoutTest)
          throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    BucketArgs bucketArgs = BucketArgs.newBuilder()
            .setBucketLayout(bucketLayoutTest).build();
    vol.createBucket(bucket, bucketArgs);
    OzoneBucket buck = vol.getBucket(bucket);
    // Create Key1 and take snapshot

    String[] key = new String[10];
    String[] snapshotName = new String[10];
    String[] snapshotKeyPrefix = new String[10];

    // create 10 incremental snapshots
    for (int i = 0; i < 10; i++) {
      key[i] = "key-" + counter.incrementAndGet();
      snapshotName[i] = "snap-" + counter.incrementAndGet();
      createFileKey(buck, key[i]);
      snapshotKeyPrefix[i] = createSnapshot(volume, bucket, snapshotName[i]);
    }

    // delete multiple snapshots - 2nd, 5th , 8th
    for (int i = 2; i < 10; i += 3) {
      store.deleteSnapshot(volume, bucket, snapshotName[i]);
    }

    // delete all keys in bucket before restoring from snapshot
    deleteKeys(buck);
    assertDoesNotThrow(() -> waitForKeyCount(buck, "key-", 0));

    String sourcePath = OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket
            + OM_KEY_PREFIX + snapshotKeyPrefix[9];
    String destPath = OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket
            + OM_KEY_PREFIX;

    for (int i = 0; i < 10; i++) {
      keyCopy(sourcePath + key[i], destPath);
    }

    assertDoesNotThrow(() -> waitForKeyCount(buck, "key-", 10));
  }

  /**
   * Waits for key count to be equal to expected number of keys.
   */
  private void waitForKeyCount(OzoneBucket bucket, String keyPrefix, int count)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      try {
        return count == keyCount(bucket, keyPrefix);
      } catch (IOException e) {
        return false;
      }
    }, 1000, 10000);
  }
}
