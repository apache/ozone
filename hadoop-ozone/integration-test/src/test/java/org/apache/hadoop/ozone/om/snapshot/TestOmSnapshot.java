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
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.leftPad;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.hdds.utils.db.IteratorType.KEY_AND_VALUE;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_PRUNE_DAEMON_RUN_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.COMPACTION_LOG_TABLE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_CACHE_CLEANUP_SERVICE_RUN_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_DISABLE_NATIVE_LIBS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.DELIMITER;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.CONTAINS_SNAPSHOT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;
import static org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse.CancelMessage.CANCEL_ALREADY_CANCELLED_JOB;
import static org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse.CancelMessage.CANCEL_JOB_NOT_EXIST;
import static org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse.CancelMessage.CANCEL_SUCCEEDED;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.CANCELLED;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.DONE;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.IN_PROGRESS;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.isDone;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.isStarting;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.COLUMN_FAMILIES_TO_TRACK;
import static org.apache.ozone.test.LambdaTestUtils.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.google.common.collect.Lists;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.CompactionLogEntryProto;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.ManagedRawSSTFileIterator;
import org.apache.hadoop.hdds.utils.db.ManagedRawSSTFileReader;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadList;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneSnapshot;
import org.apache.hadoop.ozone.client.OzoneSnapshotDiff;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.IOmMetadataReader;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.service.SnapshotDiffCleanupService;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.ozone.compaction.log.CompactionLogEntry;
import org.apache.ozone.rocksdiff.FlushNode;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Slow;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.rocksdb.LiveFileMetaData;

/**
 * Abstract class to test OmSnapshot.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestOmSnapshot {
  static {
    Logger.getLogger(ManagedRocksObjectUtils.class).setLevel(Level.DEBUG);
  }

  private static final String SNAPSHOT_DAY_PREFIX = "snap-day-";
  private static final String SNAPSHOT_WEEK_PREFIX = "snap-week-";
  private static final String SNAPSHOT_MONTH_PREFIX = "snap-month-";
  private static final String KEY_PREFIX = "key-";
  private static final String SNAPSHOT_KEY_PATTERN_STRING = "(.+)/(.+)/(.+)";
  private static final Pattern SNAPSHOT_KEY_PATTERN =
      Pattern.compile(SNAPSHOT_KEY_PATTERN_STRING);
  private static final int POLL_INTERVAL_MILLIS = 500;
  private static final int POLL_MAX_WAIT_MILLIS = 120_000;
  private static final int MIN_PART_SIZE = 5 * 1024 * 1024;

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private String volumeName;
  private String bucketName;
  private OzoneManagerProtocol writeClient;
  private ObjectStore store;
  private OzoneManager ozoneManager;
  private OzoneBucket ozoneBucket;
  private OzoneConfiguration conf;

  private final BucketLayout bucketLayout;
  private final boolean enabledFileSystemPaths;
  private final boolean forceFullSnapshotDiff;
  private final boolean disableNativeDiff;
  private final AtomicInteger counter;
  private final boolean createLinkedBucket;
  private final Map<String, String> linkedBuckets = new HashMap<>();

  public TestOmSnapshot(BucketLayout newBucketLayout,
                        boolean newEnableFileSystemPaths,
                        boolean forceFullSnapDiff,
                        boolean disableNativeDiff,
                        boolean createLinkedBucket)
      throws Exception {
    this.enabledFileSystemPaths = newEnableFileSystemPaths;
    this.bucketLayout = newBucketLayout;
    this.forceFullSnapshotDiff = forceFullSnapDiff;
    this.disableNativeDiff = disableNativeDiff;
    this.counter = new AtomicInteger();
    this.createLinkedBucket = createLinkedBucket;
    init();

    if (!disableNativeDiff) {
      assumeTrue(ManagedRawSSTFileReader.tryLoadLibrary());
    }
  }

  private void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_OM_ENABLE_FILESYSTEM_PATHS, enabledFileSystemPaths);
    conf.set(OZONE_DEFAULT_BUCKET_LAYOUT, bucketLayout.name());
    conf.setBoolean(OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF, forceFullSnapshotDiff);
    conf.setBoolean(OZONE_OM_SNAPSHOT_DIFF_DISABLE_NATIVE_LIBS, disableNativeDiff);
    conf.setEnum(HDDS_DB_PROFILE, DBProfile.TEST);
    // Enable filesystem snapshot feature for the test regardless of the default
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);
    conf.setInt(OMStorage.TESTING_INIT_LAYOUT_VERSION_KEY, OMLayoutFeature.BUCKET_LAYOUT_SUPPORT.layoutVersion());
    conf.setTimeDuration(OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setInt(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL, -1);
    conf.setTimeDuration(OZONE_OM_SNAPSHOT_CACHE_CLEANUP_SERVICE_RUN_INTERVAL, 100, TimeUnit.MILLISECONDS);
    if (!disableNativeDiff) {
      conf.setTimeDuration(OZONE_OM_SNAPSHOT_COMPACTION_DAG_PRUNE_DAEMON_RUN_INTERVAL, 0, TimeUnit.SECONDS);
    }

    cluster = MiniOzoneCluster.newBuilder(conf)
        .build();

    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    // create a volume and a bucket to be used by OzoneFileSystem
    ozoneBucket = TestDataUtil.createVolumeAndBucket(client, bucketLayout, null, createLinkedBucket);
    if (createLinkedBucket) {
      this.linkedBuckets.put(ozoneBucket.getName(), ozoneBucket.getSourceBucket());
    }
    volumeName = ozoneBucket.getVolumeName();
    bucketName = ozoneBucket.getName();
    ozoneManager = cluster.getOzoneManager();

    store = client.getObjectStore();
    writeClient = store.getClientProxy().getOzoneManagerClient();

    // stop the deletion services so that keys can still be read
    stopKeyManager();
    preFinalizationChecks();
    finalizeOMUpgrade();
  }

  private void createBucket(OzoneVolume volume, String bucketVal) throws IOException {
    if (createLinkedBucket) {
      String sourceBucketName = linkedBuckets.computeIfAbsent(bucketVal, (k) -> bucketVal + counter.incrementAndGet());
      volume.createBucket(sourceBucketName);
      TestDataUtil.createLinkedBucket(client, volume.getName(), sourceBucketName, bucketVal);
      this.linkedBuckets.put(bucketVal, sourceBucketName);
    } else {
      volume.createBucket(bucketVal);
    }
  }

  private void stopKeyManager() throws IOException {
    KeyManagerImpl keyManager = (KeyManagerImpl) HddsWhiteboxTestUtils
        .getInternalState(ozoneManager, "keyManager");
    keyManager.stop();
  }

  private void startKeyManager() throws IOException {
    KeyManagerImpl keyManager = (KeyManagerImpl) HddsWhiteboxTestUtils
        .getInternalState(ozoneManager, "keyManager");
    keyManager.start(conf);
  }

  private RDBStore getRdbStore() {
    return (RDBStore) ozoneManager.getMetadataManager().getStore();
  }

  private void preFinalizationChecks() {
    OMException omException  = assertThrows(OMException.class,
        () -> store.createSnapshot(volumeName, bucketName,
            UUID.randomUUID().toString()));
    assertFinalizationException(omException);
    omException  = assertThrows(OMException.class,
        () -> store.listSnapshot(volumeName, bucketName, null, null));
    assertFinalizationException(omException);
    omException  = assertThrows(OMException.class,
        () -> store.snapshotDiff(volumeName, bucketName,
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
          "", 1000, false, disableNativeDiff));
    assertFinalizationException(omException);
    omException  = assertThrows(OMException.class,
        () -> store.deleteSnapshot(volumeName, bucketName,
            UUID.randomUUID().toString()));
    assertFinalizationException(omException);

  }

  private static void assertFinalizationException(OMException omException) {
    assertEquals(NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION,
        omException.getResult());
    assertThat(omException.getMessage())
        .contains("cannot be invoked before finalization.");
  }

  /**
   * Trigger OM upgrade finalization from the client and block until completion
   * (status FINALIZATION_DONE).
   */
  private void finalizeOMUpgrade() throws Exception {
    // Trigger OM upgrade finalization. Ref: FinalizeUpgradeSubCommand#call
    final OzoneManagerProtocol omClient = client.getObjectStore()
        .getClientProxy().getOzoneManagerClient();
    final String upgradeClientID = "Test-Upgrade-Client-" + UUID.randomUUID();
    UpgradeFinalization.StatusAndMessages finalizationResponse =
        omClient.finalizeUpgrade(upgradeClientID);

    // The status should transition as soon as the client call above returns
    assertTrue(isStarting(finalizationResponse.status()));
    // Wait for the finalization to be marked as done.
    // 10s timeout should be plenty.
    await(POLL_MAX_WAIT_MILLIS, POLL_INTERVAL_MILLIS, () -> {
      final UpgradeFinalization.StatusAndMessages progress =
          omClient.queryUpgradeFinalizationProgress(
              upgradeClientID, false, false);
      return isDone(progress.status());
    });
  }

  @AfterAll
  void tearDown() {
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
    createBucket(volA, bucketA);
    createBucket(volA, bucketB);
    createBucket(volB, bucketA);
    createBucket(volB, bucketB);
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
    assertEquals(20, volABucketAKeyCount);

    snapshotKeyPrefix = createSnapshot(volumeA, bucketB);
    deleteKeys(volAbucketB);

    int volABucketBKeyCount = keyCount(volAbucketB,
        snapshotKeyPrefix + "key-");
    assertEquals(20, volABucketBKeyCount);

    snapshotKeyPrefix = createSnapshot(volumeB, bucketA);
    deleteKeys(volBbucketA);

    int volBBucketAKeyCount = keyCount(volBbucketA,
        snapshotKeyPrefix + "key-");
    assertEquals(20, volBBucketAKeyCount);

    snapshotKeyPrefix = createSnapshot(volumeB, bucketB);
    deleteKeys(volBbucketB);

    int volBBucketBKeyCount = keyCount(volBbucketB,
        snapshotKeyPrefix + "key-");
    assertEquals(20, volBBucketBKeyCount);

    snapshotKeyPrefix = createSnapshot(volumeA, bucketA);
    deleteKeys(volAbucketA);

    int volABucketAKeyACount = keyCount(volAbucketA,
        snapshotKeyPrefix + "key-a-");
    assertEquals(10, volABucketAKeyACount);


    int volABucketAKeyBCount = keyCount(volAbucketA,
        snapshotKeyPrefix + "key-b-");
    assertEquals(10, volABucketAKeyBCount);
  }

  @Test
  // based on TestOzoneRpcClientAbstract:testListKeyOnEmptyBucket
  public void testListKeyOnEmptyBucket()
      throws IOException, InterruptedException, TimeoutException {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buc-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    createBucket(vol, bucket);
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

    KeyInfoWithVolumeContext omKeyInfo = writeClient.getKeyInfo(keyArgs, false);
    assertEquals(omKeyInfo.getKeyInfo().getKeyName(), snapshotKeyPrefix + key1);

    KeyInfoWithVolumeContext fileInfo = writeClient.getKeyInfo(keyArgs, false);
    assertEquals(fileInfo.getKeyInfo().getKeyName(), snapshotKeyPrefix + key1);

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
    createBucket(vol, bucket);
    OzoneBucket volBucket = vol.getBucket(bucket);

    String key = "key-";
    createFileKeyWithPrefix(volBucket, key);
    String snapshotKeyPrefix = createSnapshot(volume, bucket);
    deleteKeys(volBucket);

    int volBucketKeyCount = keyCount(volBucket, snapshotKeyPrefix + "key-");
    assertEquals(1, volBucketKeyCount);

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
    createBucket(vol, bucket);
    OzoneBucket bucket1 = vol.getBucket(bucket);

    String key1 = "key-1-";
    createFileKeyWithPrefix(bucket1, key1);
    String snapshotKeyPrefix1 = createSnapshot(volume, bucket);

    String key2 = "key-2-";
    createFileKeyWithPrefix(bucket1, key2);
    String snapshotKeyPrefix2 = createSnapshot(volume, bucket);

    int volBucketKeyCount = keyCount(bucket1, snapshotKeyPrefix1 + "key-");
    assertEquals(1, volBucketKeyCount);


    int volBucketKeyCount2 = keyCount(bucket1, snapshotKeyPrefix2 + "key-");
    assertEquals(2, volBucketKeyCount2);

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

    OMException omException = assertThrows(OMException.class,
        () -> createSnapshot(volume, bucket));
    assertEquals(BUCKET_NOT_FOUND, omException.getResult());
  }

  @Test
  public void testCreateSnapshotMissingMandatoryParams() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    createBucket(volume1, bucket);
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

  private void getOmKeyInfo(String volume, String bucket,
                            String key) throws IOException {
    ResolvedBucket resolvedBucket = new ResolvedBucket(volume, bucket,
        volume, this.linkedBuckets.getOrDefault(bucket, bucket), "", bucketLayout);
    cluster.getOzoneManager().getKeyManager()
        .getKeyInfo(new OmKeyArgs.Builder()
                .setVolumeName(volume)
                .setBucketName(this.linkedBuckets.getOrDefault(bucket, bucket))
                .setKeyName(key).build(),
            resolvedBucket, null);
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Snapshot snap2 is created
   * 4) Key k1 is deleted.
   * 5) Snapshot snap3 is created.
   * 6) Snapdiff b/w snap3 & snap2 taken to assert difference of 1 key
   */
  @Test
  public void testSnapDiffHandlingReclaimWithLatestUse() throws Exception {
    String testVolumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    getOmKeyInfo(testVolumeName, testBucketName, key1);
    bucket.deleteKey(key1);
    String snap3 = "snap3";
    createSnapshot(testVolumeName, testBucketName, snap3);
    SnapshotDiffReportOzone diff =
        getSnapDiffReport(testVolumeName, testBucketName, snap1, snap2);
    assertEquals(diff.getDiffList().size(), 0);
    diff = getSnapDiffReport(testVolumeName, testBucketName, snap2, snap3);
    assertEquals(diff.getDiffList().size(), 1);
    assertEquals(diff.getDiffList(), Collections.singletonList(
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
   * 6) Snap diff b/w snap3 & snap1 taken to assert difference of 1 key.
   * 7) Snap diff b/w snap3 & snap2 taken to assert difference of 0 key.
   */
  @Test
  public void testSnapDiffHandlingReclaimWithPreviousUse() throws Exception {
    String testVolumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    getOmKeyInfo(testVolumeName, testBucketName, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    bucket.deleteKey(key1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    String snap3 = "snap3";
    createSnapshot(testVolumeName, testBucketName, snap3);
    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap3);
    assertEquals(diff.getDiffList().size(), 1);
    assertEquals(diff.getDiffList(), Collections.singletonList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1)));
    diff = getSnapDiffReport(testVolumeName, testBucketName, snap2, snap3);
    assertEquals(diff.getDiffList().size(), 0);
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Key k1 is deleted.
   * 4) Key k1 is recreated.
   * 5) Snapshot snap2 is created.
   * 6) Snap diff b/w snapshot of Active FS & snap1 taken to assert difference
   *    of 2 keys.
   * 7) Snap diff b/w snapshot of Active FS & snap2 taken to assert difference
   *    of 0 key.
   * 8) Checking rocks db to ensure the object created shouldn't be reclaimed
   *    as it is used by snapshot.
   * 9) Key k1 is deleted.
   * 10) Snap diff b/w snapshot of Active FS & snap1 taken to assert difference
   *     of 1 key.
   * 11) Snap diff b/w snapshot of Active FS & snap2 taken to assert difference
   *     of 1 key.
   */
  @Test
  public void testSnapDiffReclaimWithKeyRecreation() throws Exception {
    String testVolumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    getOmKeyInfo(testVolumeName, testBucketName, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    bucket.deleteKey(key1);
    key1 = createFileKey(bucket, key1);
    getOmKeyInfo(testVolumeName, testBucketName, key1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    String snap3 = "snap3";
    createSnapshot(testVolumeName, testBucketName, snap3);
    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap3);
    assertEquals(diff.getDiffList().size(), 2);
    assertEquals(diff.getDiffList(), Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1),
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.CREATE, key1)));
    diff = getSnapDiffReport(testVolumeName, testBucketName, snap2, snap3);
    assertEquals(diff.getDiffList().size(), 0);
    bucket.deleteKey(key1);
    String snap4 = "snap4";
    createSnapshot(testVolumeName, testBucketName, snap4);
    diff = getSnapDiffReport(testVolumeName, testBucketName, snap1, snap4);
    assertEquals(diff.getDiffList().size(), 1);
    assertEquals(diff.getDiffList(), Collections.singletonList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1)));
    diff = getSnapDiffReport(testVolumeName, testBucketName, snap2, snap4);
    assertEquals(diff.getDiffList().size(), 1);
    assertEquals(diff.getDiffList(), Collections.singletonList(
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
   * 4) Snap diff b/w snap2 & snap1 taken to assert difference of 1 key.
   */
  @Test
  public void testSnapDiffReclaimWithKeyRename() throws Exception {
    String testVolumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    String renamedKey = "renamed-" + key1;
    bucket.renameKey(key1, renamedKey);
    GenericTestUtils.waitFor(() -> {
      try {
        getOmKeyInfo(testVolumeName, testBucketName, renamedKey);
      } catch (IOException e) {
        return false;
      }
      return true;
    }, 1000, 10000);
    getOmKeyInfo(testVolumeName, testBucketName, renamedKey);
    bucket.deleteKey(renamedKey);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap2);
    assertEquals(diff.getDiffList().size(), 1);
    assertEquals(diff.getDiffList(), Collections.singletonList(
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
   * 7) Snap diff b/w Active FS & snap1 taken to assert difference of 1 key.
   */
  @Test
  public void testSnapDiffWith2RenamesAndDelete() throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket" + counter.incrementAndGet();
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    String renamedKey = "renamed-" + key1;
    bucket.renameKey(key1, renamedKey);
    GenericTestUtils.waitFor(() -> {
      try {
        getOmKeyInfo(testVolumeName, testBucketName, renamedKey);
      } catch (IOException e) {
        return false;
      }
      return true;
    }, 1000, 120000);
    String renamedRenamedKey = "renamed-" + renamedKey;
    bucket.renameKey(renamedKey, renamedRenamedKey);
    GenericTestUtils.waitFor(() -> {
      try {
        getOmKeyInfo(testVolumeName, testBucketName, renamedRenamedKey);
      } catch (IOException e) {
        return false;
      }
      return true;
    }, 1000, 120000);
    getOmKeyInfo(testVolumeName, testBucketName, renamedRenamedKey);
    bucket.deleteKey(renamedRenamedKey);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    String snap3 = "snap3";
    createSnapshot(testVolumeName, testBucketName,
            snap3);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
        snap1, snap3);
    assertEquals(diff.getDiffList().size(), 1);
    assertEquals(diff.getDiffList(), Collections.singletonList(
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
   * 6) Snap diff b/w snapshot of Active FS & snap1 taken to assert difference
   *    of 1 key.
   */
  @Test
  public void testSnapDiffWithKeyRenamesRecreationAndDelete()
          throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    String renamedKey = "renamed-" + key1;
    bucket.renameKey(key1, renamedKey);
    key1 =  createFileKey(bucket, key1);
    getOmKeyInfo(testVolumeName, testBucketName, key1);
    String snap3 = "snap3";
    createSnapshot(testVolumeName, testBucketName, snap3);
    getSnapDiffReport(testVolumeName, testBucketName,
        snap1, snap3);
    bucket.deleteKey(key1);
    String activeSnap = "activefs";
    createSnapshot(testVolumeName, testBucketName, activeSnap);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
        snap1, activeSnap);
    assertEquals(diff.getDiffList().size(), 1);
    assertEquals(diff.getDiffList(), Collections.singletonList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.RENAME,
            key1, renamedKey)
    ));
  }

  /**
   * Testing scenario:
   * 1) Snapshot snap1 created.
   * 2) Key k1 is created.
   * 3) Key k1 is deleted.
   * 4) Snapshot s2 is created before key k1 is reclaimed.
   * 5) Snap diff b/w snapshot of Active FS & snap1 taken to assert difference
   *    of 0 keys.
   * 6) Snap diff b/w snapshot of Active FS & snap2 taken to assert difference
   *    of 0 keys.
   */
  @Test
  public void testSnapDiffReclaimWithDeferredKeyDeletion() throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    getOmKeyInfo(testVolumeName, testBucketName, key1);
    bucket.deleteKey(key1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    String activeSnap = "activefs";
    createSnapshot(testVolumeName, testBucketName, activeSnap);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
        snap1, activeSnap);
    assertEquals(diff.getDiffList().size(), 0);
    diff = getSnapDiffReport(testVolumeName, testBucketName, snap2, activeSnap);
    assertEquals(diff.getDiffList().size(), 0);
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Key k1 is renamed to key k1_renamed
   * 4) Key k1_renamed is renamed to key k1
   * 5) Snap diff b/w snapshot of Active FS & snap1 taken to assert difference
   *    of 1 key with 1 Modified entry.
   */
  @Test
  public void testSnapDiffWithNoEffectiveRename() throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
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
    assertEquals(diff.getDiffList(), Collections.singletonList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.RENAME, key1, key1)));
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Dir dir1/dir2 is created.
   * 4) Key k1 is renamed to key dir1/dir2/k1_renamed
   * 5) Snap diff b/w snapshot of Active FS & snap1 taken to assert difference
   *    of 3 key
   *    with 1 rename entry & 2 dirs create entry.
   */
  @Test
  public void testSnapDiffWithDirectory() throws Exception {

    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
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
      diffEntries = Lists.newArrayList(
          SnapshotDiffReportOzone.getDiffReportEntry(
          SnapshotDiffReport.DiffType.RENAME, key1,
              "dir1/dir2/" + key1 + "_renamed"),
          SnapshotDiffReportOzone.getDiffReportEntry(
              SnapshotDiffReport.DiffType.CREATE, "dir1"),
          SnapshotDiffReportOzone.getDiffReportEntry(
              SnapshotDiffReport.DiffType.CREATE, "dir1/dir2"));
    } else {
      diffEntries = Lists.newArrayList(
          SnapshotDiffReportOzone.getDiffReportEntry(
              SnapshotDiffReport.DiffType.RENAME,
              key1, "dir1/dir2/" + key1 + "_renamed"),
          SnapshotDiffReportOzone.getDiffReportEntry(
              SnapshotDiffReport.DiffType.CREATE, "dir1/dir2"),
          SnapshotDiffReportOzone.getDiffReportEntry(
              SnapshotDiffReport.DiffType.CREATE, "dir1"));
    }
    assertEquals(diff.getDiffList(), diffEntries);
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Dir dir1/dir2 is created.
   * 4) Key k1 is renamed to key dir1/dir2/k1_renamed
   * 5) Dir dir1 is deleted.
   * 6) Snapshot snap2 created.
   * 5) Snapdiff b/w snapshot of Active FS & snap1 taken to assert difference
   *    of 1 delete key entry.
   */
  @Test
  public void testSnapDiffWithDirectoryDelete() throws Exception {
    assumeTrue(bucketLayout.isFileSystemOptimized());
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String snap1 = "snap1";
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    createSnapshot(testVolumeName, testBucketName, snap1);
    bucket.createDirectory("dir1/dir2");
    String key1Renamed = "dir1/dir2/" + key1 + "_renamed";
    bucket.renameKey(key1, key1Renamed);
    bucket.deleteDirectory("dir1", true);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
        snap1, snap2);

    List<SnapshotDiffReport.DiffReportEntry> diffEntries = Lists.newArrayList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1));
    assertEquals(diff.getDiffList(), diffEntries);
  }

  private OzoneObj buildKeyObj(OzoneBucket bucket, String key) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName())
        .setKeyName(key).build();
  }

  @Test
  public void testSnapdiffWithObjectMetaModification() throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String snap1 = "snap1";
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    createSnapshot(testVolumeName, testBucketName, snap1);
    OzoneObj keyObj = buildKeyObj(bucket, key1);
    OzoneAcl userAcl = OzoneAcl.of(USER, "user",
        DEFAULT, WRITE);
    store.addAcl(keyObj, userAcl);

    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
        snap1, snap2);
    List<DiffReportEntry> diffEntries = Lists.newArrayList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.MODIFY,
            key1));
    assertEquals(diffEntries, diff.getDiffList());
  }

  @Test
  public void testSnapdiffWithFilesystemCreate()
      throws IOException, URISyntaxException, InterruptedException,
      TimeoutException {

    assumeTrue(!bucketLayout.isObjectStore(enabledFileSystemPaths));
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket" + counter.incrementAndGet();
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, testBucketName, testVolumeName);
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
        assertEquals(fileStatuses[0].isDirectory(),
            bucketLayout.isFileSystemOptimized() &&
                idx < diff.getDiffList().size() - 1 ||
                !bucketLayout.isFileSystemOptimized() && idx > 0);
        p = fileStatuses[0].getPath();
        assertEquals(diff.getDiffList().get(idx),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.CREATE, p.toUri().getPath()
                    .substring(1)));
        if (fileStatuses[0].isFile()) {
          break;
        }
        idx += bucketLayout.isFileSystemOptimized() ? 1 : -1;
      }
    }
  }

  @Test
  public void testSnapDiffWithFilesystemDirectoryRenameOperation()
      throws IOException, URISyntaxException, InterruptedException,
      TimeoutException {
    assumeTrue(!bucketLayout.isObjectStore(enabledFileSystemPaths));
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket" + counter.incrementAndGet();
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, testBucketName, testVolumeName);
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
        assertEquals(Arrays.asList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.RENAME, "dir1/dir2", "dir1/dir3"),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.MODIFY, "dir1")),
            diff.getDiffList());
      } else {
        assertEquals(Arrays.asList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.RENAME, "dir1/dir2/key1",
                "dir1/dir3/key1"),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.RENAME, "dir1/dir2",
                "dir1/dir3")),
            diff.getDiffList());
      }

    }
  }

  @Test
  public void testSnapDiffWithFilesystemDirectoryMoveOperation()
      throws IOException, URISyntaxException, InterruptedException,
      TimeoutException {
    assumeTrue(!bucketLayout.isObjectStore(enabledFileSystemPaths));
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket" + counter.incrementAndGet();
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, testBucketName, testVolumeName);
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
        assertEquals(Arrays.asList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.RENAME, "dir1/dir2", "dir3/dir2"),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.MODIFY, "dir1"),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.MODIFY, "dir3")),
            diff.getDiffList());
      } else {
        assertEquals(Arrays.asList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.RENAME, "dir1/dir2/key1",
                "dir3/dir2/key1"),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.RENAME, "dir1/dir2",
                "dir3/dir2")),
            diff.getDiffList());
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
    createBucket(volume, bucket1);
    createBucket(volume, bucket2);
    OzoneBucket bucketWithSnapshot = volume.getBucket(bucket1);
    OzoneBucket bucketWithoutSnapshot = volume.getBucket(bucket2);
    String key = "key-";
    createFileKeyWithPrefix(bucketWithSnapshot, key);
    createFileKeyWithPrefix(bucketWithoutSnapshot, key);
    createSnapshot(volume1, bucket1);
    deleteKeys(bucketWithSnapshot);
    deleteKeys(bucketWithoutSnapshot);
    OMException omException = assertThrows(OMException.class,
        () -> volume.deleteBucket(linkedBuckets.getOrDefault(bucket1, bucket1)));
    assertEquals(CONTAINS_SNAPSHOT, omException.getResult());
    // TODO: Delete snapshot then delete bucket1 when deletion is implemented
    // no exception for bucket without snapshot
    volume.deleteBucket(bucket2);
  }

  @Test
  public void testGetSnapshotInfo() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    createBucket(volume1, bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);

    createFileKey(bucket1, "key-1");
    String snap1 = "snap-" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap1);

    createFileKey(bucket1, "key-2");
    String snap2 = "snap-" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap2);

    OzoneSnapshot snapshot1 = store.getSnapshotInfo(volume, bucket, snap1);

    assertEquals(snap1, snapshot1.getName());
    assertEquals(volume, snapshot1.getVolumeName());
    assertEquals(linkedBuckets.getOrDefault(bucket, bucket), snapshot1.getBucketName());

    OzoneSnapshot snapshot2 = store.getSnapshotInfo(volume, bucket, snap2);
    assertEquals(snap2, snapshot2.getName());
    assertEquals(volume, snapshot2.getVolumeName());
    assertEquals(linkedBuckets.getOrDefault(bucket, bucket), snapshot2.getBucketName());

    testGetSnapshotInfoFailure(null, bucket, "snapshotName",
        "volume can't be null or empty.");
    testGetSnapshotInfoFailure(volume, null, "snapshotName",
        "bucket can't be null or empty.");
    testGetSnapshotInfoFailure(volume, bucket, null,
        "snapshot name can't be null or empty.");
    testGetSnapshotInfoFailure(volume, bucket, "snapshotName",
        "Snapshot '/" + volume + "/" + linkedBuckets.getOrDefault(bucket, bucket) +
            "/snapshotName' is not found.");
    testGetSnapshotInfoFailure(volume, "bucketName", "snapshotName",
        "Bucket not found: " + volume + "/bucketName");
  }

  public void testGetSnapshotInfoFailure(String volName,
                                         String buckName,
                                         String snapName,
                                         String expectedMessage) {
    Exception ioException = assertThrows(Exception.class,
        () -> store.getSnapshotInfo(volName, buckName, snapName));
    assertEquals(expectedMessage, ioException.getMessage());
  }

  @Test
  public void testSnapDiffWithDirRename() throws Exception {
    assumeTrue(bucketLayout.isFileSystemOptimized());
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    createBucket(volume1, bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    bucket1.createDirectory("dir1");
    String snap1 = "snap1";
    createSnapshot(volume, bucket, snap1);
    bucket1.renameKey("dir1", "dir1_rename");
    String snap2 = "snap2";
    createSnapshot(volume, bucket, snap2);
    SnapshotDiffReportOzone diff = getSnapDiffReport(volume, bucket,
        snap1, snap2);
    assertEquals(Collections.singletonList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.RENAME, "dir1", "dir1_rename")),
        diff.getDiffList());
  }

  @Test
  public void testSnapDiff() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    createBucket(volume1, bucket);
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
    assertTrue(diff1.getDiffList().isEmpty());
    // Create Key2 and delete Key1, take snapshot
    String key2 = "key-2-";
    key2 = createFileKeyWithPrefix(bucket1, key2);
    bucket1.deleteKey(key1);
    String snap3 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap3);

    // Diff should have 2 entries
    SnapshotDiffReportOzone
        diff2 = getSnapDiffReport(volume, bucket, snap2, snap3);
    assertEquals(2, diff2.getDiffList().size());
    assertEquals(
        Arrays.asList(SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.CREATE, key2)),
        diff2.getDiffList());

    // Rename Key2
    String key2Renamed = key2 + "_renamed";
    bucket1.renameKey(key2, key2Renamed);
    String snap4 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap4);

    SnapshotDiffReportOzone
        diff3 = getSnapDiffReport(volume, bucket, snap3, snap4);
    assertEquals(1, diff3.getDiffList().size());
    assertThat(diff3.getDiffList()).contains(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReportOzone.DiffType.RENAME, key2,
             key2Renamed));


    // Create a directory
    String dir1 = "dir-1" +  counter.incrementAndGet();
    bucket1.createDirectory(dir1);
    String snap5 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap5);
    SnapshotDiffReportOzone
        diff4 = getSnapDiffReport(volume, bucket, snap4, snap5);
    assertEquals(1, diff4.getDiffList().size());
    assertThat(diff4.getDiffList()).contains(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReportOzone.DiffType.CREATE, dir1));

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
            SnapshotDiffReport.DiffType.RENAME, key3,
             renamedKey3),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.MODIFY, key3)
        );
    assertEquals(expectedDiffList, diff5.getDiffList());

    IOException ioException = assertThrows(IOException.class,
        () -> store.snapshotDiff(volume, bucket, snap6,
            snap7, "3", 0, forceFullSnapshotDiff, disableNativeDiff));
    assertThat(ioException.getMessage()).contains("Index (given: 3) " +
        "should be a number >= 0 and < totalDiffEntries: 2. Page size " +
        "(given: 1000) should be a positive number > 0.");

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
        fromSnapName, toSnapName, null, 0, false, disableNativeDiff);

    assertEquals(IN_PROGRESS, response.getJobStatus());

    CancelSnapshotDiffResponse cancelResponse =
        store.cancelSnapshotDiff(volumeName,
            bucketName, fromSnapName, toSnapName);

    assertEquals(CANCEL_SUCCEEDED.getMessage(), cancelResponse.getMessage());
    response = store.snapshotDiff(volumeName, bucketName, fromSnapName,
        toSnapName, null, 0, false, disableNativeDiff);

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
        null, 0, false, disableNativeDiff);
    assertEquals(CANCELLED, response.getJobStatus());

    String fromSnapshotTableKey =
        SnapshotInfo.getTableKey(volumeName, linkedBuckets.getOrDefault(bucketName, bucketName), fromSnapName);
    String toSnapshotTableKey =
        SnapshotInfo.getTableKey(volumeName, linkedBuckets.getOrDefault(bucketName, bucketName), toSnapName);

    UUID fromSnapshotID = SnapshotUtils.getSnapshotInfo(ozoneManager, fromSnapshotTableKey).getSnapshotId();
    UUID toSnapshotID = SnapshotUtils.getSnapshotInfo(ozoneManager, toSnapshotTableKey).getSnapshotId();

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
        null, 0, false, disableNativeDiff);

    assertEquals(IN_PROGRESS, response.getJobStatus());

    // Cancel success.
    cancelResponse = store.cancelSnapshotDiff(volumeName,
        bucketName, fromSnapName, toSnapName);

    assertEquals(CANCEL_SUCCEEDED.getMessage(),
        cancelResponse.getMessage());

    response = store.snapshotDiff(
        volumeName, bucketName, fromSnapName, toSnapName,
        null, 0, false, disableNativeDiff);

    assertEquals(CANCELLED, response.getJobStatus());

    // Job already cancelled.
    cancelResponse = store.cancelSnapshotDiff(volumeName,
        bucketName, fromSnapName, toSnapName);
    assertEquals(CANCEL_ALREADY_CANCELLED_JOB.getMessage(),
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
          toSnapshot, null, 0, forceFullSnapshotDiff,
          disableNativeDiff);
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
    createBucket(volume1, bucket);
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
            null, 0, false, disableNativeDiff));
    assertEquals(FILE_NOT_FOUND, omException.getResult());
    // From snapshot is invalid
    omException = assertThrows(OMException.class,
        () -> store.snapshotDiff(volume, bucket, snap2, snap1,
            null, 0, false, disableNativeDiff));

    assertEquals(FILE_NOT_FOUND, omException.getResult());
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
    createBucket(volume1, bucketa);
    OzoneBucket bucket1 = volume1.getBucket(bucketa);
    // Create Key1 and take 2 snapshots
    String key1 = "key-1-";
    createFileKeyWithPrefix(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volumea, bucketa, snap1);
    String snap2 = "snap" + counter.incrementAndGet();
    createSnapshot(volumea, bucketa, snap2);
    // Bucket is nonexistent
    OMException omException = assertThrows(OMException.class,
        () -> store.snapshotDiff(volumea, bucketb, snap1, snap2,
            null, 0, forceFullSnapshotDiff, disableNativeDiff));
    assertEquals(BUCKET_NOT_FOUND, omException.getResult());
    // Volume is nonexistent
    omException = assertThrows(OMException.class,
        () -> store.snapshotDiff(volumeb, bucketa, snap2, snap1,
            null, 0, forceFullSnapshotDiff, disableNativeDiff));
    assertEquals(VOLUME_NOT_FOUND, omException.getResult());
    omException = assertThrows(OMException.class,
        () -> store.snapshotDiff(volumeb, bucketb, snap2, snap1,
            null, 0, forceFullSnapshotDiff, disableNativeDiff));
    assertEquals(VOLUME_NOT_FOUND, omException.getResult());
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
    String testVolumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    getOmKeyInfo(testVolumeName, testBucketName, key1);
    createFileKey(bucket, key1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap2);
    getOmKeyInfo(testVolumeName, testBucketName, key1);
    assertEquals(diff.getDiffList().size(), 1);
    assertEquals(diff.getDiffList(), Lists.newArrayList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.MODIFY, key1)));
  }

  @Test
  public void testSnapDiffMissingMandatoryParams() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    createBucket(volume1, bucket);
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
    OMException omException = assertThrows(OMException.class,
        () -> store.snapshotDiff(volume, bucket, snap1, nullstr,
            null, 0, forceFullSnapshotDiff, disableNativeDiff));
    assertEquals(FILE_NOT_FOUND, omException.getResult());
    // From snapshot is empty
    omException = assertThrows(OMException.class,
        () -> store.snapshotDiff(volume, bucket, nullstr, snap1,
            null, 0, forceFullSnapshotDiff, disableNativeDiff));
    assertEquals(FILE_NOT_FOUND, omException.getResult());
    // Bucket is empty
    assertThrows(IllegalArgumentException.class,
        () -> store.snapshotDiff(volume, nullstr, snap1, snap2,
            null, 0, forceFullSnapshotDiff, disableNativeDiff));
    // Volume is empty
    assertThrows(IllegalArgumentException.class,
        () -> store.snapshotDiff(nullstr, bucket, snap1, snap2,
            null, 0, forceFullSnapshotDiff, disableNativeDiff));
  }

  @Test
  public void testSnapDiffMultipleBuckets() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucketName1 = "buck-" + counter.incrementAndGet();
    String bucketName2 = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    createBucket(volume1, bucketName1);
    createBucket(volume1, bucketName2);
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
    assertEquals(1, diff1.getDiffList().size());
  }

  @Test
  public void testListSnapshotDiffWithInvalidParameters()
      throws Exception {
    String volume = "vol-" + RandomStringUtils.secure().nextNumeric(5);
    String bucket = "buck-" + RandomStringUtils.secure().nextNumeric(5);

    String volErrorMessage = "Volume not found: " + volume;

    Exception volBucketEx = assertThrows(OMException.class,
        () -> store.listSnapshotDiffJobs(volume, bucket, "", true, null));
    assertEquals(volErrorMessage, volBucketEx.getMessage());

    // Create the volume and the bucket.
    store.createVolume(volume);
    OzoneVolume ozVolume = store.getVolume(volume);
    createBucket(ozVolume, bucket);

    assertDoesNotThrow(() ->
        store.listSnapshotDiffJobs(volume, bucket, "", true, null));

    // There are no snapshots, response should be empty.
    Iterator<OzoneSnapshotDiff> iterator = store.listSnapshotDiffJobs(volume, bucket, "", true, null);
    assertFalse(iterator.hasNext());

    OzoneBucket ozBucket = ozVolume.getBucket(bucket);
    // Create keys and take snapshots.
    String key1 = "key-1-" + RandomStringUtils.secure().nextNumeric(5);
    createFileKey(ozBucket, key1);
    String snap1 = "snap-1-" + RandomStringUtils.secure().nextNumeric(5);
    createSnapshot(volume, bucket, snap1);

    String key2 = "key-2-" + RandomStringUtils.secure().nextNumeric(5);
    createFileKey(ozBucket, key2);
    String snap2 = "snap-2-" + RandomStringUtils.secure().nextNumeric(5);
    createSnapshot(volume, bucket, snap2);

    store.snapshotDiff(volume, bucket, snap1, snap2, null, 0,
        forceFullSnapshotDiff, disableNativeDiff);

    String invalidStatus = "invalid";
    String statusErrorMessage = "Invalid job status: " + invalidStatus;

    OMException statusEx = assertThrows(OMException.class,
        () -> store.listSnapshotDiffJobs(volume, bucket, invalidStatus, false, null));
    assertEquals(statusErrorMessage, statusEx.getMessage());
  }

  /**
   * Tests snap diff when there are multiple sst files in from and to
   * snapshots pertaining to different buckets. This will test the
   * sst filtering code path.
   */
  @Test
  public void testSnapDiffWithMultipleSSTs() throws Exception {
    // Create a volume and 2 buckets
    String volumeName1 = "vol-" + counter.incrementAndGet();
    String bucketName1 = "buck1";
    String bucketName2 = "buck2";
    store.createVolume(volumeName1);
    OzoneVolume volume1 = store.getVolume(volumeName1);
    createBucket(volume1, bucketName1);
    createBucket(volume1, bucketName2);
    OzoneBucket bucket1 = volume1.getBucket(bucketName1);
    OzoneBucket bucket2 = volume1.getBucket(bucketName2);
    String keyPrefix = "key-";
    // add file to bucket1 and take snapshot
    createFileKeyWithPrefix(bucket1, keyPrefix);
    int keyTableSize = getKeyTableSstFiles().size();
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volumeName1, bucketName1, snap1); // 1.sst
    assertEquals(1, (getKeyTableSstFiles().size() - keyTableSize));
    // add files to bucket2 and flush twice to create 2 sst files
    for (int i = 0; i < 5; i++) {
      createFileKeyWithPrefix(bucket2, keyPrefix);
    }
    flushKeyTable(); // 1.sst 2.sst
    assertEquals(2, (getKeyTableSstFiles().size() - keyTableSize));
    for (int i = 0; i < 5; i++) {
      createFileKeyWithPrefix(bucket2, keyPrefix);
    }
    flushKeyTable(); // 1.sst 2.sst 3.sst
    assertEquals(3, (getKeyTableSstFiles().size() - keyTableSize));
    // add a file to bucket1 and take second snapshot
    createFileKeyWithPrefix(bucket1, keyPrefix);
    String snap2 = "snap" + counter.incrementAndGet();
    createSnapshot(volumeName1, bucketName1, snap2); // 1.sst 2.sst 3.sst 4.sst
    assertEquals(4, (getKeyTableSstFiles().size() - keyTableSize));
    SnapshotDiffReportOzone diff1 = getSnapDiffReport(volumeName1, bucketName1, snap1, snap2);
    assertEquals(1, diff1.getDiffList().size());
  }

  @Test
  public void testDeleteSnapshotTwice() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    createBucket(volume1, bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    createFileKeyWithPrefix(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap1);
    store.deleteSnapshot(volume, bucket, snap1);

    OMException omException = assertThrows(OMException.class,
            () -> store.deleteSnapshot(volume, bucket, snap1));
    assertEquals(OMException.ResultCodes.FILE_NOT_FOUND,
        omException.getResult());
  }

  @Test
  public void testDeleteSnapshotFailure() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    createBucket(volume1, bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    createFileKeyWithPrefix(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap1);

    // Delete non-existent snapshot
    OMException omException = assertThrows(OMException.class,
        () -> store.deleteSnapshot(volume, bucket, "snapnonexistent"));
    assertEquals(OMException.ResultCodes.FILE_NOT_FOUND,
        omException.getResult());

    // Delete snapshot with non-existent url
    omException = assertThrows(OMException.class,
        () -> store.deleteSnapshot(volume, "nonexistentbucket", snap1));
    assertEquals(BUCKET_NOT_FOUND,
        omException.getResult());
  }

  @Test
  public void testDeleteSnapshotMissingMandatoryParams() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    createBucket(volume1, bucket);
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

  @Test
  public void testSnapshotQuotaHandling() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    createBucket(volume1, bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    OzoneBucket originalBucket1 = volume1.getBucket(linkedBuckets.getOrDefault(bucket, bucket));
    originalBucket1.setQuota(OzoneQuota.parseQuota("102400000", "500"));
    volume1.setQuota(OzoneQuota.parseQuota("204800000", "1000"));

    long volUsedNamespaceInitial = volume1.getUsedNamespace();
    long buckUsedNamspaceInitial = bucket1.getUsedNamespace();
    long buckUsedBytesIntial = bucket1.getUsedBytes();

    String key1 = "key-1-";
    key1 = createFileKeyWithPrefix(bucket1, key1);

    long volUsedNamespaceBefore = volume1.getUsedNamespace();
    long buckUsedNamspaceBefore = bucket1.getUsedNamespace();
    long buckUsedBytesBefore = bucket1.getUsedBytes();

    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap1);

    long volUsedNamespaceAfter = volume1.getUsedNamespace();
    long buckUsedNamespaceAfter = bucket1.getUsedNamespace();
    long buckUsedBytesAfter = bucket1.getUsedBytes();

    assertEquals(volUsedNamespaceBefore, volUsedNamespaceAfter);
    assertEquals(buckUsedNamspaceBefore, buckUsedNamespaceAfter);
    assertEquals(buckUsedBytesBefore, buckUsedBytesAfter);

    store.deleteSnapshot(volume, bucket, snap1);

    long volUsedNamespaceFinal = volume1.getUsedNamespace();
    long buckUsedNamespaceFinal = bucket1.getUsedNamespace();
    long buckUsedBytesFinal = bucket1.getUsedBytes();

    assertEquals(volUsedNamespaceBefore, volUsedNamespaceFinal);
    assertEquals(buckUsedNamspaceBefore, buckUsedNamespaceFinal);
    assertEquals(buckUsedBytesBefore, buckUsedBytesFinal);

    bucket1.deleteKey(key1);

    assertEquals(volUsedNamespaceInitial, volume1.getUsedNamespace());
    assertEquals(buckUsedNamspaceInitial, bucket1.getUsedNamespace());
    assertEquals(buckUsedBytesIntial, bucket1.getUsedBytes());
  }

  @Nonnull
  private List<LiveFileMetaData> getKeyTableSstFiles()
      throws IOException {
    if (!bucketLayout.isFileSystemOptimized()) {
      return getRdbStore().getDb().getSstFileList().stream()
          .filter(x -> StringUtils.bytes2String(x.columnFamilyName()).equals(KEY_TABLE))
          .collect(Collectors.toList());
    }
    return getRdbStore().getDb().getSstFileList().stream()
        .filter(x -> StringUtils.bytes2String(x.columnFamilyName()).equals(FILE_TABLE))
        .collect(Collectors.toList());
  }

  private void flushKeyTable() throws IOException {
    if (!bucketLayout.isFileSystemOptimized()) {
      getRdbStore().getDb().flush(KEY_TABLE);
    } else {
      getRdbStore().getDb().flush(FILE_TABLE);
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
        .get(SnapshotInfo.getTableKey(volName, linkedBuckets.getOrDefault(buckName, buckName), snapshotName));
    String snapshotDirName =
        OmSnapshotManager.getSnapshotPath(ozoneManager.getConfiguration(),
            snapshotInfo, 0) + OM_KEY_PREFIX + "CURRENT";
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
    byte[] value = RandomStringUtils.secure().nextAscii(10240).getBytes(UTF_8);
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
    }, 300, 10000);
    return key;
  }

  private void createFileKey(FileSystem fs,
                             String path)
      throws IOException, InterruptedException, TimeoutException {
    byte[] value = RandomStringUtils.secure().nextAscii(10240).getBytes(UTF_8);
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
  }

  @Test
  public void testSnapshotOpensWithDisabledAutoCompaction() throws Exception {
    String snapPrefix = createSnapshot(volumeName, bucketName);
    try (UncheckedAutoCloseableSupplier<IOmMetadataReader> snapshotSupplier =
             cluster.getOzoneManager().getOmSnapshotManager()
        .getActiveFsMetadataOrSnapshot(volumeName, bucketName, snapPrefix)) {
      RDBStore snapshotDBStore = (RDBStore)((OmSnapshot)snapshotSupplier.get()).getMetadataManager().getStore();
      for (String table : snapshotDBStore.getTableNames().values()) {
        assertTrue(snapshotDBStore.getDb().getColumnFamily(table)
            .getHandle().getDescriptor()
            .getOptions().disableAutoCompactions());
      }
    }
  }

  // Test snapshot diff when OM restarts in non-HA OM env and diff job is
  // in_progress when it restarts.
  @Test
  public void testSnapshotDiffWhenOmRestart() throws Exception {
    String snapshot1 = "snap-" + RandomStringUtils.secure().nextNumeric(5);
    String snapshot2 = "snap-" + RandomStringUtils.secure().nextNumeric(5);
    createSnapshots(snapshot1, snapshot2);

    SnapshotDiffResponse response = store.snapshotDiff(volumeName, bucketName,
        snapshot1, snapshot2, null, 0, forceFullSnapshotDiff,
        disableNativeDiff);

    assertEquals(IN_PROGRESS, response.getJobStatus());

    // Restart the OM and wait for sometime to make sure that previous snapDiff
    // job finishes.
    cluster.restartOzoneManager();
    stopKeyManager();
    await(POLL_MAX_WAIT_MILLIS, POLL_INTERVAL_MILLIS,
        () -> cluster.getOzoneManager().isRunning());

    response = store.snapshotDiff(volumeName, bucketName,
        snapshot1, snapshot2, null, 0, forceFullSnapshotDiff,
        disableNativeDiff);

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
      SnapshotDiffReportOzone diffReport = fetchReportPage(volumeName,
          bucketName, snapshot1, snapshot2, null, 0);
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
    String snapshot1 = "snap-" + RandomStringUtils.secure().nextNumeric(5);
    String snapshot2 = "snap-" + RandomStringUtils.secure().nextNumeric(5);
    createSnapshots(snapshot1, snapshot2);

    SnapshotDiffReportOzone diffReport = fetchReportPage(volumeName,
        bucketName, snapshot1, snapshot2, null, pageSize);

    List<DiffReportEntry> diffReportEntries = diffReport.getDiffList();

    // Restart the OM and no need to wait because snapDiff job finished before
    // the restart.
    cluster.restartOzoneManager();
    stopKeyManager();
    await(POLL_MAX_WAIT_MILLIS, POLL_INTERVAL_MILLIS,
        () -> cluster.getOzoneManager().isRunning());

    while (isNotEmpty(diffReport.getToken())) {
      diffReport = fetchReportPage(volumeName, bucketName, snapshot1,
          snapshot2, diffReport.getToken(), pageSize);
      diffReportEntries.addAll(diffReport.getDiffList());
    }
    assertEquals(100, diffReportEntries.size());
  }

  private SnapshotDiffReportOzone fetchReportPage(String volName,
                                                  String buckName,
                                                  String fromSnapshot,
                                                  String toSnapshot,
                                                  String token,
                                                  int pageSize)
      throws IOException, InterruptedException {

    while (true) {
      SnapshotDiffResponse response = store.snapshotDiff(volName, buckName,
          fromSnapshot, toSnapshot, token, pageSize, forceFullSnapshotDiff,
          disableNativeDiff);
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

    RDBStore activeDbStore = getRdbStore();
    // RocksDBCheckpointDiffer should be not null for active DB store.
    assertNotNull(activeDbStore.getRocksDBCheckpointDiffer());
    // 1 listener for flush-based tracking (FlushCompletedListener)
    assertEquals(1,  activeDbStore.getDbOptions().listeners().size());

    try (UncheckedAutoCloseableSupplier<IOmMetadataReader> omSnapshot = cluster.getOzoneManager()
        .getOmSnapshotManager()
        .getActiveFsMetadataOrSnapshot(volumeName, bucketName, snapshotName)) {
      RDBStore snapshotDbStore =
          (RDBStore) ((OmSnapshot)omSnapshot.get()).getMetadataManager().getStore();
      // RocksDBCheckpointDiffer should be null for snapshot DB store.
      assertNull(snapshotDbStore.getRocksDBCheckpointDiffer());
      assertEquals(0, snapshotDbStore.getDbOptions().listeners().size());
    }
  }

  @Test
  @Slow("HDDS-9299")
  public void testDayWeekMonthSnapshotCreationAndExpiration() throws Exception {
    String volumeA = "vol-a-" + RandomStringUtils.secure().nextNumeric(5);
    String bucketA = "buc-a-" + RandomStringUtils.secure().nextNumeric(5);
    store.createVolume(volumeA);
    OzoneVolume volA = store.getVolume(volumeA);
    createBucket(volA, bucketA);
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
    switch (snapshotPrefix) {
    case SNAPSHOT_DAY_PREFIX:
      targetOldestIndex = oldestDayIndex;
      targetLatestIndex = latestDayIndex;
      break;
    case SNAPSHOT_WEEK_PREFIX:
      targetOldestIndex = oldestWeekIndex;
      targetLatestIndex = latestWeekIndex;
      break;
    case SNAPSHOT_MONTH_PREFIX:
      targetOldestIndex = oldestMonthIndex;
      targetLatestIndex = latestMonthIndex;
      break;
    default:
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
      assertEquals(keyName, ozoneKeyDetails.getName());
      assertEquals(linkedBuckets.getOrDefault(ozoneBucketClient.getName(), ozoneBucketClient.getName()),
          ozoneKeyDetails.getBucketName());
      assertEquals(ozoneBucketClient.getVolumeName(),
          ozoneKeyDetails.getVolumeName());

      // Validate keys data in active Ozone namespace
      try (OzoneInputStream ozoneInputStream =
               ozoneBucketClient.readKey(keyName)) {
        byte[] fileContent = new byte[keyName.length()];
        IOUtils.readFully(ozoneInputStream, fileContent);
        assertEquals(keyName, new String(fileContent, UTF_8));
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
          Matcher snapKeyNameMatcher = SNAPSHOT_KEY_PATTERN.matcher(keyName);
          if (snapKeyNameMatcher.matches()) {
            String truncatedSnapshotKeyName = snapKeyNameMatcher.group(3);
            byte[] fileContent = new byte[truncatedSnapshotKeyName.length()];
            IOUtils.readFully(ozoneInputStream, fileContent);
            assertEquals(truncatedSnapshotKeyName,
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

  private String getKeySuffix(int index) {
    return leftPad(Integer.toString(index), 10, "0");
  }

  // End-to-end test to verify that compaction DAG only tracks 'keyTable',
  // 'directoryTable' and 'fileTable' column families. And only these
  // column families are used in SST diff calculation.
  @Test
  public void testSnapshotCompactionDag() throws Exception {
    String volume1 = "volume-1-" + RandomStringUtils.secure().nextNumeric(5);
    String bucket1 = "bucket-1-" + RandomStringUtils.secure().nextNumeric(5);
    String bucket2 = "bucket-2-" + RandomStringUtils.secure().nextNumeric(5);
    String bucket3 = "bucket-3-" + RandomStringUtils.secure().nextNumeric(5);

    store.createVolume(volume1);
    OzoneVolume ozoneVolume = store.getVolume(volume1);
    createBucket(ozoneVolume, bucket1);
    OzoneBucket ozoneBucket1 = ozoneVolume.getBucket(bucket1);

    DBStore activeDbStore = ozoneManager.getMetadataManager().getStore();

    for (int i = 0; i < 100; i++) {
      String keyName = "/dir1/dir2/dir3/key-" + getKeySuffix(i);
      createFileKey(ozoneBucket1, keyName);
    }

    createSnapshot(volume1, bucket1, "bucket1-snap1");
    activeDbStore.compactDB();

    createBucket(ozoneVolume, bucket2);
    OzoneBucket ozoneBucket2 = ozoneVolume.getBucket(bucket2);

    for (int i = 100; i < 200; i++) {
      String keyName = "/dir1/dir2/dir3/key-" + getKeySuffix(i);
      createFileKey(ozoneBucket1, keyName);
      createFileKey(ozoneBucket2, keyName);
    }

    createSnapshot(volume1, bucket1, "bucket1-snap2");
    createSnapshot(volume1, bucket2, "bucket2-snap1");
    activeDbStore.compactDB();

    createBucket(ozoneVolume, bucket3);
    OzoneBucket ozoneBucket3 = ozoneVolume.getBucket(bucket3);

    for (int i = 200; i < 300; i++) {
      String keyName = "/dir1/dir2/dir3/key-" + getKeySuffix(i);
      createFileKey(ozoneBucket1, keyName);
      createFileKey(ozoneBucket2, keyName);
      createFileKey(ozoneBucket3, keyName);
    }

    createSnapshot(volume1, bucket1, "bucket1-snap3");
    createSnapshot(volume1, bucket2, "bucket2-snap2");
    createSnapshot(volume1, bucket3, "bucket3-snap1");
    activeDbStore.compactDB();

    for (int i = 300; i < 400; i++) {
      String keyName = "/dir1/dir2/dir3/key-" + getKeySuffix(i);
      createFileKey(ozoneBucket3, keyName);
      createFileKey(ozoneBucket2, keyName);
    }

    createSnapshot(volume1, bucket2, "bucket2-snap3");
    createSnapshot(volume1, bucket3, "bucket3-snap2");
    activeDbStore.compactDB();

    for (int i = 400; i < 500; i++) {
      String keyName = "/dir1/dir2/dir3/key-" + getKeySuffix(i);
      createFileKey(ozoneBucket3, keyName);
    }

    createSnapshot(volume1, bucket3, "bucket3-snap3");

    List<FlushNode> filteredNodes = ozoneManager.getMetadataManager()
        .getStore()
        .getRocksDBCheckpointDiffer()
        .getFlushLinkedList().getFlushNodeMap().values().stream()
        .filter(node ->
            !COLUMN_FAMILIES_TO_TRACK.contains(node.getColumnFamily()))
        .collect(Collectors.toList());

    assertEquals(0, filteredNodes.size());

    assertEquals(100,
        fetchReportPage(volume1, bucket1, "bucket1-snap1", "bucket1-snap2",
            null, 0).getDiffList().size());
    assertEquals(100,
        fetchReportPage(volume1, bucket1, "bucket1-snap2", "bucket1-snap3",
            null, 0).getDiffList().size());
    assertEquals(200,
        fetchReportPage(volume1, bucket1, "bucket1-snap1", "bucket1-snap3",
            null, 0).getDiffList().size());
    assertEquals(100,
        fetchReportPage(volume1, bucket2, "bucket2-snap1", "bucket2-snap2",
            null, 0).getDiffList().size());
    assertEquals(100,
        fetchReportPage(volume1, bucket2, "bucket2-snap2", "bucket2-snap3",
            null, 0).getDiffList().size());
    assertEquals(200,
        fetchReportPage(volume1, bucket2, "bucket2-snap1", "bucket2-snap3",
            null, 0).getDiffList().size());
    assertEquals(100,
        fetchReportPage(volume1, bucket3, "bucket3-snap1", "bucket3-snap2",
            null, 0).getDiffList().size());
    assertEquals(100,
        fetchReportPage(volume1, bucket3, "bucket3-snap2", "bucket3-snap3",
            null, 0).getDiffList().size());
    assertEquals(200,
        fetchReportPage(volume1, bucket3, "bucket3-snap1", "bucket3-snap3",
            null, 0).getDiffList().size());

    if (!disableNativeDiff) {
      // Prune SST files in compaction backup directory.
      RocksDatabase db = getRdbStore().getDb();
      RocksDBCheckpointDiffer differ = getRdbStore().getRocksDBCheckpointDiffer();
      differ.pruneSstFileValues();

      // Verify backup SST files are pruned on DB compactions.
      java.nio.file.Path sstBackUpDir = java.nio.file.Paths.get(differ.getSSTBackupDir());
      try (ManagedOptions managedOptions = new ManagedOptions();
           ManagedRocksIterator managedRocksIterator = new ManagedRocksIterator(
               db.getManagedRocksDb().get().newIterator(db.getColumnFamily(COMPACTION_LOG_TABLE).getHandle()))) {
        managedRocksIterator.get().seekToFirst();
        while (managedRocksIterator.get().isValid()) {
          byte[] value = managedRocksIterator.get().value();
          CompactionLogEntry compactionLogEntry = CompactionLogEntry.getFromProtobuf(
              CompactionLogEntryProto.parseFrom(value));
          compactionLogEntry.getInputFileInfoList().forEach(
              f -> {
                java.nio.file.Path file = sstBackUpDir.resolve(f.getFileName() + ".sst");
                if (COLUMN_FAMILIES_TO_TRACK.contains(f.getColumnFamily()) && java.nio.file.Files.exists(file)) {
                  assertTrue(f.isPruned());
                  try (ManagedRawSSTFileReader sstFileReader = new ManagedRawSSTFileReader(
                          managedOptions, file.toFile().getAbsolutePath(), 2 * 1024 * 1024);
                       ManagedRawSSTFileIterator<CodecBuffer> itr = sstFileReader.newIterator(
                           ManagedRawSSTFileIterator.KeyValue::getValue, null, null, KEY_AND_VALUE)) {
                    while (itr.hasNext()) {
                      assertEquals(0, itr.next().readableBytes());
                    }
                  }
                } else {
                  assertFalse(f.isPruned());
                }
              });
          managedRocksIterator.get().next();
        }
      }
    }
  }

  @Test
  public void testSnapshotReuseSnapName() throws Exception {
    // start KeyManager for this test
    startKeyManager();
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    createBucket(volume1, bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    createFileKeyWithPrefix(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    String snapshotKeyPrefix = createSnapshot(volume, bucket, snap1);

    int keyCount1 = keyCount(bucket1, snapshotKeyPrefix + "key-");
    assertEquals(1, keyCount1);

    store.deleteSnapshot(volume, bucket, snap1);

    GenericTestUtils.waitFor(() -> {
      try {
        return !ozoneManager.getMetadataManager().getSnapshotInfoTable()
            .isExist(SnapshotInfo.getTableKey(volume, bucket, snap1));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 200, 10000);

    createFileKeyWithPrefix(bucket1, key1);
    String snap2 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap2);

    String key2 = "key-2-";
    createFileKeyWithPrefix(bucket1, key2);
    createSnapshot(volume, bucket, snap1);

    int keyCount2 = keyCount(bucket1, snapshotKeyPrefix + "key-");
    assertEquals(3, keyCount2);

    // Stop key manager after testcase executed
    stopKeyManager();
  }

  @Test
  public void testSnapdiffWithUnsupportedFileSystemAPI() throws Exception {
    assumeTrue(!bucketLayout.isObjectStore(enabledFileSystemPaths));
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket" + counter.incrementAndGet();
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, testBucketName, testVolumeName);
    try (FileSystem fs = FileSystem.get(new URI(rootPath), cluster.getConf())) {
      String snap1 = "snap1";
      String key = "/key1";
      createFileKey(fs, key);
      Path pathVal = new Path(key);
      createSnapshot(testVolumeName, testBucketName, snap1);

      // Future-proofing: if we implement these APIs some day, we should
      // make sure snapshot diff report records diffs.

      // TODO: if this test fails here, it means that the APIs are implemented,
      // and we need to update the test to check that the diff report
      // records the changes.
      assertThrows(UnsupportedOperationException.class,
          () -> fs.setAcl(pathVal, null));
      assertThrows(UnsupportedOperationException.class,
          () -> fs.modifyAclEntries(pathVal, null));
      assertThrows(UnsupportedOperationException.class,
          () -> fs.removeAclEntries(pathVal, null));
      assertThrows(UnsupportedOperationException.class,
          () -> fs.removeAcl(pathVal));
      assertThrows(UnsupportedOperationException.class,
          () -> fs.removeDefaultAcl(pathVal));
      assertThrows(UnsupportedOperationException.class,
          () -> fs.setXAttr(pathVal, null, null));
      assertThrows(UnsupportedOperationException.class,
          () -> fs.removeXAttr(pathVal, null));
      assertThrows(UnsupportedOperationException.class,
          () -> fs.append(pathVal));
      assertThrows(UnsupportedOperationException.class,
          () -> fs.truncate(pathVal, 0));
      assertThrows(UnsupportedOperationException.class,
          () -> fs.concat(pathVal, new Path[]{}));
      assertThrows(UnsupportedOperationException.class,
          () -> fs.createSymlink(pathVal, pathVal, false));

      String snap2 = "snap2";
      createSnapshot(testVolumeName, testBucketName, snap2);
      SnapshotDiffReport diff = getSnapDiffReport(testVolumeName,
          testBucketName, snap1, snap2);
      // TODO: if any APIs are implemented the diff list should not be empty.
      assertEquals(0, diff.getDiffList().size());
    }
  }

  @Test
  public void testSnapdiffWithNoOpAPI() throws Exception {
    assumeTrue(!bucketLayout.isObjectStore(enabledFileSystemPaths));
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket" + counter.incrementAndGet();
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, testBucketName, testVolumeName);
    try (FileSystem fs = FileSystem.get(new URI(rootPath), cluster.getConf())) {
      String snap1 = "snap1";
      String key = "/key1";
      createFileKey(fs, key);
      // get file status object
      FileStatus fileStatus = fs.getFileStatus(new Path(key));
      // sleep 100 ms.
      Thread.sleep(100);
      Path pathVal = new Path(key);
      createSnapshot(testVolumeName, testBucketName, snap1);

      String newUserName = fileStatus.getOwner() + "new";

      fs.setPermission(pathVal, new FsPermission("+rwx"));
      fs.setOwner(pathVal, newUserName, null);
      fs.setReplication(pathVal, (short) 1);

      // TODO: if the test fails here, that means that the APIs are
      // implemented, and we need to update the test to check that the
      // diff report records the changes.
      FileStatus fileStatusAfter = fs.getFileStatus(new Path(key));
      assertEquals(fileStatus.getModificationTime(), fileStatusAfter.getModificationTime());
      assertEquals(fileStatus.getPermission(), fileStatusAfter.getPermission());
      assertEquals(fileStatus.getOwner(), fileStatusAfter.getOwner());
      assertEquals(fileStatus.getReplication(), fileStatusAfter.getReplication());

      String snap2 = "snap2";
      createSnapshot(testVolumeName, testBucketName, snap2);
      SnapshotDiffReport diff = getSnapDiffReport(testVolumeName,
          testBucketName, snap1, snap2);
      // TODO: if any APIs are implemented the diff list should not be empty.
      assertEquals(0, diff.getDiffList().size());
    }
  }

  @Test
  public void testSnapshotdiffWithObjectTagModification() throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String snap1 = "snap1";
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    createSnapshot(testVolumeName, testBucketName, snap1);

    Map<String, String> tags = new HashMap<>();
    tags.put("environment", "test");
    tags.put("owner", "testuser");
    bucket.putObjectTagging(key1, tags);

    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
        snap1, snap2);

    List<DiffReportEntry> diffEntries = Lists.newArrayList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.MODIFY,
            key1));
    assertEquals(diffEntries, diff.getDiffList());
  }

  @Test
  public void testSnapdiffWithMultipleTagOperations() throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);

    Map<String, String> initialTags = new HashMap<>();
    initialTags.put("version", "1.0");
    initialTags.put("team", "dev");

    bucket.putObjectTagging(key1, initialTags);

    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);

    Map<String, String> updatedTags = new HashMap<>();
    updatedTags.put("version", "2.0");
    updatedTags.put("team", "dev");
    updatedTags.put("priority", "high");

    bucket.putObjectTagging(key1, updatedTags);

    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
        snap1, snap2);

    List<DiffReportEntry> diffEntries = Lists.newArrayList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.MODIFY,
            key1));
    assertEquals(diffEntries, diff.getDiffList());
  }

  @Test
  public void testSnapdiffWithTagRemoval() throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);

    Map<String, String> initialTags = new HashMap<>();
    initialTags.put("env", "staging");
    initialTags.put("cost-center", "engineering");
    initialTags.put("backup", "daily");

    bucket.putObjectTagging(key1, initialTags);

    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    bucket.deleteObjectTagging(key1);

    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
        snap1, snap2);

    List<DiffReportEntry> diffEntries = Lists.newArrayList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.MODIFY,
            key1));
    assertEquals(diffEntries, diff.getDiffList());
  }

  private String createStreamKeyWithPrefix(OzoneBucket bucket, String keyPrefix)
      throws Exception {
    String key = keyPrefix + counter.incrementAndGet();
    return createStreamKey(bucket, key);
  }

  private String createStreamKey(OzoneBucket bucket, String key)
      throws Exception {
    byte[] value = RandomStringUtils.secure().nextAscii(10240).getBytes(UTF_8);
    OzoneDataStreamOutput streamKey = bucket.createStreamKey(key, value.length);
    streamKey.write(value);
    streamKey.close();
    GenericTestUtils.waitFor(() -> {
      try {
        getOmKeyInfo(bucket.getVolumeName(), bucket.getName(), key);
      } catch (IOException e) {
        return false;
      }
      return true;
    }, 1000, 30000);
    return key;
  }

  @Test
  public void testSnapDiffWithStreamKeyModification() throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String key1 = "stream-key-1";
    key1 = createStreamKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);

    String key2 = "stream-key-2";
    key2 = createStreamKeyWithPrefix(bucket, key2);
    bucket.deleteKey(key1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff =  getSnapDiffReport(testVolumeName, testBucketName,
        snap1, snap2);
    assertEquals(2, diff.getDiffList().size());
    assertEquals(Arrays.asList(SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1),
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.CREATE, key2)), diff.getDiffList());
  }

  @Test
  public void testSnapDiffWithStreamKeyRewrite() throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String key1 = "stream-key-1";
    key1 = createStreamKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);

    createStreamKey(bucket, key1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName, testBucketName, snap1, snap2);
    assertEquals(1, diff.getDiffList().size());
    assertEquals(Collections.singletonList(
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.MODIFY, key1)),
        diff.getDiffList());
  }

  @Test
  public void testSnapDiffWithStreamKeyRename() throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String key1 = "stream-key-1";
    key1 = createStreamKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);

    String renamedKey = key1 + "_renamed";
    bucket.renameKey(key1, renamedKey);
    GenericTestUtils.waitFor(() -> {
      try {
        getOmKeyInfo(testVolumeName, testBucketName, renamedKey);
      } catch (IOException e) {
        return false;
      }
      return true;
    }, 1000, 10000);

    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap2);
    assertEquals(1, diff.getDiffList().size());
    assertEquals(Collections.singletonList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.RENAME, key1, renamedKey)),
        diff.getDiffList());
  }

  @Test
  public void testSnapDiffWithStreamKeyRecreation() throws Exception {
    String testVolumeName = "vol" +  counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String key1 = "stream-key-1";
    key1 = createStreamKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);

    getOmKeyInfo(testVolumeName, testBucketName, key1);
    bucket.deleteKey(key1);
    key1 = createStreamKey(bucket, key1);
    getOmKeyInfo(testVolumeName, testBucketName, key1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap2);
    assertEquals(2, diff.getDiffList().size());
    assertEquals(Arrays.asList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.DELETE, key1),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.CREATE, key1)),
        diff.getDiffList());
  }

  @Test
  public void testSnapDiffWithMixedStreamAndRegularKeys() throws Exception {
    String testVolumeName = "vol" +  counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String regularKey1 = "regular-key-1";
    String streamKey1 = "stream-key-1";
    regularKey1 = createFileKeyWithPrefix(bucket, regularKey1);
    streamKey1 = createStreamKeyWithPrefix(bucket, streamKey1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);

    String regularKey2 = "regular-key-2";
    String streamKey2 = "stream-key-2";
    regularKey2 = createFileKeyWithPrefix(bucket, regularKey2);
    streamKey2 = createStreamKeyWithPrefix(bucket, streamKey2);
    bucket.deleteKey(regularKey1);
    bucket.deleteKey(streamKey1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap2);
    assertEquals(4, diff.getDiffList().size());
    List<SnapshotDiffReport.DiffReportEntry> expectedDiffs = Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, regularKey1),
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, streamKey1),
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.CREATE, regularKey2),
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.CREATE, streamKey2));
    assertEquals(expectedDiffs, diff.getDiffList());
  }

  private String createStreamFileWithPrefix(OzoneBucket bucket, String filePrefix)
      throws Exception {
    String file = filePrefix + counter.incrementAndGet();
    return createStreamFile(bucket, file, false);
  }

  private String createStreamFile(OzoneBucket bucket, String fileName, boolean overWrite)
      throws Exception {
    byte[] value = RandomStringUtils.secure().nextAscii(10240).getBytes(UTF_8);
    OzoneDataStreamOutput streamFile = bucket.createStreamFile(
        fileName, value.length, bucket.getReplicationConfig(), overWrite, true);
    streamFile.write(value);
    streamFile.close();
    GenericTestUtils.waitFor(() -> {
      try {
        getOmKeyInfo(bucket.getVolumeName(), bucket.getName(), fileName);
      } catch (IOException e) {
        return false;
      }
      return true;
    }, 300, 10000);
    return fileName;
  }

  @Test
  public void testSnapDiffWithStreamFileModification() throws Exception {
    assumeTrue(!bucketLayout.isObjectStore(enabledFileSystemPaths));

    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String file1 = "stream-file-1";
    file1 = createStreamFileWithPrefix(bucket, file1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);

    String file2 = "stream-file-2";
    file2 = createStreamFileWithPrefix(bucket, file2);
    bucket.deleteKey(file1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap2);
    assertEquals(2, diff.getDiffList().size());
    assertEquals(Arrays.asList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.DELETE, file1),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.CREATE, file2)),
        diff.getDiffList());
  }

  @Test
  public void testSnapDiffWithStreamFileRewrite() throws Exception {
    assumeTrue(!bucketLayout.isObjectStore(enabledFileSystemPaths));

    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String file1 = "stream-file-1";
    file1 = createStreamFileWithPrefix(bucket, file1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);

    createStreamFile(bucket, file1, true);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap2);
    assertEquals(1, diff.getDiffList().size());
    assertEquals(Collections.singletonList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.MODIFY, file1)),
        diff.getDiffList());
  }

  @Test
  public void testSnapDiffWithStreamFileRename() throws Exception {
    assumeTrue(!bucketLayout.isObjectStore(enabledFileSystemPaths));

    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String file1 = "stream-file-1";
    file1 = createStreamFileWithPrefix(bucket, file1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);

    String renamedFile = file1 + "_renamed";
    bucket.renameKey(file1, renamedFile);
    GenericTestUtils.waitFor(() -> {
      try {
        getOmKeyInfo(testVolumeName, testBucketName, renamedFile);
      } catch (IOException e) {
        return false;
      }
      return true;
    }, 1000, 30000);

    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap2);
    assertEquals(1, diff.getDiffList().size());
    assertEquals(Collections.singletonList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.RENAME, file1, renamedFile)),
        diff.getDiffList());
  }

  @Test
  public void testSnapDiffWithStreamFileRecreation() throws Exception {
    assumeTrue(!bucketLayout.isObjectStore(enabledFileSystemPaths));

    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String file1 = "stream-file-1";
    file1 = createStreamFileWithPrefix(bucket, file1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);

    getOmKeyInfo(testVolumeName, testBucketName, file1);
    bucket.deleteKey(file1);
    file1 = createStreamFile(bucket, file1, true);
    getOmKeyInfo(testVolumeName, testBucketName, file1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap2);
    assertEquals(2, diff.getDiffList().size());
    assertEquals(Arrays.asList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.DELETE, file1),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.CREATE, file1)),
        diff.getDiffList());
  }

  @Test
  public void testSnapDiffWithStreamFileAndDirectoryOperations() throws Exception {
    assumeTrue(bucketLayout.isFileSystemOptimized());
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    bucket.createDirectory("dir1/dir2");
    String file1 = "dir1/dir2/stream-file-1";
    createStreamFile(bucket, file1, false);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);

    String file2 = "dir1/dir2/stream-file-2";
    createStreamFile(bucket, file2, false);
    bucket.renameKey("dir1/dir2", "dir1/dir3");
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap2);
    assertEquals(3, diff.getDiffList().size());

    List<SnapshotDiffReport.DiffReportEntry> expectedDiffs = Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.RENAME, "dir1/dir2", "dir1/dir3"),
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.CREATE, "dir1/dir3/stream-file-2"),
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.MODIFY, "dir1"));
    assertEquals(expectedDiffs, diff.getDiffList());
  }

  private ReplicationConfig getDefaultReplication() {
    return StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE);
  }

  @Test
  public void testSnapshotDiffWithInitiateMultipartUpload() throws Exception {
    String testVolumeName = "vol-initiate" + counter.incrementAndGet();
    String testBucketName = "bucket-initiate-" + counter.incrementAndGet();

    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String snap1 = "snap-before-initiate-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap1);

    String mpuKey1 = "mpu-key-1" + counter.incrementAndGet();
    String mpuKey2 = "mpu-key-2" + counter.incrementAndGet();

    Map<String, String> metadata = new HashMap<>();
    metadata.put("test-metadata", "initiate-test");

    Map<String, String> tags = new HashMap<>();
    tags.put("environment", "test");
    tags.put("operation", "initiate");

    OmMultipartInfo mpuInfo1 = bucket.initiateMultipartUpload(mpuKey1);
    OmMultipartInfo mpuInfo2 = bucket.initiateMultipartUpload(mpuKey2, getDefaultReplication(), metadata, tags);

    String snap2 = "snap-after-initiate-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName, testBucketName, snap1, snap2);
    assertEquals(0, diff.getDiffList().size());

    OzoneMultipartUploadList mpuList = bucket.
        listMultipartUploads("", null, null, 100);
    assertEquals(2, mpuList.getUploads().size());

    bucket.abortMultipartUpload(mpuKey1, mpuInfo1.getUploadID());
    bucket.abortMultipartUpload(mpuKey2, mpuInfo2.getUploadID());
  }

  @Test
  public void testSnapshotDiffWithCreateMultipartKeys() throws Exception {
    String testVolumeName = "vol-create-part-" + counter.incrementAndGet();
    String testBucketName = "bucket-create-part-" + counter.incrementAndGet();

    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String regularPartsKey = "regular-parts-key" + counter.incrementAndGet();
    String streamPartsKey = "stream-parts-key" + counter.incrementAndGet();
    String mixedPartsKey = "mixed-parts-key" + counter.incrementAndGet();

    OmMultipartInfo regularMpuInfo = bucket.initiateMultipartUpload(regularPartsKey, getDefaultReplication());
    OmMultipartInfo streamMpuInfo = bucket.initiateMultipartUpload(streamPartsKey, getDefaultReplication());
    OmMultipartInfo mixedMpuInfo = bucket.initiateMultipartUpload(mixedPartsKey, getDefaultReplication());

    String snap1 = "snap-before-parts-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap1);

    byte[] regularPart = "regular multipart key".getBytes(UTF_8);
    try (OzoneOutputStream stream = bucket.createMultipartKey(
        regularPartsKey, regularPart.length, 1, regularMpuInfo.getUploadID())) {
      stream.write(regularPart);
    }

    byte[] streamPart = "stream data".getBytes(UTF_8);
    try (OzoneDataStreamOutput streamOut = bucket.createMultipartStreamKey(
        streamPartsKey, streamPart.length, 1, streamMpuInfo.getUploadID())) {
      streamOut.write(streamPart);
    }

    byte[] mixedPart = "mixed data".getBytes(UTF_8);
    try (OzoneOutputStream mixedStream = bucket.createMultipartKey(
        mixedPartsKey, mixedPart.length, 1, mixedMpuInfo.getUploadID())) {
      mixedStream.write(mixedPart);
    }

    assertEquals(1,
        bucket.listParts(regularPartsKey, regularMpuInfo.getUploadID(), 0, 100).getPartInfoList().size());
    assertEquals(1,
        bucket.listParts(streamPartsKey, streamMpuInfo.getUploadID(), 0, 100).getPartInfoList().size());
    assertEquals(1,
        bucket.listParts(mixedPartsKey, mixedMpuInfo.getUploadID(), 0, 100).getPartInfoList().size());

    String snap2 = "snap-after-parts-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName, testBucketName, snap1, snap2);
    assertEquals(Collections.emptyList(), diff.getDiffList());

    bucket.abortMultipartUpload(regularPartsKey, regularMpuInfo.getUploadID());
    bucket.abortMultipartUpload(streamPartsKey, streamMpuInfo.getUploadID());
    bucket.abortMultipartUpload(mixedPartsKey, mixedMpuInfo.getUploadID());
  }

  @Test
  public void testSnapshotDiffWithAbortMultipartUpload() throws Exception {
    String testVolumeName = "vol-abort-" + counter.incrementAndGet();
    String testBucketName = "bucket-abort-" + counter.incrementAndGet();

    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String snap1 = "snap-before-abort-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap1);

    // Abort immediately after initiate
    String immediateAbortKey = "immediate-abort-" + counter.incrementAndGet();
    OmMultipartInfo immediateInfo = bucket.initiateMultipartUpload(immediateAbortKey, getDefaultReplication());
    bucket.abortMultipartUpload(immediateAbortKey, immediateInfo.getUploadID());

    // Abort after uploading some parts
    String partialAbortKey = "partial-abort-" + counter.incrementAndGet();
    OmMultipartInfo partialInfo = bucket.initiateMultipartUpload(partialAbortKey, getDefaultReplication());

    byte[] part1Data = "Part 1 - will be aborted".getBytes(UTF_8);
    byte[] part2Data = "Part 2 - will be aborted".getBytes(UTF_8);
    byte[] part3Data = "Part 3 - stream part, will be aborted".getBytes(UTF_8);

    try (OzoneOutputStream part1Stream = bucket.createMultipartKey(
        partialAbortKey, part1Data.length, 1, partialInfo.getUploadID())) {
      part1Stream.write(part1Data);
    }
    try (OzoneOutputStream part2Stream = bucket.createMultipartKey(
        partialAbortKey, part2Data.length, 2, partialInfo.getUploadID())) {
      part2Stream.write(part2Data);
    }
    try (OzoneDataStreamOutput part3Stream = bucket.createMultipartStreamKey(
        partialAbortKey, part3Data.length, 3, partialInfo.getUploadID())) {
      part3Stream.write(part3Data);
    }

    OzoneMultipartUploadPartListParts partsList = bucket.listParts(
        partialAbortKey, partialInfo.getUploadID(), 0, 100);
    assertEquals(3, partsList.getPartInfoList().size());

    bucket.abortMultipartUpload(partialAbortKey, partialInfo.getUploadID());

    // Multiple aborts in same snapshot window
    String multiAbortKey1 = "multi-abort-1-" + counter.incrementAndGet();
    String multiAbortKey2 = "multi-abort-2-" + counter.incrementAndGet();

    OmMultipartInfo multiInfo1 = bucket.initiateMultipartUpload(multiAbortKey1, getDefaultReplication());
    OmMultipartInfo multiInfo2 = bucket.initiateMultipartUpload(multiAbortKey2, getDefaultReplication());

    try (OzoneOutputStream stream = bucket.createMultipartKey(
        multiAbortKey1, part1Data.length, 1, multiInfo1.getUploadID())) {
      stream.write(part1Data);
    }
    try (OzoneDataStreamOutput stream = bucket.createMultipartStreamKey(
        multiAbortKey2, part2Data.length, 1, multiInfo2.getUploadID())) {
      stream.write(part2Data);
    }

    bucket.abortMultipartUpload(multiAbortKey1, multiInfo1.getUploadID());
    bucket.abortMultipartUpload(multiAbortKey2, multiInfo2.getUploadID());

    String snap2 = "snap-after-aborts-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName, testBucketName, snap1, snap2);
    assertEquals(Collections.emptyList(), diff.getDiffList());

    OzoneMultipartUploadList finalMpuList = bucket.listMultipartUploads("", null, null, 100);
    assertEquals(0, finalMpuList.getUploads().size());
  }

  @Test
  public void testSnapshotDiffWithCompleteInvisibleMPULifecycle() throws Exception {
    String testVolumeName = "vol-invisible-" + counter.incrementAndGet();
    String testBucketName = "bucket-invisible-" + counter.incrementAndGet();

    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String regularKey1 = "regular-before-" + counter.incrementAndGet();
    String regularKey2 = "regular-after-" + counter.incrementAndGet();
    createFileKey(bucket, regularKey1);

    String snap1 = "snap-invisible-baseline-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap1);

    String mpuKey1 = "invisible-mpu-1-" + counter.incrementAndGet();
    String mpuKey2 = "invisible-mpu-2-" + counter.incrementAndGet();
    String mpuKey3 = "invisible-mpu-3-" + counter.incrementAndGet();

    OmMultipartInfo mpuInfo1 = bucket.initiateMultipartUpload(mpuKey1, getDefaultReplication());
    OmMultipartInfo mpuInfo2 = bucket.initiateMultipartUpload(mpuKey2, getDefaultReplication());
    OmMultipartInfo mpuInfo3 = bucket.initiateMultipartUpload(mpuKey3, getDefaultReplication());

    byte[] regularData1 = "Regular multipart data 1".getBytes(UTF_8);
    byte[] regularData2 = "Regular multipart data 2".getBytes(UTF_8);

    try (OzoneOutputStream stream = bucket.createMultipartKey(
        mpuKey1, regularData1.length, 1, mpuInfo1.getUploadID())) {
      stream.write(regularData1);
    }
    try (OzoneOutputStream stream = bucket.createMultipartKey(
        mpuKey1, regularData2.length, 2, mpuInfo1.getUploadID())) {
      stream.write(regularData2);
    }

    byte[] streamData1 = "Stream multipart data 1".getBytes(UTF_8);
    byte[] streamData2 = "Stream multipart data 2".getBytes(UTF_8);

    try (OzoneDataStreamOutput stream = bucket.createMultipartStreamKey(
        mpuKey2, streamData1.length, 1, mpuInfo2.getUploadID())) {
      stream.write(streamData1);
    }
    try (OzoneDataStreamOutput stream = bucket.createMultipartStreamKey(
        mpuKey2, streamData2.length, 2, mpuInfo2.getUploadID())) {
      stream.write(streamData2);
    }


    byte[] mixedRegular = "Mixed - regular part".getBytes(UTF_8);
    byte[] mixedStream = "Mixed - stream part".getBytes(UTF_8);

    try (OzoneOutputStream stream = bucket.createMultipartKey(
        mpuKey3, mixedRegular.length, 1, mpuInfo3.getUploadID())) {
      stream.write(mixedRegular);
    }
    try (OzoneDataStreamOutput stream = bucket.createMultipartStreamKey(
        mpuKey3, mixedStream.length, 2, mpuInfo3.getUploadID())) {
      stream.write(mixedStream);
    }

    assertEquals(2,
        bucket.listParts(mpuKey1, mpuInfo1.getUploadID(), 0, 100).getPartInfoList().size());
    assertEquals(2,
        bucket.listParts(mpuKey2, mpuInfo2.getUploadID(), 0, 100).getPartInfoList().size());
    assertEquals(2,
        bucket.listParts(mpuKey3, mpuInfo3.getUploadID(), 0, 100).getPartInfoList().size());

    createFileKey(bucket, regularKey2);

    bucket.abortMultipartUpload(mpuKey1, mpuInfo1.getUploadID());
    bucket.abortMultipartUpload(mpuKey2, mpuInfo2.getUploadID());
    bucket.abortMultipartUpload(mpuKey3, mpuInfo3.getUploadID());

    String snap2 = "snap-after-invisible-lifecycle-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName, testBucketName, snap1, snap2);

    List<SnapshotDiffReport.DiffReportEntry> expectedDiffs = Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.CREATE, regularKey2));
    assertEquals(expectedDiffs, diff.getDiffList());

    OzoneMultipartUploadList finalMpuList = bucket.listMultipartUploads("", null, null, 100);
    assertEquals(0, finalMpuList.getUploads().size());

    assertDoesNotThrow(() -> bucket.getKey(regularKey1));
    assertDoesNotThrow(() -> bucket.getKey(regularKey2));

    assertThrows(OMException.class, () -> bucket.getKey(mpuKey1));
    assertThrows(OMException.class, () -> bucket.getKey(mpuKey2));
    assertThrows(OMException.class, () -> bucket.getKey(mpuKey3));
  }

  private void completeSinglePartMPU(OzoneBucket bucket, String keyName, String data) throws IOException {
    OmMultipartInfo mpuInfo = bucket.initiateMultipartUpload(keyName, getDefaultReplication());
    String uploadId = mpuInfo.getUploadID();

    byte[] partData = createLargePartData(data, MIN_PART_SIZE);
    OzoneOutputStream partStream = bucket.createMultipartKey(keyName, partData.length, 1, uploadId);
    partStream.write(partData);
    partStream.close();

    OzoneMultipartUploadPartListParts partsList = bucket.listParts(keyName, uploadId, 0, 100);
    String realETag = partsList.getPartInfoList().get(0).getPartName();

    Map<Integer, String> partsMap = Collections.singletonMap(1, realETag);
    bucket.completeMultipartUpload(keyName, uploadId, partsMap);
  }

  private void completeMultiplePartMPU(
      OzoneBucket bucket, String keyName, List<String> partDataList) throws IOException {
    OmMultipartInfo mpuInfo = bucket.initiateMultipartUpload(keyName, getDefaultReplication());
    String uploadId = mpuInfo.getUploadID();

    for (int i = 0; i < partDataList.size(); i++) {
      int partNum = i + 1;
      byte[] partData = createLargePartData(partDataList.get(i), MIN_PART_SIZE);

      try (OzoneOutputStream partStream = bucket.createMultipartKey(
          keyName, partData.length, partNum, uploadId)) {
        partStream.write(partData);
      }
    }

    OzoneMultipartUploadPartListParts partsList = bucket.
        listParts(keyName, uploadId, 0, partDataList.size());
    Map<Integer, String> partsMap = new HashMap<>();

    for (OzoneMultipartUploadPartListParts.PartInfo partInfo : partsList.getPartInfoList()) {
      partsMap.put(partInfo.getPartNumber(), partInfo.getPartName());
    }

    bucket.completeMultipartUpload(keyName, uploadId, partsMap);
  }

  private void completeMixedPartMPU(
      OzoneBucket bucket, String keyName, String regularData, String streamData) throws IOException {
    OmMultipartInfo mpuInfo = bucket.initiateMultipartUpload(keyName, getDefaultReplication());
    String uploadId = mpuInfo.getUploadID();

    byte[] part1Data = createLargePartData(regularData, MIN_PART_SIZE);
    try (OzoneOutputStream partStream = bucket.createMultipartKey(
        keyName, part1Data.length, 1, uploadId)) {
      partStream.write(part1Data);
    }

    byte[] part2Data = createLargePartData(streamData, MIN_PART_SIZE);
    try (OzoneDataStreamOutput partStream = bucket.createMultipartStreamKey(
        keyName, part2Data.length, 2, uploadId)) {
      partStream.write(part2Data);
    }

    OzoneMultipartUploadPartListParts partsList = bucket.listParts(keyName, uploadId, 0, 2);
    Map<Integer, String> partsMap = new HashMap<>();

    for (OzoneMultipartUploadPartListParts.PartInfo partInfo : partsList.getPartInfoList()) {
      partsMap.put(partInfo.getPartNumber(), partInfo.getPartName());
    }

    bucket.completeMultipartUpload(keyName, uploadId, partsMap);
  }

  private void completeMPUWithReplication(
      OzoneBucket bucket, String keyName, ReplicationConfig replicationConfig) throws IOException {
    OmMultipartInfo mpuInfo;
    if (replicationConfig != null) {
      mpuInfo = bucket.initiateMultipartUpload(keyName, replicationConfig);
    } else {
      mpuInfo = bucket.initiateMultipartUpload(keyName);
    }

    String uploadId = mpuInfo.getUploadID();

    byte[] partData = createLargePartData("Replication test data for " + keyName, MIN_PART_SIZE);
    try (OzoneOutputStream partStream = bucket.createMultipartKey(
        keyName, partData.length, 1, uploadId)) {
      partStream.write(partData);
    }

    OzoneMultipartUploadPartListParts partsList = bucket.listParts(keyName, uploadId, 0, 1);
    String realETag = partsList.getPartInfoList().get(0).getPartName();

    Map<Integer, String> partsMap = Collections.singletonMap(1, realETag);
    bucket.completeMultipartUpload(keyName, uploadId, partsMap);
  }

  private void completeMPUWithMetadata(OzoneBucket bucket, String keyName,
                                       Map<String, String> metadata, Map<String, String> tags) throws IOException {

    OmMultipartInfo mpuInfo = bucket.initiateMultipartUpload(
        keyName, getDefaultReplication(), metadata, tags);
    String uploadId = mpuInfo.getUploadID();

    byte[] partData = createLargePartData("MPU with metadata and tags", MIN_PART_SIZE);
    OzoneOutputStream partStream = bucket.createMultipartKey(keyName, partData.length, 1, uploadId);
    partStream.write(partData);
    partStream.close();

    OzoneMultipartUploadPartListParts partsList = bucket.listParts(keyName, uploadId, 0, 100);
    String realETag = partsList.getPartInfoList().get(0).getPartName();

    Map<Integer, String> partsMap = Collections.singletonMap(1, realETag);
    bucket.completeMultipartUpload(keyName, uploadId, partsMap);
  }

  private byte[] createLargePartData(String baseContent, int targetSize) {
    StringBuilder sb = new StringBuilder();
    sb.append(baseContent);

    String padding = " - padding data";
    while (sb.length() < targetSize) {
      sb.append(padding);
      if (sb.length() + padding.length() > targetSize) {
        int remaining = targetSize - sb.length();
        sb.append(padding.substring(0, Math.min(remaining, padding.length())));
      }
    }

    String result = sb.toString();
    if (result.length() > targetSize) {
      result = result.substring(0, targetSize);
    }

    return result.getBytes(UTF_8);
  }

  @Test
  public void testSnapshotDiffMPUCreateNewKey() throws Exception {
    String testVolumeName = "vol-create-new-" + counter.incrementAndGet();
    String testBucketName = "bucket-create-new-" + counter.incrementAndGet();

    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String existingKey = "existing-baseline-" + counter.incrementAndGet();
    createFileKey(bucket, existingKey);

    String snap1 = "snap-before-create-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap1);

    String mpuKey1 = "multi-mpu-1-" + counter.incrementAndGet();
    String mpuKey2 = "multi-mpu-2-" + counter.incrementAndGet();
    String mpuKey3 = "multi-mpu-3-" + counter.incrementAndGet();

    completeSinglePartMPU(bucket, mpuKey1, "Single part MPU data");

    completeMultiplePartMPU(bucket, mpuKey2,
        Arrays.asList("Multi part 1", "Multi part 2", "Multi part 3"));

    completeMixedPartMPU(bucket, mpuKey3,
        "Mixed regular part", "Mixed stream part");

    String regularKey = "regular-compare-" + counter.incrementAndGet();
    createFileKey(bucket, regularKey);

    String snap2 = "snap-multiple-final-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName, testBucketName, snap1, snap2);

    List<SnapshotDiffReport.DiffReportEntry> expectedDiffs = Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.CREATE, mpuKey1),
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.CREATE, mpuKey2),
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.CREATE, mpuKey3),
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.CREATE, regularKey));
    assertEquals(expectedDiffs, diff.getDiffList());
  }

  @Test
  public void testSnapshotDiffMPUCreateMultipleKeys() throws Exception {
    String testVolumeName = "vol-create-multiple-" + counter.incrementAndGet();
    String testBucketName = "bucket-create-multiple-" + counter.incrementAndGet();

    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String snap1 = "snap-multiple-baseline-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap1);

    String mpuKey1 = "multi-mpu-1-" + counter.incrementAndGet();
    String mpuKey2 = "multi-mpu-2-" + counter.incrementAndGet();
    String mpuKey3 = "multi-mpu-3-" + counter.incrementAndGet();

    completeSinglePartMPU(bucket, mpuKey1, "Single part MPU data");
    completeMultiplePartMPU(bucket, mpuKey2,
        Arrays.asList("Multi part 1", "Multi part 2", "Multi part 3"));
    completeMixedPartMPU(bucket, mpuKey3,
        "Mixed regular part", "Mixed stream part");

    String regularKey = "regular-compare-" + counter.incrementAndGet();
    createFileKey(bucket, regularKey);

    String snap2 = "snap-multiple-final-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName, testBucketName, snap1, snap2);

    List<SnapshotDiffReport.DiffReportEntry> expectedDiffs = Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.CREATE, mpuKey1),
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.CREATE, mpuKey2),
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.CREATE, mpuKey3),
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.CREATE, regularKey));
    assertEquals(expectedDiffs, diff.getDiffList());
  }

  @Test
  public void testSnapshotDiffMPUCreateWithMetadataAndTags() throws Exception {
    String testVolumeName = "vol-create-meta-" + counter.incrementAndGet();
    String testBucketName = "bucket-create-meta-" + counter.incrementAndGet();

    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String snap1 = "snap-meta-baseline-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap1);

    String mpuKeyWithMeta = "mpu-with-metadata-" + counter.incrementAndGet();

    Map<String, String> richMetadata = new HashMap<>();
    richMetadata.put("content-type", "application/octet-stream");
    richMetadata.put("content-encoding", "gzip");
    richMetadata.put("cache-control", "no-cache");
    richMetadata.put("custom-header", "test-value");

    Map<String, String> richTags = new HashMap<>();
    richTags.put("project", "ozone-testing");
    richTags.put("environment", "integration");
    richTags.put("cost-center", "engineering");
    richTags.put("backup-policy", "daily");

    completeMPUWithMetadata(bucket, mpuKeyWithMeta, richMetadata, richTags);

    String snap2 = "snap-meta-final-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName, testBucketName, snap1, snap2);

    List<SnapshotDiffReport.DiffReportEntry> expectedDiffs = Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.CREATE, mpuKeyWithMeta));
    assertEquals(expectedDiffs, diff.getDiffList());
  }

  @Test
  public void testSnapshotDiffMPUCreateWithDifferentReplication() throws Exception {
    String testVolumeName = "vol-create-repl-" + counter.incrementAndGet();
    String testBucketName = "bucket-create-repl-" + counter.incrementAndGet();

    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String snap1 = "snap-repl-baseline-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap1);

    String standaloneKey = "standalone-mpu-" + counter.incrementAndGet();
    String defaultKey = "default-mpu-" + counter.incrementAndGet();

    ReplicationConfig standaloneConfig = StandaloneReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.ONE);
    completeMPUWithReplication(bucket, standaloneKey, standaloneConfig);
    completeMPUWithReplication(bucket, defaultKey, null);

    String snap2 = "snap-repl-final-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName, testBucketName, snap1, snap2);

    List<SnapshotDiffReport.DiffReportEntry> expectedDiffs = Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.CREATE, standaloneKey),
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.CREATE, defaultKey));
    assertEquals(expectedDiffs, diff.getDiffList());
  }

  @Test
  public void testSnapshotDiffMPUModifyExistingKey() throws Exception {
    String testVolumeName = "vol-modify-existing-" + counter.incrementAndGet();
    String testBucketName = "bucket-modify-existing-" + counter.incrementAndGet();

    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String existingKey = "existing-key-" + counter.incrementAndGet();
    createFileKey(bucket, existingKey);

    String snap1 = "snap-before-modify-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap1);

    completeSinglePartMPU(bucket, existingKey, "MPU overwritten content");

    String snap2 = "snap-after-modify-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName, testBucketName, snap1, snap2);

    List<SnapshotDiffReport.DiffReportEntry> expectedDiffs = Collections.singletonList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.MODIFY, existingKey));
    assertEquals(expectedDiffs, diff.getDiffList());
  }

  @Test
  public void testSnapshotDiffMPUModifyMultipleKeys() throws Exception {
    String testVolumeName = "vol-modify-multiple-" + counter.incrementAndGet();
    String testBucketName = "bucket-modify-multiple-" + counter.incrementAndGet();

    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String existingKey1 = "existing-1-" + counter.incrementAndGet();
    String existingKey2 = "existing-2-" + counter.incrementAndGet();
    String existingKey3 = "existing-3-" + counter.incrementAndGet();
    String unchangedKey = "unchanged-" + counter.incrementAndGet();

    createFileKey(bucket, existingKey1);
    createFileKey(bucket, existingKey2);
    createFileKey(bucket, existingKey3);
    createFileKey(bucket, unchangedKey);

    String snap1 = "snap-modify-multiple-baseline-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap1);

    completeSinglePartMPU(bucket, existingKey1, "Single part overwrite");
    completeMultiplePartMPU(bucket, existingKey2,
        Arrays.asList("Multi part overwrite 1", "Multi part overwrite 2"));
    completeMixedPartMPU(bucket, existingKey3,
        "Mixed regular overwrite", "Mixed stream overwrite");

    String snap2 = "snap-modify-multiple-final-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName, testBucketName, snap1, snap2);

    List<SnapshotDiffReport.DiffReportEntry> expectedDiffs = Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.MODIFY, existingKey1),
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.MODIFY, existingKey2),
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.MODIFY, existingKey3));
    assertEquals(expectedDiffs, diff.getDiffList());
  }

  @Test
  public void testSnapshotDiffMPUModifyWithMetadataChange() throws Exception {
    String testVolumeName = "vol-modify-meta-" + counter.incrementAndGet();
    String testBucketName = "bucket-modify-meta-" + counter.incrementAndGet();

    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String existingKey = "existing-meta-key-" + counter.incrementAndGet();
    createFileKey(bucket, existingKey);

    Map<String, String> originalTags = new HashMap<>();
    originalTags.put("version", "1.0");
    originalTags.put("environment", "test");
    bucket.putObjectTagging(existingKey, originalTags);

    String snap1 = "snap-modify-meta-baseline-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap1);

    Map<String, String> newMetadata = new HashMap<>();
    newMetadata.put("content-type", "application/json");
    newMetadata.put("updated-by", "mpu-test");

    Map<String, String> newTags = new HashMap<>();
    newTags.put("version", "2.0");
    newTags.put("environment", "updated");
    newTags.put("method", "mpu-overwrite");

    completeMPUWithMetadata(bucket, existingKey, newMetadata, newTags);

    String snap2 = "snap-modify-meta-final-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName, testBucketName, snap1, snap2);

    List<SnapshotDiffReport.DiffReportEntry> expectedDiffs = Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.MODIFY, existingKey));
    assertEquals(expectedDiffs, diff.getDiffList());
  }

  @Test
  public void testSnapshotDiffMPUMixedCreateAndModify() throws Exception {
    String testVolumeName = "vol-mixed-ops-" + counter.incrementAndGet();
    String testBucketName = "bucket-mixed-ops-" + counter.incrementAndGet();

    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String existingKey1 = "existing-1-" + counter.incrementAndGet();
    String existingKey2 = "existing-2-" + counter.incrementAndGet();
    String unchangedKey = "unchanged-" + counter.incrementAndGet();

    createFileKey(bucket, existingKey1);
    createFileKey(bucket, existingKey2);
    createFileKey(bucket, unchangedKey);

    String snap1 = "snap-mixed-baseline-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap1);

    String newKey1 = "new-mpu-1-" + counter.incrementAndGet();
    String newKey2 = "new-mpu-2-" + counter.incrementAndGet();

    completeSinglePartMPU(bucket, newKey1, "New key via single part MPU");
    completeMixedPartMPU(bucket, newKey2, "New key regular part", "New key stream part");

    completeSinglePartMPU(bucket, existingKey1, "Modified via single part MPU");
    completeMultiplePartMPU(bucket, existingKey2,
        Arrays.asList("Modified part 1", "Modified part 2"));

    String regularNewKey = "regular-new-" + counter.incrementAndGet();
    createFileKey(bucket, regularNewKey);

    String snap2 = "snap-mixed-final-" + counter.incrementAndGet();
    createSnapshot(testVolumeName, testBucketName, snap2);

    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName, testBucketName, snap1, snap2);

    List<SnapshotDiffReport.DiffReportEntry> expectedDiffs = Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.CREATE, newKey1),
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.CREATE, newKey2),
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.CREATE, regularNewKey),
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.MODIFY, existingKey1),
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.MODIFY, existingKey2));
    assertEquals(expectedDiffs, diff.getDiffList());
  }
}
