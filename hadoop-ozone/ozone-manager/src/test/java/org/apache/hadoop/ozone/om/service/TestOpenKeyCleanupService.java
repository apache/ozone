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

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_LEASE_HARD_LIMIT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.ExpiredOpenKeys;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(OrderAnnotation.class)
class TestOpenKeyCleanupService {
  private OzoneManagerProtocol writeClient;
  private OzoneManager om;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestOpenKeyCleanupService.class);

  private static final int SERVICE_INTERVAL = 20;
  private static final int EXPIRE_THRESHOLD_MS = 140;
  private static final Duration EXPIRE_THRESHOLD =
      Duration.ofMillis(EXPIRE_THRESHOLD_MS);
  private static final int WAIT_TIME = (int) Duration.ofSeconds(10).toMillis();
  private static final int NUM_MPU_PARTS = 5;
  private KeyManager keyManager;
  private OMMetadataManager omMetadataManager;

  @BeforeAll
  void setup(@TempDir Path tempDir) throws Exception {
    ExitUtils.disableSystemExit();

    OzoneConfiguration conf = new OzoneConfiguration();
    System.setProperty(DBConfigFromFile.CONFIG_DIR, "/");
    ServerUtils.setOzoneMetaDirPath(conf, tempDir.toString());
    conf.setTimeDuration(OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_INTERVAL,
        SERVICE_INTERVAL, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD,
        EXPIRE_THRESHOLD_MS, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_OM_LEASE_HARD_LIMIT,
        EXPIRE_THRESHOLD_MS, TimeUnit.MILLISECONDS);
    conf.set(OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT, "0s");
    conf.setBoolean(OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED, true);
    conf.setBoolean("ozone.client.hbase.enhancements.allowed", true);
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
    conf.setQuietMode(false);
    OmTestManagers omTestManagers = new OmTestManagers(conf);
    keyManager = omTestManagers.getKeyManager();
    omMetadataManager = omTestManagers.getMetadataManager();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();
  }

  @AfterAll
  void cleanup() {
    if (om.stop()) {
      om.join();
    }
  }

  /**
   * In this test, we create a bunch of keys and delete them. Then we start the
   * KeyDeletingService and pass a SCMClient which does not fail. We make sure
   * that all the keys that we deleted is picked up and deleted by
   * OzoneManager.
   *
   * @throws IOException - on Failure.
   */
  @ParameterizedTest
  @CsvSource({
      "9, 0, true",
      "0, 8, true",
      "6, 7, true",
      "99, 0, false",
      "0, 88, false",
      "66, 77, false"
  })
  public void testCleanupExpiredOpenKeys(
      int numDEFKeys, int numFSOKeys, boolean hsync) throws Exception {
    LOG.info("numDEFKeys={}, numFSOKeys={}, hsync? {}",
        numDEFKeys, numFSOKeys, hsync);

    OpenKeyCleanupService openKeyCleanupService =
        (OpenKeyCleanupService) keyManager.getOpenKeyCleanupService();

    openKeyCleanupService.suspend();
    // wait for submitted tasks to complete
    Thread.sleep(SERVICE_INTERVAL);
    final long oldkeyCount = openKeyCleanupService.getSubmittedOpenKeyCount();
    LOG.info("oldkeyCount={}", oldkeyCount);

    final OMMetrics metrics = om.getMetrics();
    long numKeyHSyncs = metrics.getNumKeyHSyncs();
    long numOpenKeysCleaned = metrics.getNumOpenKeysCleaned();
    long numOpenKeysHSyncCleaned = metrics.getNumOpenKeysHSyncCleaned();
    final int keyCount = numDEFKeys + numFSOKeys;
    createOpenKeys(numDEFKeys, false, BucketLayout.DEFAULT, false, false);
    createOpenKeys(numFSOKeys, hsync, BucketLayout.FILE_SYSTEM_OPTIMIZED, false, false);

    // wait for open keys to expire
    Thread.sleep(EXPIRE_THRESHOLD_MS);

    assertExpiredOpenKeys(numDEFKeys == 0, false, BucketLayout.DEFAULT);
    assertExpiredOpenKeys(numFSOKeys == 0, hsync,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    openKeyCleanupService.resume();

    GenericTestUtils.waitFor(
        () -> openKeyCleanupService.getSubmittedOpenKeyCount() >= oldkeyCount + keyCount,
        SERVICE_INTERVAL, WAIT_TIME);

    waitForOpenKeyCleanup(false, BucketLayout.DEFAULT);
    waitForOpenKeyCleanup(hsync, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    if (hsync) {
      assertAtLeast(numOpenKeysCleaned + numDEFKeys, metrics.getNumOpenKeysCleaned());
      assertAtLeast(numOpenKeysHSyncCleaned + numFSOKeys, metrics.getNumOpenKeysHSyncCleaned());
      assertEquals(numKeyHSyncs + numFSOKeys, metrics.getNumKeyHSyncs());
    } else {
      assertAtLeast(numOpenKeysCleaned + keyCount, metrics.getNumOpenKeysCleaned());
      assertEquals(numOpenKeysHSyncCleaned, metrics.getNumOpenKeysHSyncCleaned());
      assertEquals(numKeyHSyncs, metrics.getNumKeyHSyncs());
    }
  }

  /**
   * In this test, we create a bunch of hsync keys with some keys having recover flag set.
   * OpenKeyCleanupService should commit keys which don't have recovery flag and has expired.
   * Keys with recovery flag and expired should be ignored by OpenKeyCleanupService.
   * @throws IOException - on Failure.
   */
  @Test
  // Run this test first to avoid any lingering keys generated by other tests.
  @Order(1)
  public void testIgnoreExpiredRecoverhsyncKeys() throws Exception {
    OpenKeyCleanupService openKeyCleanupService =
        (OpenKeyCleanupService) keyManager.getOpenKeyCleanupService();

    openKeyCleanupService.suspend();
    // wait for submitted tasks to complete
    Thread.sleep(SERVICE_INTERVAL);
    final long oldkeyCount = openKeyCleanupService.getSubmittedOpenKeyCount();
    LOG.info("oldkeyCount={}", oldkeyCount);
    assertEquals(0, oldkeyCount);

    final OMMetrics metrics = om.getMetrics();
    assertEquals(0, metrics.getNumOpenKeysHSyncCleaned());
    int keyCount = 10;
    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE))
        .setNodes(new ArrayList<>())
        .build();

    when(om.getScmClient().getContainerClient().getContainerWithPipeline(anyLong()))
        .thenReturn(new ContainerWithPipeline(Mockito.mock(ContainerInfo.class), pipeline));

    createOpenKeys(keyCount, true, BucketLayout.FILE_SYSTEM_OPTIMIZED, false, false);
    // create 2 more key and mark recovery flag set
    createOpenKeys(2, true, BucketLayout.FILE_SYSTEM_OPTIMIZED, true, false);

    // wait for open keys to expire
    Thread.sleep(EXPIRE_THRESHOLD_MS);

    // Only 10 keys should be returned after hard limit period, as 2 key is having recovery flag set
    assertEquals(keyCount, getExpiredOpenKeys(true, BucketLayout.FILE_SYSTEM_OPTIMIZED));
    assertExpiredOpenKeys(false, true,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    openKeyCleanupService.resume();

    // 10 keys should be recovered and there should not be any expired key pending
    waitForOpenKeyCleanup(true, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    // 2 keys should still remain in openKey table
    assertEquals(2, getKeyInfo(BucketLayout.FILE_SYSTEM_OPTIMIZED, true).size());
  }

  @Test
  public void testCommitExpiredHsyncKeys() throws Exception {
    OpenKeyCleanupService openKeyCleanupService =
        (OpenKeyCleanupService) keyManager.getOpenKeyCleanupService();

    openKeyCleanupService.suspend();
    // wait for submitted tasks to complete
    Thread.sleep(SERVICE_INTERVAL);

    int keyCount = 10;
    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE))
        .setNodes(new ArrayList<>())
        .build();

    when(om.getScmClient().getContainerClient().getContainerWithPipeline(anyLong()))
        .thenReturn(new ContainerWithPipeline(Mockito.mock(ContainerInfo.class), pipeline));

    // Create 5 keys with directories
    createOpenKeys(keyCount / 2, true, BucketLayout.FILE_SYSTEM_OPTIMIZED, false, true);
    // Create 5 keys without directory
    createOpenKeys(keyCount / 2, true, BucketLayout.FILE_SYSTEM_OPTIMIZED, false, false);

    // wait for open keys to expire
    Thread.sleep(EXPIRE_THRESHOLD_MS);

    // 10 keys should be returned after hard limit period
    assertEquals(keyCount, getExpiredOpenKeys(true, BucketLayout.FILE_SYSTEM_OPTIMIZED));
    assertExpiredOpenKeys(false, true,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    openKeyCleanupService.resume();

    // keys should be recovered and there should not be any expired key pending
    waitForOpenKeyCleanup(true, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    List<OmKeyInfo> listKeyInfo = getKeyInfo(BucketLayout.FILE_SYSTEM_OPTIMIZED, false);
    // Verify keyName and fileName is same after auto commit key.
    listKeyInfo.stream().forEach(key -> assertEquals(key.getKeyName(), key.getFileName()));
  }

  /**
   * In this test, we create a bunch of incomplete MPU keys and try to run
   * openKeyCleanupService on it. We make sure that none of these incomplete
   * MPU keys are actually deleted.
   *
   * @throws IOException - on Failure.
   */
  @ParameterizedTest
  @CsvSource({
      "9, 0",
      "0, 8",
      "6, 7",
  })
  public void testExcludeMPUOpenKeys(
      int numDEFKeys, int numFSOKeys) throws Exception {
    LOG.info("numDEFMpuKeys={}, numFSOMpuKeys={}",
        numDEFKeys, numFSOKeys);

    OpenKeyCleanupService openKeyCleanupService =
        (OpenKeyCleanupService) keyManager.getOpenKeyCleanupService();

    openKeyCleanupService.suspend();
    // wait for submitted tasks to complete
    Thread.sleep(SERVICE_INTERVAL);
    final long oldkeyCount = openKeyCleanupService.getSubmittedOpenKeyCount();
    LOG.info("oldMpuKeyCount={}", oldkeyCount);

    final OMMetrics metrics = om.getMetrics();
    long numKeyHSyncs = metrics.getNumKeyHSyncs();
    long numOpenKeysCleaned = metrics.getNumOpenKeysCleaned();
    long numOpenKeysHSyncCleaned = metrics.getNumOpenKeysHSyncCleaned();
    createIncompleteMPUKeys(numDEFKeys, BucketLayout.DEFAULT, NUM_MPU_PARTS,
        true);
    createIncompleteMPUKeys(numFSOKeys, BucketLayout.FILE_SYSTEM_OPTIMIZED,
        NUM_MPU_PARTS, true);

    // wait for open keys to expire
    Thread.sleep(EXPIRE_THRESHOLD_MS);

    // All MPU open keys should be skipped
    assertExpiredOpenKeys(true, false, BucketLayout.DEFAULT);
    assertExpiredOpenKeys(true, false,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    openKeyCleanupService.resume();
    // wait for openKeyCleanupService to complete at least once
    Thread.sleep(SERVICE_INTERVAL * 2);

    // No expired open keys fetched
    assertEquals(openKeyCleanupService.getSubmittedOpenKeyCount(), oldkeyCount);
    assertExpiredOpenKeys(true, false, BucketLayout.DEFAULT);
    assertExpiredOpenKeys(true, false,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    assertEquals(numKeyHSyncs, metrics.getNumKeyHSyncs());
    assertEquals(numOpenKeysCleaned, metrics.getNumOpenKeysCleaned());
    assertEquals(numOpenKeysHSyncCleaned, metrics.getNumOpenKeysHSyncCleaned());
  }

  /**
   * In this test, we create a bunch of MPU keys with uncommitted parts, then
   * we will start the OpenKeyCleanupService. The OpenKeyCleanupService
   * should only delete the open MPU part keys (not the open MPU key).
   *
   * @throws IOException - on Failure.
   */
  @ParameterizedTest
  @CsvSource({
      "9, 0",
      "0, 8",
      "6, 7",
  })
  public void testCleanupExpiredOpenMPUPartKeys(
      int numDEFKeys, int numFSOKeys) throws Exception {
    LOG.info("numDEFMpuKeys={}, numFSOMpuKeys={}",
        numDEFKeys, numFSOKeys);

    OpenKeyCleanupService openKeyCleanupService =
        (OpenKeyCleanupService) keyManager.getOpenKeyCleanupService();

    openKeyCleanupService.suspend();
    // wait for submitted tasks to complete
    Thread.sleep(SERVICE_INTERVAL);
    final long oldkeyCount = openKeyCleanupService.getSubmittedOpenKeyCount();
    LOG.info("oldMpuKeyCount={},", oldkeyCount);

    final OMMetrics metrics = om.getMetrics();
    long numOpenKeysCleaned = metrics.getNumOpenKeysCleaned();
    final int keyCount = numDEFKeys + numFSOKeys;
    final int partCount = NUM_MPU_PARTS * keyCount;
    createIncompleteMPUKeys(numDEFKeys, BucketLayout.DEFAULT, NUM_MPU_PARTS,
        false);
    createIncompleteMPUKeys(numFSOKeys, BucketLayout.FILE_SYSTEM_OPTIMIZED,
        NUM_MPU_PARTS, false);

    Thread.sleep(EXPIRE_THRESHOLD_MS);

    // Each MPU keys create 1 MPU open key and some MPU open part keys
    // only the MPU open part keys will be deleted
    assertExpiredOpenKeys(numDEFKeys == 0, false,
        BucketLayout.DEFAULT);
    assertExpiredOpenKeys(numFSOKeys == 0, false,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    openKeyCleanupService.resume();

    GenericTestUtils.waitFor(
        () -> openKeyCleanupService.getSubmittedOpenKeyCount() >= oldkeyCount + partCount,
        SERVICE_INTERVAL, WAIT_TIME);

    // No expired MPU parts fetched
    waitForOpenKeyCleanup(false, BucketLayout.DEFAULT);
    waitForOpenKeyCleanup(false, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    assertAtLeast(numOpenKeysCleaned + partCount, metrics.getNumOpenKeysCleaned());
  }

  private static void assertAtLeast(long expectedMinimum, long actual) {
    assertThat(actual).isGreaterThanOrEqualTo(expectedMinimum);
  }

  private void assertExpiredOpenKeys(boolean expectedToEmpty, boolean hsync,
      BucketLayout layout) {
    final int size = getExpiredOpenKeys(hsync, layout);
    assertEquals(expectedToEmpty, size == 0,
        () -> "size=" + size + ", layout=" + layout);
  }

  private int getExpiredOpenKeys(boolean hsync, BucketLayout layout) {
    try {
      final ExpiredOpenKeys expired = keyManager.getExpiredOpenKeys(
          EXPIRE_THRESHOLD, 100, layout, EXPIRE_THRESHOLD);
      return (hsync ? expired.getHsyncKeys() : expired.getOpenKeyBuckets())
          .size();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private List<OmKeyInfo> getKeyInfo(BucketLayout bucketLayout, boolean openKey) {
    List<OmKeyInfo> omKeyInfo = new ArrayList<>();

    Table<String, OmKeyInfo> fileTable;
    if (openKey) {
      fileTable = om.getMetadataManager().getOpenKeyTable(bucketLayout);
    } else {
      fileTable = om.getMetadataManager().getKeyTable(bucketLayout);
    }
    try (Table.KeyValueIterator<String, OmKeyInfo>
             iterator = fileTable.iterator()) {
      while (iterator.hasNext()) {
        omKeyInfo.add(iterator.next().getValue());
      }
    } catch (Exception e) {
    }
    return omKeyInfo;
  }

  void waitForOpenKeyCleanup(boolean hsync, BucketLayout layout)
      throws Exception {
    GenericTestUtils.waitFor(() -> 0 == getExpiredOpenKeys(hsync, layout),
        SERVICE_INTERVAL, WAIT_TIME);
  }

  private void createOpenKeys(int keyCount, boolean hsync,
      BucketLayout bucketLayout, boolean recovery, boolean withDir) throws IOException {
    String volume = UUID.randomUUID().toString();
    String bucket = UUID.randomUUID().toString();
    for (int x = 0; x < keyCount; x++) {
      if (RandomUtils.secure().randomBoolean()) {
        bucket = UUID.randomUUID().toString();
        if (RandomUtils.secure().randomBoolean()) {
          volume = UUID.randomUUID().toString();
        }
      }
      String key = withDir ? "dir1/dir2/" + UUID.randomUUID() : UUID.randomUUID().toString();
      createVolumeAndBucket(volume, bucket, bucketLayout);

      final int numBlocks = RandomUtils.secure().randomInt(1, 3);
      // Create the key
      createOpenKey(volume, bucket, key, numBlocks, hsync, recovery);
    }
  }

  private void createVolumeAndBucket(String volumeName, String bucketName,
      BucketLayout bucketLayout) throws IOException {
    // cheat here, just create a volume and bucket entry so that we can
    // create the keys, we put the same data for key and value since the
    // system does not decode the object
    OMRequestTestUtils.addVolumeToOM(omMetadataManager,
        OmVolumeArgs.newBuilder()
            .setOwnerName("o")
            .setAdminName("a")
            .setVolume(volumeName)
            .build());

    OMRequestTestUtils.addBucketToOM(omMetadataManager,
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketLayout(bucketLayout)
            .build());
  }

  private void createOpenKey(String volumeName, String bucketName,
      String keyName, int numBlocks, boolean hsync, boolean recovery) throws IOException {
    OmKeyArgs keyArg =
        new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setAcls(Collections.emptyList())
            .setReplicationConfig(RatisReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.ONE))
            .setLocationInfoList(new ArrayList<>())
            .setOwnerName(UserGroupInformation.getCurrentUser()
                .getShortUserName())
            .build();

    // Open and write the key without commit it.
    OpenKeySession session = writeClient.openKey(keyArg);
    for (int i = 0; i < numBlocks; i++) {
      keyArg.addLocationInfo(writeClient.allocateBlock(keyArg, session.getId(),
          new ExcludeList()));
    }
    if (hsync) {
      writeClient.hsyncKey(keyArg, session.getId());
      if (recovery) {
        writeClient.recoverLease(volumeName, bucketName, keyName, false);
      }
    }
  }

  private void createIncompleteMPUKeys(int mpuKeyCount,
       BucketLayout bucketLayout, int numParts, boolean arePartsCommitted)
      throws IOException {
    String volume = UUID.randomUUID().toString();
    String bucket = UUID.randomUUID().toString();
    for (int x = 0; x < mpuKeyCount; x++) {
      if (RandomUtils.secure().randomBoolean()) {
        bucket = UUID.randomUUID().toString();
        if (RandomUtils.secure().randomBoolean()) {
          volume = UUID.randomUUID().toString();
        }
      }
      String key = UUID.randomUUID().toString();
      createVolumeAndBucket(volume, bucket, bucketLayout);

      // Create the MPU key
      createIncompleteMPUKey(volume, bucket, key, numParts, arePartsCommitted);
    }
  }

  /**
   * Create inflight multipart upload that are not completed / aborted yet.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @throws IOException
   */
  private void createIncompleteMPUKey(String volumeName, String bucketName,
      String keyName, int numParts, boolean arePartsCommitted)
      throws IOException {
    // Initiate MPU
    OmKeyArgs keyArgs =
        new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setAcls(Collections.emptyList())
            .setReplicationConfig(RatisReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.ONE))
            .setLocationInfoList(new ArrayList<>())
            .setOwnerName(UserGroupInformation.getCurrentUser().getShortUserName())
            .build();

    OmMultipartInfo omMultipartInfo = writeClient.
        initiateMultipartUpload(keyArgs);

    // Commit MPU parts
    for (int i = 1; i <= numParts; i++) {
      OmKeyArgs partKeyArgs =
          new OmKeyArgs.Builder()
              .setVolumeName(volumeName)
              .setBucketName(bucketName)
              .setKeyName(keyName)
              .setIsMultipartKey(true)
              .setMultipartUploadID(omMultipartInfo.getUploadID())
              .setMultipartUploadPartNumber(i)
              .setAcls(Collections.emptyList())
              .setReplicationConfig(RatisReplicationConfig.getInstance(
                  HddsProtos.ReplicationFactor.ONE))
              .setOwnerName(UserGroupInformation.getCurrentUser().getShortUserName())
              .build();

      OpenKeySession openKey = writeClient.openKey(partKeyArgs);

      if (arePartsCommitted) {
        OmKeyArgs commitPartKeyArgs =
            new OmKeyArgs.Builder()
                .setVolumeName(volumeName)
                .setBucketName(bucketName)
                .setKeyName(keyName)
                .setIsMultipartKey(true)
                .setMultipartUploadID(omMultipartInfo.getUploadID())
                .setMultipartUploadPartNumber(i)
                .setAcls(Collections.emptyList())
                .setReplicationConfig(RatisReplicationConfig.getInstance(
                    HddsProtos.ReplicationFactor.ONE))
                .setLocationInfoList(Collections.emptyList())
                .addMetadata(OzoneConsts.ETAG, DigestUtils.md5Hex(UUID.randomUUID()
                    .toString()))
                .build();

        writeClient.commitMultipartUploadPart(commitPartKeyArgs,
            openKey.getId());
      }
    }

    // MPU key is not completed / aborted, so it's still in the
    // multipartInfoTable
  }
}
