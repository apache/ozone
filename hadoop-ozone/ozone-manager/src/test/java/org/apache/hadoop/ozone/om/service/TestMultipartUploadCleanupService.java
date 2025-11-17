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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MPU_CLEANUP_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MPU_EXPIRE_THRESHOLD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MPU_PARTS_CLEANUP_LIMIT_PER_TASK;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.assertj.core.api.Assertions.assertThat;

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
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExpiredMultipartUploadsBucket;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Test Multipart Upload Cleanup Service.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestMultipartUploadCleanupService {
  private OzoneManagerProtocol writeClient;
  private OzoneManager om;

  private static final Duration SERVICE_INTERVAL = Duration.ofMillis(100);
  private static final Duration EXPIRE_THRESHOLD = Duration.ofMillis(200);
  private KeyManager keyManager;
  private OMMetadataManager omMetadataManager;

  @BeforeAll
  void setup(@TempDir Path tempDir) throws Exception {
    ExitUtils.disableSystemExit();

    OzoneConfiguration conf = new OzoneConfiguration();
    System.setProperty(DBConfigFromFile.CONFIG_DIR, "/");
    ServerUtils.setOzoneMetaDirPath(conf, tempDir.toString());
    conf.setTimeDuration(OZONE_OM_MPU_CLEANUP_SERVICE_INTERVAL,
        SERVICE_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_OM_MPU_EXPIRE_THRESHOLD,
        EXPIRE_THRESHOLD.toMillis(), TimeUnit.MILLISECONDS);
    conf.setInt(OZONE_OM_MPU_PARTS_CLEANUP_LIMIT_PER_TASK, 1000);
    conf.setQuietMode(false);
    OmTestManagers omTestManagers = new OmTestManagers(conf);
    keyManager = omTestManagers.getKeyManager();
    omMetadataManager = omTestManagers.getMetadataManager();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();
  }

  @AfterAll
  void cleanup() {
    om.stop();
  }

  /**
   * Create a bunch incomplete/inflight multipart upload info. Then we start
   * the MultipartUploadCleanupService. We make sure that all the multipart
   * upload info is picked up and aborted by OzoneManager.
   */
  @ParameterizedTest
  @CsvSource({
      "99,0",
      "0, 88",
      "66, 77"
  })
  void deletesExpiredUpload(int numDEFKeys, int numFSOKeys) throws Exception {

    MultipartUploadCleanupService multipartUploadCleanupService =
        (MultipartUploadCleanupService)
            keyManager.getMultipartUploadCleanupService();

    multipartUploadCleanupService.suspend();
    // wait for submitted tasks to complete
    Thread.sleep(SERVICE_INTERVAL.toMillis());
    final long oldMpuInfoCount =
        multipartUploadCleanupService.getSubmittedMpuInfoCount();
    final long oldRunCount =
        multipartUploadCleanupService.getRunCount();

    createIncompleteMPUKeys(numDEFKeys, BucketLayout.DEFAULT);
    createIncompleteMPUKeys(numFSOKeys, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    // wait for MPU info to expire
    Thread.sleep(EXPIRE_THRESHOLD.toMillis());

    assertThat(getExpiredMultipartUploads()).isNotEmpty();

    multipartUploadCleanupService.resume();

    // wait for requests to complete
    waitFor(() -> getExpiredMultipartUploads().isEmpty(),
        (int) SERVICE_INTERVAL.toMillis(),
        15 * (int) SERVICE_INTERVAL.toMillis());

    assertThat(multipartUploadCleanupService.getRunCount())
        .isGreaterThan(oldRunCount);
    assertThat(multipartUploadCleanupService.getSubmittedMpuInfoCount())
        .isGreaterThanOrEqualTo(oldMpuInfoCount + numDEFKeys + numFSOKeys);
  }

  private List<ExpiredMultipartUploadsBucket> getExpiredMultipartUploads() {
    try {
      return keyManager.getExpiredMultipartUploads(EXPIRE_THRESHOLD, 10000);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void createIncompleteMPUKeys(int mpuKeyCount,
      BucketLayout bucketLayout) throws IOException {
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

      final int numParts = RandomUtils.secure().randomInt(0, 5);
      // Create the MPU key
      createIncompleteMPUKey(volume, bucket, key, numParts);
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

  /**
   * Create inflight multipart upload that are not completed / aborted yet.
   */
  private void createIncompleteMPUKey(String volumeName, String bucketName,
      String keyName, int numParts) throws IOException {
    // Initiate MPU
    OmKeyArgs keyArgs =
        new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setAcls(Collections.emptyList())
            .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
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
              .setReplicationConfig(
                  StandaloneReplicationConfig.getInstance(ONE))
              .setOwnerName(UserGroupInformation.getCurrentUser().getShortUserName())
              .build();

      OpenKeySession openKey = writeClient.openKey(partKeyArgs);

      OmKeyArgs commitPartKeyArgs =
          new OmKeyArgs.Builder()
              .setVolumeName(volumeName)
              .setBucketName(bucketName)
              .setKeyName(keyName)
              .setIsMultipartKey(true)
              .setMultipartUploadID(omMultipartInfo.getUploadID())
              .setMultipartUploadPartNumber(i)
              .setAcls(Collections.emptyList())
              .addMetadata(OzoneConsts.ETAG,
                  DigestUtils.md5Hex(UUID.randomUUID().toString()))
              .setReplicationConfig(
                  StandaloneReplicationConfig.getInstance(ONE))
              .setLocationInfoList(Collections.emptyList())
              .build();

      writeClient.commitMultipartUploadPart(commitPartKeyArgs, openKey.getId());
    }

    // MPU key is not completed / aborted, so it's still in the
    // multipartInfoTable
  }
}
