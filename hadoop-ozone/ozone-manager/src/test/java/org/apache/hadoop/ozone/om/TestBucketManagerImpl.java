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

package org.apache.hadoop.ozone.om;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.OzoneTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * This class tests the Bucket Manager Implementation using Mockito.
 */
@ExtendWith(MockitoExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestBucketManagerImpl extends OzoneTestBase {

  private OmTestManagers omTestManagers;
  private OzoneManagerProtocol writeClient;

  @BeforeAll
  void setup(@TempDir File folder) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    ServerUtils.setOzoneMetaDirPath(conf, folder.toString());

    omTestManagers = new OmTestManagers(conf);
    writeClient = omTestManagers.getWriteClient();
  }

  @AfterAll
  void cleanup() throws Exception {
    omTestManagers.getOzoneManager().stop();
  }

  public String volumeName() {
    return uniqueObjectName();
  }

  private void createSampleVol(String volume) throws IOException {
    // This is a simple hack for testing, we just test if the volume via a
    // null check, do not parse the value part. So just write some dummy value.
    OmVolumeArgs args =
        OmVolumeArgs.newBuilder()
            .setVolume(volume)
            .setAdminName("bilbo")
            .setOwnerName("bilbo")
            .build();
    writeClient.createVolume(args);
  }

  @Test
  void testCreateBucketWithoutVolume() {
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName())
        .setBucketName("bucket-one")
        .build();
    OMException omEx = assertThrows(OMException.class, () -> writeClient.createBucket(bucketInfo));
    assertEquals(ResultCodes.VOLUME_NOT_FOUND, omEx.getResult());
    assertEquals("Volume doesn't exist", omEx.getMessage());
  }

  @Test
  void testCreateEncryptedBucket() throws Exception {
    String volume = volumeName();
    createSampleVol(volume);
    KeyProviderCryptoExtension kmsProvider = omTestManagers.kmsProviderInit();

    String testBekName = "key1";
    String testCipherName = "AES/CTR/NoPadding";

    KeyProvider.Metadata mockMetadata = mock(KeyProvider.Metadata.class);
    when(kmsProvider.getMetadata(testBekName)).thenReturn(mockMetadata);
    when(mockMetadata.getCipher()).thenReturn(testCipherName);

    BucketManager bucketManager = omTestManagers.getBucketManager();

    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volume)
        .setBucketName("bucket-one")
        .setBucketEncryptionKey(new
            BucketEncryptionKeyInfo.Builder().setKeyName("key1").build())
        .build();
    writeClient.createBucket(bucketInfo);
    assertNotNull(bucketManager.getBucketInfo(volume, "bucket-one"));

    OmBucketInfo bucketInfoRead =
        bucketManager.getBucketInfo(volume, "bucket-one");

    assertEquals(bucketInfoRead.getEncryptionKeyInfo().getKeyName(),
        bucketInfo.getEncryptionKeyInfo().getKeyName());
  }

  @Test
  public void testCreateBucket() throws Exception {
    String volume = volumeName();
    createSampleVol(volume);

    BucketManager bucketManager = omTestManagers.getBucketManager();
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volume)
        .setBucketName("bucket-one")
        .build();
    writeClient.createBucket(bucketInfo);
    assertNotNull(bucketManager.getBucketInfo(volume, "bucket-one"));
  }

  @Test
  public void testCreateAlreadyExistingBucket() throws Exception {
    String volume = volumeName();
    createSampleVol(volume);

    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volume)
        .setBucketName("bucket-one")
        .build();
    writeClient.createBucket(bucketInfo);

    OMException omEx = assertThrows(OMException.class,
        () -> writeClient.createBucket(bucketInfo));
    assertEquals(ResultCodes.BUCKET_ALREADY_EXISTS, omEx.getResult());
    assertEquals("Bucket already exist", omEx.getMessage());
  }

  @Test
  public void testGetBucketInfoForInvalidBucket() throws Exception {
    String volume = volumeName();
    createSampleVol(volume);

    BucketManager bucketManager = omTestManagers.getBucketManager();

    OMException exception = assertThrows(OMException.class,
        () -> bucketManager.getBucketInfo(volume, "bucket-one"));
    assertThat(exception.getMessage()).contains("Bucket not found");
    assertEquals(ResultCodes.BUCKET_NOT_FOUND, exception.getResult());
  }

  @Test
  void testGetBucketInfo() throws Exception {
    final String volumeName = volumeName();
    final String bucketName = "bucket-one";

    OMMetadataManager metaMgr = omTestManagers.getMetadataManager();
    BucketManager bucketManager = omTestManagers.getBucketManager();
    // Check exception thrown when volume does not exist
    OMException omEx = assertThrows(OMException.class,
        () -> bucketManager.getBucketInfo(volumeName, bucketName));
    assertEquals(ResultCodes.VOLUME_NOT_FOUND, omEx.getResult(),
        "getBucketInfo() should have thrown " +
            "VOLUME_NOT_FOUND as the parent volume is not created!");

    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setStorageType(StorageType.DISK)
        .setIsVersionEnabled(false)
        .build();
    // Note: the helper method createBucket() in this scope won't create the
    // parent volume DB entry. In order to verify getBucketInfo's behavior, we
    // need to create the volume entry in DB manually.
    OmVolumeArgs args = OmVolumeArgs.newBuilder()
            .setVolume(volumeName)
            .setAdminName("bilbo")
            .setOwnerName("bilbo")
            .build();
    writeClient.createVolume(args);
    // Create bucket
    createBucket(metaMgr, bucketInfo);

    // Check exception thrown when bucket does not exist
    OMException e2 = assertThrows(OMException.class,
        () -> bucketManager.getBucketInfo(volumeName, "bucketNotExist"));
    assertEquals(ResultCodes.BUCKET_NOT_FOUND, e2.getResult());

    OmBucketInfo result = bucketManager.getBucketInfo(volumeName, bucketName);
    assertEquals(volumeName, result.getVolumeName());
    assertEquals(bucketName, result.getBucketName());
    assertEquals(StorageType.DISK, result.getStorageType());
    assertFalse(result.getIsVersionEnabled());
  }

  private void createBucket(OMMetadataManager metadataManager,
      OmBucketInfo bucketInfo) throws IOException {
    OMRequestTestUtils.addBucketToOM(metadataManager, bucketInfo);
  }

  @Test
  public void testSetBucketPropertyChangeStorageType() throws Exception {
    String volume = volumeName();
    createSampleVol(volume);

    OMMetadataManager metaMgr = omTestManagers.getMetadataManager();
    BucketManager bucketManager = omTestManagers.getBucketManager();
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volume)
        .setBucketName("bucket-one")
        .setStorageType(StorageType.DISK)
        .build();
    createBucket(metaMgr, bucketInfo);
    OmBucketInfo result = bucketManager.getBucketInfo(
        volume, "bucket-one");
    assertEquals(StorageType.DISK,
        result.getStorageType());
    OmBucketArgs bucketArgs = OmBucketArgs.newBuilder()
        .setVolumeName(volume)
        .setBucketName("bucket-one")
        .setStorageType(StorageType.SSD)
        .build();
    writeClient.setBucketProperty(bucketArgs);
    OmBucketInfo updatedResult = bucketManager.getBucketInfo(
        volume, "bucket-one");
    assertEquals(StorageType.SSD,
        updatedResult.getStorageType());
  }

  @Test
  public void testSetBucketPropertyChangeVersioning() throws Exception {
    String volume = volumeName();
    createSampleVol(volume);

    BucketManager bucketManager = omTestManagers.getBucketManager();
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volume)
        .setBucketName("bucket-one")
        .setIsVersionEnabled(false)
        .build();
    writeClient.createBucket(bucketInfo);
    OmBucketInfo result = bucketManager.getBucketInfo(
        volume, "bucket-one");
    assertFalse(result.getIsVersionEnabled());
    OmBucketArgs bucketArgs = OmBucketArgs.newBuilder()
        .setVolumeName(volume)
        .setBucketName("bucket-one")
        .setIsVersionEnabled(true)
        .build();
    writeClient.setBucketProperty(bucketArgs);
    OmBucketInfo updatedResult = bucketManager.getBucketInfo(
        volume, "bucket-one");
    assertTrue(updatedResult.getIsVersionEnabled());
  }

  @Test
  void testDeleteBucket() throws Exception {
    String volume = volumeName();
    createSampleVol(volume);

    BucketManager bucketManager = omTestManagers.getBucketManager();
    for (int i = 0; i < 5; i++) {
      OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
          .setVolumeName(volume)
          .setBucketName("bucket-" + i)
          .build();
      writeClient.createBucket(bucketInfo);
    }
    for (int i = 0; i < 5; i++) {
      assertEquals("bucket-" + i,
          bucketManager.getBucketInfo(
              volume, "bucket-" + i).getBucketName());
    }
    writeClient.deleteBucket(volume, "bucket-1");
    assertNotNull(bucketManager.getBucketInfo(volume, "bucket-2"));

    OMException omEx = assertThrows(OMException.class, () -> {
      bucketManager.getBucketInfo(volume, "bucket-1");
    });
    assertEquals(ResultCodes.BUCKET_NOT_FOUND,
          omEx.getResult());
    assertThat(omEx.getMessage()).contains("Bucket not found");
  }

  @Test
  public void testDeleteNonEmptyBucket() throws Exception {
    String volume = volumeName();
    createSampleVol(volume);

    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volume)
        .setBucketName("bucket-one")
        .build();
    writeClient.createBucket(bucketInfo);
    //Create keys in bucket
    OmKeyArgs args1 = new OmKeyArgs.Builder()
            .setVolumeName(volume)
            .setBucketName("bucket-one")
            .setKeyName("key-one")
            .setOwnerName(
                UserGroupInformation.getCurrentUser().getShortUserName())
            .setAcls(Collections.emptyList())
            .setLocationInfoList(new ArrayList<>())
            .setReplicationConfig(
              StandaloneReplicationConfig.getInstance(ReplicationFactor.ONE))
            .build();

    OpenKeySession session1 = writeClient.openKey(args1);
    writeClient.commitKey(args1, session1.getId());

    OmKeyArgs args2 = new OmKeyArgs.Builder()
            .setVolumeName(volume)
            .setBucketName("bucket-one")
            .setKeyName("key-two")
            .setOwnerName(
                UserGroupInformation.getCurrentUser().getShortUserName())
            .setAcls(Collections.emptyList())
            .setLocationInfoList(new ArrayList<>())
            .setReplicationConfig(
              StandaloneReplicationConfig.getInstance(ReplicationFactor.ONE))
            .build();

    OpenKeySession session2 = writeClient.openKey(args2);
    writeClient.commitKey(args2, session2.getId());
    OMException omEx = assertThrows(OMException.class, () -> {
      writeClient.deleteBucket(volume, "bucket-one");
    });
    assertEquals(ResultCodes.BUCKET_NOT_EMPTY,
          omEx.getResult());
    assertEquals("Bucket is not empty", omEx.getMessage());
  }

  @Test
  public void testLinkedBucketResolution() throws Exception {
    String volume = volumeName();
    createSampleVol(volume);

    ECReplicationConfig ecConfig = new ECReplicationConfig(3, 2);
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volume)
        .setBucketName("bucket-one")
        .setDefaultReplicationConfig(
            new DefaultReplicationConfig(
                ecConfig))
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .setQuotaInBytes(42 * 1024)
        .setQuotaInNamespace(24 * 1024)
        .setUsedBytes(10 * 1024)
        .setUsedNamespace(5 * 1024)
        .setStorageType(StorageType.SSD)
        .setIsVersionEnabled(true)
        .addAllMetadata(singletonMap("CustomKey", "CustomValue"))
        .build();
    writeClient.createBucket(bucketInfo);

    OmBucketInfo bucketLinkInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volume)
        .setBucketName("link-one")
        .setSourceVolume(volume)
        .setSourceBucket("bucket-one")
        .build();
    writeClient.createBucket(bucketLinkInfo);

    OmBucketInfo bucketLink2 = OmBucketInfo.newBuilder()
        .setVolumeName(volume)
        .setBucketName("link-two")
        .setSourceVolume(volume)
        .setSourceBucket("link-one")
        .build();
    writeClient.createBucket(bucketLink2);

    OmBucketInfo storedLinkBucket =
        writeClient.getBucketInfo(volume, "link-two");
    assertNotNull(storedLinkBucket.getDefaultReplicationConfig(),
        "Replication config is not set");
    assertEquals(ecConfig,
                        storedLinkBucket
                            .getDefaultReplicationConfig()
                            .getReplicationConfig());

    assertEquals(
        "link-two", storedLinkBucket.getBucketName());
    assertEquals(
        volume, storedLinkBucket.getVolumeName());

    assertEquals(
        "link-one", storedLinkBucket.getSourceBucket());
    assertEquals(
        volume, storedLinkBucket.getSourceVolume());

    assertEquals(
        bucketInfo.getBucketLayout(),
        storedLinkBucket.getBucketLayout());
    assertEquals(
        bucketInfo.getQuotaInBytes(),
        storedLinkBucket.getQuotaInBytes());
    assertEquals(
        bucketInfo.getQuotaInNamespace(),
        storedLinkBucket.getQuotaInNamespace());
    assertEquals(
        bucketInfo.getUsedBytes(),
        storedLinkBucket.getUsedBytes());
    assertEquals(
        bucketInfo.getUsedNamespace(),
        storedLinkBucket.getUsedNamespace());
    assertEquals(
        bucketInfo.getSnapshotUsedBytes(),
        storedLinkBucket.getSnapshotUsedBytes());
    assertEquals(
        bucketInfo.getSnapshotUsedNamespace(),
        storedLinkBucket.getSnapshotUsedNamespace());
    assertEquals(
        bucketInfo.getDefaultReplicationConfig(),
        storedLinkBucket.getDefaultReplicationConfig());
    assertEquals(
        bucketInfo.getMetadata(),
        storedLinkBucket.getMetadata());
    assertEquals(
        bucketInfo.getStorageType(),
        storedLinkBucket.getStorageType());
    assertEquals(
        bucketInfo.getIsVersionEnabled(),
        storedLinkBucket.getIsVersionEnabled());
  }
}
