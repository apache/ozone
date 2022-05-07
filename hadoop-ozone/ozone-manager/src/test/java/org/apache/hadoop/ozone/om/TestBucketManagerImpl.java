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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;

import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.Assert;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Tests BucketManagerImpl, mocks OMMetadataManager for testing.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestBucketManagerImpl {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OmTestManagers omTestManagers;
  private OzoneManagerProtocol writeClient;
  private OzoneManager om;

  @After
  public void cleanup() throws Exception {
    om = omTestManagers.getOzoneManager();
    om.stop();
  }

  private OzoneConfiguration createNewTestPath() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    return conf;
  }

  private void createSampleVol() throws IOException, AuthenticationException {
    OzoneConfiguration conf = createNewTestPath();
    omTestManagers = new OmTestManagers(conf);
    writeClient = omTestManagers.getWriteClient();

    // This is a simple hack for testing, we just test if the volume via a
    // null check, do not parse the value part. So just write some dummy value.
    OmVolumeArgs args =
        OmVolumeArgs.newBuilder()
            .setVolume("sample-vol")
            .setAdminName("bilbo")
            .setOwnerName("bilbo")
            .build();
    writeClient.createVolume(args);
  }

  @Test
  public void testCreateBucketWithoutVolume() throws Exception {
    thrown.expectMessage("Volume doesn't exist");
    OzoneConfiguration conf = createNewTestPath();
    omTestManagers = new OmTestManagers(conf);
    try {
      writeClient = omTestManagers.getWriteClient();

      OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
          .setVolumeName("sample-vol")
          .setBucketName("bucket-one")
          .build();
      writeClient.createBucket(bucketInfo);
    } catch (OMException omEx) {
      Assert.assertEquals(ResultCodes.VOLUME_NOT_FOUND,
          omEx.getResult());
      throw omEx;
    }
  }

  @Test
  public void testCreateBucket() throws Exception {
    createSampleVol();
    KeyProviderCryptoExtension kmsProvider = omTestManagers.kmsProviderInit();

    String testBekName = "key1";
    String testCipherName = "AES/CTR/NoPadding";

    KeyProvider.Metadata mockMetadata = Mockito.mock(KeyProvider.Metadata
        .class);
    Mockito.when(kmsProvider.getMetadata(testBekName)).thenReturn(mockMetadata);
    Mockito.when(mockMetadata.getCipher()).thenReturn(testCipherName);

    BucketManager bucketManager = omTestManagers.getBucketManager();

    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("sample-vol")
        .setBucketName("bucket-one")
        .setBucketEncryptionKey(new
            BucketEncryptionKeyInfo.Builder().setKeyName("key1").build())
        .build();
    writeClient.createBucket(bucketInfo);
    Assert.assertNotNull(bucketManager.getBucketInfo("sample-vol",
        "bucket-one"));

    OmBucketInfo bucketInfoRead =
        bucketManager.getBucketInfo("sample-vol", "bucket-one");

    Assert.assertTrue(bucketInfoRead.getEncryptionKeyInfo().getKeyName()
        .equals(bucketInfo.getEncryptionKeyInfo().getKeyName()));
  }


  @Test
  public void testCreateEncryptedBucket() throws Exception {
    createSampleVol();

    BucketManager bucketManager = omTestManagers.getBucketManager();
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("sample-vol")
        .setBucketName("bucket-one")
        .build();
    writeClient.createBucket(bucketInfo);
    Assert.assertNotNull(bucketManager.getBucketInfo("sample-vol",
        "bucket-one"));
  }

  @Test
  public void testCreateAlreadyExistingBucket() throws Exception {
    thrown.expectMessage("Bucket already exist");
    createSampleVol();

    try {
      OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
          .setVolumeName("sample-vol")
          .setBucketName("bucket-one")
          .build();
      writeClient.createBucket(bucketInfo);
      writeClient.createBucket(bucketInfo);
    } catch (OMException omEx) {
      Assert.assertEquals(ResultCodes.BUCKET_ALREADY_EXISTS,
          omEx.getResult());
      throw omEx;
    }
  }

  @Test
  public void testGetBucketInfoForInvalidBucket() throws Exception {
    thrown.expectMessage("Bucket not found");

    createSampleVol();
    try {

      BucketManager bucketManager = omTestManagers.getBucketManager();
      bucketManager.getBucketInfo("sample-vol", "bucket-one");
    } catch (OMException omEx) {
      Assert.assertEquals(ResultCodes.BUCKET_NOT_FOUND,
          omEx.getResult());
      throw omEx;
    }
  }

  @Test
  public void testGetBucketInfo() throws Exception {
    final String volumeName = "sample-vol";
    final String bucketName = "bucket-one";

    OzoneConfiguration conf = createNewTestPath();
    omTestManagers = new OmTestManagers(conf);
    writeClient = omTestManagers.getWriteClient();

    OMMetadataManager metaMgr = omTestManagers.getMetadataManager();
    BucketManager bucketManager = omTestManagers.getBucketManager();
    // Check exception thrown when volume does not exist
    try {
      bucketManager.getBucketInfo(volumeName, bucketName);
      Assert.fail("Should have thrown OMException");
    } catch (OMException omEx) {
      Assert.assertEquals("getBucketInfo() should have thrown " +
              "VOLUME_NOT_FOUND as the parent volume is not created!",
          ResultCodes.VOLUME_NOT_FOUND, omEx.getResult());
    }
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
    try {
      bucketManager.getBucketInfo(volumeName, "bucketNotExist");
      Assert.fail("Should have thrown OMException");
    } catch (OMException omEx) {
      Assert.assertEquals("getBucketInfo() should have thrown " +
              "BUCKET_NOT_FOUND as the parent volume exists but bucket " +
              "doesn't!", ResultCodes.BUCKET_NOT_FOUND, omEx.getResult());
    }
    OmBucketInfo result = bucketManager.getBucketInfo(volumeName, bucketName);
    Assert.assertEquals(volumeName, result.getVolumeName());
    Assert.assertEquals(bucketName, result.getBucketName());
    Assert.assertEquals(StorageType.DISK, result.getStorageType());
    Assert.assertFalse(result.getIsVersionEnabled());
  }

  private void createBucket(OMMetadataManager metadataManager,
      OmBucketInfo bucketInfo) throws IOException {
    OMRequestTestUtils.addBucketToOM(metadataManager, bucketInfo);
  }

  @Test
  public void testSetBucketPropertyChangeStorageType() throws Exception {

    createSampleVol();
    OMMetadataManager metaMgr = omTestManagers.getMetadataManager();
    BucketManager bucketManager = omTestManagers.getBucketManager();
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("sample-vol")
        .setBucketName("bucket-one")
        .setStorageType(StorageType.DISK)
        .build();
    createBucket(metaMgr, bucketInfo);
    OmBucketInfo result = bucketManager.getBucketInfo(
        "sample-vol", "bucket-one");
    Assert.assertEquals(StorageType.DISK,
        result.getStorageType());
    OmBucketArgs bucketArgs = OmBucketArgs.newBuilder()
        .setVolumeName("sample-vol")
        .setBucketName("bucket-one")
        .setStorageType(StorageType.SSD)
        .build();
    writeClient.setBucketProperty(bucketArgs);
    OmBucketInfo updatedResult = bucketManager.getBucketInfo(
        "sample-vol", "bucket-one");
    Assert.assertEquals(StorageType.SSD,
        updatedResult.getStorageType());
  }

  @Test
  public void testSetBucketPropertyChangeVersioning() throws Exception {
    createSampleVol();

    BucketManager bucketManager = omTestManagers.getBucketManager();
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("sample-vol")
        .setBucketName("bucket-one")
        .setIsVersionEnabled(false)
        .build();
    writeClient.createBucket(bucketInfo);
    OmBucketInfo result = bucketManager.getBucketInfo(
        "sample-vol", "bucket-one");
    Assert.assertFalse(result.getIsVersionEnabled());
    OmBucketArgs bucketArgs = OmBucketArgs.newBuilder()
        .setVolumeName("sample-vol")
        .setBucketName("bucket-one")
        .setIsVersionEnabled(true)
        .build();
    writeClient.setBucketProperty(bucketArgs);
    OmBucketInfo updatedResult = bucketManager.getBucketInfo(
        "sample-vol", "bucket-one");
    Assert.assertTrue(updatedResult.getIsVersionEnabled());
  }

  @Test
  public void testDeleteBucket() throws Exception {
    thrown.expectMessage("Bucket not found");
    createSampleVol();
    BucketManager bucketManager = omTestManagers.getBucketManager();
    for (int i = 0; i < 5; i++) {
      OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
          .setVolumeName("sample-vol")
          .setBucketName("bucket-" + i)
          .build();
      writeClient.createBucket(bucketInfo);
    }
    for (int i = 0; i < 5; i++) {
      Assert.assertEquals("bucket-" + i,
          bucketManager.getBucketInfo(
              "sample-vol", "bucket-" + i).getBucketName());
    }
    try {
      writeClient.deleteBucket("sample-vol", "bucket-1");
      Assert.assertNotNull(bucketManager.getBucketInfo(
          "sample-vol", "bucket-2"));
    } catch (IOException ex) {
      Assert.fail(ex.getMessage());
    }
    try {
      bucketManager.getBucketInfo("sample-vol", "bucket-1");
    } catch (OMException omEx) {
      Assert.assertEquals(ResultCodes.BUCKET_NOT_FOUND,
          omEx.getResult());
      throw omEx;
    }
  }

  @Test
  public void testDeleteNonEmptyBucket() throws Exception {
    thrown.expectMessage("Bucket is not empty");
    createSampleVol();
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("sample-vol")
        .setBucketName("bucket-one")
        .build();
    writeClient.createBucket(bucketInfo);
    //Create keys in bucket
    OmKeyArgs args1 = new OmKeyArgs.Builder()
            .setVolumeName("sample-vol")
            .setBucketName("bucket-one")
            .setKeyName("key-one")
            .setAcls(Collections.emptyList())
            .setLocationInfoList(new ArrayList<>())
            .setReplicationConfig(
              StandaloneReplicationConfig.getInstance(ReplicationFactor.ONE))
            .build();

    OpenKeySession session1 = writeClient.openKey(args1);
    writeClient.commitKey(args1, session1.getId());

    OmKeyArgs args2 = new OmKeyArgs.Builder()
            .setVolumeName("sample-vol")
            .setBucketName("bucket-one")
            .setKeyName("key-two")
            .setAcls(Collections.emptyList())
            .setLocationInfoList(new ArrayList<>())
            .setReplicationConfig(
              StandaloneReplicationConfig.getInstance(ReplicationFactor.ONE))
            .build();

    OpenKeySession session2 = writeClient.openKey(args2);
    writeClient.commitKey(args2, session2.getId());
    try {
      writeClient.deleteBucket("sample-vol", "bucket-one");
    } catch (OMException omEx) {
      Assert.assertEquals(ResultCodes.BUCKET_NOT_EMPTY,
          omEx.getResult());
      throw omEx;
    }
  }

  private BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }
}
