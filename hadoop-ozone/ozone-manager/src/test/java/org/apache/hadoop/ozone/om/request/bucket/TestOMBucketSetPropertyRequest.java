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

package org.apache.hadoop.ozone.om.request.bucket;

import static org.apache.hadoop.hdds.client.ReplicationType.EC;
import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetBucketPropertyRequest;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.Test;

/**
 * Tests OMBucketSetPropertyRequest class which handles OMSetBucketProperty
 * request.
 */
public class TestOMBucketSetPropertyRequest extends TestBucketRequest {

  private static final String TEST_KEY = "key1";

  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OMRequest omRequest = createSetBucketPropertyRequest(volumeName,
        bucketName, true, Long.MAX_VALUE);

    OMBucketSetPropertyRequest omBucketSetPropertyRequest =
        new OMBucketSetPropertyRequest(omRequest);

    OMRequest preExecuteRequest =
        omBucketSetPropertyRequest.preExecute(ozoneManager);
    // When preExecute() of bucket setProperty,
    // the new modification time is greater than origin one.
    long originModTime = omRequest.getSetBucketPropertyRequest()
        .getModificationTime();
    long newModTime = preExecuteRequest.getSetBucketPropertyRequest()
        .getModificationTime();
    assertThat(newModTime).isGreaterThan(originModTime);

    // As user info gets added.
    assertNotEquals(omRequest, omBucketSetPropertyRequest.preExecute(ozoneManager));
  }

  @Test
  public void preExecutePermissionDeniedWhenAclEnabled() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    when(ozoneManager.getAclsEnabled()).thenReturn(true);

    IAccessAuthorizer authorizer = mock(IAccessAuthorizer.class);
    when(authorizer.isNative()).thenReturn(false);
    when(ozoneManager.getAccessAuthorizer()).thenReturn(authorizer);

    OMRequest originalRequest = createSetBucketPropertyRequest(
        volumeName, bucketName, true, Long.MAX_VALUE);

    OMBucketSetPropertyRequest req = new OMBucketSetPropertyRequest(originalRequest) {
      @Override
      public void checkAcls(OzoneManager ozoneManager, 
          OzoneObj.ResourceType resType,
          OzoneObj.StoreType storeType,
          IAccessAuthorizer.ACLType aclType,
          String vol, String bucket, String key) throws IOException 
                            String vol, String bucket, String key) throws java.io.IOException {
        throw new OMException("denied", OMException.ResultCodes.PERMISSION_DENIED);
      }
    };

    OMException e = assertThrows(OMException.class, () -> req.preExecute(ozoneManager));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, e.getResult());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OMRequest omRequest = createSetBucketPropertyRequest(volumeName,
        bucketName, true, Long.MAX_VALUE);

    // Create with default BucketInfo values
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMBucketSetPropertyRequest omBucketSetPropertyRequest =
        new OMBucketSetPropertyRequest(omRequest);

    OMClientResponse omClientResponse =
        omBucketSetPropertyRequest.validateAndUpdateCache(ozoneManager, 1);

    assertTrue(omMetadataManager.getBucketTable().get(
            omMetadataManager.getBucketKey(volumeName, bucketName))
        .getIsVersionEnabled());

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testNonDefaultLayout() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OMRequest omRequest = createSetBucketPropertyRequest(volumeName,
        bucketName, true, Long.MAX_VALUE);

    // Create FSO Bucket
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMBucketSetPropertyRequest omBucketSetPropertyRequest =
        new OMBucketSetPropertyRequest(omRequest);

    OMClientResponse omClientResponse =
        omBucketSetPropertyRequest.validateAndUpdateCache(ozoneManager, 1);

    assertEquals(BucketLayout.FILE_SYSTEM_OPTIMIZED,
        omMetadataManager.getBucketTable().get(
            omMetadataManager.getBucketKey(volumeName, bucketName))
            .getBucketLayout());

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheFails() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OMRequest omRequest = createSetBucketPropertyRequest(volumeName,
        bucketName, true, Long.MAX_VALUE);

    OMBucketSetPropertyRequest omBucketSetPropertyRequest =
        new OMBucketSetPropertyRequest(omRequest);

    OMClientResponse omClientResponse =
        omBucketSetPropertyRequest.validateAndUpdateCache(ozoneManager, 1);

    assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    assertNull(omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName)));

  }

  private OMRequest createSetBucketPropertyRequest(String volumeName,
      String bucketName, boolean isVersionEnabled, long quotaInBytes) {
    return OMRequest.newBuilder().setSetBucketPropertyRequest(
        SetBucketPropertyRequest.newBuilder().setBucketArgs(
            BucketArgs.newBuilder().setBucketName(bucketName)
                .setVolumeName(volumeName)
                .setQuotaInBytes(quotaInBytes)
                .setQuotaInNamespace(1000L)
                .setIsVersionEnabled(isVersionEnabled).build()))
        .setCmdType(OzoneManagerProtocolProtos.Type.SetBucketProperty)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  @Test
  public void testValidateAndUpdateCacheWithQuota() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeToDB(
        volumeName, omMetadataManager, 10 * GB);
    OMRequestTestUtils.addBucketToDB(
        volumeName, bucketName, omMetadataManager, 8 * GB);
    OMRequest omRequest = createSetBucketPropertyRequest(volumeName,
        bucketName, true, 20 * GB);

    OMBucketSetPropertyRequest omBucketSetPropertyRequest =
        new OMBucketSetPropertyRequest(omRequest);

    LogCapturer logs = LogCapturer.captureLogs(OMBucketSetPropertyRequest.class);

    OMClientResponse omClientResponse = omBucketSetPropertyRequest
        .validateAndUpdateCache(ozoneManager, 1);

    //capture the error log
    assertThat(logs.getOutput()).contains(
        "Setting bucket property failed for bucket");

    assertFalse(omClientResponse.getOMResponse().getSuccess());
    assertEquals(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.QUOTA_EXCEEDED);
    assertThat(omClientResponse.getOMResponse().getMessage()).
        contains("Total buckets quota in this volume " +
            "should not be greater than volume quota");
  }

  @Test
  public void rejectsSettingQuotaOnLink() throws Exception {
    // GIVEN
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String linkName = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    OmBucketInfo.Builder link = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(linkName)
        .setSourceVolume(volumeName)
        .setSourceBucket(bucketName);
    OMRequestTestUtils.addBucketToDB(omMetadataManager, link);
    OMRequest omRequest = createSetBucketPropertyRequest(volumeName,
        linkName, false, 20 * GB);

    OMBucketSetPropertyRequest omBucketSetPropertyRequest =
        new OMBucketSetPropertyRequest(omRequest);

    // WHEN
    OMClientResponse omClientResponse = omBucketSetPropertyRequest
        .validateAndUpdateCache(ozoneManager, 1);

    // THEN
    assertFalse(omClientResponse.getOMResponse().getSuccess());
    assertEquals(
        OzoneManagerProtocolProtos.Status.NOT_SUPPORTED_OPERATION,
        omClientResponse.getOMResponse().getStatus());
    String message = omClientResponse.getOMResponse().getMessage();
    assertThat(message).contains("Cannot set property on link");
  }

  @Test
  public void testSettingRepConfigWithQuota() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeToDB(
        volumeName, omMetadataManager, 10 * GB);
    OMRequestTestUtils.addBucketToDB(
        volumeName, bucketName, omMetadataManager, 8 * GB);

    BucketArgs bucketArgs = OmBucketArgs.newBuilder()
        .setDefaultReplicationConfig(new DefaultReplicationConfig(
            new ECReplicationConfig(3, 2)))
        .setBucketName(bucketName)
        .setVolumeName(volumeName)
        .setIsVersionEnabled(true)
        .build()
        .getProtobuf();

    OMRequest omRequest = OMRequest.newBuilder().setSetBucketPropertyRequest(
        SetBucketPropertyRequest.newBuilder().setBucketArgs(bucketArgs))
        .setCmdType(OzoneManagerProtocolProtos.Type.SetBucketProperty)
        .setClientId(UUID.randomUUID().toString()).build();

    OMBucketSetPropertyRequest omBucketSetPropertyRequest =
        new OMBucketSetPropertyRequest(omRequest);

    OMClientResponse omClientResponse = omBucketSetPropertyRequest
        .validateAndUpdateCache(ozoneManager, 1);

    assertTrue(omClientResponse.getOMResponse().getSuccess());

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo dbBucketInfo =
        omMetadataManager.getBucketTable().get(bucketKey);

    assertEquals(8 * GB, dbBucketInfo.getQuotaInBytes());
    assertEquals(EC, dbBucketInfo.getDefaultReplicationConfig().getType());
  }

  @Test
  public void testSettingRepConfigWithEncryption() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OmBucketInfo.Builder bucketInfo = new OmBucketInfo.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketEncryptionKey(new BucketEncryptionKeyInfo.Builder()
                    .setKeyName(TEST_KEY).build());

    OMRequestTestUtils.addVolumeToDB(
            volumeName, omMetadataManager);
    OMRequestTestUtils.addBucketToDB(
            omMetadataManager, bucketInfo);

    BucketArgs bucketArgs = OmBucketArgs.newBuilder()
            .setDefaultReplicationConfig(new DefaultReplicationConfig(
                    new ECReplicationConfig(3, 2)))
            .setBucketName(bucketName)
            .setVolumeName(volumeName)
            .setIsVersionEnabled(true)
            .build()
            .getProtobuf();

    OMRequest omRequest = OMRequest.newBuilder().setSetBucketPropertyRequest(
            SetBucketPropertyRequest.newBuilder().setBucketArgs(bucketArgs))
            .setCmdType(OzoneManagerProtocolProtos.Type.SetBucketProperty)
            .setClientId(UUID.randomUUID().toString()).build();

    OMBucketSetPropertyRequest omBucketSetPropertyRequest =
            new OMBucketSetPropertyRequest(omRequest);

    OMClientResponse omClientResponse = omBucketSetPropertyRequest
        .validateAndUpdateCache(ozoneManager, 1);

    assertTrue(omClientResponse.getOMResponse().getSuccess());

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo dbBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);

    assertEquals(TEST_KEY, dbBucketInfo.getEncryptionKeyInfo().getKeyName());
    assertEquals(EC, dbBucketInfo.getDefaultReplicationConfig().getType());
  }

  @Test
  public void testSettingQuotaWithEncryptionAndOwner() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OmBucketInfo.Builder bucketInfo = new OmBucketInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setOwner("testUser")
        .setBucketEncryptionKey(new BucketEncryptionKeyInfo.Builder()
            .setKeyName(TEST_KEY).build());

    OMRequestTestUtils.addVolumeToDB(
            volumeName, omMetadataManager);
    OMRequestTestUtils.addBucketToDB(
            omMetadataManager, bucketInfo);

    OMRequest omRequest = createSetBucketPropertyRequest(volumeName,
            bucketName, true, 20 * GB);

    OMBucketSetPropertyRequest omBucketSetPropertyRequest =
            new OMBucketSetPropertyRequest(omRequest);

    OMClientResponse omClientResponse = omBucketSetPropertyRequest
        .validateAndUpdateCache(ozoneManager, 1);

    assertTrue(omClientResponse.getOMResponse().getSuccess());

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo dbBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);

    assertEquals(TEST_KEY, dbBucketInfo.getEncryptionKeyInfo().getKeyName());
    assertEquals(20 * GB, dbBucketInfo.getQuotaInBytes());
    assertEquals(1000L, dbBucketInfo.getQuotaInNamespace());
    assertEquals("testUser", dbBucketInfo.getOwner());
  }

  @Test
  public void testValidateAndUpdateCacheWithQuotaSpaceUsed() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeToDB(
        volumeName, omMetadataManager, 10 * GB);
    OMRequestTestUtils.addBucketToDB(
        volumeName, bucketName, omMetadataManager, 8 * GB);
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    CacheValue<OmBucketInfo> cacheValue = omMetadataManager.getBucketTable()
        .getCacheValue(new CacheKey<>(bucketKey));
    cacheValue.getCacheValue().incrUsedBytes(5 * GB);
    cacheValue.getCacheValue().incrUsedNamespace(10);
    OMRequest omRequest = createSetBucketPropertyRequest(volumeName,
        bucketName, true, GB);

    OMBucketSetPropertyRequest omBucketSetPropertyRequest =
        new OMBucketSetPropertyRequest(omRequest);

    OMClientResponse omClientResponse = omBucketSetPropertyRequest
        .validateAndUpdateCache(ozoneManager, 1);

    assertFalse(omClientResponse.getOMResponse().getSuccess());
    assertEquals(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.QUOTA_ERROR);
    assertThat(omClientResponse.getOMResponse().getMessage()).
        contains("Cannot update bucket quota. Requested spaceQuota less than " +
            "used spaceQuota");
  }

  @Test
  public void testValidateAndUpdateCacheWithQuotaNamespaceUsed()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeToDB(
        volumeName, omMetadataManager, 10 * GB);
    OMRequestTestUtils.addBucketToDB(
        volumeName, bucketName, omMetadataManager, 8 * GB);
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    CacheValue<OmBucketInfo> cacheValue = omMetadataManager.getBucketTable()
        .getCacheValue(new CacheKey<>(bucketKey));
    cacheValue.getCacheValue().incrUsedBytes(5 * GB);
    cacheValue.getCacheValue().incrUsedNamespace(2000);
    OMRequest omRequest = createSetBucketPropertyRequest(volumeName,
        bucketName, true, 9 * GB);

    OMBucketSetPropertyRequest omBucketSetPropertyRequest =
        new OMBucketSetPropertyRequest(omRequest);

    OMClientResponse omClientResponse = omBucketSetPropertyRequest
        .validateAndUpdateCache(ozoneManager, 1);

    assertFalse(omClientResponse.getOMResponse().getSuccess());
    assertEquals(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.QUOTA_ERROR);
    assertThat(omClientResponse.getOMResponse().getMessage()).
        contains("Cannot update bucket quota. NamespaceQuota requested " +
            "is less than used namespaceQuota");
  }

  @Test
  public void testSettingQuotaRetainsReplication() throws Exception {
    String volumeName1 = UUID.randomUUID().toString();
    String bucketName1 = UUID.randomUUID().toString();
    String volumeName2 = UUID.randomUUID().toString();
    String bucketName2 = UUID.randomUUID().toString();

    /* Bucket with default replication */
    OMRequestTestUtils.addVolumeAndBucketToDB(
            volumeName1, bucketName1, omMetadataManager);

    String bucketKey = omMetadataManager
            .getBucketKey(volumeName1, bucketName1);

    OmBucketInfo dbBucketInfoBefore =
            omMetadataManager.getBucketTable().get(bucketKey);

    /* Setting quota on a bucket with default replication */
    OMRequest omRequest = createSetBucketPropertyRequest(volumeName1,
            bucketName1, true, 20 * GB);

    OMBucketSetPropertyRequest omBucketSetPropertyRequest =
            new OMBucketSetPropertyRequest(omRequest);

    OMClientResponse omClientResponse = omBucketSetPropertyRequest
        .validateAndUpdateCache(ozoneManager, 1);

    assertTrue(omClientResponse.getOMResponse().getSuccess());

    OmBucketInfo dbBucketInfoAfter =
            omMetadataManager.getBucketTable().get(bucketKey);

    assertNull(dbBucketInfoAfter.getDefaultReplicationConfig());
    assertNull(dbBucketInfoBefore.getDefaultReplicationConfig());
    assertEquals(20 * GB, dbBucketInfoAfter.getQuotaInBytes());
    assertEquals(1000L, dbBucketInfoAfter.getQuotaInNamespace());

    /* Bucket with EC replication */
    OmBucketInfo.Builder bucketInfo = new OmBucketInfo.Builder()
            .setVolumeName(volumeName2)
            .setBucketName(bucketName2)
            .setDefaultReplicationConfig(new DefaultReplicationConfig(
                    new ECReplicationConfig(3, 2)));

    OMRequestTestUtils.addVolumeToDB(volumeName2, omMetadataManager);
    OMRequestTestUtils.addBucketToDB(omMetadataManager, bucketInfo);

    bucketKey = omMetadataManager
            .getBucketKey(volumeName2, bucketName2);
    dbBucketInfoBefore =
            omMetadataManager.getBucketTable().get(bucketKey);

    /* Setting quota on a bucket with non-default EC replication */
    omRequest = createSetBucketPropertyRequest(volumeName2,
            bucketName2, true, 20 * GB);

    omBucketSetPropertyRequest =
            new OMBucketSetPropertyRequest(omRequest);

    omClientResponse = omBucketSetPropertyRequest
        .validateAndUpdateCache(ozoneManager, 1);

    assertTrue(omClientResponse.getOMResponse().getSuccess());

    dbBucketInfoAfter =
            omMetadataManager.getBucketTable().get(bucketKey);

    assertEquals(EC, dbBucketInfoAfter.getDefaultReplicationConfig().getType());
    assertEquals(
            dbBucketInfoBefore.getDefaultReplicationConfig(),
            dbBucketInfoAfter.getDefaultReplicationConfig());
    assertEquals(20 * GB, dbBucketInfoAfter.getQuotaInBytes());
    assertEquals(1000L, dbBucketInfoAfter.getQuotaInNamespace());
  }
}
