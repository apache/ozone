/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.request.bucket;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .StorageTypeProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.util.Time;

import static org.mockito.Mockito.when;

/**
 * Tests OMBucketCreateRequest class, which handles CreateBucket request.
 */
public class TestOMBucketCreateRequest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;


  @Before
  public void setup() throws Exception {
    ozoneManager = Mockito.mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
  }

  @After
  public void stop() {
    omMetrics.unRegister();
  }


  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    doPreExecute(volumeName, bucketName);
  }


  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OMBucketCreateRequest omBucketCreateRequest = doPreExecute(volumeName,
        bucketName);

    doValidateAndUpdateCache(volumeName, bucketName,
        omBucketCreateRequest.getOmRequest());

  }

  @Test
  public void testValidateAndUpdateCacheWithNoVolume() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OMRequest originalRequest = createBucketRequest(bucketName, volumeName,
            false, StorageTypeProto.SSD);

    OMBucketCreateRequest omBucketCreateRequest =
        new OMBucketCreateRequest(originalRequest);

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);

    // As we have not still called validateAndUpdateCache, get() should
    // return null.

    Assert.assertNull(omMetadataManager.getBucketTable().get(bucketKey));

    OMClientResponse omClientResponse =
        omBucketCreateRequest.validateAndUpdateCache(ozoneManager, 1);

    OMResponse omResponse = omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateBucketResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omResponse.getStatus());

    // As request is invalid bucket table should not have entry.
    Assert.assertNull(omMetadataManager.getBucketTable().get(bucketKey));
  }


  @Test
  public void testValidateAndUpdateCacheWithBucketAlreadyExists()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OMBucketCreateRequest omBucketCreateRequest =
        doPreExecute(volumeName, bucketName);

    doValidateAndUpdateCache(volumeName, bucketName,
        omBucketCreateRequest.getOmRequest());

    // Try create same bucket again
    OMClientResponse omClientResponse =
        omBucketCreateRequest.validateAndUpdateCache(ozoneManager, 2);

    OMResponse omResponse = omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateBucketResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_ALREADY_EXISTS,
        omResponse.getStatus());
  }


  private OMBucketCreateRequest doPreExecute(String volumeName,
      String bucketName) throws Exception {
    addCreateVolumeToTable(volumeName, omMetadataManager);
    OMRequest originalRequest = createBucketRequest(bucketName, volumeName,
            false, StorageTypeProto.SSD);

    OMBucketCreateRequest omBucketCreateRequest =
        new OMBucketCreateRequest(originalRequest);

    OMRequest modifiedRequest = omBucketCreateRequest.preExecute(ozoneManager);
    verifyRequest(modifiedRequest, originalRequest);
    return new OMBucketCreateRequest(modifiedRequest);
  }

  private void doValidateAndUpdateCache(String volumeName, String bucketName,
      OMRequest modifiedRequest) throws Exception {
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);

    // As we have not still called validateAndUpdateCache, get() should
    // return null.

    Assert.assertNull(omMetadataManager.getBucketTable().get(bucketKey));
    OMBucketCreateRequest omBucketCreateRequest =
        new OMBucketCreateRequest(modifiedRequest);


    OMClientResponse omClientResponse =
        omBucketCreateRequest.validateAndUpdateCache(ozoneManager, 1);

    // As now after validateAndUpdateCache it should add entry to cache, get
    // should return non null value.
    OmBucketInfo omBucketInfo =
        omMetadataManager.getBucketTable().get(bucketKey);
    Assert.assertNotNull(omMetadataManager.getBucketTable().get(bucketKey));

    // verify table data with actual request data.
    Assert.assertEquals(OmBucketInfo.getFromProtobuf(
        modifiedRequest.getCreateBucketRequest().getBucketInfo()),
        omBucketInfo);

    // verify OMResponse.
    verifySuccessCreateBucketResponse(omClientResponse.getOMResponse());

  }


  private void verifyRequest(OMRequest modifiedOmRequest,
      OMRequest originalRequest) {
    OzoneManagerProtocolProtos.BucketInfo original =
        originalRequest.getCreateBucketRequest().getBucketInfo();
    OzoneManagerProtocolProtos.BucketInfo updated =
        modifiedOmRequest.getCreateBucketRequest().getBucketInfo();

    Assert.assertEquals(original.getBucketName(), updated.getBucketName());
    Assert.assertEquals(original.getVolumeName(), updated.getVolumeName());
    Assert.assertEquals(original.getIsVersionEnabled(),
        updated.getIsVersionEnabled());
    Assert.assertEquals(original.getStorageType(), updated.getStorageType());
    Assert.assertEquals(original.getMetadataList(), updated.getMetadataList());
    Assert.assertNotEquals(original.getCreationTime(),
        updated.getCreationTime());
  }

  public static void verifySuccessCreateBucketResponse(OMResponse omResponse) {
    Assert.assertNotNull(omResponse.getCreateBucketResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Type.CreateBucket,
        omResponse.getCmdType());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());
  }

  public static void addCreateVolumeToTable(String volumeName,
      OMMetadataManager omMetadataManager) throws Exception {
    OmVolumeArgs omVolumeArgs =
        OmVolumeArgs.newBuilder().setCreationTime(Time.now())
            .setVolume(volumeName).setAdminName(UUID.randomUUID().toString())
            .setOwnerName(UUID.randomUUID().toString()).build();
    omMetadataManager.getVolumeTable().put(
        omMetadataManager.getVolumeKey(volumeName), omVolumeArgs);
  }


  public static OMRequest createBucketRequest(String bucketName,
      String volumeName, boolean isVersionEnabled,
      StorageTypeProto storageTypeProto) {
    OzoneManagerProtocolProtos.BucketInfo bucketInfo =
        OzoneManagerProtocolProtos.BucketInfo.newBuilder()
            .setBucketName(bucketName)
            .setVolumeName(volumeName)
            .setIsVersionEnabled(isVersionEnabled)
            .setStorageType(storageTypeProto)
            .addAllMetadata(getMetadataList()).build();
    OzoneManagerProtocolProtos.CreateBucketRequest.Builder req =
        OzoneManagerProtocolProtos.CreateBucketRequest.newBuilder();
    req.setBucketInfo(bucketInfo);
    return OMRequest.newBuilder().setCreateBucketRequest(req)
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateBucket)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  public static List< HddsProtos.KeyValue> getMetadataList() {
    List<HddsProtos.KeyValue> metadataList = new ArrayList<>();
    metadataList.add(HddsProtos.KeyValue.newBuilder().setKey("key1").setValue(
        "value1").build());
    metadataList.add(HddsProtos.KeyValue.newBuilder().setKey("key2").setValue(
        "value2").build());
    return metadataList;
  }

}
