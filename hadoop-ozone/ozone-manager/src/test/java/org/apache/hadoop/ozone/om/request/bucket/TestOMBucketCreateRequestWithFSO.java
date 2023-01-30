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

import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.StorageTypeProto;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.mockito.Mockito.when;

/**
 * Tests OMBucketCreateRequest class, which handles CreateBucket request.
 */
public class TestOMBucketCreateRequestWithFSO
    extends TestOMBucketCreateRequest {

  @Before
  public void setupWithFSO() {
    ozoneManager.getConfiguration()
        .set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
            OMConfigKeys.OZONE_BUCKET_LAYOUT_FILE_SYSTEM_OPTIMIZED);
  }

  @Test
  public void testValidateAndUpdateCacheWithFSO() throws Exception {
    when(ozoneManager.getOMDefaultBucketLayout()).thenReturn(
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);

    Assert.assertEquals(0, omMetrics.getNumFSOBucketCreates());

    OMBucketCreateRequest omBucketCreateRequest = doPreExecute(volumeName,
        bucketName, true);

    doValidateAndUpdateCache(volumeName, bucketName,
        omBucketCreateRequest.getOmRequest());

    Assert.assertEquals(1, omMetrics.getNumFSOBucketCreates());
    Assert.assertEquals(BucketLayout.FILE_SYSTEM_OPTIMIZED,
        omMetadataManager.getBucketTable().get(bucketKey).getBucketLayout());
  }

  /**
   * Gets the bucket layout from the ozone configuration and
   * creates a bucket request with the latest client version.
   * Checking that the configuration layout is the same with
   * the one used for the request.
   * @throws Exception
   */
  @Test
  public void testValidateAndUpdateCacheVerifyBucketLayoutWithFSO()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);

    // Checking bucket layout from configuration
    Assert.assertEquals(OMConfigKeys.OZONE_BUCKET_LAYOUT_FILE_SYSTEM_OPTIMIZED,
        ozoneManager.getConfiguration()
            .get(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT));

    Assert.assertEquals(0, omMetrics.getNumFSOBucketCreates());

    // OzoneManager is mocked, the bucket layout will return null
    when(ozoneManager.getOMDefaultBucketLayout()).thenReturn(
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMBucketCreateRequest omBucketCreateRequest =
        doPreExecute(volumeName, bucketName, false);

    doValidateAndUpdateCache(volumeName, bucketName,
        omBucketCreateRequest.getOmRequest());

    Assert.assertEquals(1, omMetrics.getNumFSOBucketCreates());
    Assert.assertEquals(BucketLayout.FILE_SYSTEM_OPTIMIZED,
        omMetadataManager.getBucketTable().get(bucketKey).getBucketLayout());
  }

  private OMBucketCreateRequest doPreExecute(String volumeName,
      String bucketName, boolean layoutFSOFromCli)
      throws Exception {
    addCreateVolumeToTable(volumeName, omMetadataManager);

    OMRequest originalRequest;

    if (layoutFSOFromCli) {
      originalRequest =
          OMRequestTestUtils.createBucketReqFSO(bucketName, volumeName,
              false, StorageTypeProto.SSD);
    } else {
      originalRequest = OMRequestTestUtils
          .createBucketRequest(bucketName, volumeName,
              false, StorageTypeProto.SSD);
    }

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
        omBucketCreateRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    // As now after validateAndUpdateCache it should add entry to cache, get
    // should return non null value.
    OmBucketInfo dbBucketInfo =
        omMetadataManager.getBucketTable().get(bucketKey);
    Assert.assertNotNull(omMetadataManager.getBucketTable().get(bucketKey));

    BucketLayout bucketLayout = BucketLayout
        .fromString(ozoneManager.getConfiguration()
            .get(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT));

    // verify table data with actual request data.
    OmBucketInfo bucketInfoFromProto = OmBucketInfo.getFromProtobuf(
        modifiedRequest.getCreateBucketRequest().getBucketInfo(), bucketLayout);

    Assert.assertEquals(bucketInfoFromProto.getCreationTime(),
        dbBucketInfo.getCreationTime());
    Assert.assertEquals(bucketInfoFromProto.getModificationTime(),
        dbBucketInfo.getModificationTime());
    Assert.assertEquals(bucketInfoFromProto.getAcls(),
        dbBucketInfo.getAcls());
    Assert.assertEquals(bucketInfoFromProto.getIsVersionEnabled(),
        dbBucketInfo.getIsVersionEnabled());
    Assert.assertEquals(bucketInfoFromProto.getStorageType(),
        dbBucketInfo.getStorageType());
    Assert.assertEquals(bucketInfoFromProto.getMetadata(),
        dbBucketInfo.getMetadata());
    Assert.assertEquals(bucketInfoFromProto.getEncryptionKeyInfo(),
        dbBucketInfo.getEncryptionKeyInfo());
    Assert.assertEquals(bucketInfoFromProto.getBucketLayout(),
        dbBucketInfo.getBucketLayout());

    // verify OMResponse.
    verifySuccessCreateBucketResponse(omClientResponse.getOMResponse());

  }
}
