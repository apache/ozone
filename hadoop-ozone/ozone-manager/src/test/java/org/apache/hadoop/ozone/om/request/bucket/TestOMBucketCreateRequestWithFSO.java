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

import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.newBucketInfoBuilder;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.newCreateBucketRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketLayoutProto.FILE_SYSTEM_OPTIMIZED;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests OMBucketCreateRequest class, which handles CreateBucket request.
 */
public class TestOMBucketCreateRequestWithFSO
    extends TestOMBucketCreateRequest {

  @BeforeEach
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

    assertEquals(0, omMetrics.getNumFSOBucketCreates());

    OMBucketCreateRequest omBucketCreateRequest = doPreExecute(volumeName,
        bucketName, true);

    doValidateAndUpdateCache(volumeName, bucketName,
        omBucketCreateRequest.getOmRequest());

    assertEquals(1, omMetrics.getNumFSOBucketCreates());
    assertEquals(BucketLayout.FILE_SYSTEM_OPTIMIZED,
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
    assertEquals(
        OMConfigKeys.OZONE_BUCKET_LAYOUT_FILE_SYSTEM_OPTIMIZED,
        ozoneManager.getConfiguration()
            .get(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT));

    assertEquals(0, omMetrics.getNumFSOBucketCreates());

    // OzoneManager is mocked, the bucket layout will return null
    when(ozoneManager.getOMDefaultBucketLayout()).thenReturn(
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMBucketCreateRequest omBucketCreateRequest =
        doPreExecute(volumeName, bucketName, false);

    doValidateAndUpdateCache(volumeName, bucketName,
        omBucketCreateRequest.getOmRequest());

    assertEquals(1, omMetrics.getNumFSOBucketCreates());
    assertEquals(BucketLayout.FILE_SYSTEM_OPTIMIZED,
        omMetadataManager.getBucketTable().get(bucketKey).getBucketLayout());
  }

  private OMBucketCreateRequest doPreExecute(String volumeName,
      String bucketName, boolean layoutFSOFromCli)
      throws Exception {
    addCreateVolumeToTable(volumeName, omMetadataManager);

    OzoneManagerProtocolProtos.BucketInfo.Builder bucketInfo =
        newBucketInfoBuilder(bucketName, volumeName);

    if (layoutFSOFromCli) {
      bucketInfo
          .setBucketLayout(FILE_SYSTEM_OPTIMIZED)
          .addMetadata(OMRequestTestUtils.fsoMetadata());
    }

    OMRequest originalRequest = newCreateBucketRequest(bucketInfo).build();
    OMBucketCreateRequest omBucketCreateRequest =
        new OMBucketCreateRequest(originalRequest);

    OMRequest modifiedRequest = omBucketCreateRequest.preExecute(ozoneManager);
    verifyRequest(modifiedRequest, originalRequest);
    return new OMBucketCreateRequest(modifiedRequest);
  }

  @Override
  protected void doValidateAndUpdateCache(String volumeName, String bucketName,
      OMRequest modifiedRequest) throws Exception {
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);

    // As we have not still called validateAndUpdateCache, get() should
    // return null.

    assertNull(omMetadataManager.getBucketTable().get(bucketKey));
    OMBucketCreateRequest omBucketCreateRequest =
        new OMBucketCreateRequest(modifiedRequest);
    omBucketCreateRequest.setUGI(UserGroupInformation.getCurrentUser());

    OMClientResponse omClientResponse =
        omBucketCreateRequest.validateAndUpdateCache(ozoneManager, 1);

    // As now after validateAndUpdateCache it should add entry to cache, get
    // should return non null value.
    OmBucketInfo dbBucketInfo =
        omMetadataManager.getBucketTable().get(bucketKey);
    assertNotNull(omMetadataManager.getBucketTable().get(bucketKey));

    BucketLayout bucketLayout = BucketLayout
        .fromString(ozoneManager.getConfiguration()
            .get(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT));

    // verify table data with actual request data.
    OmBucketInfo bucketInfoFromProto = OmBucketInfo.getFromProtobuf(
        modifiedRequest.getCreateBucketRequest().getBucketInfo(), bucketLayout);

    assertEquals(bucketInfoFromProto.getCreationTime(),
        dbBucketInfo.getCreationTime());
    assertEquals(bucketInfoFromProto.getModificationTime(),
        dbBucketInfo.getModificationTime());
    assertTrue(dbBucketInfo.getAcls().containsAll(bucketInfoFromProto.getAcls()));
    assertEquals(bucketInfoFromProto.getIsVersionEnabled(),
        dbBucketInfo.getIsVersionEnabled());
    assertEquals(bucketInfoFromProto.getStorageType(),
        dbBucketInfo.getStorageType());
    assertEquals(bucketInfoFromProto.getMetadata(),
        dbBucketInfo.getMetadata());
    assertEquals(bucketInfoFromProto.getEncryptionKeyInfo(),
        dbBucketInfo.getEncryptionKeyInfo());
    assertEquals(bucketInfoFromProto.getBucketLayout(),
        dbBucketInfo.getBucketLayout());

    // verify OMResponse.
    verifySuccessCreateBucketResponse(omClientResponse.getOMResponse());

  }

  @Test
  public void testNonS3BucketNameAllowedForFSOWhenStrictDisabled() throws Exception {
    // Arrange
    ozoneManager.getConfiguration().setBoolean(
        OMConfigKeys.OZONE_OM_NAMESPACE_STRICT_S3, false); // 如果常數名稱不同，改成實際的 key

    when(ozoneManager.getOMDefaultBucketLayout()).thenReturn(
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    String volumeName = UUID.randomUUID().toString();
    String bucketName = "bucket_with_underscore"; // non-S3-compliant name
    addCreateVolumeToTable(volumeName, omMetadataManager);

    OzoneManagerProtocolProtos.BucketInfo.Builder bucketInfo =
        newBucketInfoBuilder(bucketName, volumeName)
            .setBucketLayout(FILE_SYSTEM_OPTIMIZED)
            .addMetadata(OMRequestTestUtils.fsoMetadata());

    OMRequest originalRequest = newCreateBucketRequest(bucketInfo).build();
    OMBucketCreateRequest req = new OMBucketCreateRequest(originalRequest);

    // Act
    OMRequest modifiedRequest = req.preExecute(ozoneManager);

    // Assert: validateAndUpdateCache should succeed
    assertDoesNotThrow(() -> {
      OMBucketCreateRequest omReq = new OMBucketCreateRequest(modifiedRequest);
      omReq.setUGI(UserGroupInformation.getCurrentUser());
      omReq.validateAndUpdateCache(ozoneManager, 1);
    });
  }
}
