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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.newDeleteBucketRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.util.OMMultipartUploadUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Tests OMBucketDeleteRequest class which handles DeleteBucket request.
 */
public class TestOMBucketDeleteRequest extends TestBucketRequest {

  @Test
  public void testPreExecute() throws Exception {
    OMRequest omRequest =
        createDeleteBucketRequest(UUID.randomUUID().toString(),
            UUID.randomUUID().toString());

    OMBucketDeleteRequest omBucketDeleteRequest =
        new OMBucketDeleteRequest(omRequest);

    // As user info gets added.
    assertNotEquals(omRequest, omBucketDeleteRequest.preExecute(ozoneManager));
  }

  @Test
  public void preExecutePermissionDeniedWhenAclEnabled() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    when(ozoneManager.getAclsEnabled()).thenReturn(true);

    OMRequest originalRequest = newDeleteBucketRequest(volumeName, bucketName);

    OMBucketDeleteRequest req = spy(new OMBucketDeleteRequest(originalRequest));
    doThrow(new OMException("denied", OMException.ResultCodes.PERMISSION_DENIED))
        .when(req).checkAcls(any(), any(), any(), any(), any(), any(), any());

    OMException e = assertThrows(OMException.class,
        () -> req.preExecute(ozoneManager));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, e.getResult());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OMRequest omRequest =
        createDeleteBucketRequest(volumeName, bucketName);

    OMBucketDeleteRequest omBucketDeleteRequest =
        new OMBucketDeleteRequest(omRequest);

    // Create Volume and bucket entries in DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    omBucketDeleteRequest.validateAndUpdateCache(ozoneManager, 1);

    assertNull(omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName)));
  }

  @Test
  public void testValidateAndUpdateCacheFailure() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OMRequest omRequest =
        createDeleteBucketRequest(volumeName, bucketName);

    OMBucketDeleteRequest omBucketDeleteRequest =
        new OMBucketDeleteRequest(omRequest);


    OMClientResponse omClientResponse =
        omBucketDeleteRequest.validateAndUpdateCache(ozoneManager, 1);

    assertNull(omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName)));

    assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
  }

  @Test
  public void testBucketContainsIncompleteMPUs() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OMRequest omRequest =
        createDeleteBucketRequest(volumeName, bucketName);

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMBucketDeleteRequest omBucketDeleteRequest =
        new OMBucketDeleteRequest(omRequest);

    // Create a MPU key in the MPU table to simulate incomplete MPU
    String uploadId = OMMultipartUploadUtils.getMultipartUploadId();
    final OmKeyInfo keyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, UUID.randomUUID().toString(),
            RatisReplicationConfig.getInstance(ONE), new OmKeyLocationInfoGroup(0L, new ArrayList<>(), true))
        .build();
    final OmMultipartKeyInfo multipartKeyInfo = OMRequestTestUtils.
        createOmMultipartKeyInfo(uploadId, Time.now(),
            HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.ONE, 0L);
    OMRequestTestUtils.addMultipartInfoToTable(false, keyInfo,
        multipartKeyInfo, 0L, omMetadataManager);

    // Bucket delete request should fail since there are still incomplete MPUs
    OMClientResponse omClientResponse =
        omBucketDeleteRequest.validateAndUpdateCache(ozoneManager, 1L);

    assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_EMPTY,
        omClientResponse.getOMResponse().getStatus());
    assertNotNull(omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName)));

    // Remove the MPU keys to simulate MPU aborts / completes
    String multipartKey = omMetadataManager.getMultipartKey(
        volumeName, bucketName, keyInfo.getKeyName(), uploadId);
    omMetadataManager.getMultipartInfoTable().addCacheEntry(
        new CacheKey<>(multipartKey), CacheValue.get(2L));
    omMetadataManager.getMultipartInfoTable().delete(multipartKey);

    // Bucket delete request should succeed now
    omClientResponse =
        omBucketDeleteRequest.validateAndUpdateCache(ozoneManager, 3L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
    assertNull(omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName)));
  }

  private OMRequest createDeleteBucketRequest(String volumeName,
      String bucketName) {
    return OMRequest.newBuilder().setDeleteBucketRequest(
        DeleteBucketRequest.newBuilder()
            .setBucketName(bucketName).setVolumeName(volumeName))
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteBucket)
        .setClientId(UUID.randomUUID().toString()).build();
  }
}
