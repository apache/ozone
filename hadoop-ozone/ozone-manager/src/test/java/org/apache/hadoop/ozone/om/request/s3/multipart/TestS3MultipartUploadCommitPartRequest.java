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

package org.apache.hadoop.ozone.om.request.s3.multipart;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

/**
 * Tests S3 Multipart upload commit part request.
 */
public class TestS3MultipartUploadCommitPartRequest
    extends TestS3MultipartRequest {

  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    doPreExecuteCommitMPU(volumeName, bucketName, keyName, Time.now(),
        UUID.randomUUID().toString(), 1);
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    createParentPath(volumeName, bucketName);

    OMRequest initiateMPURequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
            getS3InitiateMultipartUploadReq(initiateMPURequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 1L);

    long clientID = Time.now();
    String multipartUploadID = omClientResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);

    // Add key to open key table.
    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID);

    omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);


    assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.OK);

    String multipartOpenKey = getMultipartOpenKey(volumeName, bucketName,
        keyName, multipartUploadID);

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    assertNotNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));
    assertEquals(1, omMetadataManager.getMultipartInfoTable()
        .get(multipartKey).getPartKeyInfoMap().size());

    OmKeyInfo mpuOpenKeyInfo = omMetadataManager
        .getOpenKeyTable(s3MultipartUploadCommitPartRequest.getBucketLayout())
        .get(multipartOpenKey);
    assertNotNull(mpuOpenKeyInfo);
    assertNotNull(mpuOpenKeyInfo.getLatestVersionLocations());
    assertTrue(mpuOpenKeyInfo.getLatestVersionLocations()
        .isMultipartKey());

    String partKey = getOpenKey(volumeName, bucketName, keyName, clientID);
    assertNull(omMetadataManager
        .getOpenKeyTable(s3MultipartUploadCommitPartRequest.getBucketLayout())
        .get(partKey));
  }

  @Test
  public void testValidateAndUpdateCacheMultipartNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    createParentPath(volumeName, bucketName);

    long clientID = Time.now();
    String multipartUploadID = UUID.randomUUID().toString();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);

    // Add key to open key table.
    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID);

    OMClientResponse omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.NO_SUCH_MULTIPART_UPLOAD_ERROR);

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    assertNull(omMetadataManager.getMultipartInfoTable().get(multipartKey));

  }

  @Test
  public void testValidateAndUpdateCacheKeyNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());


    long clientID = Time.now();
    String multipartUploadID = UUID.randomUUID().toString();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    // Don't add key to open table entry, and we are trying to commit this MPU
    // part. It will fail with KEY_NOT_FOUND

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);


    OMClientResponse omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    if (getBucketLayout() == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
      assertSame(omClientResponse.getOMResponse().getStatus(),
          OzoneManagerProtocolProtos.Status.DIRECTORY_NOT_FOUND);
    } else {
      assertSame(omClientResponse.getOMResponse().getStatus(),
          OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND);
    }

  }


  @Test
  public void testValidateAndUpdateCacheBucketFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);


    long clientID = Time.now();
    String multipartUploadID = UUID.randomUUID().toString();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    // Don't add key to open table entry, and we are trying to commit this MPU
    // part. It will fail with KEY_NOT_FOUND

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);


    OMClientResponse omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND);

  }

  protected void addKeyToOpenKeyTable(String volumeName, String bucketName,
      String keyName, long clientID) throws Exception {
    OMRequestTestUtils.addKeyToTable(true, true,  volumeName, bucketName,
            keyName, clientID, HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.ONE, omMetadataManager);
  }

  protected String getKeyName() {
    return UUID.randomUUID().toString();
  }

  protected String getMultipartOpenKey(String volumeName, String bucketName,
      String keyName, String multipartUploadID) throws IOException {
    return omMetadataManager
        .getMultipartKey(volumeName, bucketName, keyName, multipartUploadID);
  }

  protected String getOpenKey(String volumeName, String bucketName,
      String keyName, long clientID) throws IOException {
    return omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, clientID);
  }

  protected void createParentPath(String volumeName, String bucketName)
          throws Exception {
    // no parent hierarchy
  }
}
