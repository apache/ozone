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

package org.apache.hadoop.ozone.om.request.s3.multipart;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.junit.jupiter.api.Test;

/**
 * Test Multipart upload abort request.
 */
public class TestS3MultipartUploadAbortRequest extends TestS3MultipartRequest {

  @Test
  public void testPreExecute() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    doPreExecuteAbortMPU(volumeName, bucketName, keyName,
        UUID.randomUUID().toString());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
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

    String multipartUploadID = omClientResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    OMRequest abortMPURequest =
        doPreExecuteAbortMPU(volumeName, bucketName, keyName,
            multipartUploadID);

    S3MultipartUploadAbortRequest s3MultipartUploadAbortRequest =
        getS3MultipartUploadAbortReq(abortMPURequest);

    omClientResponse =
        s3MultipartUploadAbortRequest.validateAndUpdateCache(ozoneManager, 2L);

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    String multipartOpenKey = getMultipartOpenKey(volumeName, bucketName,
        keyName, multipartUploadID);

    // Check table and response.
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
    assertNull(omMetadataManager.getMultipartInfoTable().get(multipartKey));
    assertNull(omMetadataManager
        .getOpenKeyTable(s3MultipartUploadAbortRequest.getBucketLayout())
        .get(multipartOpenKey));

  }

  @Test
  public void testValidateAndUpdateCacheMultipartNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    String multipartUploadID = "randomMPU";

    OMRequest abortMPURequest =
        doPreExecuteAbortMPU(volumeName, bucketName, keyName,
            multipartUploadID);

    S3MultipartUploadAbortRequest s3MultipartUploadAbortRequest =
        getS3MultipartUploadAbortReq(abortMPURequest);

    OMClientResponse omClientResponse =
        s3MultipartUploadAbortRequest.validateAndUpdateCache(ozoneManager, 2L);

    // Check table and response.
    assertEquals(
        OzoneManagerProtocolProtos.Status.NO_SUCH_MULTIPART_UPLOAD_ERROR,
        omClientResponse.getOMResponse().getStatus());

  }

  @Test
  public void testValidateAndUpdateCacheVolumeNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();


    String multipartUploadID = "randomMPU";

    OMRequest abortMPURequest =
        doPreExecuteAbortMPU(volumeName, bucketName, keyName,
            multipartUploadID);

    S3MultipartUploadAbortRequest s3MultipartUploadAbortRequest =
        new S3MultipartUploadAbortRequest(abortMPURequest);

    OMClientResponse omClientResponse =
        s3MultipartUploadAbortRequest.validateAndUpdateCache(ozoneManager, 2L);

    // Check table and response.
    assertEquals(
        OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

  }

  @Test
  public void testValidateAndUpdateCacheBucketNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();


    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);

    String multipartUploadID = "randomMPU";

    OMRequest abortMPURequest =
        doPreExecuteAbortMPU(volumeName, bucketName, keyName,
            multipartUploadID);

    S3MultipartUploadAbortRequest s3MultipartUploadAbortRequest =
        new S3MultipartUploadAbortRequest(abortMPURequest);

    OMClientResponse omClientResponse =
        s3MultipartUploadAbortRequest.validateAndUpdateCache(ozoneManager, 2L);

    // Check table and response.
    assertEquals(
        OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

  }

  protected String getKeyName() {
    return UUID.randomUUID().toString();
  }

  protected void createParentPath(String volumeName, String bucketName)
      throws Exception {
    // no parent hierarchy
  }

  protected String getMultipartOpenKey(String volumeName, String bucketName,
      String keyName, String multipartUploadID) throws IOException {
    return omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);
  }
}
