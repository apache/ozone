/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.s3.multipart;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Part;
import org.apache.hadoop.util.Time;


/**
 * Tests S3 Multipart Upload Complete request.
 */
public class TestS3MultipartUploadCompleteRequest
    extends TestS3MultipartRequest {

  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    doPreExecuteCompleteMPU(volumeName, bucketName, keyName,
        UUID.randomUUID().toString(), new ArrayList<>());
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMRequest initiateMPURequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(initiateMPURequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager,
            1L, ozoneManagerDoubleBufferHelper);

    long clientID = Time.now();
    String multipartUploadID = omClientResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);

    // Add key to open key table.
    addKeyToTable(volumeName, bucketName, keyName, clientID);

    s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager,
        2L, ozoneManagerDoubleBufferHelper);

    List<Part> partList = new ArrayList<>();

    String partName = getPartName(volumeName, bucketName, keyName,
        multipartUploadID, 1);
    partList.add(Part.newBuilder().setPartName(partName).setPartNumber(1)
        .build());

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, multipartUploadID, partList);

    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
        getS3MultipartUploadCompleteReq(completeMultipartRequest);

    omClientResponse =
        s3MultipartUploadCompleteRequest.validateAndUpdateCache(ozoneManager,
            3L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName,
            multipartUploadID);

    Assert.assertNull(
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(multipartKey));
    Assert.assertNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));
    Assert.assertNotNull(omMetadataManager
        .getKeyTable(getBucketLayout(omMetadataManager, volumeName, bucketName))
        .get(getOzoneDBKey(volumeName, bucketName, keyName)));
  }

  @Test
  public void testInvalidPartOrderError() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager);

    OMRequest initiateMPURequest = doPreExecuteInitiateMPU(volumeName,
            bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
            getS3InitiateMultipartUploadReq(initiateMPURequest);

    OMClientResponse omClientResponse =
            s3InitiateMultipartUploadRequest.validateAndUpdateCache(
                    ozoneManager, 1L, ozoneManagerDoubleBufferHelper);

    long clientID = Time.now();
    String multipartUploadID = omClientResponse.getOMResponse()
            .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
            bucketName, keyName, clientID, multipartUploadID, 1);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
            getS3MultipartUploadCommitReq(commitMultipartRequest);

    // Add key to open key table.
    addKeyToTable(volumeName, bucketName, keyName, clientID);

    s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager,
            2L, ozoneManagerDoubleBufferHelper);

    List<Part> partList = new ArrayList<>();

    String partName= getPartName(volumeName, bucketName, keyName,
        multipartUploadID, 23);

    partList.add(Part.newBuilder().setPartName(partName).setPartNumber(23)
            .build());

    partName = getPartName(volumeName, bucketName, keyName,
        multipartUploadID, 1);
    partList.add(Part.newBuilder().setPartName(partName).setPartNumber(1)
            .build());

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
            bucketName, keyName, multipartUploadID, partList);

    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
            getS3MultipartUploadCompleteReq(completeMultipartRequest);

    omClientResponse =
            s3MultipartUploadCompleteRequest.validateAndUpdateCache(
                    ozoneManager, 3L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.INVALID_PART_ORDER,
            omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheVolumeNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    List<Part> partList = new ArrayList<>();

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, UUID.randomUUID().toString(), partList);

    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
        getS3MultipartUploadCompleteReq(completeMultipartRequest);

    OMClientResponse omClientResponse =
        s3MultipartUploadCompleteRequest.validateAndUpdateCache(ozoneManager,
            3L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

  }

  @Test
  public void testValidateAndUpdateCacheBucketNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    TestOMRequestUtils.addVolumeToDB(volumeName, omMetadataManager);
    List<Part> partList = new ArrayList<>();

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, UUID.randomUUID().toString(), partList);

    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
            getS3MultipartUploadCompleteReq(completeMultipartRequest);

    OMClientResponse omClientResponse =
        s3MultipartUploadCompleteRequest.validateAndUpdateCache(ozoneManager,
            3L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

  }

  @Test
  public void testValidateAndUpdateCacheNoSuchMultipartUploadError()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    List<Part> partList = new ArrayList<>();

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, UUID.randomUUID().toString(), partList);

    // Doing  complete multipart upload request with out initiate.
    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
            getS3MultipartUploadCompleteReq(completeMultipartRequest);

    OMClientResponse omClientResponse =
        s3MultipartUploadCompleteRequest.validateAndUpdateCache(ozoneManager,
            3L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(
        OzoneManagerProtocolProtos.Status.NO_SUCH_MULTIPART_UPLOAD_ERROR,
        omClientResponse.getOMResponse().getStatus());

  }

  protected void addKeyToTable(String volumeName, String bucketName,
                             String keyName, long clientID) throws Exception {
    TestOMRequestUtils.addKeyToTable(true, volumeName, bucketName,
            keyName, clientID, HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.ONE, omMetadataManager);
  }

  protected String getMultipartKey(String volumeName, String bucketName,
      String keyName, String multipartUploadID) throws IOException {
    return omMetadataManager.getMultipartKey(volumeName,
            bucketName, keyName, multipartUploadID);
  }

  private String getPartName(String volumeName, String bucketName,
      String keyName, String uploadID, int partNumber) {

    String dbOzoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);
    return S3MultipartUploadCommitPartRequest.getPartName(dbOzoneKey, uploadID,
        partNumber);
  }

  protected String getOzoneDBKey(String volumeName, String bucketName,
                                 String keyName) throws IOException {
    return omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
  }

  protected String getKeyName() {
    return UUID.randomUUID().toString();
  }
}

