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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Part;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

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
  public void testPreExecuteRewritesExpectedETagToGeneration()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    String expectedETag = "matching-etag";
    long expectedGeneration = 10L;

    addVolumeAndBucket(volumeName, bucketName);
    addCommittedKeyToTable(volumeName, bucketName, keyName, expectedGeneration,
        expectedETag);

    OMRequest initiateMPURequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);
    OMClientResponse initiateResponse = getS3InitiateMultipartUploadReq(
        initiateMPURequest).validateAndUpdateCache(ozoneManager, 1L);
    String uploadID = initiateResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    OMRequest request = withCompleteKeyArgs(
        OMRequestTestUtils.createCompleteMPURequest(volumeName, bucketName,
            keyName, uploadID, new ArrayList<>()),
        keyArgs -> keyArgs.setExpectedETag(expectedETag));

    OMRequest modifiedRequest = getS3MultipartUploadCompleteReq(request)
        .preExecute(ozoneManager);
    KeyArgs resolvedKeyArgs = modifiedRequest
        .getCompleteMultiPartUploadRequest().getKeyArgs();

    assertFalse(resolvedKeyArgs.hasExpectedETag());
    assertTrue(resolvedKeyArgs.hasExpectedDataGeneration());
    assertEquals(expectedGeneration,
        resolvedKeyArgs.getExpectedDataGeneration());
  }

  @Test
  public void testConditionalCompleteReturnsConflictBeforeMissingUpload()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    addVolumeAndBucket(volumeName, bucketName);
    addCommittedKeyToTable(volumeName, bucketName, keyName, 20L,
        "new-etag");

    OMRequest request = withCompleteKeyArgs(
        OMRequestTestUtils.createCompleteMPURequest(volumeName, bucketName,
            keyName, UUID.randomUUID().toString(), new ArrayList<>()),
        keyArgs -> keyArgs.setExpectedDataGeneration(10L));

    OMClientResponse response = getS3MultipartUploadCompleteReq(request)
        .validateAndUpdateCache(ozoneManager, 3L);

    assertEquals(OzoneManagerProtocolProtos.Status.ATOMIC_WRITE_CONFLICT,
        response.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    Map<String, String> customMetadata = new HashMap<>();
    customMetadata.put("custom-key1", "custom-value1");
    customMetadata.put("custom-key2", "custom-value2");

    Map<String, String> tags = new HashMap<>();
    tags.put("tag-key1", "tag-value1");
    tags.put("tag-key2", "tag-value2");


    String uploadId = checkValidateAndUpdateCacheSuccess(
        volumeName, bucketName, keyName, customMetadata, tags);
    checkDeleteTableCount(volumeName, bucketName, keyName, 0, uploadId);

    customMetadata.remove("custom-key1");
    customMetadata.remove("custom-key2");
    customMetadata.put("custom-key3", "custom-value3");

    tags.remove("tag-key1");
    tags.remove("tag-key2");
    tags.put("tag-key3", "tag-value3");

    // Do it twice to test overwrite
    uploadId = checkValidateAndUpdateCacheSuccess(volumeName, bucketName,
        keyName, customMetadata, tags);
    // After overwrite, one entry must be in delete table
    checkDeleteTableCount(volumeName, bucketName, keyName, 1, uploadId);
  }

  public void checkDeleteTableCount(String volumeName,
      String bucketName, String keyName, int count, String uploadId)
      throws Exception {
    String dbOzoneKey = getMultipartKey(volumeName, bucketName, keyName,
        uploadId);
    List<? extends Table.KeyValue<String, RepeatedOmKeyInfo>> rangeKVs
        = omMetadataManager.getDeletedTable().getRangeKVs(
        null, 100, dbOzoneKey);

    // deleted key entries count is expected to be 0
    if (count == 0) {
      assertEquals(0, rangeKVs.size());
      return;
    }

    assertThat(rangeKVs.size()).isGreaterThanOrEqualTo(1);

    // Count must consider unused parts on commit
    assertEquals(count,
        rangeKVs.get(0).getValue().getOmKeyInfoList().size());
  }

  private String checkValidateAndUpdateCacheSuccess(String volumeName,
      String bucketName, String keyName, Map<String, String> metadata, Map<String, String> tags) throws Exception {

    OMRequest initiateMPURequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName, metadata, tags);

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
    addKeyToTable(volumeName, bucketName, keyName, clientID);

    s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    List<Part> partList = new ArrayList<>();

    String eTag = s3MultipartUploadCommitPartRequest.getOmRequest()
        .getCommitMultiPartUploadRequest()
        .getKeyArgs()
        .getMetadataList()
        .stream()
        .filter(keyValue -> keyValue.getKey().equals(OzoneConsts.ETAG))
        .findFirst().get().getValue();
    partList.add(Part.newBuilder().setETag(eTag).setPartName(eTag).setPartNumber(1)
        .build());

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, multipartUploadID, partList);

    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
        getS3MultipartUploadCompleteReq(completeMultipartRequest);

    omClientResponse =
        s3MultipartUploadCompleteRequest.validateAndUpdateCache(ozoneManager, 3L);

    BatchOperation batchOperation
        = omMetadataManager.getStore().initBatchOperation();
    omClientResponse.checkAndUpdateDB(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName,
            multipartUploadID);

    assertNull(omMetadataManager
        .getOpenKeyTable(s3MultipartUploadCompleteRequest.getBucketLayout())
        .get(multipartKey));
    assertNull(omMetadataManager.getMultipartInfoTable().get(multipartKey));
    OmKeyInfo multipartKeyInfo = omMetadataManager
        .getKeyTable(s3MultipartUploadCompleteRequest.getBucketLayout())
        .get(getOzoneDBKey(volumeName, bucketName, keyName));
    assertNotNull(multipartKeyInfo);
    assertNotNull(multipartKeyInfo.getLatestVersionLocations());
    assertTrue(multipartKeyInfo.getLatestVersionLocations()
        .isMultipartKey());
    if (metadata != null) {
      assertThat(multipartKeyInfo.getMetadata()).containsAllEntriesOf(metadata);
    }
    if (tags != null) {
      assertThat(multipartKeyInfo.getTags()).containsAllEntriesOf(tags);
    }

    OmBucketInfo omBucketInfo = omMetadataManager.getBucketTable()
        .getCacheValue(new CacheKey<>(
            omMetadataManager.getBucketKey(volumeName, bucketName)))
        .getCacheValue();
    assertEquals(getNamespaceCount(),
        omBucketInfo.getUsedNamespace());
    return multipartUploadID;
  }

  protected void addVolumeAndBucket(String volumeName, String bucketName)
      throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
  }

  protected OmKeyInfo addCommittedKeyToTable(String volumeName,
      String bucketName, String keyName, long updateID, String eTag)
      throws Exception {
    OmKeyInfo keyInfo = OMRequestTestUtils.createOmKeyInfo(
        volumeName, bucketName, keyName,
        RatisReplicationConfig.getInstance(ONE))
        .setUpdateID(updateID)
        .addMetadata(OzoneConsts.ETAG, eTag)
        .build();
    omMetadataManager.getKeyTable(getBucketLayout())
        .put(getOzoneDBKey(volumeName, bucketName, keyName), keyInfo);
    return keyInfo;
  }

  @Test
  public void testInvalidPartOrderError() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

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
    addKeyToTable(volumeName, bucketName, keyName, clientID);

    s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    List<Part> partList = new ArrayList<>();

    String partName = getPartName(volumeName, bucketName, keyName,
        multipartUploadID, 23);

    partList.add(Part.newBuilder().setETag(partName).setPartName(partName).setPartNumber(23).build());

    partName = getPartName(volumeName, bucketName, keyName, multipartUploadID, 1);
    partList.add(Part.newBuilder().setETag(partName).setPartName(partName).setPartNumber(1).build());

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, multipartUploadID, partList);

    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
        getS3MultipartUploadCompleteReq(completeMultipartRequest);

    omClientResponse =
        s3MultipartUploadCompleteRequest.validateAndUpdateCache(ozoneManager, 3L);

    assertEquals(OzoneManagerProtocolProtos.Status
        .INVALID_PART_ORDER, omClientResponse.getOMResponse().getStatus());
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
        s3MultipartUploadCompleteRequest.validateAndUpdateCache(ozoneManager, 3L);

    assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

  }

  @Test
  public void testValidateAndUpdateCacheBucketNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);
    List<Part> partList = new ArrayList<>();

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, UUID.randomUUID().toString(), partList);

    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
            getS3MultipartUploadCompleteReq(completeMultipartRequest);

    OMClientResponse omClientResponse =
        s3MultipartUploadCompleteRequest.validateAndUpdateCache(ozoneManager, 3L);

    assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

  }

  @Test
  public void testValidateAndUpdateCacheNoSuchMultipartUploadError()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    List<Part> partList = new ArrayList<>();

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, UUID.randomUUID().toString(), partList);

    // Doing  complete multipart upload request with out initiate.
    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
        getS3MultipartUploadCompleteReq(completeMultipartRequest);

    OMClientResponse omClientResponse =
        s3MultipartUploadCompleteRequest.validateAndUpdateCache(ozoneManager, 3L);

    assertEquals(
        OzoneManagerProtocolProtos.Status.NO_SUCH_MULTIPART_UPLOAD_ERROR,
        omClientResponse.getOMResponse().getStatus());

  }

  protected void addKeyToTable(String volumeName, String bucketName,
                             String keyName, long clientID) throws Exception {
    OMRequestTestUtils.addKeyToTable(true, true, volumeName, bucketName,
        keyName, clientID, RatisReplicationConfig.getInstance(ONE), omMetadataManager);
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

  protected long getNamespaceCount() {
    return 1L;
  }

  private OMRequest withCompleteKeyArgs(OMRequest request,
      KeyArgsUpdater updater) {
    OzoneManagerProtocolProtos.MultipartUploadCompleteRequest completeRequest =
        request.getCompleteMultiPartUploadRequest();
    KeyArgs.Builder keyArgs = completeRequest.getKeyArgs().toBuilder();
    updater.update(keyArgs);
    return request.toBuilder()
        .setCompleteMultiPartUploadRequest(completeRequest.toBuilder()
            .setKeyArgs(keyArgs))
        .build();
  }

  private interface KeyArgsUpdater {
    void update(KeyArgs.Builder keyArgs);
  }
}
