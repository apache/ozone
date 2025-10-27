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

package org.apache.hadoop.ozone.om.response.s3.multipart;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Test multipart upload commit part response for non-FSO (default) buckets.
 */
public class TestS3MultipartUploadCommitPartResponse
    extends TestS3MultipartResponse {

  @Test
  public void testAddDBToBatch() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    String multipartUploadID = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    createParentPath(volumeName, bucketName);
    String multipartKey = omMetadataManager
        .getMultipartKey(volumeName, bucketName, keyName, multipartUploadID);
    long clientId = Time.now();
    String openKey = getOpenKey(volumeName, bucketName, keyName, clientId);

    S3MultipartUploadCommitPartResponse s3MultipartUploadCommitPartResponse =
        getS3CommitMPUResponse(volumeName, bucketName, keyName,
            multipartUploadID, null, null,
            OzoneManagerProtocolProtos.Status.OK, openKey);

    s3MultipartUploadCommitPartResponse.addToDBBatch(omMetadataManager,
        batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    assertNull(omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey));
    assertNotNull(omMetadataManager.getMultipartInfoTable().get(multipartKey));

    // As no parts are created, so no entries should be there in delete table.
    assertEquals(0, omMetadataManager.countRowsInTable(
        omMetadataManager.getDeletedTable()));
  }

  @Test
  public void testAddDBToBatchWithParts() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    String multipartUploadID = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    createParentPath(volumeName, bucketName);

    String multipartKey = omMetadataManager
        .getMultipartKey(volumeName, bucketName, keyName, multipartUploadID);

    S3InitiateMultipartUploadResponse s3InitiateMultipartUploadResponse =
        getS3InitiateMPUResponse(volumeName, bucketName, keyName,
            multipartUploadID);

    s3InitiateMultipartUploadResponse.addToDBBatch(omMetadataManager,
        batchOperation);

    // Add some dummy parts for testing.
    // Not added any key locations, as this just test is to see entries are
    // adding to delete table or not.
    OmMultipartKeyInfo omMultipartKeyInfo =
        s3InitiateMultipartUploadResponse.getOmMultipartKeyInfo();

    PartKeyInfo part1 = createPartKeyInfo(volumeName, bucketName, keyName, 1);

    addPart(1, part1, omMultipartKeyInfo);

    long clientId = Time.now();

    String openKey = getOpenKey(volumeName, bucketName, keyName, clientId);

    S3MultipartUploadCommitPartResponse s3MultipartUploadCommitPartResponse =
        getS3CommitMPUResponse(volumeName, bucketName, keyName,
            multipartUploadID,
            omMultipartKeyInfo.getPartKeyInfo(1),
            omMultipartKeyInfo,
            OzoneManagerProtocolProtos.Status.OK, openKey);

    s3MultipartUploadCommitPartResponse.checkAndUpdateDB(omMetadataManager,
        batchOperation);

    assertNull(
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey));
    assertNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // As 1 parts are created, so 1 entry should be there in delete table.
    assertEquals(1, omMetadataManager.countRowsInTable(
        omMetadataManager.getDeletedTable()));

    String part1DeletedKeyName = omMetadataManager.getOzoneDeletePathKey(
        omMultipartKeyInfo.getPartKeyInfo(1).getPartKeyInfo().getObjectID(),
        multipartKey);

    assertNotNull(omMetadataManager.getDeletedTable().get(
        part1DeletedKeyName));

    RepeatedOmKeyInfo ro =
        omMetadataManager.getDeletedTable().get(part1DeletedKeyName);
    assertEquals(OmKeyInfo.getFromProtobuf(part1.getPartKeyInfo()),
        ro.getOmKeyInfoList().get(0));
  }

  @Test
  public void testWithMultipartUploadError() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    String multipartUploadID = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    createParentPath(volumeName, bucketName);

    String multipartKey = getMultipartOpenKey(volumeName, bucketName, keyName, multipartUploadID);

    S3InitiateMultipartUploadResponse s3InitiateMultipartUploadResponse =
        getS3InitiateMPUResponse(volumeName, bucketName, keyName,
            multipartUploadID);

    s3InitiateMultipartUploadResponse.addToDBBatch(omMetadataManager,
        batchOperation);

    // Add some dummy parts for testing.
    // Not added any key locations, as this just test is to see entries are
    // adding to delete table or not.
    OmMultipartKeyInfo omMultipartKeyInfo =
        s3InitiateMultipartUploadResponse.getOmMultipartKeyInfo();

    PartKeyInfo part1 = createPartKeyInfo(volumeName, bucketName, keyName, 1);

    addPart(1, part1, omMultipartKeyInfo);

    long clientId = Time.now();
    String openKey = getOpenKey(volumeName, bucketName, keyName, clientId);

    String keyNameInvalid = keyName + "invalid";
    S3MultipartUploadCommitPartResponse s3MultipartUploadCommitPartResponse =
        getS3CommitMPUResponse(volumeName, bucketName,
            keyNameInvalid, multipartUploadID,
            omMultipartKeyInfo.getPartKeyInfo(1),
            omMultipartKeyInfo, OzoneManagerProtocolProtos.Status
                .NO_SUCH_MULTIPART_UPLOAD_ERROR, openKey);

    s3MultipartUploadCommitPartResponse.checkAndUpdateDB(omMetadataManager,
        batchOperation);

    assertNull(
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey));
    assertNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // openkey entry should be there in delete table.
    assertEquals(1, omMetadataManager.countRowsInTable(
        omMetadataManager.getDeletedTable()));
    String deletedKey = omMetadataManager
        .getMultipartKey(volumeName, bucketName, keyNameInvalid,
            multipartUploadID);
    List<? extends Table.KeyValue<String, RepeatedOmKeyInfo>> rangeKVs =
        omMetadataManager.getDeletedTable().getRangeKVs(null, 100, deletedKey);
    assertThat(rangeKVs.size()).isGreaterThan(0);
  }

  protected String getKeyName() {
    return UUID.randomUUID().toString();
  }

  protected void createParentPath(String volumeName, String bucketName)
      throws Exception {
    // no parent hierarchy
  }

  protected String getOpenKey(String volumeName, String bucketName, String keyName, long clientId)
      throws IOException {
    return omMetadataManager.getOpenKey(volumeName, bucketName, keyName, clientId);
  }

  protected String getMultipartOpenKey(String volumeName, String bucketName,
                                       String keyName, String multipartUploadID) throws IOException {
    return omMetadataManager
        .getMultipartKey(volumeName, bucketName, keyName, multipartUploadID);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  protected S3MultipartUploadCommitPartResponse getS3CommitMPUResponse(
      String volumeName, String bucketName, String keyName,
      String multipartUploadID,
      PartKeyInfo oldPartKeyInfo,
      OmMultipartKeyInfo multipartKeyInfo,
      OzoneManagerProtocolProtos.Status status, String openKey)
      throws IOException {
    return createS3CommitMPUResponse(volumeName, bucketName, keyName,
        multipartUploadID, oldPartKeyInfo, multipartKeyInfo, status, openKey);
  }

  protected S3InitiateMultipartUploadResponse getS3InitiateMPUResponse(
      String volumeName, String bucketName, String keyName,
      String multipartUploadID
  ) throws IOException {
    return createS3InitiateMPUResponse(volumeName, bucketName, keyName,
        multipartUploadID);
  }
}
