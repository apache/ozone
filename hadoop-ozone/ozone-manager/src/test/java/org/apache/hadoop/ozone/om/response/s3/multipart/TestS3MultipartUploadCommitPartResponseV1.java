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

package org.apache.hadoop.ozone.om.response.s3.multipart;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.UUID;

/**
 * Test multipart upload commit part response.
 */
public class TestS3MultipartUploadCommitPartResponseV1
    extends TestS3MultipartResponse {

  private String dirName = "a/b/c/";

  private long parentID;

  @Test
  public void testAddDBToBatch() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    String multipartUploadID = UUID.randomUUID().toString();

    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager);

    createParentPath(volumeName, bucketName);
    String fileName = OzoneFSUtils.getFileName(keyName);
    String multipartKey = omMetadataManager.getMultipartKey(parentID, fileName,
            multipartUploadID);
    long clientId = Time.now();
    String openKey = omMetadataManager.getOpenFileName(parentID, fileName,
            clientId);

    S3MultipartUploadCommitPartResponse s3MultipartUploadCommitPartResponse =
        createS3CommitMPUResponseV1(volumeName, bucketName, parentID, keyName,
            multipartUploadID, null, null,
                OzoneManagerProtocolProtos.Status.OK, openKey);

    s3MultipartUploadCommitPartResponse.addToDBBatch(omMetadataManager,
        batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertNull(omMetadataManager.getOpenKeyTable().get(multipartKey));
    Assert.assertNotNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));

    // As no parts are created, so no entries should be there in delete table.
    Assert.assertEquals(0, omMetadataManager.countRowsInTable(
            omMetadataManager.getDeletedTable()));
  }

  @Test
  public void testAddDBToBatchWithParts() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager);
    createParentPath(volumeName, bucketName);

    String multipartUploadID = UUID.randomUUID().toString();

    String fileName = OzoneFSUtils.getFileName(keyName);
    String multipartKey = omMetadataManager.getMultipartKey(parentID, fileName,
            multipartUploadID);

    S3InitiateMultipartUploadResponse s3InitiateMultipartUploadResponseV1 =
            createS3InitiateMPUResponseV1(volumeName, bucketName, parentID,
                    keyName, multipartUploadID, new ArrayList<>());

    s3InitiateMultipartUploadResponseV1.addToDBBatch(omMetadataManager,
            batchOperation);

    // Add some dummy parts for testing.
    // Not added any key locations, as this just test is to see entries are
    // adding to delete table or not.
    OmMultipartKeyInfo omMultipartKeyInfo =
            s3InitiateMultipartUploadResponseV1.getOmMultipartKeyInfo();

    PartKeyInfo part1 = createPartKeyInfoV1(volumeName, bucketName, parentID,
        fileName, 1);

    addPart(1, part1, omMultipartKeyInfo);

    long clientId = Time.now();
    String openKey = omMetadataManager.getOpenFileName(parentID, fileName,
            clientId);

    S3MultipartUploadCommitPartResponse s3MultipartUploadCommitPartResponse =
            createS3CommitMPUResponseV1(volumeName, bucketName, parentID,
                    keyName, multipartUploadID,
                    omMultipartKeyInfo.getPartKeyInfo(1),
                    omMultipartKeyInfo,
                    OzoneManagerProtocolProtos.Status.OK,  openKey);

    s3MultipartUploadCommitPartResponse.checkAndUpdateDB(omMetadataManager,
            batchOperation);

    Assert.assertNull(omMetadataManager.getOpenKeyTable().get(multipartKey));
    Assert.assertNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // As 1 parts are created, so 1 entry should be there in delete table.
    Assert.assertEquals(1, omMetadataManager.countRowsInTable(
        omMetadataManager.getDeletedTable()));

    String part1DeletedKeyName =
        omMultipartKeyInfo.getPartKeyInfo(1).getPartName();

    Assert.assertNotNull(omMetadataManager.getDeletedTable().get(
        part1DeletedKeyName));

    RepeatedOmKeyInfo ro =
        omMetadataManager.getDeletedTable().get(part1DeletedKeyName);
    Assert.assertEquals(OmKeyInfo.getFromProtobuf(part1.getPartKeyInfo()),
        ro.getOmKeyInfoList().get(0));
  }

  @Test
  public void testWithMultipartUploadError() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager);
    createParentPath(volumeName, bucketName);

    String multipartUploadID = UUID.randomUUID().toString();

    String fileName = OzoneFSUtils.getFileName(keyName);
    String multipartKey = omMetadataManager.getMultipartKey(parentID, fileName,
            multipartUploadID);

    S3InitiateMultipartUploadResponse s3InitiateMultipartUploadResponseV1 =
            createS3InitiateMPUResponseV1(volumeName, bucketName, parentID,
                    keyName, multipartUploadID, new ArrayList<>());

    s3InitiateMultipartUploadResponseV1.addToDBBatch(omMetadataManager,
            batchOperation);

    // Add some dummy parts for testing.
    // Not added any key locations, as this just test is to see entries are
    // adding to delete table or not.
    OmMultipartKeyInfo omMultipartKeyInfo =
            s3InitiateMultipartUploadResponseV1.getOmMultipartKeyInfo();

    PartKeyInfo part1 = createPartKeyInfoV1(volumeName, bucketName, parentID,
            fileName, 1);

    addPart(1, part1, omMultipartKeyInfo);

    long clientId = Time.now();
    String openKey = omMetadataManager.getOpenFileName(parentID, fileName,
            clientId);

    S3MultipartUploadCommitPartResponse s3MultipartUploadCommitPartResponse =
            createS3CommitMPUResponseV1(volumeName, bucketName, parentID,
                    keyName + "invalid", multipartUploadID,
                    omMultipartKeyInfo.getPartKeyInfo(1),
                    omMultipartKeyInfo, OzoneManagerProtocolProtos.Status
                            .NO_SUCH_MULTIPART_UPLOAD_ERROR, openKey);

    s3MultipartUploadCommitPartResponse.checkAndUpdateDB(omMetadataManager,
            batchOperation);

    Assert.assertNull(omMetadataManager.getOpenKeyTable().get(multipartKey));
    Assert.assertNull(
            omMetadataManager.getMultipartInfoTable().get(multipartKey));

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // openkey entry should be there in delete table.
    Assert.assertEquals(1, omMetadataManager.countRowsInTable(
            omMetadataManager.getDeletedTable()));

    Assert.assertNotNull(omMetadataManager.getDeletedTable().get(
            openKey));
  }

  private String getKeyName() {
    return dirName + UUID.randomUUID().toString();
  }

  private void createParentPath(String volumeName, String bucketName)
      throws Exception {
    // Create parent dirs for the path
    parentID = TestOMRequestUtils.addParentsToDirTable(volumeName, bucketName,
            dirName, omMetadataManager);
  }
}
