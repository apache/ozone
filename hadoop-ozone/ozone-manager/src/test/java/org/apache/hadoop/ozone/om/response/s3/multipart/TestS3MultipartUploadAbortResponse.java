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

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .PartKeyInfo;
import org.apache.hadoop.util.Time;

/**
 * Test multipart upload abort response.
 */
public class TestS3MultipartUploadAbortResponse
    extends TestS3MultipartResponse {


  @Test
  public void testAddDBToBatch() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    String multipartUploadID = UUID.randomUUID().toString();

    addVolumeToDB(volumeName);
    addBucketToDB(volumeName, bucketName);
    String multipartOpenKey = getMultipartOpenKey(volumeName, bucketName,
        keyName, multipartUploadID);

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    String buckDBKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
        omMetadataManager.getBucketTable().get(buckDBKey);
    long volumeId = omMetadataManager.getVolumeId(volumeName);
    S3InitiateMultipartUploadResponse s3InitiateMultipartUploadResponse =
        getS3InitiateMultipartUploadResponse(volumeName, bucketName, keyName,
            multipartUploadID, volumeId, omBucketInfo.getObjectID());

    s3InitiateMultipartUploadResponse.addToDBBatch(omMetadataManager,
        batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // Make sure key is present in OpenKeyTable and MPU table before Abort.
    Assert.assertNotNull(
        omMetadataManager.getOpenKeyTable(getBucketLayout())
            .get(multipartOpenKey));
    Assert.assertNotNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));

    S3MultipartUploadAbortResponse s3MultipartUploadAbortResponse =
        createS3AbortMPUResponse(multipartKey, multipartOpenKey,
            s3InitiateMultipartUploadResponse.getOmMultipartKeyInfo(),
            omBucketInfo);

    s3MultipartUploadAbortResponse.addToDBBatch(omMetadataManager,
        batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // Key should be deleted from OpenKeyTable and MPU table after Abort.
    Assert.assertNull(
        omMetadataManager.getOpenKeyTable(getBucketLayout())
            .get(multipartOpenKey));
    Assert.assertNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));

    // As no parts are created, so no entries should be there in delete table.
    Assert.assertTrue(omMetadataManager.countRowsInTable(
        omMetadataManager.getDeletedTable()) == 0);
  }

  protected S3InitiateMultipartUploadResponse
        getS3InitiateMultipartUploadResponse(
      String volumeName, String bucketName, String keyName,
      String multipartUploadID, long volumeId, long bucketId) {
    return createS3InitiateMPUResponse(volumeName, bucketName, keyName,
        multipartUploadID);
  }

  @Test
  public void testAddDBToBatchWithParts() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    String multipartUploadID = UUID.randomUUID().toString();
    addVolumeToDB(volumeName);
    addBucketToDB(volumeName, bucketName);
    String multipartOpenKey = getMultipartOpenKey(volumeName, bucketName,
        keyName, multipartUploadID);
    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    OmBucketInfo omBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setCreationTime(Time.now()).build();

    S3InitiateMultipartUploadResponse s3InitiateMultipartUploadResponse =
        createS3InitiateMPUResponse(volumeName, bucketName, keyName,
            multipartUploadID);

    s3InitiateMultipartUploadResponse.addToDBBatch(omMetadataManager,
        batchOperation);


    // Add some dummy parts for testing.
    // Not added any key locations, as this just test is to see entries are
    // adding to delete table or not.

    OmMultipartKeyInfo omMultipartKeyInfo =
        s3InitiateMultipartUploadResponse.getOmMultipartKeyInfo();

    PartKeyInfo part1 = createPartKeyInfo(volumeName, bucketName,
        keyName, 1);
    PartKeyInfo part2 = createPartKeyInfo(volumeName, bucketName,
        keyName, 2);

    addPart(1, part1, omMultipartKeyInfo);
    addPart(2, part2, omMultipartKeyInfo);


    S3MultipartUploadAbortResponse s3MultipartUploadAbortResponse =
        createS3AbortMPUResponse(multipartKey, multipartOpenKey,
            s3InitiateMultipartUploadResponse.getOmMultipartKeyInfo(),
            omBucketInfo);

    s3MultipartUploadAbortResponse.addToDBBatch(omMetadataManager,
        batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertNull(
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(multipartKey));
    Assert.assertNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));

    // As 2 parts are created, so 2 entries should be there in delete table.
    Assert.assertTrue(omMetadataManager.countRowsInTable(
        omMetadataManager.getDeletedTable()) == 2);

    String part1DeletedKeyName =
        omMultipartKeyInfo.getPartKeyInfo(1).getPartName();

    String part2DeletedKeyName =
        omMultipartKeyInfo.getPartKeyInfo(2).getPartName();

    Assert.assertNotNull(omMetadataManager.getDeletedTable().get(
        part1DeletedKeyName));
    Assert.assertNotNull(omMetadataManager.getDeletedTable().get(
        part2DeletedKeyName));

    RepeatedOmKeyInfo ro =
        omMetadataManager.getDeletedTable().get(part1DeletedKeyName);
    Assert.assertEquals(OmKeyInfo.getFromProtobuf(part1.getPartKeyInfo()),
        ro.getOmKeyInfoList().get(0));

    ro = omMetadataManager.getDeletedTable().get(part2DeletedKeyName);
    Assert.assertEquals(OmKeyInfo.getFromProtobuf(part2.getPartKeyInfo()),
        ro.getOmKeyInfoList().get(0));
  }

  protected String getKeyName() {
    return UUID.randomUUID().toString();
  }

  protected String getMultipartOpenKey(String volumeName, String bucketName,
      String keyName, String multipartUploadID) throws IOException {
    return omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);
  }

}
