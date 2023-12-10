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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Test multipart upload complete response.
 */
public class TestS3MultipartUploadCompleteResponseWithFSO
    extends TestS3MultipartResponse {

  private String dirName = "a/b/c/";

  private long parentID;

  @Test
  public void testAddDBToBatch() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    String multipartUploadID = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager);

    long txnId = 50;
    long objectId = parentID + 1;
    String fileName = OzoneFSUtils.getFileName(keyName);
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    String dbMultipartKey = omMetadataManager.getMultipartKey(volumeName,
            bucketName, keyName, multipartUploadID);
    String dbMultipartOpenKey = omMetadataManager.getMultipartKey(volumeId,
            bucketId, parentID, fileName, multipartUploadID);
    long clientId = Time.now();

    // add MPU entry to OpenFileTable
    List<OmDirectoryInfo> parentDirInfos = new ArrayList<>();
    S3InitiateMultipartUploadResponse s3InitiateMultipartUploadResponseFSO =
        createS3InitiateMPUResponseFSO(volumeName, bucketName, parentID,
            keyName, multipartUploadID, parentDirInfos, volumeId, bucketId);

    s3InitiateMultipartUploadResponseFSO.addToDBBatch(omMetadataManager,
        batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    String dbOpenKey = omMetadataManager.getOpenFileName(volumeId, bucketId,
            parentID, fileName, clientId);
    String dbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
            parentID, fileName);
    OmKeyInfo omKeyInfoFSO =
            OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
                    HddsProtos.ReplicationType.RATIS,
                    HddsProtos.ReplicationFactor.ONE, objectId, parentID, txnId,
                    Time.now(), true);

    // add key to openFileTable
    omKeyInfoFSO.setKeyName(fileName);
    OMRequestTestUtils.addFileToKeyTable(true, false,
            fileName, omKeyInfoFSO, clientId, omKeyInfoFSO.getObjectID(),
            omMetadataManager);

    addS3MultipartUploadCommitPartResponseFSO(volumeName, bucketName, keyName,
            multipartUploadID, dbOpenKey);

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
        omMetadataManager.getBucketTable().get(bucketKey);

    Assertions.assertNotNull(
        omMetadataManager.getMultipartInfoTable().get(dbMultipartKey));
    Assertions.assertNotNull(omMetadataManager.getOpenKeyTable(
        getBucketLayout()).get(dbMultipartOpenKey));

    List<OmKeyInfo> unUsedParts = new ArrayList<>();
    S3MultipartUploadCompleteResponse s3MultipartUploadCompleteResponse =
            createS3CompleteMPUResponseFSO(volumeName, bucketName, parentID,
                keyName, multipartUploadID, omKeyInfoFSO,
                OzoneManagerProtocolProtos.Status.OK, unUsedParts,
                omBucketInfo);

    s3MultipartUploadCompleteResponse.addToDBBatch(omMetadataManager,
        batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assertions.assertNotNull(
        omMetadataManager.getKeyTable(getBucketLayout()).get(dbKey));
    Assertions.assertNull(
        omMetadataManager.getMultipartInfoTable().get(dbMultipartKey));
    Assertions.assertNull(omMetadataManager.getOpenKeyTable(getBucketLayout())
        .get(dbMultipartOpenKey));

    // As no parts are created, so no entries should be there in delete table.
    Assertions.assertEquals(0, omMetadataManager.countRowsInTable(
            omMetadataManager.getDeletedTable()));
  }



  @Test
  // similar to testAddDBToBatch(), but omBucketInfo is null
  public void testAddDBToBatchWithNullBucketInfo() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    String multipartUploadID = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    long txnId = 150;
    long objectId = parentID + 1;
    String fileName = OzoneFSUtils.getFileName(keyName);
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
        bucketName);
    String dbMultipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);
    String dbMultipartOpenKey = omMetadataManager.getMultipartKey(volumeId,
        bucketId, parentID, fileName, multipartUploadID);
    long clientId = Time.now();

    // add MPU entry to OpenFileTable
    List<OmDirectoryInfo> parentDirInfos = new ArrayList<>();
    S3InitiateMultipartUploadResponse s3InitiateMultipartUploadResponseFSO =
        createS3InitiateMPUResponseFSO(volumeName, bucketName, parentID,
            keyName, multipartUploadID, parentDirInfos, volumeId, bucketId);

    s3InitiateMultipartUploadResponseFSO.addToDBBatch(omMetadataManager,
        batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    String dbOpenKey = omMetadataManager.getOpenFileName(volumeId, bucketId,
        parentID, fileName, clientId);
    String dbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
        parentID, fileName);
    OmKeyInfo omKeyInfoFSO =
        OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
            HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.ONE, objectId, parentID, txnId,
            Time.now(), true);

    // add key to openFileTable
    omKeyInfoFSO.setKeyName(fileName);
    OMRequestTestUtils.addFileToKeyTable(true, false,
        fileName, omKeyInfoFSO, clientId, omKeyInfoFSO.getObjectID(),
        omMetadataManager);

    addS3MultipartUploadCommitPartResponseFSO(volumeName, bucketName, keyName,
        multipartUploadID, dbOpenKey);

    Assertions.assertNotNull(
        omMetadataManager.getMultipartInfoTable().get(dbMultipartKey));
    Assertions.assertNotNull(omMetadataManager.getOpenKeyTable(
        getBucketLayout()).get(dbMultipartOpenKey));

    // S3MultipartUploadCompleteResponseWithFSO should accept null bucketInfo
    List<OmKeyInfo> unUsedParts = new ArrayList<>();
    S3MultipartUploadCompleteResponse s3MultipartUploadCompleteResponse =
        createS3CompleteMPUResponseFSO(volumeName, bucketName, parentID,
            keyName, multipartUploadID, omKeyInfoFSO,
            OzoneManagerProtocolProtos.Status.OK, unUsedParts,
            null);

    s3MultipartUploadCompleteResponse.addToDBBatch(omMetadataManager,
        batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assertions.assertNotNull(
        omMetadataManager.getKeyTable(getBucketLayout()).get(dbKey));
    Assertions.assertNull(
        omMetadataManager.getMultipartInfoTable().get(dbMultipartKey));
    Assertions.assertNull(omMetadataManager.getOpenKeyTable(getBucketLayout())
        .get(dbMultipartOpenKey));

    // As no parts are created, so no entries should be there in delete table.
    Assertions.assertEquals(0, omMetadataManager.countRowsInTable(
        omMetadataManager.getDeletedTable()));
  }

  @Test
  public void testAddDBToBatchWithParts() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager);
    createParentPath(volumeName, bucketName);
    runAddDBToBatchWithParts(volumeName, bucketName, keyName, 0);

    // As 1 unused parts exists, so 1 unused entry should be there in delete
    // table.
    Assertions.assertEquals(2, omMetadataManager.countRowsInTable(
            omMetadataManager.getDeletedTable()));
  }

  @Test
  public void testAddDBToBatchWithPartsWithKeyInDeleteTable() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager);
    createParentPath(volumeName, bucketName);

    // Put an entry to delete table with the same key prior to multipart commit
    OmKeyInfo prevKey = OMRequestTestUtils.createOmKeyInfo(volumeName,
            bucketName, keyName,
            HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.ONE,
            parentID + 8,
            parentID, 8, Time.now(), true);
    RepeatedOmKeyInfo prevKeys = new RepeatedOmKeyInfo(prevKey);
    String ozoneKey = omMetadataManager
            .getOzoneKey(prevKey.getVolumeName(),
                    prevKey.getBucketName(), prevKey.getFileName());
    omMetadataManager.getDeletedTable().put(ozoneKey, prevKeys);

    long oId = runAddDBToBatchWithParts(volumeName, bucketName, keyName, 1);

    // Make sure new object isn't in delete table
    RepeatedOmKeyInfo ds = omMetadataManager.getDeletedTable().get(ozoneKey);
    for (OmKeyInfo omKeyInfo : ds.getOmKeyInfoList()) {
      Assertions.assertNotEquals(oId, omKeyInfo.getObjectID());
    }

    // As 1 unused parts and 1 previously put-and-deleted object exist,
    // so 2 entries should be there in delete table.
    Assertions.assertEquals(3, omMetadataManager.countRowsInTable(
            omMetadataManager.getDeletedTable()));
  }

  private long runAddDBToBatchWithParts(String volumeName,
      String bucketName, String keyName, int deleteEntryCount)
          throws Exception {

    String multipartUploadID = UUID.randomUUID().toString();
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);

    String fileName = OzoneFSUtils.getFileName(keyName);
    String dbMultipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);
    String dbMultipartOpenKey = omMetadataManager.getMultipartKey(volumeId,
            bucketId, parentID, fileName, multipartUploadID);

    S3InitiateMultipartUploadResponse s3InitiateMultipartUploadResponseFSO =
            addS3InitiateMultipartUpload(volumeName, bucketName, keyName,
                    multipartUploadID, volumeId, bucketId);

    // Add some dummy parts for testing.
    // Not added any key locations, as this just test is to see entries are
    // adding to delete table or not.
    OmMultipartKeyInfo omMultipartKeyInfo =
            s3InitiateMultipartUploadResponseFSO.getOmMultipartKeyInfo();

    // After commits, it adds an entry to the deleted table. Incrementing the
    // variable before the method call, because this method also has entry
    // count check inside.
    deleteEntryCount++;
    OmKeyInfo omKeyInfoFSO = commitS3MultipartUpload(volumeName, bucketName,
            keyName, multipartUploadID, fileName, dbMultipartKey,
            omMultipartKeyInfo, deleteEntryCount);

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);

    OmKeyInfo omKeyInfo =
            OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
                    HddsProtos.ReplicationType.RATIS,
                    HddsProtos.ReplicationFactor.ONE,
                    parentID + 9,
                    parentID, 100, Time.now(), true);
    List<OmKeyInfo> unUsedParts = new ArrayList<>();
    unUsedParts.add(omKeyInfo);
    S3MultipartUploadCompleteResponse s3MultipartUploadCompleteResponse =
            createS3CompleteMPUResponseFSO(volumeName, bucketName, parentID,
                    keyName, multipartUploadID, omKeyInfoFSO,
                    OzoneManagerProtocolProtos.Status.OK, unUsedParts,
                    omBucketInfo);

    s3MultipartUploadCompleteResponse.addToDBBatch(omMetadataManager,
            batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    String dbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
            parentID, omKeyInfoFSO.getFileName());
    Assertions.assertNotNull(
        omMetadataManager.getKeyTable(getBucketLayout()).get(dbKey));
    Assertions.assertNull(
            omMetadataManager.getMultipartInfoTable().get(dbMultipartKey));
    Assertions.assertNull(omMetadataManager.getOpenKeyTable(getBucketLayout())
        .get(dbMultipartOpenKey));

    return omKeyInfoFSO.getObjectID();
  }

  @SuppressWarnings("parameterNumber")
  private OmKeyInfo commitS3MultipartUpload(String volumeName,
      String bucketName, String keyName, String multipartUploadID,
      String fileName, String multipartKey,
      OmMultipartKeyInfo omMultipartKeyInfo,
      int deleteEntryCount) throws IOException {

    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);

    PartKeyInfo part1 = createPartKeyInfoFSO(volumeName, bucketName, parentID,
        fileName, 1);

    addPart(1, part1, omMultipartKeyInfo);

    long clientId = Time.now();
    String openKey = omMetadataManager.getOpenFileName(volumeId, bucketId,
            parentID, fileName, clientId);

    S3MultipartUploadCommitPartResponse s3MultipartUploadCommitPartResponse =
            createS3CommitMPUResponseFSO(volumeName, bucketName, parentID,
                    keyName, multipartUploadID,
                    omMultipartKeyInfo.getPartKeyInfo(1),
                    omMultipartKeyInfo,
                    OzoneManagerProtocolProtos.Status.OK,  openKey);

    s3MultipartUploadCommitPartResponse.checkAndUpdateDB(omMetadataManager,
            batchOperation);

    Assertions.assertNull(
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(multipartKey));
    Assertions.assertNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // As 1 parts are created, so 1 entry should be there in delete table.
    Assertions.assertEquals(deleteEntryCount,
        omMetadataManager.countRowsInTable(
        omMetadataManager.getDeletedTable()));

    String part1DeletedKeyName = omMetadataManager.getOzoneDeletePathKey(
        omMultipartKeyInfo.getPartKeyInfo(1).getPartKeyInfo().getObjectID(),
        multipartKey);

    Assertions.assertNotNull(omMetadataManager.getDeletedTable().get(
        part1DeletedKeyName));

    RepeatedOmKeyInfo ro =
        omMetadataManager.getDeletedTable().get(part1DeletedKeyName);
    OmKeyInfo omPartKeyInfo = OmKeyInfo.getFromProtobuf(part1.getPartKeyInfo());
    Assertions.assertEquals(omPartKeyInfo, ro.getOmKeyInfoList().get(0));

    return omPartKeyInfo;
  }

  private S3InitiateMultipartUploadResponse addS3InitiateMultipartUpload(
          String volumeName, String bucketName, String keyName,
          String multipartUploadID, long volumeId,
          long bucketId) throws IOException {

    S3InitiateMultipartUploadResponse s3InitiateMultipartUploadResponseFSO =
        createS3InitiateMPUResponseFSO(volumeName, bucketName, parentID,
            keyName, multipartUploadID, new ArrayList<>(), volumeId,
            bucketId);

    s3InitiateMultipartUploadResponseFSO.addToDBBatch(omMetadataManager,
            batchOperation);

    return s3InitiateMultipartUploadResponseFSO;
  }

  private String getKeyName() {
    return dirName + UUID.randomUUID().toString();
  }

  private void createParentPath(String volumeName, String bucketName)
      throws Exception {
    // Create parent dirs for the path
    parentID = OMRequestTestUtils.addParentsToDirTable(volumeName, bucketName,
            dirName, omMetadataManager);
  }

  private void addS3MultipartUploadCommitPartResponseFSO(String volumeName,
      String bucketName, String keyName, String multipartUploadID,
      String openKey) throws IOException {
    S3MultipartUploadCommitPartResponse s3MultipartUploadCommitPartResponse =
            createS3CommitMPUResponseFSO(volumeName, bucketName, parentID,
                    keyName, multipartUploadID, null, null,
                    OzoneManagerProtocolProtos.Status.OK, openKey);

    s3MultipartUploadCommitPartResponse.addToDBBatch(omMetadataManager,
            batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
