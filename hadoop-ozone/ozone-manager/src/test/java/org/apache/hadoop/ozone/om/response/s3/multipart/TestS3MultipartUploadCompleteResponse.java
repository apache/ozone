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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.util.OMMultipartUploadUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Test multipart upload complete response.
 */
public class TestS3MultipartUploadCompleteResponse
    extends TestS3MultipartResponse {

  @Test
  public void testAddDBToBatch() throws Exception {
    runAddDBToBatch(true);
  }

  @Test
  // similar to testAddDBToBatch(), but omBucketInfo is null
  public void testAddDBToBatchWithNullBucketInfo() throws Exception {
    runAddDBToBatch(false);
  }

  private void runAddDBToBatch(boolean withBucketInfo) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    String multipartUploadID = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager);
    createParentPath(volumeName, bucketName);

    String dbMultipartKey = omMetadataManager.getMultipartKey(volumeName,
            bucketName, keyName, multipartUploadID);
    String dbMultipartOpenKey = getMultipartOpenKey(volumeName, bucketName,
            keyName, multipartUploadID);

    // add MPU entry to open table and multipart info table
    S3InitiateMultipartUploadResponse s3InitiateMultipartUploadResponse =
        createInitiateMPUResponse(volumeName, bucketName, keyName,
            multipartUploadID);
    s3InitiateMultipartUploadResponse.addToDBBatch(omMetadataManager,
        batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // commit a part without any overwritten part
    OmMultipartKeyInfo omMultipartKeyInfo =
        s3InitiateMultipartUploadResponse.getOmMultipartKeyInfo();
    addCommittedPart(volumeName, bucketName, keyName, multipartUploadID,
        omMultipartKeyInfo);

    OmKeyInfo omKeyInfo = createCompletedKeyInfo(volumeName, bucketName,
        keyName, 1000, 50);

    OmBucketInfo omBucketInfo = withBucketInfo ? omMetadataManager
        .getBucketTable().get(omMetadataManager
            .getBucketKey(volumeName, bucketName)) : null;

    assertNotNull(omMetadataManager.getMultipartInfoTable().get(dbMultipartKey));
    assertNotNull(omMetadataManager.getOpenKeyTable(
        getBucketLayout()).get(dbMultipartOpenKey));

    List<OmKeyInfo> unUsedParts = new ArrayList<>();
    S3MultipartUploadCompleteResponse s3MultipartUploadCompleteResponse =
            createCompleteMPUResponse(volumeName, bucketName, keyName,
                multipartUploadID, omKeyInfo,
                OzoneManagerProtocolProtos.Status.OK, unUsedParts,
                omBucketInfo);

    s3MultipartUploadCompleteResponse.addToDBBatch(omMetadataManager,
        batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    assertNotNull(omMetadataManager.getKeyTable(getBucketLayout())
        .get(getFinalDbKey(omKeyInfo)));
    assertNull(omMetadataManager.getMultipartInfoTable().get(dbMultipartKey));
    assertNull(omMetadataManager.getOpenKeyTable(getBucketLayout())
        .get(dbMultipartOpenKey));

    // As no parts are unused, so no entries should be there in delete table.
    assertEquals(0, omMetadataManager.countRowsInTable(
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

    // As 1 unused part exists, so 1 unused entry should be there in delete
    // table, in addition to the 1 overwritten part committed earlier.
    assertEquals(2, omMetadataManager.countRowsInTable(
            omMetadataManager.getDeletedTable()));
  }

  @Test
  public void testAddDBToBatchWithPartsWithKeyInDeleteTable() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OmBucketInfo bucketInfo = OMRequestTestUtils.addVolumeAndBucketToDB(
        volumeName, bucketName, omMetadataManager);
    createParentPath(volumeName, bucketName);

    // Put an entry to delete table with the same key prior to multipart commit
    OmKeyInfo prevKey = OMRequestTestUtils.createOmKeyInfo(volumeName,
            bucketName, keyName, RatisReplicationConfig.getInstance(ONE),
            new OmKeyLocationInfoGroup(0L, new ArrayList<>(), true))
        .setObjectID(8)
        .setUpdateID(8)
        .build();
    RepeatedOmKeyInfo prevKeys = new RepeatedOmKeyInfo(prevKey,
        bucketInfo.getObjectID());
    String ozoneKey = omMetadataManager
        .getOzoneKey(prevKey.getVolumeName(),
            prevKey.getBucketName(), prevKey.getFileName());
    omMetadataManager.getDeletedTable().put(ozoneKey, prevKeys);

    long oId = runAddDBToBatchWithParts(volumeName, bucketName, keyName, 1);

    // Make sure new object isn't in delete table
    RepeatedOmKeyInfo ds = omMetadataManager.getDeletedTable().get(ozoneKey);
    for (OmKeyInfo omKeyInfo : ds.getOmKeyInfoList()) {
      assertNotEquals(oId, omKeyInfo.getObjectID());
    }

    // As 1 unused part, 1 overwritten part and 1 previously put-and-deleted
    // object exist, so 3 entries should be there in delete table.
    assertEquals(3, omMetadataManager.countRowsInTable(
            omMetadataManager.getDeletedTable()));
  }

  private long runAddDBToBatchWithParts(String volumeName,
      String bucketName, String keyName, int deleteEntryCount)
          throws Exception {

    String multipartUploadID = UUID.randomUUID().toString();

    String dbMultipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);
    String dbMultipartOpenKey = getMultipartOpenKey(volumeName, bucketName,
        keyName, multipartUploadID);

    S3InitiateMultipartUploadResponse s3InitiateMultipartUploadResponse =
        createInitiateMPUResponse(volumeName, bucketName, keyName,
            multipartUploadID);
    s3InitiateMultipartUploadResponse.addToDBBatch(omMetadataManager,
            batchOperation);

    OmMultipartKeyInfo omMultipartKeyInfo =
            s3InitiateMultipartUploadResponse.getOmMultipartKeyInfo();

    // After commit, it adds an entry to the deleted table. Incrementing the
    // variable before the method call, because this method also has entry
    // count check inside.
    deleteEntryCount++;
    OmKeyInfo committedPartKeyInfo = commitOnePart(volumeName, bucketName,
        keyName, multipartUploadID, dbMultipartKey, omMultipartKeyInfo,
        deleteEntryCount);

    OmBucketInfo omBucketInfo = omMetadataManager.getBucketTable()
        .get(omMetadataManager.getBucketKey(volumeName, bucketName));

    // 1 unused part that should be moved to the deleted table on completion.
    OmKeyInfo unUsedPartKeyInfo =
        OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
                RatisReplicationConfig.getInstance(ONE),
                new OmKeyLocationInfoGroup(0L, new ArrayList<>(), true))
            .setObjectID(9)
            .setUpdateID(100)
            .build();
    List<OmKeyInfo> unUsedParts = new ArrayList<>();
    unUsedParts.add(unUsedPartKeyInfo);
    S3MultipartUploadCompleteResponse s3MultipartUploadCompleteResponse =
            createCompleteMPUResponse(volumeName, bucketName, keyName,
                multipartUploadID, committedPartKeyInfo,
                OzoneManagerProtocolProtos.Status.OK, unUsedParts,
                omBucketInfo);

    s3MultipartUploadCompleteResponse.addToDBBatch(omMetadataManager,
            batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    assertNotNull(omMetadataManager.getKeyTable(getBucketLayout())
        .get(getFinalDbKey(committedPartKeyInfo)));
    assertNull(omMetadataManager.getMultipartInfoTable().get(dbMultipartKey));
    assertNull(omMetadataManager.getOpenKeyTable(getBucketLayout())
        .get(dbMultipartOpenKey));

    return committedPartKeyInfo.getObjectID();
  }

  /**
   * Commit a single part with an overwritten part and assert that the
   * overwritten part is moved to the deleted table.
   *
   * @return the committed part key info, used as the completed key.
   */
  private OmKeyInfo commitOnePart(String volumeName, String bucketName,
      String keyName, String multipartUploadID, String dbMultipartKey,
      OmMultipartKeyInfo omMultipartKeyInfo, int deleteEntryCount)
      throws Exception {

    PartKeyInfo part1 = createPartKeyInfo(volumeName, bucketName, keyName, 1);

    addPart(1, part1, omMultipartKeyInfo);

    long clientId = Time.now();
    String openKey = getPartOpenKey(volumeName, bucketName, keyName, clientId);

    S3MultipartUploadCommitPartResponse s3MultipartUploadCommitPartResponse =
            createCommitMPUResponse(volumeName, bucketName, keyName,
                    multipartUploadID,
                    omMultipartKeyInfo.getPartKeyInfo(1),
                    omMultipartKeyInfo,
                    OzoneManagerProtocolProtos.Status.OK, openKey);

    s3MultipartUploadCommitPartResponse.checkAndUpdateDB(omMetadataManager,
            batchOperation);

    assertNull(
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey));
    assertNull(
        omMetadataManager.getMultipartInfoTable().get(dbMultipartKey));

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // The overwritten part is added to the deleted table.
    assertEquals(deleteEntryCount,
        omMetadataManager.countRowsInTable(
        omMetadataManager.getDeletedTable()));

    String part1DeletedKeyName = omMetadataManager.getOzoneDeletePathKey(
        omMultipartKeyInfo.getPartKeyInfo(1).getPartKeyInfo().getObjectID(),
        dbMultipartKey);

    assertNotNull(omMetadataManager.getDeletedTable().get(
        part1DeletedKeyName));

    RepeatedOmKeyInfo ro =
        omMetadataManager.getDeletedTable().get(part1DeletedKeyName);
    OmKeyInfo omPartKeyInfo = OmKeyInfo.getFromProtobuf(part1.getPartKeyInfo());
    assertEquals(omPartKeyInfo, ro.getOmKeyInfoList().get(0));

    return omPartKeyInfo;
  }

  /**
   * Commit a single part without an overwritten part. Used by the
   * {@link #runAddDBToBatch(boolean)} flow.
   */
  private void addCommittedPart(String volumeName, String bucketName,
      String keyName, String multipartUploadID,
      OmMultipartKeyInfo omMultipartKeyInfo) throws Exception {
    long clientId = Time.now();
    String openKey = getPartOpenKey(volumeName, bucketName, keyName, clientId);

    S3MultipartUploadCommitPartResponse s3MultipartUploadCommitPartResponse =
            createCommitMPUResponse(volumeName, bucketName, keyName,
                    multipartUploadID, null, omMultipartKeyInfo,
                    OzoneManagerProtocolProtos.Status.OK, openKey);

    s3MultipartUploadCommitPartResponse.addToDBBatch(omMetadataManager,
            batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);
  }

  protected String getKeyName() {
    return UUID.randomUUID().toString();
  }

  /**
   * Set up the parent path. No-op for legacy/OBS buckets; FSO buckets
   * override this to create the parent directories.
   */
  protected void createParentPath(String volumeName, String bucketName)
      throws Exception {
  }

  protected String getMultipartOpenKey(String volumeName, String bucketName,
      String keyName, String multipartUploadID) throws IOException {
    return OMMultipartUploadUtils.getMultipartOpenKey(volumeName, bucketName,
        keyName, multipartUploadID, omMetadataManager, getBucketLayout());
  }

  protected String getPartOpenKey(String volumeName, String bucketName,
      String keyName, long clientId) throws IOException {
    return omMetadataManager.getOpenKey(volumeName, bucketName, keyName,
        String.valueOf(clientId));
  }

  protected String getFinalDbKey(OmKeyInfo omKeyInfo) throws IOException {
    return omMetadataManager.getOzoneKey(omKeyInfo.getVolumeName(),
        omKeyInfo.getBucketName(), omKeyInfo.getKeyName());
  }

  protected OmKeyInfo createCompletedKeyInfo(String volumeName,
      String bucketName, String keyName, long objectId, long txnId) {
    return OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
            RatisReplicationConfig.getInstance(ONE),
            new OmKeyLocationInfoGroup(0L, new ArrayList<>(), true))
        .setObjectID(objectId)
        .setUpdateID(txnId)
        .build();
  }

  protected S3InitiateMultipartUploadResponse createInitiateMPUResponse(
      String volumeName, String bucketName, String keyName,
      String multipartUploadID) throws IOException {
    return createS3InitiateMPUResponse(volumeName, bucketName, keyName,
        multipartUploadID);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  protected S3MultipartUploadCommitPartResponse createCommitMPUResponse(
      String volumeName, String bucketName, String keyName,
      String multipartUploadID, PartKeyInfo oldPartKeyInfo,
      OmMultipartKeyInfo multipartKeyInfo,
      OzoneManagerProtocolProtos.Status status, String openKey)
      throws IOException {
    return createS3CommitMPUResponse(volumeName, bucketName, keyName,
        multipartUploadID, oldPartKeyInfo, multipartKeyInfo, status, openKey);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  protected S3MultipartUploadCompleteResponse createCompleteMPUResponse(
      String volumeName, String bucketName, String keyName,
      String multipartUploadID, OmKeyInfo omKeyInfo,
      OzoneManagerProtocolProtos.Status status,
      List<OmKeyInfo> allKeyInfoToRemove, OmBucketInfo omBucketInfo)
      throws IOException {
    return createS3CompleteMPUResponse(volumeName, bucketName, keyName,
        multipartUploadID, omKeyInfo, status, allKeyInfoToRemove, omBucketInfo);
  }
}
