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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;

/**
 * Test multipart upload complete response.
 */
public class TestS3MultipartUploadCompleteResponseWithFSO
    extends TestS3MultipartUploadCompleteResponse {

  private String dirName = "a/b/c/";

  private long parentID;

  @Override
  protected String getKeyName() {
    return dirName + UUID.randomUUID().toString();
  }

  @Override
  protected void createParentPath(String volumeName, String bucketName)
      throws Exception {
    // Create parent dirs for the path
    parentID = OMRequestTestUtils.addParentsToDirTable(volumeName, bucketName,
        dirName, omMetadataManager);
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  @Override
  protected String getOpenKey(String volumeName, String bucketName,
                              String keyName, long clientID) throws IOException {
    String fileName = OzoneFSUtils.getFileName(keyName);
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
        bucketName);
    return omMetadataManager.getOpenFileName(volumeId, bucketId,
        parentID, fileName, clientID);
  }

  @Override
  protected String getMultipartOpenKey(String volumeName, String bucketName,
                                   String keyName, String multipartUploadID) throws IOException {
    String fileName = OzoneFSUtils.getFileName(keyName);
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
        bucketName);
    return omMetadataManager.getMultipartKey(volumeId, bucketId,
        parentID, fileName, multipartUploadID);
  }

  @Override
  protected long getObjectId() {
    return parentID + 1;
  }

  @Override
  public PartKeyInfo createPartKeyInfo(
      String volumeName, String bucketName, String keyName, int partNumber)
      throws IOException {
    String fileName = OzoneFSUtils.getFileName(keyName);
    return createPartKeyInfoFSO(volumeName, bucketName, parentID, fileName,
        partNumber);
  }

  @Override
  protected OmKeyInfo getOmKeyInfo(
      long txnId, long objectId,
      String volumeName, String bucketName, String keyName,
      ReplicationConfig replicationConfig, OmKeyLocationInfoGroup omKeyLocationInfoGroup) {
    return OMRequestTestUtils.createOmKeyInfo(
            volumeName, bucketName, keyName,
            RatisReplicationConfig.getInstance(ONE),
            new OmKeyLocationInfoGroup(0L, new ArrayList<>(), true))
        .setObjectID(objectId)
        .setParentObjectID(parentID)
        .setUpdateID(txnId)
        .build();
  }

  @Override
  protected void addFileToKeyTable(String keyName, OmKeyInfo omKeyInfo, long clientID) throws Exception {
    String fileName = OzoneFSUtils.getFileName(keyName);
    OMRequestTestUtils.addFileToKeyTable(true, false,
        fileName, omKeyInfo, clientID, omKeyInfo.getObjectID(), omMetadataManager);
  }

  @Override
  protected String getDBKey(String volumeName, String bucketName, String keyName) throws IOException {
    String fileName = OzoneFSUtils.getFileName(keyName);
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
        bucketName);
    return omMetadataManager.getOzonePathKey(volumeId, bucketId,
        parentID, fileName);
  }

  @Override
  protected S3InitiateMultipartUploadResponse getS3InitiateMPUResponse(
      String volumeName, String bucketName, String keyName,
      String multipartUploadID) throws IOException {
    final List<OmDirectoryInfo> parentDirInfos = new ArrayList<>();
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
        bucketName);
    return createS3InitiateMPUResponseFSO(volumeName, bucketName, parentID,
        keyName, multipartUploadID, parentDirInfos, volumeId, bucketId);
  }

  @Override
  protected void addS3MultipartUploadCommitPartResponse(String volumeName,
                                                        String bucketName, String keyName, String multipartUploadID,
                                                        String openKey) throws IOException {
    S3MultipartUploadCommitPartResponse s3MultipartUploadCommitPartResponse =
        createS3CommitMPUResponseFSO(volumeName, bucketName, parentID, keyName,
            multipartUploadID, null, null,
            OzoneManagerProtocolProtos.Status.OK, openKey);

    s3MultipartUploadCommitPartResponse.addToDBBatch(omMetadataManager,
        batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  @Override
  protected S3MultipartUploadCommitPartResponse getS3CommitMPUResponse(
      String volumeName, String bucketName, String keyName,
      String multipartUploadID,
      OzoneManagerProtocolProtos.PartKeyInfo oldPartKeyInfo,
      OmMultipartKeyInfo multipartKeyInfo,
      OzoneManagerProtocolProtos.Status status, String openKey)
      throws IOException {
    return createS3CommitMPUResponseFSO(volumeName, bucketName, parentID, keyName,
        multipartUploadID, oldPartKeyInfo, multipartKeyInfo, status, openKey);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  @Override
  protected S3MultipartUploadCompleteResponse getS3CompleteMPUResponse(
      String volumeName, String bucketName, String keyName,
      String multipartUploadID, OmKeyInfo omKeyInfo,
      OzoneManagerProtocolProtos.Status status,
      List<OmKeyInfo> allKeyInfoToRemove,
      OmBucketInfo omBucketInfo) throws IOException {
    return createS3CompleteMPUResponseFSO(volumeName, bucketName, parentID,
        keyName, multipartUploadID, omKeyInfo, status, allKeyInfoToRemove, omBucketInfo);
  }
}
