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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Tests S3 Multipart upload commit part request.
 */
public class TestS3MultipartUploadCommitPartRequestWithFSO
    extends TestS3MultipartUploadCommitPartRequest {

  private String dirName = "a/b/c/";

  private long parentID;

  @Override
  protected S3MultipartUploadCommitPartRequest getS3MultipartUploadCommitReq(
      OMRequest omRequest) throws IOException {
    S3MultipartUploadCommitPartRequest request = new S3MultipartUploadCommitPartRequestWithFSO(omRequest,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    request.setUGI(UserGroupInformation.getCurrentUser());
    return request;
  }

  @Override
  protected S3InitiateMultipartUploadRequest getS3InitiateMultipartUploadReq(
      OMRequest initiateMPURequest) throws IOException {
    S3InitiateMultipartUploadRequest request = new S3InitiateMultipartUploadRequestWithFSO(initiateMPURequest,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    request.setUGI(UserGroupInformation.getCurrentUser());
    return request;
  }

  @Override
  protected String getKeyName() {
    return dirName + UUID.randomUUID().toString();
  }

  @Override
  protected void addKeyToOpenKeyTable(String volumeName, String bucketName,
      String keyName, long clientID) throws Exception {
    long txnLogId = 0L;
    OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
            RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
            new OmKeyLocationInfoGroup(0L, new ArrayList<>(), true))
        .setObjectID(parentID + 1)
        .setParentObjectID(parentID)
        .setUpdateID(txnLogId)
        .build();
    String fileName = OzoneFSUtils.getFileName(keyName);
    OMRequestTestUtils.addFileToKeyTable(true, false,
        fileName, omKeyInfo, clientID, txnLogId, omMetadataManager);
  }

  @Override
  protected String addKeyToOpenKeyTable(String volumeName, String bucketName,
      String keyName, long clientID, List<OmKeyLocationInfo> locationList) throws Exception {
    long txnLogId = 0L;
    OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
            RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
            new OmKeyLocationInfoGroup(0L, locationList, true))
        .setObjectID(parentID + 1)
        .setParentObjectID(parentID)
        .setUpdateID(txnLogId)
        .build();
    String fileName = OzoneFSUtils.getFileName(keyName);
    OMRequestTestUtils.addFileToKeyTable(true, false,
        fileName, omKeyInfo, clientID, txnLogId, omMetadataManager);
    return getOpenKey(volumeName, bucketName, keyName, clientID);
  }

  @Override
  protected String getMultipartOpenKey(String volumeName, String bucketName,
      String keyName, String multipartUploadID) throws IOException {
    String fileName = StringUtils.substringAfter(keyName, dirName);
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    return omMetadataManager.getMultipartKey(volumeId, bucketId,
            parentID, fileName, multipartUploadID);
  }

  @Override
  protected String getOpenKey(String volumeName, String bucketName,
      String keyName, long clientID) throws IOException {
    String fileName = StringUtils.substringAfter(keyName, dirName);
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    return omMetadataManager.getOpenFileName(volumeId, bucketId,
            parentID, fileName, clientID);
  }

  @Override
  protected OMRequest doPreExecuteInitiateMPU(String volumeName,
      String bucketName, String keyName) throws Exception {
    OMRequest omRequest =
            OMRequestTestUtils.createInitiateMPURequest(volumeName, bucketName,
                    keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        new S3InitiateMultipartUploadRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMRequest modifiedRequest =
            s3InitiateMultipartUploadRequest.preExecute(ozoneManager);

    assertNotEquals(omRequest, modifiedRequest);
    assertTrue(modifiedRequest.hasInitiateMultiPartUploadRequest());
    assertNotNull(modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());
    assertThat(modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getModificationTime()).isGreaterThan(0);

    return modifiedRequest;
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
}
