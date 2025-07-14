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
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Tests S3 Multipart Upload Complete request.
 */
public class TestS3MultipartUploadCompleteRequestWithFSO
    extends TestS3MultipartUploadCompleteRequest {

  @Override
  protected String getKeyName() {
    String parentDir = UUID.randomUUID().toString() + "/a/b/c";
    String fileName = "file1";
    String keyName = parentDir + OzoneConsts.OM_KEY_PREFIX + fileName;
    return keyName;
  }

  @Override
  protected long getNamespaceCount() {
    // parent directory count which is also created
    return 5L;
  }

  @Override
  protected void addVolumeAndBucket(String volumeName, String bucketName)
      throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  @Override
  protected void addKeyToTable(String volumeName, String bucketName,
      String keyName, long clientID) throws Exception {
    // need to initialize parentID
    String parentDir = OzoneFSUtils.getParentDir(keyName);
    assertNotEquals("Parent doesn't exists!", parentDir, keyName);

    // add parentDir to dirTable
    long parentID = getParentID(volumeName, bucketName, keyName);
    long txnId = 2;
    long objectId = parentID + 1;

    OmKeyInfo omKeyInfoFSO =
        OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
                RatisReplicationConfig.getInstance(ONE), new OmKeyLocationInfoGroup(0L, new ArrayList<>(), true))
            .setObjectID(objectId)
            .setParentObjectID(parentID)
            .setUpdateID(txnId)
            .build();

    // add key to openFileTable
    String fileName = OzoneFSUtils.getFileName(keyName);
    omKeyInfoFSO.setKeyName(fileName);
    OMRequestTestUtils.addFileToKeyTable(true, false,
            fileName, omKeyInfoFSO, clientID, omKeyInfoFSO.getObjectID(),
            omMetadataManager);
  }

  private long getParentID(String volumeName, String bucketName,
                           String keyName) throws IOException {
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    return OMFileRequest.getParentID(volumeId, bucketId,
            keyName, omMetadataManager);
  }

  @Override
  protected String getOzoneDBKey(String volumeName, String bucketName,
                                 String keyName) throws IOException {
    long parentID = getParentID(volumeName, bucketName, keyName);
    String fileName = OzoneFSUtils.getFileName(keyName);
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    return omMetadataManager.getOzonePathKey(volumeId, bucketId,
            parentID, fileName);
  }

  @Override
  protected S3MultipartUploadCompleteRequest getS3MultipartUploadCompleteReq(
          OMRequest omRequest) throws IOException {
    S3MultipartUploadCompleteRequest request = new S3MultipartUploadCompleteRequestWithFSO(omRequest,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    request.setUGI(UserGroupInformation.getCurrentUser());
    return request;
  }

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
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
