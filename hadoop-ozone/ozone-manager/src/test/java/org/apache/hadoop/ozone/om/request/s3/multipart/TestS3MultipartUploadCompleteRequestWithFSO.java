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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.util.Time;
import org.junit.Assert;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.UUID;

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
    Assert.assertNotEquals("Parent doesn't exists!", parentDir, keyName);

    // add parentDir to dirTable
    long parentID = getParentID(volumeName, bucketName, keyName);
    long txnId = 50;
    long objectId = parentID + 1;

    OmKeyInfo omKeyInfoFSO =
            OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
                    HddsProtos.ReplicationType.RATIS,
                    HddsProtos.ReplicationFactor.ONE, objectId, parentID, txnId,
                    Time.now());

    // add key to openFileTable
    String fileName = OzoneFSUtils.getFileName(keyName);
    omKeyInfoFSO.setKeyName(fileName);
    OMRequestTestUtils.addFileToKeyTable(true, false,
            fileName, omKeyInfoFSO, clientID, omKeyInfoFSO.getObjectID(),
            omMetadataManager);
  }

  @Override
  protected String getMultipartKey(String volumeName, String bucketName,
      String keyName, String multipartUploadID) throws IOException {
    OzoneFileStatus keyStatus = OMFileRequest.getOMKeyInfoIfExists(
            omMetadataManager, volumeName,
            bucketName, keyName, 0);

    Assert.assertNotNull("key not found in DB!", keyStatus);
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    return omMetadataManager.getMultipartKey(volumeId, bucketId,
            keyStatus.getKeyInfo().getParentObjectID(),
            keyStatus.getTrimmedName(), multipartUploadID);
  }

  private long getParentID(String volumeName, String bucketName,
                           String keyName) throws IOException {
    Path keyPath = Paths.get(keyName);
    Iterator<Path> elements = keyPath.iterator();
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    return OMFileRequest.getParentID(volumeId, bucketId,
            elements, keyName, omMetadataManager);
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
          OMRequest omRequest) {
    return new S3MultipartUploadCompleteRequestWithFSO(omRequest,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  @Override
  protected S3MultipartUploadCommitPartRequest getS3MultipartUploadCommitReq(
      OMRequest omRequest) {
    return new S3MultipartUploadCommitPartRequestWithFSO(omRequest,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  @Override
  protected S3InitiateMultipartUploadRequest getS3InitiateMultipartUploadReq(
      OMRequest initiateMPURequest) {
    return new S3InitiateMultipartUploadRequestWithFSO(initiateMPURequest,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}