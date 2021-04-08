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
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.UUID;

/**
 * Tests S3 Multipart Upload Complete request.
 */
public class TestS3MultipartUploadCompleteRequestV1
    extends TestS3MultipartUploadCompleteRequest {

  @BeforeClass
  public static void init() {
    OzoneManagerRatisUtils.setBucketFSOptimized(true);
  }

  protected String getKeyName() {
    String parentDir = UUID.randomUUID().toString() + "/a/b/c";
    String fileName = "file1";
    String keyName = parentDir + OzoneConsts.OM_KEY_PREFIX + fileName;
    return keyName;
  }

  protected void addKeyToTable(String volumeName, String bucketName,
      String keyName, long clientID) throws Exception {
    // need to initialize parentID
    String parentDir = OzoneFSUtils.getParentDir(keyName);
    Assert.assertNotEquals("Parent doesn't exists!", parentDir, keyName);

    // add parentDir to dirTable
    long parentID = getParentID(volumeName, bucketName, keyName);
    long txnId = 50;
    long objectId = parentID + 1;

    OmKeyInfo omKeyInfoV1 =
            TestOMRequestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
                    HddsProtos.ReplicationType.RATIS,
                    HddsProtos.ReplicationFactor.ONE, objectId, parentID, txnId,
                    Time.now());

    // add key to openFileTable
    String fileName = OzoneFSUtils.getFileName(keyName);
    omKeyInfoV1.setKeyName(fileName);
    TestOMRequestUtils.addFileToKeyTable(true, false,
            fileName, omKeyInfoV1, clientID, omKeyInfoV1.getObjectID(),
            omMetadataManager);
  }

  protected String getMultipartKey(String volumeName, String bucketName,
      String keyName, String multipartUploadID) throws IOException {
    OzoneFileStatus keyStatus = OMFileRequest.getOMKeyInfoIfExists(
            omMetadataManager, volumeName,
            bucketName, keyName, 0);

    Assert.assertNotNull("key not found in DB!", keyStatus);

    return omMetadataManager.getMultipartKey(keyStatus.getKeyInfo()
                    .getParentObjectID(), keyStatus.getTrimmedName(),
            multipartUploadID);
  }

  private long getParentID(String volumeName, String bucketName,
                           String keyName) throws IOException {
    Path keyPath = Paths.get(keyName);
    Iterator<Path> elements = keyPath.iterator();
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);

    return OMFileRequest.getParentID(omBucketInfo.getObjectID(),
            elements, keyName, omMetadataManager);
  }

  protected String getOzoneDBKey(String volumeName, String bucketName,
                                 String keyName) throws IOException {
    long parentID = getParentID(volumeName, bucketName, keyName);
    String fileName = OzoneFSUtils.getFileName(keyName);
    return omMetadataManager.getOzonePathKey(parentID, fileName);
  }

  protected S3MultipartUploadCompleteRequest getS3MultipartUploadCompleteReq(
          OMRequest omRequest) {
    return new S3MultipartUploadCompleteRequestV1(omRequest);
  }

  protected S3MultipartUploadCommitPartRequest getS3MultipartUploadCommitReq(
          OMRequest omRequest) {
    return new S3MultipartUploadCommitPartRequestV1(omRequest);
  }

  protected S3InitiateMultipartUploadRequest getS3InitiateMultipartUploadReq(
          OMRequest initiateMPURequest) {
    return new S3InitiateMultipartUploadRequestV1(initiateMPURequest);
  }

}

