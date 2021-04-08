/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.request.s3.multipart;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.util.Time;
import org.junit.Assert;

import java.util.UUID;

/**
 * Tests S3 Multipart upload commit part request.
 */
public class TestS3MultipartUploadCommitPartRequestV1
    extends TestS3MultipartUploadCommitPartRequest {

  private String dirName = "a/b/c/";

  private long parentID;

  protected S3MultipartUploadCommitPartRequest getS3MultipartUploadCommitReq(
          OMRequest omRequest) {
    return new S3MultipartUploadCommitPartRequestV1(omRequest);
  }

  protected S3InitiateMultipartUploadRequest getS3InitiateMultipartUploadReq(
          OMRequest initiateMPURequest) {
    return new S3InitiateMultipartUploadRequestV1(initiateMPURequest);
  }

  protected String getKeyName() {
    return dirName + UUID.randomUUID().toString();
  }

  protected void addKeyToOpenKeyTable(String volumeName, String bucketName,
      String keyName, long clientID) throws Exception {
    long txnLogId = 10000;
    OmKeyInfo omKeyInfo = TestOMRequestUtils.createOmKeyInfo(volumeName,
            bucketName, keyName, HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.ONE, parentID + 1, parentID,
            txnLogId, Time.now());
    String fileName = OzoneFSUtils.getFileName(keyName);
    TestOMRequestUtils.addFileToKeyTable(true, false,
            fileName, omKeyInfo, clientID, txnLogId, omMetadataManager);
  }

  protected String getMultipartKey(String volumeName, String bucketName,
      String keyName, String multipartUploadID) {
    String fileName = StringUtils.substringAfter(keyName, dirName);
    return omMetadataManager.getMultipartKey(parentID, fileName,
            multipartUploadID);
  }

  protected OMRequest doPreExecuteInitiateMPU(String volumeName,
      String bucketName, String keyName) throws Exception {
    OMRequest omRequest =
            TestOMRequestUtils.createInitiateMPURequest(volumeName, bucketName,
                    keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
            new S3InitiateMultipartUploadRequestV1(omRequest);

    OMRequest modifiedRequest =
            s3InitiateMultipartUploadRequest.preExecute(ozoneManager);

    Assert.assertNotEquals(omRequest, modifiedRequest);
    Assert.assertTrue(modifiedRequest.hasInitiateMultiPartUploadRequest());
    Assert.assertNotNull(modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());
    Assert.assertTrue(modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getModificationTime() > 0);

    return modifiedRequest;
  }

  protected void createParentPath(String volumeName, String bucketName)
      throws Exception {
    // Create parent dirs for the path
    parentID = TestOMRequestUtils.addParentsToDirTable(volumeName, bucketName,
            dirName, omMetadataManager);
  }
}
