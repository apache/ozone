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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;

import java.io.IOException;
import java.util.UUID;

/**
 * Test Multipart upload abort request.
 */
public class TestS3MultipartUploadAbortRequestWithFSO
    extends TestS3MultipartUploadAbortRequest {

  private String dirName = "a/b/c/";

  private long parentID;

  @Override
  protected S3MultipartUploadAbortRequest getS3MultipartUploadAbortReq(
      OMRequest omRequest) {
    return new S3MultipartUploadAbortRequestWithFSO(omRequest,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  @Override
  protected S3InitiateMultipartUploadRequest getS3InitiateMultipartUploadReq(
      OMRequest initiateMPURequest) {
    return new S3InitiateMultipartUploadRequestWithFSO(initiateMPURequest,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

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
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
