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

import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;

/**
 * Test multipart upload commit part response for FSO bucket.
 */
public class TestS3MultipartUploadCommitPartResponseWithFSO
    extends TestS3MultipartUploadCommitPartResponse {

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
  protected String getPartOpenKey(String volumeName, String bucketName,
      String keyName, long clientId) throws IOException {
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName, bucketName);
    String fileName = OzoneFSUtils.getFileName(keyName);
    return omMetadataManager.getOpenFileName(volumeId, bucketId, parentID,
        fileName, clientId);
  }

  @Override
  protected S3InitiateMultipartUploadResponse createInitiateMPUResponse(
      String volumeName, String bucketName, String keyName,
      String multipartUploadID) throws IOException {
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName, bucketName);
    return createS3InitiateMPUResponseFSO(volumeName, bucketName, parentID,
        keyName, multipartUploadID, new ArrayList<>(), volumeId, bucketId);
  }

  @Override
  @SuppressWarnings("checkstyle:ParameterNumber")
  protected S3MultipartUploadCommitPartResponse createCommitMPUResponse(
      String volumeName, String bucketName, String keyName,
      String multipartUploadID, PartKeyInfo oldPartKeyInfo,
      OmMultipartKeyInfo multipartKeyInfo,
      OzoneManagerProtocolProtos.Status status, String openKey)
      throws IOException {
    return createS3CommitMPUResponseFSO(volumeName, bucketName, parentID,
        keyName, multipartUploadID, oldPartKeyInfo, multipartKeyInfo, status,
        openKey);
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
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
