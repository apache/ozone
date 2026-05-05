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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Test multipart upload abort response.
 */
public class TestS3MultipartUploadAbortResponseWithFSO
    extends TestS3MultipartUploadAbortResponse {

  private String dirName = "abort/b/c/";

  private long parentID = 1027;

  @Override
  protected String getKeyName() {
    return dirName + UUID.randomUUID().toString();
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  @Override
  protected String getMultipartOpenKey(String volumeName, String bucketName,
      String keyName, String multipartUploadID) throws IOException {
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    String fileName = StringUtils.substringAfter(keyName, dirName);
    return omMetadataManager.getMultipartKey(volumeId, bucketId,
            parentID, fileName, multipartUploadID);
  }

  @Override
  protected S3InitiateMultipartUploadResponse getS3InitiateMultipartUploadResp(
      OmMultipartKeyInfo multipartKeyInfo, OmKeyInfo omKeyInfo,
      OzoneManagerProtocolProtos.OMResponse omResponse, long volumeId,
      long bucketId) throws IOException {

    String mpuDBKey =
        omMetadataManager.getMultipartKey(omKeyInfo.getVolumeName(),
        omKeyInfo.getBucketName(), omKeyInfo.getKeyName(),
        multipartKeyInfo.getUploadID());

    String buckDBKey = omMetadataManager.getBucketKey(omKeyInfo.getVolumeName(),
        omKeyInfo.getBucketName());
    OmBucketInfo omBucketInfo =
        omMetadataManager.getBucketTable().get(buckDBKey);

    return new S3InitiateMultipartUploadResponseWithFSO(omResponse,
        multipartKeyInfo, omKeyInfo, mpuDBKey, new ArrayList<>(),
        getBucketLayout(), volumeId, bucketId, omBucketInfo);
  }

  @Override
  protected S3InitiateMultipartUploadResponse
      getS3InitiateMultipartUploadResponse(
      String volumeName, String bucketName, String keyName,
      String multipartUploadID, long volumeId, long bucketId)
      throws IOException {
    return createS3InitiateMPUResponseFSO(volumeName, bucketName, parentID,
        keyName,
        multipartUploadID, new ArrayList<>(), volumeId, bucketId);
  }

  @Override
  protected S3MultipartUploadAbortResponse getS3MultipartUploadAbortResp(
      String multipartKey, String multipartOpenKey,
      OmMultipartKeyInfo omMultipartKeyInfo, OmBucketInfo omBucketInfo,
      OzoneManagerProtocolProtos.OMResponse omResponse) {
    return new S3MultipartUploadAbortResponseWithFSO(omResponse, multipartKey,
        multipartOpenKey, omMultipartKeyInfo, omBucketInfo,
        getBucketLayout());
  }

  @Override
  public OzoneManagerProtocolProtos.PartKeyInfo createPartKeyInfo(
      String volumeName, String bucketName, String keyName, int partNumber)
          throws IOException {

    String fileName = OzoneFSUtils.getFileName(keyName);
    return createPartKeyInfoFSO(volumeName, bucketName, parentID, fileName,
        partNumber);
  }
}
