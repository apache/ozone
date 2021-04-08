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

package org.apache.hadoop.ozone.om.response.s3.multipart;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import java.util.ArrayList;
import java.util.UUID;

/**
 * Test multipart upload abort response.
 */
public class TestS3MultipartUploadAbortResponseV1
    extends TestS3MultipartUploadAbortResponse {

  private String dirName = "abort/b/c/";

  private long parentID = 1027;

  protected String getKeyName() {
    return dirName + UUID.randomUUID().toString();
  }

  protected String getMultipartKey(String volumeName, String bucketName,
      String keyName, String multipartUploadID) {
    String fileName = StringUtils.substringAfter(keyName, dirName);
    return omMetadataManager.getMultipartKey(parentID, fileName,
        multipartUploadID);
  }

  protected S3InitiateMultipartUploadResponse getS3InitiateMultipartUploadResp(
      OmMultipartKeyInfo multipartKeyInfo, OmKeyInfo omKeyInfo,
      OzoneManagerProtocolProtos.OMResponse omResponse) {
    return new S3InitiateMultipartUploadResponseV1(omResponse, multipartKeyInfo,
        omKeyInfo, new ArrayList<>());
  }

  protected S3MultipartUploadAbortResponse getS3MultipartUploadAbortResp(
      String multipartKey, OmMultipartKeyInfo omMultipartKeyInfo,
      OmBucketInfo omBucketInfo,
      OzoneManagerProtocolProtos.OMResponse omResponse) {
    return new S3MultipartUploadAbortResponseV1(omResponse, multipartKey,
        omMultipartKeyInfo, true, omBucketInfo);
  }

  @Override
  public OzoneManagerProtocolProtos.PartKeyInfo createPartKeyInfo(
      String volumeName, String bucketName, String keyName, int partNumber) {

    String fileName = OzoneFSUtils.getFileName(keyName);
    return createPartKeyInfoV1(volumeName, bucketName, parentID, fileName,
        partNumber);
  }
}
