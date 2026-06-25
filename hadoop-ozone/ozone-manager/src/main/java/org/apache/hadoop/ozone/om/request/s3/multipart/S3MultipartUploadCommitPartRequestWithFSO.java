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

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmFSOFile;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3MultipartUploadCommitPartResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3MultipartUploadCommitPartResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;

/**
 * Handle Multipart upload commit upload part file.
 */
public class S3MultipartUploadCommitPartRequestWithFSO
        extends S3MultipartUploadCommitPartRequest {

  public S3MultipartUploadCommitPartRequestWithFSO(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  protected String getOpenKey(String volumeName, String bucketName,
      String keyName, OMMetadataManager omMetadataManager, long clientID)
      throws IOException {

    return new OmFSOFile.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setKeyName(keyName)
          .setOmMetadataManager(omMetadataManager)
          .build().getOpenFileName(clientID);
  }

  @Override
  protected OmKeyInfo getOmKeyInfo(OMMetadataManager omMetadataManager,
      String openKey, String keyName) throws IOException {

    return OMFileRequest.getOmKeyInfoFromFileTable(true,
        omMetadataManager, openKey, keyName);
  }

  @Override
  @SuppressWarnings("checkstyle:ParameterNumber")
  protected S3MultipartUploadCommitPartResponse getOmClientResponse(
      OzoneManager ozoneManager,
      Map<String, RepeatedOmKeyInfo> keyToDeleteMap, String openKey,
      OmKeyInfo omKeyInfo, String multipartKey,
      OmMultipartKeyInfo multipartKeyInfo,
      OzoneManagerProtocolProtos.OMResponse build, OmBucketInfo omBucketInfo, long bucketId) {

    return new S3MultipartUploadCommitPartResponseWithFSO(build, multipartKey,
        openKey, multipartKeyInfo, keyToDeleteMap, omKeyInfo,
        omBucketInfo, bucketId, getBucketLayout());
  }
}
