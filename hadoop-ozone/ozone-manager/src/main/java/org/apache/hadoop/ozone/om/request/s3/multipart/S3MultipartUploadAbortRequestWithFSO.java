/**
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

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequestUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3MultipartUploadAbortResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadAbortResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Handles Abort of multipart upload request.
 */
public class S3MultipartUploadAbortRequestWithFSO
    extends S3MultipartUploadAbortRequest {

  public S3MultipartUploadAbortRequestWithFSO(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
    OMClientRequestUtils.checkFSOClientRequestPreconditions(getBucketLayout());
  }

  @Override
  protected OMClientResponse getOmClientResponse(IOException exception,
      OMResponse.Builder omResponse) {

    return new S3MultipartUploadAbortResponseWithFSO(createErrorOMResponse(
        omResponse, exception), getBucketLayout());
  }

  @Override
  protected OMClientResponse getOmClientResponse(OzoneManager ozoneManager,
      OmMultipartKeyInfo multipartKeyInfo, String multipartKey,
      String multipartOpenKey, OMResponse.Builder omResponse,
      OmBucketInfo omBucketInfo) {

    OMClientResponse omClientResp = new S3MultipartUploadAbortResponseWithFSO(
        omResponse.setAbortMultiPartUploadResponse(
            MultipartUploadAbortResponse.newBuilder()).build(), multipartKey,
        multipartOpenKey, multipartKeyInfo, ozoneManager.isRatisEnabled(),
        omBucketInfo.copyObject(), getBucketLayout());
    return omClientResp;
  }

  @Override
  protected String getMultipartOpenKey(String multipartUploadID,
      String volumeName, String bucketName, String keyName,
      OMMetadataManager omMetadataManager) throws IOException {

    String fileName = OzoneFSUtils.getFileName(keyName);
    Iterator<Path> pathComponents = Paths.get(keyName).iterator();
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
        omMetadataManager.getBucketTable().get(bucketKey);
    long bucketId = omBucketInfo.getObjectID();
    long parentID = OMFileRequest.getParentID(bucketId, pathComponents,
        keyName, omMetadataManager);

    String multipartKey = omMetadataManager.getMultipartKey(parentID,
        fileName, multipartUploadID);

    return multipartKey;
  }
}
