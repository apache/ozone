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

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3MultipartUploadAbortResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadAbortResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Handles Abort of multipart upload request.
 */
public class S3MultipartUploadAbortRequestWithFSO
    extends S3MultipartUploadAbortRequest {

  public S3MultipartUploadAbortRequestWithFSO(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  protected OMClientResponse getOmClientResponse(Exception exception,
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
        multipartOpenKey, multipartKeyInfo,
        omBucketInfo.copyObject(), getBucketLayout());
    return omClientResp;
  }
}
