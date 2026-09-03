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

package org.apache.hadoop.ozone.om.request.s3.tagging;

import java.io.IOException;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.key.OMKeyInfoUpdateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PutObjectTaggingResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles put object tagging request.
 */
public class S3PutObjectTaggingRequest extends OMKeyInfoUpdateRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3PutObjectTaggingRequest.class);

  public S3PutObjectTaggingRequest(OMRequest omRequest, BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  protected KeyArgs getKeyArgs(OMRequest omRequest) {
    return omRequest.getPutObjectTaggingRequest().getKeyArgs();
  }

  @Override
  protected OMRequest buildUpdatedRequest(OMRequest preExecutedRequest, KeyArgs resolvedKeyArgs)
      throws IOException {
    return getOmRequest().toBuilder()
        .setUserInfo(getUserInfo())
        .setPutObjectTaggingRequest(
            preExecutedRequest.getPutObjectTaggingRequest().toBuilder().setKeyArgs(resolvedKeyArgs))
        .build();
  }

  @Override
  protected OmKeyInfo.Builder applyUpdate(OmKeyInfo existingKeyInfo, KeyArgs keyArgs) {
    // Note: Key modification time is not changed because S3 last modified
    // time only changes when there are changes in the object content
    return existingKeyInfo.toBuilder()
        .setTags(KeyValueUtil.getFromProtobuf(keyArgs.getTagsList()));
  }

  @Override
  protected void setSuccessResponse(OMResponse.Builder omResponse) {
    omResponse.setPutObjectTaggingResponse(PutObjectTaggingResponse.newBuilder());
  }

  @Override
  protected OMAction getAuditAction() {
    return OMAction.PUT_OBJECT_TAGGING;
  }

  @Override
  protected void incRequestMetric(OMMetrics omMetrics) {
    omMetrics.incNumPutObjectTagging();
  }

  @Override
  protected void incFailureMetric(OMMetrics omMetrics) {
    omMetrics.incNumPutObjectTaggingFails();
  }

  @Override
  protected String getOperationName() {
    return "PutObjectTagging";
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
