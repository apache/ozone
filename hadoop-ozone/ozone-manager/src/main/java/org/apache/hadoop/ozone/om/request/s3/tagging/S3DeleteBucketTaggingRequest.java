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
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteBucketTaggingRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteBucketTaggingResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Handles DeleteBucketTagging (S3 bucket tagging).
 */
public class S3DeleteBucketTaggingRequest extends S3BucketTaggingRequestBase {

  /**
   * Creates a delete-bucket-tagging request from the incoming OM RPC payload.
   */
  public S3DeleteBucketTaggingRequest(OMRequest omRequest) {
    super(omRequest);
  }

  /**
   * Returns bucket args from the delete-bucket-tagging sub-request.
   */
  @Override
  protected BucketArgs getRequestBucketArgs(OMRequest omRequest) {
    DeleteBucketTaggingRequest deleteBucketTaggingRequest =
        omRequest.getDeleteBucketTaggingRequest();
    Objects.requireNonNull(deleteBucketTaggingRequest,
        "deleteBucketTaggingRequest == null");
    return deleteBucketTaggingRequest.getBucketArgs();
  }

  /**
   *  Returns the modification time stamped during preExecute.
   */
  @Override
  protected long getModificationTime(OMRequest omRequest) {
    return omRequest.getDeleteBucketTaggingRequest().getModificationTime();
  }

  /**
   * Rebuilds the OM request with resolved bucket args and modification time.
   */
  @Override
  protected OMRequest buildUpdatedOMRequest(OMRequest baseRequest,
      BucketArgs bucketArgs, long modificationTime) throws IOException {
    DeleteBucketTaggingRequest deleteBucketTaggingRequest =
        baseRequest.getDeleteBucketTaggingRequest();

    DeleteBucketTaggingRequest.Builder req =
        deleteBucketTaggingRequest.toBuilder();
    req.setModificationTime(modificationTime);
    req.setBucketArgs(bucketArgs);

    return baseRequest.toBuilder()
        .setDeleteBucketTaggingRequest(req.build())
        .setUserInfo(getUserInfo())
        .build();
  }

  /**
   * Clears all tags when deleting bucket tagging.
   */
  @Override
  protected Map<String, String> getTagsToApply(BucketArgs bucketArgs) {
    return Collections.emptyMap();
  }

  /**
   * Sets the successful delete-bucket-tagging response on the OM response.
   */
  @Override
  protected void setSuccessResponse(OMResponse.Builder omResponse) {
    omResponse.setDeleteBucketTaggingResponse(
        DeleteBucketTaggingResponse.newBuilder().build());
  }

  /**
   * Returns the audit action for delete bucket tagging.
   */
  @Override
  protected OMAction getAuditAction() {
    return OMAction.DELETE_BUCKET_TAGGING;
  }

  /**
   * Increments the delete-bucket-tagging request metric.
   */
  @Override
  protected void incRequestMetric(OMMetrics omMetrics) {
    omMetrics.incNumDeleteBucketTagging();
  }

  /**
   * Increments the delete-bucket-tagging failure metric.
   */
  @Override
  protected void incRequestFailMetric(OMMetrics omMetrics) {
    omMetrics.incNumDeleteBucketTaggingFails();
  }

  /**
   * Returns the operation label used in debug and error logs.
   */
  @Override
  protected String getOperationName() {
    return "Delete";
  }
}
