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

package org.apache.hadoop.ozone.s3.util;

import jakarta.annotation.Nullable;
import org.apache.hadoop.ozone.audit.S3GAction;

/**
 * Maps S3 Gateway operations to AWS IAM S3 action names. Values align with
 * {@code org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver} so STS session
 * policies and Ranger policy conditions use the same vocabulary.
 */
public final class S3GActionIamMapper {

  private S3GActionIamMapper() {
  }

  /**
   * @return S3 action string, or null if not applicable to IAM S3
   */
  public static @Nullable String toS3ActionString(@Nullable S3GAction action) {
    if (action == null) {
      return null;
    }
    switch (action) {
    case GET_BUCKET:
    case HEAD_BUCKET:
      return "ListBucket";
    case CREATE_BUCKET:
      return "CreateBucket";
    case DELETE_BUCKET:
      return "DeleteBucket";
    case GET_ACL:
      return "GetBucketAcl";
    case PUT_ACL:
      return "PutBucketAcl";
    case LIST_MULTIPART_UPLOAD:
      return "ListBucketMultipartUploads";
    case MULTI_DELETE:
    case DELETE_KEY:
      return "DeleteObject";
    case LIST_S3_BUCKETS:
      return "ListAllMyBuckets";
    case CREATE_MULTIPART_KEY:
    case CREATE_KEY:
    case INIT_MULTIPART_UPLOAD:
    case COMPLETE_MULTIPART_UPLOAD:
    case CREATE_DIRECTORY:
      return "PutObject";
    case LIST_PARTS:
      return "ListMultipartUploadParts";
    case GET_KEY:
    case HEAD_KEY:
      return "GetObject";
    case ABORT_MULTIPART_UPLOAD:
      return "AbortMultipartUpload";
    case GET_OBJECT_TAGGING:
      return "GetObjectTagging";
    case PUT_OBJECT_TAGGING:
      return "PutObjectTagging";
    case DELETE_OBJECT_TAGGING:
      return "DeleteObjectTagging";
    case PUT_OBJECT_ACL:
      return "PutObjectAcl";
    case COPY_OBJECT:
    case CREATE_MULTIPART_KEY_BY_COPY:
      // CopyObject / UploadPartCopy require distinct source (GetObject) and destination (PutObject)
      // authorization. The endpoint code explicitly sets the IAM action string for each phase.
      return null;
    case GENERATE_SECRET:
    case REVOKE_SECRET:
    case ASSUME_ROLE:
    default:
      return null;
    }
  }
}
