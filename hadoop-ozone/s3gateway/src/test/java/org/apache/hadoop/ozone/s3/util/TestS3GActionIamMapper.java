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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hadoop.ozone.audit.S3GAction;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link S3GActionIamMapper}. */
public class TestS3GActionIamMapper {

  @Test
  public void mapsCoreObjectActions() {
    assertEquals("ListBucket", S3GActionIamMapper.toS3ActionString(S3GAction.GET_BUCKET));
    assertEquals("ListBucket", S3GActionIamMapper.toS3ActionString(S3GAction.HEAD_BUCKET));
    assertEquals("CreateBucket", S3GActionIamMapper.toS3ActionString(S3GAction.CREATE_BUCKET));
    assertEquals("DeleteBucket", S3GActionIamMapper.toS3ActionString(S3GAction.DELETE_BUCKET));
    assertEquals("GetBucketAcl", S3GActionIamMapper.toS3ActionString(S3GAction.GET_ACL));
    assertEquals("PutBucketAcl", S3GActionIamMapper.toS3ActionString(S3GAction.PUT_ACL));
    assertEquals("ListBucketMultipartUploads", S3GActionIamMapper.toS3ActionString(S3GAction.LIST_MULTIPART_UPLOAD));
    assertEquals("DeleteObject", S3GActionIamMapper.toS3ActionString(S3GAction.MULTI_DELETE));
    assertEquals("DeleteObject", S3GActionIamMapper.toS3ActionString(S3GAction.DELETE_KEY));
    assertEquals("ListAllMyBuckets", S3GActionIamMapper.toS3ActionString(S3GAction.LIST_S3_BUCKETS));
    assertEquals("PutObject", S3GActionIamMapper.toS3ActionString(S3GAction.CREATE_MULTIPART_KEY));
    assertEquals("PutObject", S3GActionIamMapper.toS3ActionString(S3GAction.CREATE_KEY));
    assertEquals("PutObject", S3GActionIamMapper.toS3ActionString(S3GAction.INIT_MULTIPART_UPLOAD));
    assertEquals("PutObject", S3GActionIamMapper.toS3ActionString(S3GAction.COMPLETE_MULTIPART_UPLOAD));
    assertEquals("PutObject", S3GActionIamMapper.toS3ActionString(S3GAction.CREATE_DIRECTORY));
    assertEquals("ListMultipartUploadParts", S3GActionIamMapper.toS3ActionString(S3GAction.LIST_PARTS));
    assertEquals("GetObject", S3GActionIamMapper.toS3ActionString(S3GAction.GET_KEY));
    assertEquals("GetObject", S3GActionIamMapper.toS3ActionString(S3GAction.HEAD_KEY));
    assertEquals("AbortMultipartUpload", S3GActionIamMapper.toS3ActionString(S3GAction.ABORT_MULTIPART_UPLOAD));
    assertEquals("GetObjectTagging", S3GActionIamMapper.toS3ActionString(S3GAction.GET_OBJECT_TAGGING));
    assertEquals("PutObjectTagging", S3GActionIamMapper.toS3ActionString(S3GAction.PUT_OBJECT_TAGGING));
    assertEquals("DeleteObjectTagging", S3GActionIamMapper.toS3ActionString(S3GAction.DELETE_OBJECT_TAGGING));
    assertEquals("PutObjectAcl", S3GActionIamMapper.toS3ActionString(S3GAction.PUT_OBJECT_ACL));
  }

  @Test
  public void copyActionsReturnNull() {
    assertNull(S3GActionIamMapper.toS3ActionString(S3GAction.COPY_OBJECT));
    assertNull(S3GActionIamMapper.toS3ActionString(S3GAction.CREATE_MULTIPART_KEY_BY_COPY));
  }

  @Test
  public void nonIamActionsReturnNull() {
    assertNull(S3GActionIamMapper.toS3ActionString(S3GAction.ASSUME_ROLE));
    assertNull(S3GActionIamMapper.toS3ActionString(S3GAction.GENERATE_SECRET));
    assertNull(S3GActionIamMapper.toS3ActionString(S3GAction.REVOKE_SECRET));
  }
}
