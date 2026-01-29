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

package org.apache.hadoop.ozone.audit;

/**
 * Enum to define Audit Action types for S3Gateway.
 */
public enum S3GAction implements AuditAction {

  //BucketEndpoint
  GET_BUCKET,
  CREATE_BUCKET,
  HEAD_BUCKET,
  DELETE_BUCKET,
  GET_ACL,
  PUT_ACL,
  LIST_MULTIPART_UPLOAD,
  MULTI_DELETE,

  //RootEndpoint
  LIST_S3_BUCKETS,

  //ObjectEndpoint
  CREATE_MULTIPART_KEY,
  CREATE_MULTIPART_KEY_BY_COPY,
  COPY_OBJECT,
  CREATE_KEY,
  LIST_PARTS,
  GET_KEY,
  HEAD_KEY,
  INIT_MULTIPART_UPLOAD,
  COMPLETE_MULTIPART_UPLOAD,
  ABORT_MULTIPART_UPLOAD,
  DELETE_KEY,
  CREATE_DIRECTORY,
  GENERATE_SECRET,
  REVOKE_SECRET,
  GET_OBJECT_TAGGING,
  PUT_OBJECT_TAGGING,
  DELETE_OBJECT_TAGGING,
  PUT_OBJECT_ACL,

  // STS endpoint
  ASSUME_ROLE;

  @Override
  public String getAction() {
    return this.toString();
  }

}
