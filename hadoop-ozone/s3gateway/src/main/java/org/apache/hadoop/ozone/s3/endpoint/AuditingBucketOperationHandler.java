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

package org.apache.hadoop.ozone.s3.endpoint;

import java.io.IOException;
import java.io.InputStream;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

/** Performs audit logging for {@link BucketOperationHandler}s. */
class AuditingBucketOperationHandler extends BucketOperationHandler {
  private final BucketOperationHandler delegate;

  AuditingBucketOperationHandler(BucketOperationHandler delegate) {
    this.delegate = delegate;
    delegate.copyDependenciesTo(this);
  }

  @Override
  Response handleDeleteRequest(S3RequestContext context, String bucketName) throws IOException, OS3Exception {
    try {
      Response response = delegate.handleDeleteRequest(context, bucketName);
      auditWriteSuccess(context.getAction(), context.getPerf());
      return response;
    } catch (Exception e) {
      auditWriteFailure(context.getAction(), e);
      throw e;
    }
  }

  @Override
  Response handleGetRequest(S3RequestContext context, String bucketName) throws IOException, OS3Exception {
    try {
      Response response = delegate.handleGetRequest(context, bucketName);
      auditReadSuccess(context.getAction(), context.getPerf());
      return response;
    } catch (Exception e) {
      auditReadFailure(context.getAction(), e);
      throw e;
    }
  }

  @Override
  Response handlePutRequest(S3RequestContext context, String bucketName, InputStream body)
      throws IOException, OS3Exception {
    try {
      Response response = delegate.handlePutRequest(context, bucketName, body);
      auditWriteSuccess(context.getAction(), context.getPerf());
      return response;
    } catch (Exception e) {
      auditWriteFailure(context.getAction(), e);
      throw e;
    }
  }
}
