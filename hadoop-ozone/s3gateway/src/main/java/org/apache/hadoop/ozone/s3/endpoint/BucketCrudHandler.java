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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;

import java.io.IOException;
import java.io.InputStream;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.apache.hadoop.util.Time;
import org.apache.http.HttpStatus;

/**
 * Handler for default bucket CRUD operations.
 * Implements PUT (create bucket) and DELETE operations when no
 * subresource query parameters are present.
 *
 * This handler processes bucket-level requests that do not target
 * specific subresources (such as {@code ?acl}, {@code ?uploads},
 * or {@code ?delete}), which are handled by dedicated handlers.
 *
 * This handler extends EndpointBase to inherit all required functionality
 * (configuration, headers, request context, audit logging, metrics, etc.).
 */
public class BucketCrudHandler extends EndpointBase implements BucketOperationHandler {

  private boolean shouldHandlePutCreateBucket() {
    return queryParams().get(QueryParams.ACL) == null
        && queryParams().get(QueryParams.UPLOADS) == null
        && queryParams().get(QueryParams.DELETE) == null;
  }

  @Override
  public Response handlePutRequest(String bucketName, InputStream body)
      throws IOException, OS3Exception {

    if (!shouldHandlePutCreateBucket()) {
      return null;
    }

    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.CREATE_BUCKET;

    try {
      String location = createS3Bucket(bucketName);
      auditWriteSuccess(s3GAction);
      getMetrics().updateCreateBucketSuccessStats(startNanos);
      return Response.status(HttpStatus.SC_OK).header("Location", location)
          .build();
    } catch (OMException exception) {
      auditWriteFailure(s3GAction, exception);
      getMetrics().updateCreateBucketFailureStats(startNanos);
      if (exception.getResult() == OMException.ResultCodes.INVALID_BUCKET_NAME) {
        throw newError(S3ErrorTable.INVALID_BUCKET_NAME, bucketName, exception);
      }
      throw exception;
    } catch (Exception ex) {
      auditWriteFailure(s3GAction, ex);
      throw ex;
    }
  }

  @Override
  public Response handleDeleteRequest(String bucketName)
      throws IOException, OS3Exception {

    if (queryParams().get(QueryParams.ACL) != null
        || queryParams().get(QueryParams.UPLOADS) != null
        || queryParams().get(QueryParams.DELETE) != null) {
      return null;
    }

    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.DELETE_BUCKET;

    try {
      if (S3Owner.hasBucketOwnershipVerificationConditions(getHeaders())) {
        OzoneBucket bucket = getBucket(bucketName);
        S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName, bucket.getOwner());
      }
      deleteS3Bucket(bucketName);
    } catch (OMException ex) {
      auditWriteFailure(s3GAction, ex);
      getMetrics().updateDeleteBucketFailureStats(startNanos);
      if (ex.getResult() == OMException.ResultCodes.BUCKET_NOT_EMPTY) {
        throw newError(S3ErrorTable.BUCKET_NOT_EMPTY, bucketName, ex);
      } else if (ex.getResult() == OMException.ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName, ex);
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, bucketName, ex);
      } else {
        throw ex;
      }
    } catch (Exception ex) {
      auditWriteFailure(s3GAction, ex);
      throw ex;
    }

    auditWriteSuccess(s3GAction);
    getMetrics().updateDeleteBucketSuccessStats(startNanos);
    return Response
        .status(HttpStatus.SC_NO_CONTENT)
        .build();
  }
}
