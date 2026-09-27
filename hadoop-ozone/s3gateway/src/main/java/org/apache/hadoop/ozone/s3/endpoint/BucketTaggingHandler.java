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

import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_BUCKET_NUM_LIMIT;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.function.Supplier;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.apache.ratis.util.MemoizedSupplier;

/**
 *  S3 bucket tagging (?tagging).
 */
public class BucketTaggingHandler extends BucketOperationHandler {

  private static final Supplier<MessageUnmarshaller<S3Tagging>> UNMARSHALLER =
      MemoizedSupplier.valueOf(() -> new MessageUnmarshaller<>(S3Tagging.class));

  @Override
  public Response handlePutRequest(S3RequestContext context, String bucketName, InputStream body)
      throws IOException, OS3Exception {
    S3GAction action = resolveAction();
    if (action == null) {
      return null;
    }
    context.setAction(action);

    try {
      S3Tagging tagging;
      try {
        tagging = UNMARSHALLER.get().readFrom(body);
        tagging.validate();
      } catch (Exception ex) {
        OS3Exception exception = S3ErrorTable.newError(S3ErrorTable.MALFORMED_XML, bucketName, ex);
        exception.setErrorMessage(exception.getErrorMessage() + ". " + ex.getMessage());
        throw exception;
      }

      Map<String, String> tags = validateAndGetTagging(
          tagging.getTagSet().getTags(),
          S3Tagging.Tag::getKey,
          S3Tagging.Tag::getValue,
          TAG_BUCKET_NUM_LIMIT
      );

      context.getVolume().getBucket(bucketName).putBucketTagging(tags);

      getMetrics().updatePutBucketTaggingSuccessStats(context.getStartNanos());

      return Response.ok().build();
    } catch (Exception e) {
      getMetrics().updatePutBucketTaggingFailureStats(context.getStartNanos());
      throw e;
    }
  }

  @Override
  public Response handleDeleteRequest(S3RequestContext context, String bucketName)
      throws IOException, OS3Exception {
    S3GAction action = resolveAction();
    if (action == null) {
      return null;
    }
    context.setAction(action);

    try {
      context.getVolume().getBucket(bucketName).deleteBucketTagging();
      getMetrics().updateDeleteBucketTaggingSuccessStats(context.getStartNanos());
      return Response.noContent().build();
    } catch (OMException ex) {
      getMetrics().updateDeleteBucketTaggingFailureStats(context.getStartNanos());
      if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw S3ErrorTable.newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName);
      }
      throw ex;
    } catch (IOException | RuntimeException ex) {
      getMetrics().updateDeleteBucketTaggingFailureStats(context.getStartNanos());
      throw ex;
    }
  }

  @Override
  public Response handleGetRequest(S3RequestContext context, String bucketName)
      throws IOException, OS3Exception {
    S3GAction action = resolveAction();
    if (action == null) {
      return null;
    }
    context.setAction(action);

    try {
      Map<String, String> tagMap = context.getVolume().getBucket(bucketName).getBucketTagging();
      if (tagMap.isEmpty()) {
        throw S3ErrorTable.newError(S3ErrorTable.NO_SUCH_TAG_SET, bucketName);
      }
      getMetrics().updateGetBucketTaggingSuccessStats(context.getStartNanos());
      return Response.ok(S3Tagging.fromMap(tagMap), MediaType.APPLICATION_XML_TYPE).build();
    } catch (OMException ex) {
      getMetrics().updateGetBucketTaggingFailureStats(context.getStartNanos());
      if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw S3ErrorTable.newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName);
      }
      throw ex;
    } catch (Exception e) {
      getMetrics().updateGetBucketTaggingFailureStats(context.getStartNanos());
      throw e;
    }
  }

  private S3GAction resolveAction() {
    if (queryParams().get(S3Consts.QueryParams.TAGGING) == null) {
      return null;
    }

    switch (getContext().getMethod()) {
    case HttpMethod.DELETE:
      return S3GAction.DELETE_BUCKET_TAGGING;
    case HttpMethod.GET:
      return S3GAction.GET_BUCKET_TAGGING;
    case HttpMethod.PUT:
      return S3GAction.PUT_BUCKET_TAGGING;
    default:
      return null;
    }
  }
}
