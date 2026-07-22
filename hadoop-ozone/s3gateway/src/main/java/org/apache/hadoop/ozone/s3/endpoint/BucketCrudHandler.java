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

import static org.apache.hadoop.ozone.s3.util.S3Consts.EXPECTED_BUCKET_OWNER_HEADER;

import java.io.IOException;
import java.io.InputStream;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneLifecycleConfiguration;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleConfiguration;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for default bucket CRUD operations.
 * Implements PUT (create bucket) and DELETE operations when no
 * subresource query parameters are present.
 *
 * This handler processes bucket-level requests that do not target
 * specific subresources (such as {@code ?acl}, {@code ?uploads},
 * {@code ?delete} or {@code ?tagging}), which are handled by dedicated handlers.
 *
 * This handler extends EndpointBase to inherit all required functionality
 * (configuration, headers, request context, audit logging, metrics, etc.).
 */
public class BucketCrudHandler extends BucketOperationHandler {
  private static final Logger LOG = LoggerFactory.getLogger(BucketCrudHandler.class);

  /**
   * Handle only plain PUT bucket (create bucket), not subresources.
   */
  private boolean shouldHandle() {
    return queryParams().get(QueryParams.ACL) == null
        && queryParams().get(QueryParams.UPLOADS) == null
        && queryParams().get(QueryParams.DELETE) == null
        && queryParams().get(QueryParams.TAGGING) == null;
  }

  /**
   * Handle GET /{bucket} for bucket LIFECYCLE configuration.
   */
  @Override
  Response handleGetRequest(S3RequestContext context, String bucketName)
      throws IOException, OS3Exception {
    final String lifecycleMarker = queryParams().get(QueryParams.LIFECYCLE);

    if (lifecycleMarker != null) {
      context.setAction(S3GAction.GET_BUCKET_LIFECYCLE);
      return getBucketLifecycleConfiguration(context, bucketName);
    }
    return null;
  }

  /**
   * Handle PUT /{bucket} for bucket creation.
   */
  @Override
  Response handlePutRequest(S3RequestContext context, String bucketName, InputStream body)
      throws IOException, OS3Exception {

    final String lifecycleMarker = queryParams().get(QueryParams.LIFECYCLE);
    if (lifecycleMarker != null) {
      context.setAction(S3GAction.PUT_BUCKET_LIFECYCLE);
      return putBucketLifecycleConfiguration(context, bucketName, body);
    }

    if (!shouldHandle()) {
      return null;
    }

    context.setAction(S3GAction.CREATE_BUCKET);

    try {
      getClient().getObjectStore().createS3Bucket(bucketName);
      getMetrics().updateCreateBucketSuccessStats(context.getStartNanos());
      return Response.status(HttpStatus.SC_OK)
          .header(HttpHeaders.LOCATION, "/" + bucketName)
          .build();
    } catch (Exception e) {
      getMetrics().updateCreateBucketFailureStats(context.getStartNanos());
      throw e;
    }
  }

  /**
   * Handle DELETE /{bucket} for bucket deletion.
   */
  @Override
  Response handleDeleteRequest(S3RequestContext context, String bucketName)
      throws IOException, OS3Exception {

    final String lifecycleMarker = queryParams().get(QueryParams.LIFECYCLE);
    if (lifecycleMarker != null) {
      context.setAction(S3GAction.DELETE_BUCKET_LIFECYCLE);
      return deleteBucketLifecycleConfiguration(context, bucketName);
    }

    if (!shouldHandle()) {
      return null;
    }

    context.setAction(S3GAction.DELETE_BUCKET);

    try {
      if (S3Owner.hasBucketOwnershipVerificationConditions(getHeaders())) {
        OzoneBucket bucket = context.getVolume().getBucket(bucketName);
        S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName, bucket.getOwner());
      }
      context.getVolume().deleteBucket(bucketName);
    } catch (Exception ex) {
      getMetrics().updateDeleteBucketFailureStats(context.getStartNanos());
      throw ex;
    }

    getMetrics().updateDeleteBucketSuccessStats(context.getStartNanos());
    return Response
        .status(HttpStatus.SC_NO_CONTENT)
        .build();
  }

  public Response deleteBucketLifecycleConfiguration(S3RequestContext context, String bucketName)
      throws IOException, OS3Exception {
    verifyBucketOwner(context, bucketName);
    deleteLifecycleConfiguration(context, bucketName);
    return Response.noContent().build();
  }

  protected void deleteLifecycleConfiguration(S3RequestContext context, String bucketName)
      throws IOException, OS3Exception {
    try {
      context.getVolume().getBucket(bucketName).deleteLifecycleConfiguration();
    } catch (OMException ex) {
      if (ex.getResult() == OMException.ResultCodes.LIFECYCLE_CONFIGURATION_NOT_FOUND) {
        throw S3ErrorTable.newError(
            S3ErrorTable.NO_SUCH_LIFECYCLE_CONFIGURATION, bucketName);
      }
      throw ex;
    }
  }

  private void verifyBucketOwner(S3RequestContext context, String bucketName) throws OS3Exception {
    HttpHeaders httpHeaders = getHeaders();
    if (httpHeaders == null) {
      return;
    }
    String expectedBucketOwner = httpHeaders.getHeaderString(EXPECTED_BUCKET_OWNER_HEADER);
    if (expectedBucketOwner == null || expectedBucketOwner.isEmpty()) {
      return;
    }

    try {
      String actualOwner = context.getVolume().getBucket(bucketName).getOwner();
      if (actualOwner != null && !actualOwner.equals(expectedBucketOwner)) {
        LOG.debug("Bucket: {}, ExpectedBucketOwner: {}, ActualBucketOwner: {}",
            bucketName, expectedBucketOwner, actualOwner);
        throw S3ErrorTable.newError(S3ErrorTable.ACCESS_DENIED, bucketName);
      }
    } catch (Exception ex) {
      LOG.error("Owner verification failed for bucket: {}", bucketName, ex);
      throw S3ErrorTable.newError(S3ErrorTable.ACCESS_DENIED, bucketName);
    }
  }

  public Response putBucketLifecycleConfiguration(S3RequestContext context, String bucketName, InputStream body)
      throws IOException, OS3Exception {
    verifyBucketOwner(context, bucketName);
    S3LifecycleConfiguration s3LifecycleConfiguration;
    OzoneBucket ozoneBucket = context.getVolume().getBucket(bucketName);
    try {
      s3LifecycleConfiguration = new PutBucketLifecycleConfigurationUnmarshaller().readFrom(null,
          null, null, null, null, body);
      OmLifecycleConfiguration lcc =
          s3LifecycleConfiguration.toOmLifecycleConfiguration(ozoneBucket);
      ozoneBucket.setLifecycleConfiguration(lcc);
    } catch (WebApplicationException ex) {
      throw S3ErrorTable.newError(S3ErrorTable.MALFORMED_XML, bucketName);
    } catch (OMException ex) {
      if (ex.getResult() == OMException.ResultCodes.ACCESS_DENIED) {
        throw S3ErrorTable.newError(S3ErrorTable.ACCESS_DENIED, bucketName);
      } else if (ex.getResult() == OMException.ResultCodes.INVALID_REQUEST) {
        throw S3ErrorTable.newError(S3ErrorTable.INVALID_REQUEST, bucketName, ex);
      }
    }
    return Response.ok().build();
  }

  public Response getBucketLifecycleConfiguration(S3RequestContext context, String bucketName)
      throws IOException, OS3Exception {
    verifyBucketOwner(context, bucketName);
    OzoneLifecycleConfiguration ozoneLifecycleConfiguration =
        getLifecycleConfiguration(context, bucketName);
    return Response.ok(S3LifecycleConfiguration.fromOzoneLifecycleConfiguration(
        ozoneLifecycleConfiguration), MediaType.APPLICATION_XML_TYPE).build();
  }

  protected OzoneLifecycleConfiguration getLifecycleConfiguration(
      S3RequestContext context, String bucketName) throws IOException, OS3Exception {
    try {
      OzoneBucket ozoneBucket = context.getVolume().getBucket(bucketName);
      return ozoneBucket.getLifecycleConfiguration();
    } catch (OMException ex) {
      if (ex.getResult() == OMException.ResultCodes.LIFECYCLE_CONFIGURATION_NOT_FOUND) {
        throw S3ErrorTable.newError(
            S3ErrorTable.NO_SUCH_LIFECYCLE_CONFIGURATION, bucketName);
      }
      throw ex;
    }
  }
}
