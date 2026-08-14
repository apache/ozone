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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for S3 bucket lifecycle configuration operations.
 */

public class BucketLifecycleHandler extends BucketOperationHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(BucketLifecycleHandler.class);

  private boolean shouldHandle() {
    return queryParams().get(QueryParams.LIFECYCLE) != null;
  }

  @Override
  Response handleGetRequest(S3RequestContext context, String bucketName)
      throws IOException, OS3Exception {
    if (!shouldHandle()) {
      return null;
    }

    context.setAction(S3GAction.GET_BUCKET_LIFECYCLE);
    return getBucketLifecycleConfiguration(context, bucketName);
  }

  @Override
  Response handlePutRequest(
      S3RequestContext context, String bucketName, InputStream body)
      throws IOException, OS3Exception {
    if (!shouldHandle()) {
      return null;
    }

    context.setAction(S3GAction.PUT_BUCKET_LIFECYCLE);
    return putBucketLifecycleConfiguration(context, bucketName, body);
  }

  @Override
  Response handleDeleteRequest(S3RequestContext context, String bucketName)
      throws IOException, OS3Exception {
    if (!shouldHandle()) {
      return null;
    }

    context.setAction(S3GAction.DELETE_BUCKET_LIFECYCLE);
    return deleteBucketLifecycleConfiguration(context, bucketName);
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
      // DeleteBucketLifecycle is idempotent: deleting a missing config
      // must still return 204, not 404 — same as normal key deletion.
      if (ex.getResult() != OMException.ResultCodes.LIFECYCLE_CONFIGURATION_NOT_FOUND) {
        throw S3ErrorTable.newError(bucketName, ex);
      }
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
      throw S3ErrorTable.newError(bucketName, ex);
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
