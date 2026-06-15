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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_ARGUMENT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;

import java.io.IOException;
import java.util.Iterator;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.s3.commontypes.BucketMetadata;
import org.apache.hadoop.ozone.s3.commontypes.DirectoryBucketMetadata;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.util.ContinueToken;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Top level rest endpoint.
 */
@Path("/")
public class RootEndpoint extends EndpointBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(RootEndpoint.class);

  /**
   * Rest endpoint to list all the buckets of the current user.
   *
   * See https://docs.aws.amazon.com/AmazonS3/latest/API/RESTServiceGET.html
   * for more details.
   */
  @GET
  public Response get()
      throws OS3Exception, IOException {
    if (isS3ExpressSignedRequest()) {
      return listDirectoryBuckets();
    }
    return listAllBuckets();
  }

  /**
   * AWS SDKs and CLI sign requests to S3 Express Regional endpoints (s3express-control.<region>.amazonaws.com)
   * using "s3express" as the SigV4 service name in the credential scope
   * (<date>/<region>/s3express/aws4_request), even though this is not explicitly documented
   * on the ListDirectoryBuckets API page.
   */
  private boolean isS3ExpressSignedRequest() {
    if (signatureInfo == null) {
      return false;
    }
    String credentialScope = signatureInfo.getCredentialScope();
    if (credentialScope == null || credentialScope.isEmpty()) {
      return false;
    }
    String[] parts = credentialScope.split("/");
    return parts.length >= 3 && S3Consts.S3_EXPRESS_SERVICE.equals(parts[2]);
  }

  /**
   * Rest endpoint to list FSO (directory) buckets owned by the current user.
   */
  private Response listDirectoryBuckets()
      throws OS3Exception, IOException {
    long startNanos = Time.monotonicNowNanos();
    boolean auditSuccess = true;
    try {
      final String continueToken = queryParams().get(QueryParams.CONTINUATION_TOKEN);
      int maxDirectoryBuckets =
          queryParams().getInt(QueryParams.MAX_DIRECTORY_BUCKETS, S3Consts.MAX_DIRECTORY_BUCKETS_LIMIT);
      maxDirectoryBuckets = validateMaxDirectoryBuckets(maxDirectoryBuckets);

      ListDirectoryBucketsResponse response = new ListDirectoryBucketsResponse();
      String previousBucket = null;
      if (continueToken != null) {
        previousBucket = ContinueToken.decodeFromString(continueToken).getLastKey();
      }

      Iterator<? extends OzoneBucket> bucketIterator;
      try {
        if (previousBucket == null) {
          bucketIterator = listS3Buckets(null, volume -> { });
        } else {
          bucketIterator = listS3Buckets(null, previousBucket, volume -> { });
        }
      } catch (Exception e) {
        getMetrics().updateListS3BucketsFailureStats(startNanos);
        throw e;
      }

      String bucketRegion = resolveBucketRegion();
      String accountId = S3Owner.DEFAULT_S3OWNER_ID;
      int count = 0;
      String lastBucketName = null;
      while (bucketIterator.hasNext() && count < maxDirectoryBuckets) {
        OzoneBucket bucket = bucketIterator.next();
        if (!bucket.getBucketLayout().isFileSystemOptimized()) {
          continue;
        }
        DirectoryBucketMetadata metadata = new DirectoryBucketMetadata();
        metadata.setName(bucket.getName());
        metadata.setCreationDate(bucket.getCreationTime());
        metadata.setBucketRegion(bucketRegion);
        metadata.setBucketArn(buildDirectoryBucketArn(bucketRegion, accountId,
            bucket.getName()));
        response.addBucket(metadata);
        lastBucketName = bucket.getName();
        count++;
      }

      if (lastBucketName != null && bucketIterator.hasNext()) {
        response.setContinuationToken(
            new ContinueToken(lastBucketName, null).encodeToString());
      }

      getMetrics().updateListS3BucketsSuccessStats(startNanos);
      return Response.ok(response).build();
    } catch (Exception ex) {
      auditSuccess = false;
      auditReadFailure(S3GAction.LIST_DIRECTORY_BUCKETS, ex);
      throw ex;
    } finally {
      if (auditSuccess) {
        auditReadSuccess(S3GAction.LIST_DIRECTORY_BUCKETS);
      }
    }
  }

  private Response listAllBuckets()
      throws OS3Exception, IOException {
    long startNanos = Time.monotonicNowNanos();
    boolean auditSuccess = true;
    try {
      ListBucketResponse response = new ListBucketResponse();
      Iterator<? extends OzoneBucket> bucketIterator;
      try {
        bucketIterator =
            listS3Buckets(null, volume -> response.setOwner(S3Owner.of(volume.getOwner())));
      } catch (Exception e) {
        getMetrics().updateListS3BucketsFailureStats(startNanos);
        throw e;
      }

      while (bucketIterator.hasNext()) {
        OzoneBucket next = bucketIterator.next();
        BucketMetadata bucketMetadata = new BucketMetadata();
        bucketMetadata.setName(next.getName());
        bucketMetadata.setCreationDate(next.getCreationTime());
        response.addBucket(bucketMetadata);
      }

      getMetrics().updateListS3BucketsSuccessStats(startNanos);
      return Response.ok(response).build();
    } catch (Exception ex) {
      auditSuccess = false;
      auditReadFailure(S3GAction.LIST_S3_BUCKETS, ex);
      throw ex;
    } finally {
      if (auditSuccess) {
        auditReadSuccess(S3GAction.LIST_S3_BUCKETS);
      }
    }
  }

  private int validateMaxDirectoryBuckets(int maxDirectoryBuckets) throws OS3Exception {
    if (maxDirectoryBuckets < 0) {
      throw newError(INVALID_ARGUMENT, "max-directory-buckets must be >= 0");
    }
    return Math.min(maxDirectoryBuckets, S3Consts.MAX_DIRECTORY_BUCKETS_LIMIT);
  }

  private String resolveBucketRegion() {
    if (signatureInfo != null) {
      String credentialScope = signatureInfo.getCredentialScope();
      if (credentialScope != null && !credentialScope.isEmpty()) {
        String[] parts = credentialScope.split("/");
        if (parts.length >= 2 && !parts[1].isEmpty()) {
          return parts[1];
        }
      }
    }
    return S3Consts.DEFAULT_S3_REGION;
  }

  static String buildDirectoryBucketArn(String region, String accountId,
      String bucketName) {
    return String.format("arn:aws:s3express:%s:%s:bucket/%s",
        region, accountId, bucketName);
  }
}
