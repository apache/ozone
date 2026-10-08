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

import com.google.common.collect.ImmutableSet;
import java.io.InputStream;
import java.util.Set;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;

/**
 * Rejects operations on bucket subresources that no other handler recognizes.
 * <p>
 * Subresources that have a handler ({@link BucketAclHandler}, {@link BucketGetLocationHandler},
 * {@link BucketLifecycleHandler}, {@link BucketTaggingHandler}, {@link ListMultipartUploadsHandler}) reject their own
 * unsupported HTTP methods.
 * <p>
 * Must be added to the chain before {@link BucketCrudHandler}. Otherwise, the requests fall through to another
 * operation of the same HTTP method: a GET returns a {@code ListBucketResult} body, a PUT creates the bucket and a
 * DELETE deletes it.
 * <p>
 * {@code ?delete} is only valid for POST ({@code DeleteObjects}), which does not go through this chain.
 */
class BucketOperationNotImplementedHandler extends BucketOperationHandler {

  private static final Set<String> SUBRESOURCES = ImmutableSet.of(
      QueryParams.ABAC, QueryParams.ACCELERATE, QueryParams.ANALYTICS, QueryParams.CORS, QueryParams.DELETE,
      QueryParams.ENCRYPTION, QueryParams.INTELLIGENT_TIERING, QueryParams.INVENTORY, QueryParams.LOGGING,
      QueryParams.METADATA_ANNOTATION_TABLE, QueryParams.METADATA_CONFIGURATION, QueryParams.METADATA_INVENTORY_TABLE,
      QueryParams.METADATA_JOURNAL_TABLE, QueryParams.METADATA_TABLE, QueryParams.METRICS, QueryParams.NOTIFICATION,
      QueryParams.OBJECT_LOCK, QueryParams.OWNERSHIP_CONTROLS, QueryParams.POLICY, QueryParams.POLICY_STATUS,
      QueryParams.PUBLIC_ACCESS_BLOCK, QueryParams.REPLICATION, QueryParams.REQUEST_PAYMENT, QueryParams.SESSION,
      QueryParams.VERSIONING, QueryParams.VERSIONS, QueryParams.WEBSITE);

  @Override
  Response handleGetRequest(S3RequestContext context, String bucketName) {
    return rejectNotImplemented(context, SUBRESOURCES, bucketName);
  }

  @Override
  Response handlePutRequest(S3RequestContext context, String bucketName, InputStream body) {
    return rejectNotImplemented(context, SUBRESOURCES, bucketName);
  }

  @Override
  Response handleDeleteRequest(S3RequestContext context, String bucketName) {
    return rejectNotImplemented(context, SUBRESOURCES, bucketName);
  }
}
