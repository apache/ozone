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
import java.util.Iterator;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.s3.commontypes.BucketMetadata;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
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

  @Override
  public void init() {

  }
}
