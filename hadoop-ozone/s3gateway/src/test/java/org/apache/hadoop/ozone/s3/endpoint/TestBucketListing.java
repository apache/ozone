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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestBucketListing {

  private static final String TEST_BUCKET_NAME = "test-bucket";

  private BucketEndpoint bucketEndpoint;

  @BeforeEach
  void setUp() throws IOException {
    OzoneClientStub client = new OzoneClientStub();
    bucketEndpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .build();
    client.getObjectStore().createS3Bucket(TEST_BUCKET_NAME);
  }

  private S3RequestContext newContext() {
    return new S3RequestContext(bucketEndpoint, S3GAction.GET_BUCKET);
  }

  @Test
  void testFromQueryParams() throws Exception {
    bucketEndpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    bucketEndpoint.queryParamsForTest().setInt(QueryParams.MAX_KEYS, 100);
    bucketEndpoint.queryParamsForTest().set(QueryParams.PREFIX, "test/");

    BucketListing listing = BucketListing.fromQueryParams(newContext(), bucketEndpoint,
        TEST_BUCKET_NAME);

    assertEquals(100, listing.getMaxKeys());
    assertEquals("test/", listing.getPrefix());
  }

  @Test
  void testBuildResponseUsesCappedMaxKeys() throws Exception {
    bucketEndpoint.queryParamsForTest().setInt(QueryParams.MAX_KEYS, 5000);

    BucketListing listing = BucketListing.fromQueryParams(newContext(), bucketEndpoint,
        TEST_BUCKET_NAME);

    try (Response response = listing.buildResponse()) {
      ListObjectResponse entity = (ListObjectResponse) response.getEntity();
      assertEquals(1000, entity.getMaxKeys());
      assertEquals(0, entity.getKeyCount());
    }
  }

  @Test
  void testFromQueryParamsWithInvalidEncodingType() {
    bucketEndpoint.queryParamsForTest().set(QueryParams.ENCODING_TYPE, "invalid");

    OS3Exception exception = assertThrows(OS3Exception.class,
        () -> BucketListing.fromQueryParams(newContext(), bucketEndpoint, TEST_BUCKET_NAME));

    assertEquals(S3ErrorTable.INVALID_ARGUMENT.getCode(), exception.getCode());
  }

  @Test
  void testFromQueryParamsWithNonexistentBucket() {
    OMException exception = assertThrows(OMException.class,
        () -> BucketListing.fromQueryParams(newContext(), bucketEndpoint, "nonexistent-bucket"));

    assertEquals(OMException.ResultCodes.BUCKET_NOT_FOUND, exception.getResult());
  }
}
