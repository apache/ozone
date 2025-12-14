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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import javax.ws.rs.core.HttpHeaders;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class for BucketEndpointContext.
 */
public class TestBucketEndpointContext {

  private static final String BUCKET_NAME = OzoneConsts.S3_BUCKET;
  private OzoneClient client;
  private BucketEndpointContext context;

  @BeforeEach
  public void setup() throws IOException {
    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(BUCKET_NAME);

    HttpHeaders headers = mock(HttpHeaders.class);

    BucketEndpoint bucketEndpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .setHeaders(headers)
        .build();

    context = new BucketEndpointContext(bucketEndpoint);
  }

  @AfterEach
  public void clean() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  @Test
  public void testGetBucket() throws IOException, OS3Exception {
    OzoneBucket bucket = context.getBucket(BUCKET_NAME);
    assertNotNull(bucket, "Bucket should not be null");
    assertEquals(BUCKET_NAME, bucket.getName(),
        "Bucket name should match");
  }

  @Test
  public void testGetBucketNotFound() {
    assertThrows(OS3Exception.class, () -> {
      context.getBucket("nonexistent-bucket");
    }, "Should throw OS3Exception for non-existent bucket");
  }

  @Test
  public void testGetVolume() throws IOException, OS3Exception {
    OzoneVolume volume = context.getVolume();
    assertNotNull(volume, "Volume should not be null");
  }

  @Test
  public void testIsAccessDeniedWithPermissionDenied() {
    OMException exception = new OMException("Access denied",
        OMException.ResultCodes.PERMISSION_DENIED);

    assertTrue(context.isAccessDenied(exception),
        "Should return true for PERMISSION_DENIED");
  }

  @Test
  public void testIsAccessDeniedWithAccessDenied() {
    OMException exception = new OMException("Access denied",
        OMException.ResultCodes.ACCESS_DENIED);

    assertTrue(context.isAccessDenied(exception),
        "Should return true for ACCESS_DENIED");
  }

  @Test
  public void testIsAccessDeniedWithBucketNotFound() {
    OMException exception = new OMException("Bucket not found",
        OMException.ResultCodes.BUCKET_NOT_FOUND);

    assertFalse(context.isAccessDenied(exception),
        "Should return false for BUCKET_NOT_FOUND");
  }

  @Test
  public void testIsAccessDeniedWithKeyNotFound() {
    OMException exception = new OMException("Key not found",
        OMException.ResultCodes.KEY_NOT_FOUND);

    assertFalse(context.isAccessDenied(exception),
        "Should return false for KEY_NOT_FOUND");
  }

  @Test
  public void testIsAccessDeniedWithIOException() {
    IOException exception = new IOException("I/O error");

    assertFalse(context.isAccessDenied(exception),
        "Should return false for non-OMException");
  }

  @Test
  public void testIsAccessDeniedWithNullException() {
    assertFalse(context.isAccessDenied(null),
        "Should return false for null exception");
  }

  @Test
  public void testIsAccessDeniedWithRuntimeException() {
    RuntimeException exception = new RuntimeException("Runtime error");

    assertFalse(context.isAccessDenied(exception),
        "Should return false for RuntimeException");
  }

  @Test
  public void testGetEndpoint() {
    BucketEndpoint endpoint = context.getEndpoint();
    assertNotNull(endpoint, "Endpoint should not be null");
  }

  @Test
  public void testContextDelegatesCorrectly() throws IOException, OS3Exception {
    // Test that context properly delegates to endpoint methods
    OzoneBucket bucket = context.getBucket(BUCKET_NAME);
    OzoneVolume volume = context.getVolume();

    assertNotNull(bucket, "Delegated getBucket should work");
    assertNotNull(volume, "Delegated getVolume should work");
  }

  @Test
  public void testIsAccessDeniedWithMultipleResultCodes() {
    // Test all OMException result codes to ensure only access-related ones
    // return true

    OMException[] accessDeniedExceptions = {
        new OMException("", OMException.ResultCodes.PERMISSION_DENIED),
        new OMException("", OMException.ResultCodes.ACCESS_DENIED)
    };

    for (OMException ex : accessDeniedExceptions) {
      assertTrue(context.isAccessDenied(ex),
          "Should return true for " + ex.getResult());
    }

    OMException[] otherExceptions = {
        new OMException("", OMException.ResultCodes.BUCKET_NOT_FOUND),
        new OMException("", OMException.ResultCodes.KEY_NOT_FOUND),
        new OMException("", OMException.ResultCodes.VOLUME_NOT_FOUND),
        new OMException("", OMException.ResultCodes.INTERNAL_ERROR)
    };

    for (OMException ex : otherExceptions) {
      assertFalse(context.isAccessDenied(ex),
          "Should return false for " + ex.getResult());
    }
  }

  @Test
  public void testBucketOperationsWithContext() throws Exception {
    // Create a second bucket to test multiple operations
    String secondBucket = "test-bucket-2";
    client.getObjectStore().createS3Bucket(secondBucket);

    // Test getting different buckets through context
    OzoneBucket bucket1 = context.getBucket(BUCKET_NAME);
    OzoneBucket bucket2 = context.getBucket(secondBucket);

    assertNotNull(bucket1, "First bucket should not be null");
    assertNotNull(bucket2, "Second bucket should not be null");
    assertEquals(BUCKET_NAME, bucket1.getName(),
        "First bucket name should match");
    assertEquals(secondBucket, bucket2.getName(),
        "Second bucket name should match");
  }
}
