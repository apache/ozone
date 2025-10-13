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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Simple unit tests for the refactored BucketEndpoint methods.
 */
public class TestBucketEndpoint {

  private static final String TEST_BUCKET_NAME = "test-bucket";
  private BucketEndpoint bucketEndpoint;

  @BeforeEach
  public void setUp() throws IOException {
    OzoneClientStub client = new OzoneClientStub();
    
    bucketEndpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .build();
    
    // Create a test bucket
    client.getObjectStore().createS3Bucket(TEST_BUCKET_NAME);
  }

  // Note: validateMaxKeys is a private helper and is covered indirectly by
  // public API tests elsewhere. We avoid calling private helpers directly
  // from unit tests in this module to keep encapsulation intact.

  @Test
  public void testBucketListingContextCreation() throws OS3Exception, IOException {
    // Test basic context creation
    String bucketName = TEST_BUCKET_NAME;
    String delimiter = "/";
    String encodingType = null;
    String marker = null;
    int maxKeys = 100;
    String prefix = "test/";
    String continueToken = null;
    String startAfter = null;
    
    BucketEndpoint.BucketListingContext context = bucketEndpoint.validateAndPrepareParameters(
        bucketName, delimiter, encodingType, marker, maxKeys, prefix, 
        continueToken, startAfter);
    
    assertNotNull(context);
    assertEquals(bucketName, context.getBucketName());
    assertEquals(delimiter, context.getDelimiter());
    assertEquals(maxKeys, context.getMaxKeys());
    assertEquals(prefix, context.getPrefix());
  }

  @Test
  public void testInitializeListObjectResponse() {
    // Test response initialization
    String bucketName = TEST_BUCKET_NAME;
    String delimiter = "/";
    String encodingType = "url";
    String marker = "test-marker";
    int maxKeys = 100;
    String prefix = "test/";
    String continueToken = "test-token";
    String startAfter = "test-start";
    
    Object response = bucketEndpoint.initializeListObjectResponse(
        bucketName, delimiter, encodingType, marker, maxKeys, prefix, 
        continueToken, startAfter);
    
    assertNotNull(response);
    // Basic validation only; avoid depending on ListObjectResponse type here.
    assertEquals("ListObjectResponse", response.getClass().getSimpleName());
  }

  @Test
  public void testValidateAndPrepareParametersWithInvalidEncodingType() {
    // Test invalid encoding type
    String bucketName = TEST_BUCKET_NAME;
    String delimiter = "/";
    String encodingType = "invalid";
    String marker = null;
    int maxKeys = 100;
    String prefix = "test/";
    String continueToken = null;
    String startAfter = null;
    
    OS3Exception exception = assertThrows(OS3Exception.class, () -> 
        bucketEndpoint.validateAndPrepareParameters(
            bucketName, delimiter, encodingType, marker, maxKeys, prefix, 
            continueToken, startAfter));
    
    assertEquals(S3ErrorTable.INVALID_ARGUMENT.getCode(), exception.getCode());
  }

  @Test
  public void testValidateAndPrepareParametersWithNonexistentBucket() {
    // Test with non-existent bucket
    String bucketName = "nonexistent-bucket";
    String delimiter = "/";
    String encodingType = null;
    String marker = null;
    int maxKeys = 100;
    String prefix = "test/";
    String continueToken = null;
    String startAfter = null;
    
    OS3Exception exception = assertThrows(OS3Exception.class, () -> 
        bucketEndpoint.validateAndPrepareParameters(
            bucketName, delimiter, encodingType, marker, maxKeys, prefix, 
            continueToken, startAfter));
    
    assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), exception.getCode());
  }
}
