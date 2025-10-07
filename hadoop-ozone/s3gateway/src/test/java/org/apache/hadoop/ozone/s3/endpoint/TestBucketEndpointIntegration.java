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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.RequestIdentifier;
import org.apache.hadoop.ozone.s3.commontypes.ListObjectResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the refactored BucketEndpoint.get() method.
 */
public class TestBucketEndpointIntegration {

  private BucketEndpoint bucketEndpoint;
  private OzoneClientStub client;

  @BeforeEach
  public void setUp() throws IOException {
    bucketEndpoint = new BucketEndpoint();
    client = new OzoneClientStub();
    
    bucketEndpoint.setClient(client);
    bucketEndpoint.setRequestIdentifier(new RequestIdentifier());
    bucketEndpoint.setOzoneConfiguration(new OzoneConfiguration());
    bucketEndpoint.init();
    
    // Create a test bucket with some keys
    client.getObjectStore().createS3Bucket("test-bucket");
    var bucket = client.getObjectStore().getS3Bucket("test-bucket");
    bucket.createKey("file1", 0).close();
    bucket.createKey("file2", 0).close();
    bucket.createKey("dir1/file3", 0).close();
    bucket.createKey("dir1/file4", 0).close();
  }

  @Test
  public void testGetMethodBasicFunctionality() throws Exception {
    // Test basic bucket listing functionality
    var response = bucketEndpoint.get(
        "test-bucket", null, null, null, 100, null, null, null, null, null, null, null, 1000);
    
    assertNotNull(response);
    assertEquals(200, response.getStatus());
    
    var listResponse = (ListObjectResponse) response.getEntity();
    assertNotNull(listResponse);
    assertEquals("test-bucket", listResponse.getName());
    assertTrue(listResponse.getContents().size() >= 4);
  }

  @Test
  public void testGetMethodWithPrefix() throws Exception {
    // Test bucket listing with prefix
    var response = bucketEndpoint.get(
        "test-bucket", null, null, null, 100, "dir1/", null, null, null, null, null, null, 1000);
    
    assertNotNull(response);
    assertEquals(200, response.getStatus());
    
    var listResponse = (ListObjectResponse) response.getEntity();
    assertNotNull(listResponse);
    assertEquals("test-bucket", listResponse.getName());
    assertEquals(2, listResponse.getContents().size());
  }

  @Test
  public void testGetMethodWithDelimiter() throws Exception {
    // Test bucket listing with delimiter
    var response = bucketEndpoint.get(
        "test-bucket", "/", null, null, 100, null, null, null, null, null, null, null, 1000);
    
    assertNotNull(response);
    assertEquals(200, response.getStatus());
    
    var listResponse = (ListObjectResponse) response.getEntity();
    assertNotNull(listResponse);
    assertEquals("test-bucket", listResponse.getName());
    assertTrue(listResponse.getCommonPrefixes().size() >= 1);
  }

  @Test
  public void testGetMethodWithMaxKeys() throws Exception {
    // Test bucket listing with maxKeys limit
    var response = bucketEndpoint.get(
        "test-bucket", null, null, null, 2, null, null, null, null, null, null, null, 1000);
    
    assertNotNull(response);
    assertEquals(200, response.getStatus());
    
    var listResponse = (ListObjectResponse) response.getEntity();
    assertNotNull(listResponse);
    assertEquals("test-bucket", listResponse.getName());
    assertEquals(2, listResponse.getContents().size());
    assertTrue(listResponse.isTruncated());
  }

  @Test
  public void testGetMethodWithEncodingType() throws Exception {
    // Test bucket listing with encoding type
    var response = bucketEndpoint.get(
        "test-bucket", null, "url", null, 100, null, null, null, null, null, null, null, 1000);
    
    assertNotNull(response);
    assertEquals(200, response.getStatus());
    
    var listResponse = (ListObjectResponse) response.getEntity();
    assertNotNull(listResponse);
    assertEquals("test-bucket", listResponse.getName());
    assertEquals("url", listResponse.getEncodingType());
  }

  @Test
  public void testGetMethodWithInvalidEncodingType() throws Exception {
    // Test bucket listing with invalid encoding type
    try {
      bucketEndpoint.get(
          "test-bucket", null, "invalid", null, 100, null, null, null, null, null, null, null, 1000);
      assertTrue(false, "Should have thrown OS3Exception");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Invalid Argument"));
    }
  }

  @Test
  public void testGetMethodWithNegativeMaxKeys() throws Exception {
    // Test bucket listing with negative maxKeys
    try {
      bucketEndpoint.get(
          "test-bucket", null, null, null, -1, null, null, null, null, null, null, null, 1000);
      assertTrue(false, "Should have thrown OS3Exception");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Invalid Argument"));
    }
  }

  @Test
  public void testGetMethodWithNonexistentBucket() throws Exception {
    // Test bucket listing with non-existent bucket
    try {
      bucketEndpoint.get(
          "nonexistent-bucket", null, null, null, 100, null, null, null, null, null, null, null, 1000);
      assertTrue(false, "Should have thrown OS3Exception");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("No Such Bucket"));
    }
  }

  @Test
  public void testGetMethodWithZeroMaxKeys() throws Exception {
    // Test bucket listing with zero maxKeys
    var response = bucketEndpoint.get(
        "test-bucket", null, null, null, 0, null, null, null, null, null, null, null, 1000);
    
    assertNotNull(response);
    assertEquals(200, response.getStatus());
    
    var listResponse = (ListObjectResponse) response.getEntity();
    assertNotNull(listResponse);
    assertEquals("test-bucket", listResponse.getName());
    assertEquals(0, listResponse.getContents().size());
    assertTrue(!listResponse.isTruncated());
  }
}
