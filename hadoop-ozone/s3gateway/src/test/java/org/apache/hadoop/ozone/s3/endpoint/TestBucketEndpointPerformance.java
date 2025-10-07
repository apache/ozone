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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.RequestIdentifier;
import org.apache.hadoop.ozone.s3.commontypes.ListObjectResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Performance tests for the refactored BucketEndpoint.get() method.
 */
public class TestBucketEndpointPerformance {

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
    
    // Create a test bucket with many keys for performance testing
    client.getObjectStore().createS3Bucket("perf-test-bucket");
    var bucket = client.getObjectStore().getS3Bucket("perf-test-bucket");
    
    // Create 1000 keys for performance testing
    for (int i = 0; i < 1000; i++) {
      bucket.createKey("file" + i, 0).close();
    }
  }

  @Test
  public void testPerformanceWithLargeNumberOfKeys() throws Exception {
    // Test performance with large number of keys
    int iterations = 10;
    List<Long> executionTimes = new ArrayList<>();
    
    for (int i = 0; i < iterations; i++) {
      long startTime = System.nanoTime();
      
      var response = bucketEndpoint.get(
          "perf-test-bucket", null, null, null, 100, null, null, null, null, null, null, null, 1000);
      
      long endTime = System.nanoTime();
      long executionTime = endTime - startTime;
      executionTimes.add(executionTime);
      
      // Verify response is correct
      assertTrue(response.getStatus() == 200);
      var listResponse = (ListObjectResponse) response.getEntity();
      assertTrue(listResponse.getContents().size() == 100);
    }
    
    // Calculate average execution time
    long totalTime = executionTimes.stream().mapToLong(Long::longValue).sum();
    long averageTime = totalTime / iterations;
    
    // Performance should be reasonable (less than 1 second for 100 keys)
    assertTrue(averageTime < 1_000_000_000L, 
        "Average execution time should be less than 1 second, was: " + averageTime + " ns");
    
    System.out.println("Average execution time for 100 keys: " + averageTime + " ns");
  }

  @Test
  public void testPerformanceWithDifferentMaxKeys() throws Exception {
    // Test performance with different maxKeys values
    int[] maxKeysValues = {10, 50, 100, 500};
    
    for (int maxKeys : maxKeysValues) {
      long startTime = System.nanoTime();
      
      var response = bucketEndpoint.get(
          "perf-test-bucket", null, null, null, maxKeys, null, null, null, null, null, null, null, 1000);
      
      long endTime = System.nanoTime();
      long executionTime = endTime - startTime;
      
      // Verify response is correct
      assertTrue(response.getStatus() == 200);
      var listResponse = (ListObjectResponse) response.getEntity();
      assertTrue(listResponse.getContents().size() == maxKeys);
      
      System.out.println("Execution time for " + maxKeys + " keys: " + executionTime + " ns");
      
      // Performance should scale reasonably with maxKeys
      assertTrue(executionTime < 2_000_000_000L, 
          "Execution time should be less than 2 seconds for " + maxKeys + " keys");
    }
  }

  @Test
  public void testPerformanceWithPrefix() throws Exception {
    // Test performance with prefix filtering
    long startTime = System.nanoTime();
    
    var response = bucketEndpoint.get(
        "perf-test-bucket", null, null, null, 100, "file1", null, null, null, null, null, null, 1000);
    
    long endTime = System.nanoTime();
    long executionTime = endTime - startTime;
    
    // Verify response is correct
    assertTrue(response.getStatus() == 200);
    var listResponse = (ListObjectResponse) response.getEntity();
    assertTrue(listResponse.getContents().size() > 0);
    
    System.out.println("Execution time with prefix: " + executionTime + " ns");
    
    // Performance with prefix should be reasonable
    assertTrue(executionTime < 1_000_000_000L, 
        "Execution time with prefix should be less than 1 second");
  }

  @Test
  public void testPerformanceWithDelimiter() throws Exception {
    // Create some nested structure for delimiter testing
    var bucket = client.getObjectStore().getS3Bucket("perf-test-bucket");
    for (int i = 0; i < 100; i++) {
      bucket.createKey("dir" + i + "/file" + i, 0).close();
    }
    
    long startTime = System.nanoTime();
    
    var response = bucketEndpoint.get(
        "perf-test-bucket", "/", null, null, 100, null, null, null, null, null, null, null, 1000);
    
    long endTime = System.nanoTime();
    long executionTime = endTime - startTime;
    
    // Verify response is correct
    assertTrue(response.getStatus() == 200);
    var listResponse = (ListObjectResponse) response.getEntity();
    assertTrue(listResponse.getCommonPrefixes().size() > 0);
    
    System.out.println("Execution time with delimiter: " + executionTime + " ns");
    
    // Performance with delimiter should be reasonable
    assertTrue(executionTime < 1_000_000_000L, 
        "Execution time with delimiter should be less than 1 second");
  }

  @Test
  public void testPerformanceConsistency() throws Exception {
    // Test that performance is consistent across multiple calls
    int iterations = 20;
    List<Long> executionTimes = new ArrayList<>();
    
    for (int i = 0; i < iterations; i++) {
      long startTime = System.nanoTime();
      
      var response = bucketEndpoint.get(
          "perf-test-bucket", null, null, null, 50, null, null, null, null, null, null, null, 1000);
      
      long endTime = System.nanoTime();
      long executionTime = endTime - startTime;
      executionTimes.add(executionTime);
      
      assertTrue(response.getStatus() == 200);
    }
    
    // Calculate statistics
    long totalTime = executionTimes.stream().mapToLong(Long::longValue).sum();
    long averageTime = totalTime / iterations;
    long minTime = executionTimes.stream().mapToLong(Long::longValue).min().orElse(0);
    long maxTime = executionTimes.stream().mapToLong(Long::longValue).max().orElse(0);
    
    System.out.println("Performance consistency test:");
    System.out.println("  Average time: " + averageTime + " ns");
    System.out.println("  Min time: " + minTime + " ns");
    System.out.println("  Max time: " + maxTime + " ns");
    System.out.println("  Variance: " + (maxTime - minTime) + " ns");
    
    // Performance should be consistent (max should not be more than 3x min)
    assertTrue(maxTime <= minTime * 3, 
        "Performance should be consistent, max time should not be more than 3x min time");
  }
}
