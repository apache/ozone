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

package org.apache.hadoop.ozone.local;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Integration tests for the S3 Gateway of the {@code ozone local} runtime.
 */
class TestLocalOzoneS3 {

  @TempDir
  private Path tempDir;

  /**
   * The gateway serves signed S3 requests on a loopback-only listener, and accepts any
   * credentials because the local runtime leaves security off. Regression guard rather than a
   * fix: non-secure OM has always skipped signature validation, so these assertions do not go red
   * against a runtime that also stores a secret in OM.
   */
  @Test
  void s3GatewayServesRequests() throws Exception {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(tempDir.resolve("local-ozone-s3")).build();

    try (LocalOzoneCluster cluster = new LocalOzoneCluster(config, new OzoneConfiguration())) {
      cluster.start();

      assertTrue(cluster.getS3gPort() > 0);
      assertTrue(cluster.getS3gBoundAddress().getAddress().isLoopbackAddress(),
          () -> cluster.getS3gBoundAddress().toString());

      assertBucketRoundTrip(cluster.getS3Endpoint(), "local-smoke",
          LocalOzoneClusterConfig.LOCAL_S3_ACCESS_KEY, LocalOzoneClusterConfig.LOCAL_S3_SECRET_KEY);
      // Credentials the runtime never saw work the same, for the reason given on this test.
      String unknown = UUID.randomUUID().toString().replace("-", "");
      assertBucketRoundTrip(cluster.getS3Endpoint(), "local-smoke-" + unknown, unknown, unknown);
    }
  }

  /**
   * The SDK sends a SigV4-signed request, which is what AuthorizationFilter requires of every
   * caller; whether that signature verifies is decided separately, in OM.
   */
  private static void assertBucketRoundTrip(String endpoint, String bucket,
      String accessKey, String secretKey) {
    AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
    try (S3Client s3 = S3Client.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.of(LocalOzoneClusterConfig.LOCAL_S3_REGION))
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .forcePathStyle(true)
        .build()) {
      s3.createBucket(request -> request.bucket(bucket));
      assertTrue(s3.listBuckets().buckets().stream().anyMatch(b -> bucket.equals(b.name())));
    }
  }
}
