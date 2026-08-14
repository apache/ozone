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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;

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
      // Loopback-only by default: any credentials are accepted below, so binding a wider
      // interface must stay an explicit --bind-host choice.
      assertTrue(cluster.getS3gBoundAddress().getAddress().isLoopbackAddress(),
          () -> cluster.getS3gBoundAddress().toString());

      assertBucketRoundTrip(cluster.getS3Endpoint(), "local-smoke",
          LocalOzoneClusterConfig.LOCAL_S3_ACCESS_KEY, LocalOzoneClusterConfig.LOCAL_S3_SECRET_KEY);
      // Credentials the runtime has never seen work just as well: the local cluster leaves
      // security off, and S3SecurityUtil#validateS3Credential only verifies a signature when
      // ozone.security.enabled is true. This is what the summary tells the user, and what makes
      // it correct for the launcher to store no secret in OM.
      String unknown = UUID.randomUUID().toString().replace("-", "");
      assertBucketRoundTrip(cluster.getS3Endpoint(), "local-smoke-" + unknown, unknown, unknown);
    }
  }

  /**
   * Creates {@code bucket} through the endpoint the runtime advertises and reads it back from
   * ListBuckets, using the suggested AWS settings (path-style addressing, the printed region) and
   * the given credentials. The SDK sends a SigV4-signed request, which is what AuthorizationFilter
   * requires of every caller; whether that signature verifies is decided separately, in OM.
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

  /**
   * The object round-trip an S3 client actually performs: create, list, put, get. Signed with
   * credentials the launcher never saw, since the local runtime leaves security off and the
   * access key id only names the caller.
   */
  @Test
  void awsSdkCanCreateListPutAndGetAgainstLocalRuntime() throws Exception {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(
            tempDir.resolve("local-ozone-s3-sdk"))
        .setStartupTimeout(Duration.ofMinutes(3))
        .build();

    String bucketName = "local-" + UUID.randomUUID().toString().replace("-", "");
    String keyName = "key-" + UUID.randomUUID().toString().replace("-", "");
    String payload = "local-ozone-s3";

    try (LocalOzoneCluster cluster = new LocalOzoneCluster(config, new OzoneConfiguration())) {
      cluster.start();

      try (S3Client client = S3Client.builder()
          .region(Region.of(LocalOzoneClusterConfig.LOCAL_S3_REGION))
          .endpointOverride(URI.create(cluster.getS3Endpoint()))
          .credentialsProvider(StaticCredentialsProvider.create(
              AwsBasicCredentials.create("localuser", "localsecret")))
          .forcePathStyle(true)
          .build()) {
        client.createBucket(builder -> builder.bucket(bucketName));

        ListBucketsResponse buckets = client.listBuckets();
        assertTrue(buckets.buckets().stream()
            .anyMatch(bucket -> bucketName.equals(bucket.name())));

        client.putObject(builder -> builder.bucket(bucketName).key(keyName),
            RequestBody.fromString(payload));

        ResponseBytes<GetObjectResponse> response = client.getObjectAsBytes(
            builder -> builder.bucket(bucketName).key(keyName));
        assertEquals(payload, response.asUtf8String());
      }
    }
  }
}
