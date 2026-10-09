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
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.ozone.ClientConfigForTesting;
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
import software.amazon.awssdk.services.s3.model.StorageClass;

/**
 * Integration tests for the S3 Gateway of the {@code ozone local} runtime.
 */
class TestLocalOzoneS3 {

  @TempDir
  private Path tempDir;

  /**
   * Security is off, so the S3 Gateway accepts credentials it has never seen. The object carries an explicit STANDARD
   * storage class, which succeeds on the single default datanode only because ozone local sets
   * {@code ozone.s3g.standard.storage-class.use-client-default}; otherwise S3StorageType maps it to RATIS/THREE.
   */
  @Test
  void s3GatewayServesRequests() throws Exception {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(tempDir.resolve("local-ozone-s3")).build();
    OzoneConfiguration conf = new OzoneConfiguration();
    // Match the test module's 128 MB containers; production blocks default to 256 MB.
    ClientConfigForTesting.newBuilder(StorageUnit.MB).applyTo(conf);

    try (LocalOzoneCluster cluster = new LocalOzoneCluster(config, conf)) {
      cluster.start();

      assertTrue(cluster.getS3gPort() > 0);
      assertTrue(cluster.getS3gBoundAddress().getAddress().isLoopbackAddress(),
          () -> cluster.getS3gBoundAddress().toString());

      assertBucketRoundTrip(cluster.getS3Endpoint(), "local-smoke",
          LocalOzoneClusterConfig.LOCAL_S3_ACCESS_KEY, LocalOzoneClusterConfig.LOCAL_S3_SECRET_KEY);
      String unknown = UUID.randomUUID().toString().replace("-", "");
      assertBucketRoundTrip(cluster.getS3Endpoint(), "local-smoke-" + unknown, unknown, unknown);

      try (S3Client s3 = s3Client(cluster.getS3Endpoint(),
          LocalOzoneClusterConfig.LOCAL_S3_ACCESS_KEY, LocalOzoneClusterConfig.LOCAL_S3_SECRET_KEY)) {
        s3.putObject(request -> request.bucket("local-smoke").key("object").storageClass(StorageClass.STANDARD),
            RequestBody.fromString("payload"));
        assertEquals("payload",
            s3.getObjectAsBytes(request -> request.bucket("local-smoke").key("object")).asUtf8String());
      }
    }
  }

  private static void assertBucketRoundTrip(String endpoint, String bucket, String accessKey, String secretKey) {
    try (S3Client s3 = s3Client(endpoint, accessKey, secretKey)) {
      s3.createBucket(request -> request.bucket(bucket));
      assertTrue(s3.listBuckets().buckets().stream().anyMatch(b -> bucket.equals(b.name())));
    }
  }

  private static S3Client s3Client(String endpoint, String accessKey, String secretKey) {
    return S3Client.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.of(LocalOzoneClusterConfig.LOCAL_S3_REGION))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
        .forcePathStyle(true)
        .build();
  }

  /** Exercises create, list, put and get with explicit SDK credentials. */
  @Test
  void awsSdkCanCreateListPutAndGetAgainstLocalRuntime() throws Exception {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(
            tempDir.resolve("local-ozone-s3-sdk"))
        .setStartupTimeout(Duration.ofMinutes(3))
        .build();

    String bucketName = "local-" + UUID.randomUUID().toString().replace("-", "");
    String keyName = "key-" + UUID.randomUUID().toString().replace("-", "");
    String payload = "local-ozone-s3";
    OzoneConfiguration conf = new OzoneConfiguration();
    // Match the test module's 128 MB containers; production blocks default to 256 MB.
    ClientConfigForTesting.newBuilder(StorageUnit.MB).applyTo(conf);

    try (LocalOzoneCluster cluster = new LocalOzoneCluster(config, conf)) {
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
