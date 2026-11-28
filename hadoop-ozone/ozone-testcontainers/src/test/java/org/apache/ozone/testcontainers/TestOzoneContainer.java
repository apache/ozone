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

package org.apache.ozone.testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.URI;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/** S3 smoke test, which is skipped when Docker is unavailable. */
class TestOzoneContainer {

  @Test
  void putGetThroughS3Gateway() {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(),
        "Docker is not available");
    try (OzoneContainer ozone = new OzoneContainer(smokeTestImage())) {
      // Nothing in the image listens on this port, so it must not hold up readiness.
      ozone.addExposedPort(9999);
      ozone.start();

      try (S3Client s3 = S3Client.builder()
          .endpointOverride(URI.create(ozone.getS3Endpoint()))
          .credentialsProvider(StaticCredentialsProvider.create(
              AwsBasicCredentials.create(ozone.getAccessKey(), ozone.getSecretKey())))
          .region(Region.of(ozone.getRegion()))
          .forcePathStyle(true)
          .overrideConfiguration(c -> c.retryStrategy(b -> b.maxAttempts(1)))
          .build()) {

        String bucket = "test-bucket";
        String key = "hello.txt";
        String body = "hello ozone";

        s3.createBucket(b -> b.bucket(bucket));
        s3.putObject(b -> b.bucket(bucket).key(key), RequestBody.fromString(body));

        ResponseBytes<GetObjectResponse> got =
            s3.getObjectAsBytes(b -> b.bucket(bucket).key(key));

        assertEquals(body, got.asUtf8String());
      }
    }
  }

  /** CI sets {@code OZONE_IMAGE} to the ghcr.io mirror to avoid Docker Hub rate limits. */
  private static DockerImageName smokeTestImage() {
    String repository = System.getenv().getOrDefault("OZONE_IMAGE", "apache/ozone");
    return DockerImageName.parse(repository + ":2.2.1-all-in-one");
  }
}
