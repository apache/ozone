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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/** Readiness tests and an S3 smoke test, which is skipped when Docker is unavailable. */
class TestOzoneContainer {

  @Test
  void retriesSafeModeCheck() throws IOException, InterruptedException {
    WaitStrategyTarget target = targetWithoutPorts();
    ExecResult notReady = execResult(1);
    ExecResult ready = execResult(0);
    when(target.execInContainer(any(String[].class))).thenReturn(notReady, ready);

    try (TestableOzoneContainer ozone = new TestableOzoneContainer()) {
      ozone.withStartupTimeout(Duration.ofSeconds(2));
      ozone.awaitReady(target);
    }

    ArgumentCaptor<String[]> command = ArgumentCaptor.forClass(String[].class);
    verify(target, times(2)).execInContainer(command.capture());
    assertArrayEquals(new String[] {"/bin/sh", "-c", "ozone admin safemode wait -t 5"}, command.getValue());
  }

  @Test
  void retriesTransientExecFailure() throws IOException, InterruptedException {
    WaitStrategyTarget target = targetWithoutPorts();
    ExecResult ready = execResult(0);
    when(target.execInContainer(any(String[].class)))
        .thenThrow(new IOException("SCM is starting"))
        .thenReturn(ready);

    try (TestableOzoneContainer ozone = new TestableOzoneContainer()) {
      ozone.withStartupTimeout(Duration.ofSeconds(2));
      ozone.awaitReady(target);
    }

    verify(target, times(2)).execInContainer(any(String[].class));
  }

  @Test
  void respectsStartupTimeout() throws IOException, InterruptedException {
    WaitStrategyTarget target = targetWithoutPorts();
    ExecResult notReady = execResult(1);
    when(target.execInContainer(any(String[].class))).thenAnswer(invocation -> {
      Thread.sleep(50);
      return notReady;
    });

    try (TestableOzoneContainer ozone = new TestableOzoneContainer()) {
      ozone.withStartupTimeout(Duration.ofSeconds(2));
      assertTimeoutPreemptively(Duration.ofSeconds(10),
          () -> assertThrows(RuntimeException.class, () -> ozone.awaitReady(target)));
    }

    verify(target, atLeast(2)).execInContainer(any(String[].class));
  }

  @Test
  void putGetThroughS3Gateway() {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(),
        "Docker is not available");
    try (OzoneContainer ozone = new OzoneContainer(smokeTestImage())) {
      ozone.start();

      try (S3Client s3 = S3Client.builder()
          .endpointOverride(URI.create(ozone.getS3Endpoint()))
          .credentialsProvider(StaticCredentialsProvider.create(
              AwsBasicCredentials.create(ozone.getAccessKey(), ozone.getSecretKey())))
          .region(Region.of(ozone.getRegion()))
          .forcePathStyle(true)
          .build()) {

        String bucket = "test-bucket";
        String key = "hello.txt";
        String body = "hello ozone";

        // The container is ready (out of safe mode) after start(), so no retry.
        s3.createBucket(b -> b.bucket(bucket));
        s3.putObject(b -> b.bucket(bucket).key(key), RequestBody.fromString(body));

        ResponseBytes<GetObjectResponse> got =
            s3.getObjectAsBytes(b -> b.bucket(bucket).key(key));

        assertEquals(body, got.asUtf8String());
        assertThat(s3.listBuckets().buckets()).extracting(Bucket::name).contains(bucket);
      }
    }
  }

  /** CI sets {@code OZONE_IMAGE} to the ghcr.io mirror to avoid Docker Hub rate limits. */
  private static DockerImageName smokeTestImage() {
    String repository = System.getenv().getOrDefault("OZONE_IMAGE", "apache/ozone");
    return DockerImageName.parse(repository + ":2.2.1-all-in-one").asCompatibleSubstituteFor("apache/ozone");
  }

  private static ExecResult execResult(int exitCode) {
    ExecResult result = mock(ExecResult.class);
    when(result.getExitCode()).thenReturn(exitCode);
    return result;
  }

  private static WaitStrategyTarget targetWithoutPorts() {
    WaitStrategyTarget target = mock(WaitStrategyTarget.class, RETURNS_DEEP_STUBS);
    // Exercise the production command strategy; the Docker smoke test covers port readiness.
    when(target.getLivenessCheckPortNumbers()).thenReturn(Collections.emptySet());
    return target;
  }

  private static final class TestableOzoneContainer extends OzoneContainer {
    void awaitReady(WaitStrategyTarget target) {
      getWaitStrategy().waitUntilReady(target);
    }
  }
}
