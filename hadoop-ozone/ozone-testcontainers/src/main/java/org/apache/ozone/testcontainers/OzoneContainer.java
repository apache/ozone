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

import java.io.IOException;
import java.time.Duration;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;

/**
 * Testcontainers wrapper for a single-node Ozone with S3 Gateway, so JVM
 * integration tests can start Ozone the way they use the MinIO or LocalStack
 * modules. Wraps the all-in-one quickstart image (HDDS-14452 / ozone-docker#49),
 * which runs SCM + OM + datanode + S3 Gateway in one container and exposes S3G
 * on 9878. Non-secure S3G accepts any access key / secret.
 *
 * <p>Credentials: in non-secure mode S3G accepts any access key / secret, so
 * {@link #getAccessKey()} / {@link #getSecretKey()} / {@link #getRegion()} just
 * provide a consistent set for the caller to sign requests with; the fluent
 * setters change those returned values, not the container itself.
 *
 * <p>Readiness: the container waits for the S3 Gateway port to listen and then
 * for SCM to leave safe mode ({@code ozone admin safemode wait}), so the cluster
 * is ready for S3 operations as soon as {@link #start()} returns.
 *
 * <p><b>Incubating:</b> this module is new and its API may change in a future
 * release.
 */
public class OzoneContainer extends GenericContainer<OzoneContainer> {

  public static final int S3G_PORT = 9878;

  private static final DockerImageName DEFAULT_IMAGE =
      DockerImageName.parse("apache/ozone:2.2.0-all-in-one");

  private String accessKey = "testuser";
  private String secretKey = "testsecret";
  private String region = "us-east-1";

  public OzoneContainer() {
    this(DEFAULT_IMAGE);
  }

  public OzoneContainer(DockerImageName image) {
    super(image);
    withExposedPorts(S3G_PORT);
    waitingFor(new WaitAllStrategy()
        .withStrategy(Wait.forListeningPort())
        .withStrategy(new SafeModeWaitStrategy())
        .withStartupTimeout(Duration.ofMinutes(4)));
  }

  public OzoneContainer withAccessKey(String value) {
    this.accessKey = value;
    return this;
  }

  public OzoneContainer withSecretKey(String value) {
    this.secretKey = value;
    return this;
  }

  public OzoneContainer withRegion(String value) {
    this.region = value;
    return this;
  }

  /** @return S3 Gateway endpoint reachable from the host, e.g. http://localhost:32773 */
  public String getS3Endpoint() {
    return "http://" + getHost() + ":" + getMappedPort(S3G_PORT);
  }

  public String getAccessKey() {
    return accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public String getRegion() {
    return region;
  }

  /**
   * Waits until SCM leaves safe mode by running {@code ozone admin safemode wait}
   * inside the container, so the cluster is ready for S3 operations.
   */
  private static final class SafeModeWaitStrategy extends AbstractWaitStrategy {
    @Override
    protected void waitUntilReady() {
      try {
        Container.ExecResult result = waitStrategyTarget.execInContainer(
            "ozone", "admin", "safemode", "wait", "-t", "120");
        if (result.getExitCode() != 0) {
          throw new ContainerLaunchException(
              "Ozone did not leave safe mode: " + result.getStderr());
        }
      } catch (IOException e) {
        throw new ContainerLaunchException("Failed to wait for Ozone readiness", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ContainerLaunchException("Interrupted waiting for Ozone readiness", e);
      }
    }
  }
}
