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

package org.apache.hadoop.ozone.s3;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.ozone.test.ClusterForTests;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Integration test for the S3 Gateway health endpoints against a live
 * {@link MiniOzoneCluster}.  Covers both the ready path (OM reachable) and the
 * not-ready path (OM stopped).  The OM-stop step is destructive and the cluster
 * is shared across the class, so the checks are ordered.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TestS3GatewayHealthCheck extends ClusterForTests<MiniOzoneCluster> {

  private final S3GatewayService s3g = new S3GatewayService();

  @Override
  protected OzoneConfiguration createOzoneConfig() {
    OzoneConfiguration conf = createBaseConfiguration();
    // Poll OM often and time out quickly so readiness flips fast in the test.
    conf.setTimeDuration("ozone.s3g.health-check.probe.interval", 500, TimeUnit.MILLISECONDS);
    conf.setTimeDuration("ozone.s3g.health-check.probe.timeout", 2, TimeUnit.SECONDS);
    return conf;
  }

  @Override
  protected MiniOzoneCluster createCluster() throws Exception {
    return newClusterBuilder()
        .addService(s3g)
        .build();
  }

  @Test
  @Order(1)
  void livenessReturnsOk() throws Exception {
    assertEquals(HTTP_OK, responseCode(healthUrl("/health/live")));
    assertEquals("OK", body(healthUrl("/health/live")));
  }

  @Test
  @Order(2)
  void readinessReturnsReadyWhenOmReachable() throws Exception {
    GenericTestUtils.waitFor(() -> responseCode(healthUrl("/health/ready")) == HTTP_OK,
        200, 30_000);
    assertEquals("READY", body(healthUrl("/health/ready")));
  }

  @Test
  @Order(3)
  void readinessReturnsNotReadyWhenOmUnreachable() throws Exception {
    getCluster().getOzoneManager().stop();

    GenericTestUtils.waitFor(() -> responseCode(healthUrl("/health/ready")) == HTTP_UNAVAILABLE,
        200, 30_000);
    assertEquals("NOT READY", body(healthUrl("/health/ready")));

    // Liveness stays up regardless of OM reachability.
    assertEquals(HTTP_OK, responseCode(healthUrl("/health/live")));
  }

  private String healthUrl(String path) {
    String address = s3g.getConf()
        .get(S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTP_ADDRESS_KEY);
    return "http://" + address + path;
  }

  private static int responseCode(String url) {
    HttpURLConnection connection = null;
    try {
      connection = (HttpURLConnection) new URL(url).openConnection();
      connection.setRequestMethod("GET");
      return connection.getResponseCode();
    } catch (Exception e) {
      return -1;
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private static String body(String url) throws Exception {
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestMethod("GET");
    int code = connection.getResponseCode();
    try (InputStream in = code < 400
        ? connection.getInputStream() : connection.getErrorStream()) {
      return IOUtils.toString(in, StandardCharsets.UTF_8).trim();
    } finally {
      connection.disconnect();
    }
  }
}
