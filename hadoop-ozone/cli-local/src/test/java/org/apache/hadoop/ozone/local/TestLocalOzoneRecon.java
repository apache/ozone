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

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_SYNC_TASK_INTERVAL_DELAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.http.BaseHttpServer;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Integration tests for Recon in the {@code ozone local} runtime.
 */
class TestLocalOzoneRecon {

  @TempDir
  private Path tempDir;

  static Stream<Arguments> services() {
    return Stream.of(false, true).flatMap(s3g -> Stream.of(false, true).map(recon -> Arguments.of(s3g, recon)));
  }

  /** A 200 from Recon's /api/v1/clusterState shows that Recon resources can be injected in the shared JVM. */
  @ParameterizedTest(name = "s3g={0}, recon={1}")
  @MethodSource("services")
  void enabledServicesServeAndStopOnClose(boolean s3gEnabled, boolean reconEnabled) throws Exception {
    List<String> endpoints = new ArrayList<>();
    try (LocalOzoneCluster cluster =
        new LocalOzoneCluster(config(s3gEnabled, reconEnabled), new OzoneConfiguration())) {
      cluster.start();

      assertEquals(reconEnabled, syncThreadRunning());
      if (reconEnabled) {
        assertEquals(200, responseCode(cluster.getReconEndpoint() + "/api/v1/clusterState"));
        endpoints.add(cluster.getReconEndpoint());
      }
      if (s3gEnabled) {
        assertTrue(responseCode(cluster.getS3Endpoint()) > 0);
        endpoints.add(cluster.getS3Endpoint());
      } else {
        assertEquals(-1, cluster.getS3gPort());
      }
    }

    GenericTestUtils.waitFor(() -> !syncThreadRunning(), 100, 10_000);
    for (String endpoint : endpoints) {
      assertThrows(ConnectException.class, () -> responseCode(endpoint), endpoint);
    }
  }

  /**
   * Regression test: ReconServer#call() logged an initialization failure and returned normally, so when
   * ReconStorageContainerManagerFacade#start() failed after the Recon HTTP server started, start() succeeded without
   * the Recon datanode RPC server. A zero sync interval fails it there, because
   * ScheduledExecutorService#scheduleWithFixedDelay rejects a delay that is not positive.
   */
  @ParameterizedTest(name = "s3g={0}")
  @ValueSource(booleans = {false, true})
  void startFailsAndStopsReconWhenReconFailsAfterHttpServerStarts(boolean s3gEnabled) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_RECON_SCM_CONTAINER_SYNC_TASK_INTERVAL_DELAY, "0s");

    try (LogCapturer httpLogs = LogCapturer.captureLogs(BaseHttpServer.class);
        LocalOzoneCluster cluster = new LocalOzoneCluster(config(s3gEnabled, true), conf)) {
      assertThrows(IllegalArgumentException.class, cluster::start);
      assertThat(httpLogs.getOutput()).contains("HTTP server of recon listening");
      assertThrows(ConnectException.class, () -> responseCode(cluster.getReconEndpoint()));
    }
  }

  private LocalOzoneClusterConfig config(boolean s3gEnabled, boolean reconEnabled) {
    return LocalOzoneClusterConfig.builder(tempDir.resolve("local-ozone-recon"))
        .setS3gEnabled(s3gEnabled)
        .setReconEnabled(reconEnabled)
        .setStartupTimeout(Duration.ofMinutes(2))
        .build();
  }

  private static boolean syncThreadRunning() {
    return Thread.getAllStackTraces().keySet().stream()
        .anyMatch(thread -> thread.getName().contains("SyncSCMContainerInfo"));
  }

  private static int responseCode(String endpoint) throws Exception {
    HttpURLConnection connection = (HttpURLConnection) new URL(endpoint).openConnection();
    try {
      connection.setConnectTimeout(1_000);
      connection.setReadTimeout(1_000);
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }
}
