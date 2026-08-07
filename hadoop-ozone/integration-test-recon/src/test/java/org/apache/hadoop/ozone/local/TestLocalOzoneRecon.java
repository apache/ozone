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

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for Recon in the {@code ozone local} runtime.
 */
class TestLocalOzoneRecon {

  @TempDir
  private Path tempDir;

  @Test
  void reconServesRequestsWhenEnabled() throws Exception {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(
            tempDir.resolve("local-ozone-recon"))
        .setS3gEnabled(false)
        .setReconEnabled(true)
        .setStartupTimeout(Duration.ofMinutes(2))
        .build();

    try (LocalOzoneCluster cluster = new LocalOzoneCluster(config, new OzoneConfiguration())) {
      cluster.start();

      assertTrue(cluster.getReconPort() > 0);
      assertHttpEndpointResponds(cluster.getReconEndpoint());
    }
  }

  private static void assertHttpEndpointResponds(String endpoint) throws Exception {
    HttpURLConnection connection = (HttpURLConnection) new URL(endpoint).openConnection();
    try {
      connection.setConnectTimeout(1_000);
      connection.setReadTimeout(1_000);
      // Any HTTP response proves Recon is serving requests.
      assertTrue(connection.getResponseCode() > 0, endpoint);
    } finally {
      connection.disconnect();
    }
  }
}
