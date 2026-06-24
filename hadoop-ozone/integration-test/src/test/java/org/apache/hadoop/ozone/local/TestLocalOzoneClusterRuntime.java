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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for the SCM and OM portion of {@link LocalOzoneCluster}.
 */
class TestLocalOzoneClusterRuntime {

  @TempDir
  private Path tempDir;

  @Test
  void scmAndOmStartAndReuseExistingMetadata() throws Exception {
    String volumeName = uniqueName("vol");
    String bucketName = uniqueName("bucket");
    Path dataDir = tempDir.resolve("local-ozone-runtime");
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(dataDir)
        .setS3gEnabled(false)
        .setStartupTimeout(Duration.ofMinutes(2))
        .build();

    startRuntimeAndCreateBucket(config, volumeName, bucketName);
    restartRuntimeAndVerifyBucket(config, volumeName, bucketName);
  }

  @Test
  void formatNeverRejectsUninitializedScmOmStorage() throws Exception {
    Path dataDir = tempDir.resolve("local-ozone-runtime");
    LocalOzoneClusterConfig initialConfig =
        LocalOzoneClusterConfig.builder(dataDir).build();
    try (LocalOzoneCluster cluster =
             new LocalOzoneCluster(initialConfig, new OzoneConfiguration())) {
      cluster.prepareConfiguration();
    }

    LocalOzoneClusterConfig neverFormatConfig =
        LocalOzoneClusterConfig.builder(dataDir)
            .setFormatMode(LocalOzoneClusterConfig.FormatMode.NEVER)
            .setS3gEnabled(false)
            .build();

    IOException error = assertThrows(IOException.class, () -> {
      try (LocalOzoneCluster cluster =
               new LocalOzoneCluster(neverFormatConfig,
                   new OzoneConfiguration())) {
        cluster.start();
      }
    });

    assertTrue(error.getMessage().contains("storage is not initialized"),
        error.getMessage());
  }

  private void startRuntimeAndCreateBucket(LocalOzoneClusterConfig config,
      String volumeName, String bucketName) throws Exception {
    try (LocalOzoneCluster cluster =
             new LocalOzoneCluster(config, new OzoneConfiguration())) {
      OzoneConfiguration clientConf =
          cluster.prepareConfiguration().getConfiguration();
      cluster.start();

      assertServicePortsReachable(cluster);

      try (OzoneClient client = OzoneClientFactory.getRpcClient(clientConf)) {
        OzoneVolume volume = createVolume(client, volumeName);
        createBucket(volume, bucketName);
      }
    }
  }

  private void restartRuntimeAndVerifyBucket(LocalOzoneClusterConfig config,
      String volumeName, String bucketName) throws Exception {
    try (LocalOzoneCluster cluster =
             new LocalOzoneCluster(config, new OzoneConfiguration())) {
      OzoneConfiguration clientConf =
          cluster.prepareConfiguration().getConfiguration();
      cluster.start();

      assertServicePortsReachable(cluster);

      try (OzoneClient client = OzoneClientFactory.getRpcClient(clientConf)) {
        OzoneVolume volume = client.getObjectStore().getVolume(volumeName);
        OzoneBucket bucket = volume.getBucket(bucketName);
        assertEquals(bucketName, bucket.getName());
      }
    }
  }

  private static void assertServicePortsReachable(LocalOzoneCluster cluster)
      throws IOException {
    assertTrue(cluster.getScmPort() > 0);
    assertTrue(cluster.getOmPort() > 0);
    assertPortReachable(cluster.getDisplayHost(), cluster.getScmPort());
    assertPortReachable(cluster.getDisplayHost(), cluster.getOmPort());
  }

  private static void assertPortReachable(String host, int port)
      throws IOException {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(host, port), 1_000);
    }
  }

  private static OzoneVolume createVolume(OzoneClient client, String volumeName)
      throws IOException {
    client.getObjectStore().createVolume(volumeName);
    return client.getObjectStore().getVolume(volumeName);
  }

  private static OzoneBucket createBucket(OzoneVolume volume, String bucketName)
      throws IOException {
    volume.createBucket(bucketName);
    return volume.getBucket(bucketName);
  }

  private static String uniqueName(String prefix) {
    return prefix + UUID.randomUUID().toString().replace("-", "");
  }
}
