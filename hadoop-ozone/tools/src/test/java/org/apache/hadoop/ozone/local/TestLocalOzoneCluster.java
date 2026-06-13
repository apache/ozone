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

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_CONTAINER_RATIS_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link LocalOzoneCluster}.
 *
 * <p>These tests focus on configuration generation without starting
 * actual services, which require SCM.</p>
 */
class TestLocalOzoneCluster {

  @TempDir
  private Path tempDir;

  @Test
  void prepareBaseConfigurationSetsReplicationDefaults() throws Exception {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(tempDir)
        .build();

    LocalOzoneCluster cluster = new LocalOzoneCluster(config,
        new OzoneConfiguration());

    OzoneConfiguration baseConf = cluster.prepareBaseConfiguration();

    assertEquals(ReplicationFactor.ONE.name(),
        baseConf.get(OZONE_REPLICATION));
    assertEquals(ReplicationType.STAND_ALONE.name(),
        baseConf.get(OZONE_REPLICATION_TYPE));
    assertFalse(baseConf.getBoolean(HDDS_CONTAINER_RATIS_ENABLED_KEY, true));
  }

  @Test
  void prepareBaseConfigurationCreatesMetadataDir() throws Exception {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(tempDir)
        .build();

    LocalOzoneCluster cluster = new LocalOzoneCluster(config,
        new OzoneConfiguration());

    OzoneConfiguration baseConf = cluster.prepareBaseConfiguration();

    String metadataDir = baseConf.get(OZONE_METADATA_DIRS);
    assertTrue(Files.exists(Paths.get(metadataDir)),
        "Metadata directory should be created");
    assertTrue(metadataDir.contains("metadata"),
        "Metadata dir path should contain 'metadata'");
  }

  @Test
  void getDisplayHostReturnsConfiguredHost() throws Exception {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(tempDir)
        .setHost("192.168.1.100")
        .build();

    LocalOzoneCluster cluster = new LocalOzoneCluster(config,
        new OzoneConfiguration());

    assertEquals("192.168.1.100", cluster.getDisplayHost());
  }

  @Test
  void getDisplayHostReturnsLocalhostForBindAll() throws Exception {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(tempDir)
        .setHost("0.0.0.0")
        .build();

    LocalOzoneCluster cluster = new LocalOzoneCluster(config,
        new OzoneConfiguration());

    assertEquals("127.0.0.1", cluster.getDisplayHost());
  }

  @Test
  void getDatanodeCountReturnsZeroBeforeStart() throws Exception {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(tempDir)
        .setDatanodes(3)
        .build();

    LocalOzoneCluster cluster = new LocalOzoneCluster(config,
        new OzoneConfiguration());

    assertEquals(0, cluster.getDatanodeCount(),
        "Should have zero datanodes before start");
  }

  @Test
  void closeIsIdempotent() throws Exception {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(tempDir)
        .build();

    LocalOzoneCluster cluster = new LocalOzoneCluster(config,
        new OzoneConfiguration());

    // Multiple closes should not throw
    cluster.close();
    cluster.close();
    cluster.close();
  }
}
