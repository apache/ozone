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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_CONTAINER_RATIS_ENABLED_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link LocalOzoneCluster}.
 */
class TestLocalOzoneCluster {

  private static final String METADATA_DIR_NAME = "metadata";
  private static final String PORTS_STATE_FILE_NAME = "ports.properties";

  @TempDir
  private Path tempDir;

  @Test
  void startPreparesConfiguration() throws Exception {
    Path dataDir = tempDir.resolve("local-ozone");
    LocalOzoneClusterConfig config =
        LocalOzoneClusterConfig.builder(dataDir).build();

    try (LocalOzoneCluster cluster = newCluster(config)) {
      cluster.start();

      assertTrue(Files.isDirectory(metadataDir(dataDir)));
      assertTrue(Files.isRegularFile(portStateFile(dataDir)));
      assertTrue(cluster.getScmPort() > 0);
      assertTrue(cluster.getOmPort() > 0);
      assertEquals(-1, cluster.getS3gPort());
      assertEquals("", cluster.getS3Endpoint());
    }
  }

  @Test
  void prepareConfigurationCreatesBaseLayoutAndLocalDefaults()
      throws Exception {
    Path dataDir = tempDir.resolve("local-ozone");
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(dataDir)
        .setScmPort(9860)
        .setOmPort(9862)
        .setDatanodes(2)
        .build();

    LocalOzoneCluster.PreparedConfiguration prepared = prepare(config);
    OzoneConfiguration conf = prepared.getConfiguration();

    assertTrue(Files.isDirectory(metadataDir(dataDir)));
    assertTrue(Files.isRegularFile(portStateFile(dataDir)));
    assertEquals(metadataDir(dataDir).toString(), conf.get(OZONE_METADATA_DIRS));
    assertEquals(ReplicationFactor.ONE.name(), conf.get(OZONE_REPLICATION));
    assertEquals(ReplicationType.STAND_ALONE.name(),
        conf.get(OZONE_REPLICATION_TYPE));
    assertEquals(ReplicationFactor.ONE.name(),
        conf.get(OZONE_SERVER_DEFAULT_REPLICATION_KEY));
    assertEquals(ReplicationType.STAND_ALONE.name(),
        conf.get(OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY));
    assertFalse(conf.getBoolean(HDDS_CONTAINER_RATIS_ENABLED_KEY, true));
    assertFalse(conf.getBoolean(HDDS_SCM_SAFEMODE_PIPELINE_CREATION, true));
    assertEquals(2, conf.getInt(HDDS_SCM_SAFEMODE_MIN_DATANODE, 0));
    assertTrue(conf.get(OZONE_SCM_CLIENT_ADDRESS_KEY).endsWith(":9860"));
    assertTrue(conf.get(OZONE_OM_ADDRESS_KEY).endsWith(":9862"));
    assertTrue(conf.getTrimmedStringCollection(OZONE_SCM_NAMES).iterator()
        .next().contains(":"));
    assertEquals(9860, prepared.getScmPort());
    assertEquals(9862, prepared.getOmPort());
  }

  @Test
  void prepareConfigurationPersistsDynamicPortsAcrossInstances()
      throws Exception {
    Path dataDir = tempDir.resolve("local-ozone");
    LocalOzoneClusterConfig config =
        LocalOzoneClusterConfig.builder(dataDir).build();

    LocalOzoneCluster.PreparedConfiguration first = prepare(config);
    LocalOzoneCluster.PreparedConfiguration second = prepare(config);

    assertTrue(first.getScmPort() > 0);
    assertTrue(first.getOmPort() > 0);
    assertEquals(first.getScmPort(), second.getScmPort());
    assertEquals(first.getOmPort(), second.getOmPort());
  }

  @Test
  void prepareConfigurationIsIdempotent() throws Exception {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(
        tempDir.resolve("local-ozone")).build();

    try (LocalOzoneCluster cluster = newCluster(config)) {
      LocalOzoneCluster.PreparedConfiguration first =
          cluster.prepareConfiguration();
      LocalOzoneCluster.PreparedConfiguration second =
          cluster.prepareConfiguration();

      assertSame(first, second);
      assertEquals(first.getScmPort(), second.getScmPort());
      assertEquals(first.getOmPort(), second.getOmPort());
    }
  }

  @Test
  void prepareConfigurationRejectsDuplicateConfiguredPorts() {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(
            tempDir.resolve("local-ozone"))
        .setScmPort(9860)
        .setOmPort(9860)
        .build();

    IOException error = assertPrepareFails(config);

    assertMessageContains(error, "more than once");
  }

  @Test
  void formatIfNeededPreservesExistingState() throws Exception {
    Path dataDir = tempDir.resolve("local-ozone");
    Path marker = writeMarker(dataDir, "existing");
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(dataDir)
        .setFormatMode(LocalOzoneClusterConfig.FormatMode.IF_NEEDED)
        .build();

    prepare(config);

    assertTrue(Files.exists(marker));
    assertTrue(Files.isDirectory(metadataDir(dataDir)));
  }

  @Test
  void formatAlwaysClearsExistingState() throws Exception {
    Path dataDir = tempDir.resolve("local-ozone");
    Path marker = writeMarker(dataDir, "stale");
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(dataDir)
        .setFormatMode(LocalOzoneClusterConfig.FormatMode.ALWAYS)
        .build();

    prepare(config);

    assertFalse(Files.exists(marker));
    assertTrue(Files.isDirectory(metadataDir(dataDir)));
  }

  @Test
  void formatNeverAcceptsExistingLayout() throws Exception {
    Path dataDir = tempDir.resolve("local-ozone");
    LocalOzoneClusterConfig initial =
        LocalOzoneClusterConfig.builder(dataDir).build();
    LocalOzoneCluster.PreparedConfiguration initialConfiguration =
        prepare(initial);
    LocalOzoneClusterConfig never = LocalOzoneClusterConfig.builder(dataDir)
        .setFormatMode(LocalOzoneClusterConfig.FormatMode.NEVER)
        .build();

    LocalOzoneCluster.PreparedConfiguration prepared = prepare(never);

    assertEquals(initialConfiguration.getScmPort(), prepared.getScmPort());
    assertEquals(initialConfiguration.getOmPort(), prepared.getOmPort());
  }

  @Test
  void formatNeverRejectsMissingLayout() {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(
            tempDir.resolve("missing-local-ozone"))
        .setFormatMode(LocalOzoneClusterConfig.FormatMode.NEVER)
        .build();

    IOException error = assertPrepareFails(config);

    assertMessageContains(error, "does not exist");
  }

  @Test
  void formatNeverRejectsInvalidLayout() throws Exception {
    Path dataDir = tempDir.resolve("local-ozone");
    Files.createDirectories(dataDir);
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(dataDir)
        .setFormatMode(LocalOzoneClusterConfig.FormatMode.NEVER)
        .build();

    IOException error = assertPrepareFails(config);

    assertMessageContains(error, METADATA_DIR_NAME);
  }

  @Test
  void formatNeverRejectsMissingPortState() throws Exception {
    Path dataDir = tempDir.resolve("local-ozone");
    Files.createDirectories(metadataDir(dataDir));
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(dataDir)
        .setFormatMode(LocalOzoneClusterConfig.FormatMode.NEVER)
        .build();

    IOException error = assertPrepareFails(config);

    assertMessageContains(error, PORTS_STATE_FILE_NAME);
  }

  @Test
  void closeDeletesEphemeralDataDir() throws Exception {
    Path dataDir = tempDir.resolve("ephemeral-local-ozone");
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(dataDir)
        .setEphemeral(true)
        .build();

    try (LocalOzoneCluster cluster = newCluster(config)) {
      cluster.prepareConfiguration();
      writeMarker(dataDir, "ephemeral");
    }

    assertFalse(Files.exists(dataDir));
  }

  @Test
  void prepareConfigurationRejectsRegularFileDataDir() throws Exception {
    Path dataDir = tempDir.resolve("not-a-directory");
    Files.write(dataDir, "file".getBytes(UTF_8));
    LocalOzoneClusterConfig config =
        LocalOzoneClusterConfig.builder(dataDir).build();

    IOException error = assertPrepareFails(config);

    assertMessageContains(error, "not a directory");
    assertTrue(Files.isRegularFile(dataDir));
  }

  @Test
  void prepareConfigurationRejectsCorruptPortsFile() throws Exception {
    Path dataDir = tempDir.resolve("local-ozone");
    Files.createDirectories(metadataDir(dataDir));
    Files.write(portStateFile(dataDir), "scm.client=not-a-port\n".getBytes(UTF_8));
    LocalOzoneClusterConfig config =
        LocalOzoneClusterConfig.builder(dataDir).build();

    IOException error = assertPrepareFails(config);

    assertMessageContains(error, "Invalid port value");
  }

  @Test
  void persistedPortFileContainsDistinctAllocatedPorts() throws Exception {
    Path dataDir = tempDir.resolve("local-ozone");
    LocalOzoneClusterConfig config =
        LocalOzoneClusterConfig.builder(dataDir).build();

    prepare(config);

    Properties properties = loadPortState(dataDir);
    assertPositivePort(properties, "scm.client");
    assertPositivePort(properties, "scm.block");
    assertPositivePort(properties, "scm.datanode");
    assertPositivePort(properties, "scm.security");
    assertPositivePort(properties, "scm.http");
    assertPositivePort(properties, "scm.https");
    assertPositivePort(properties, "scm.ratis");
    assertPositivePort(properties, "scm.grpc");
    assertPositivePort(properties, "om.rpc");
    assertPositivePort(properties, "om.http");
    assertPositivePort(properties, "om.ratis");
    assertNotEquals(properties.getProperty("scm.client"),
        properties.getProperty("om.rpc"));
  }

  private LocalOzoneCluster.PreparedConfiguration prepare(
      LocalOzoneClusterConfig config) throws IOException {
    try (LocalOzoneCluster cluster = newCluster(config)) {
      return cluster.prepareConfiguration();
    }
  }

  private LocalOzoneCluster newCluster(LocalOzoneClusterConfig config) {
    return new LocalOzoneCluster(config, new OzoneConfiguration());
  }

  private IOException assertPrepareFails(LocalOzoneClusterConfig config) {
    return assertThrows(IOException.class, () -> {
      try (LocalOzoneCluster cluster = newCluster(config)) {
        cluster.prepareConfiguration();
      }
    });
  }

  private void assertMessageContains(IOException error, String expectedText) {
    assertTrue(error.getMessage().contains(expectedText), error.getMessage());
  }

  private Path writeMarker(Path dataDir, String content) throws IOException {
    Files.createDirectories(dataDir);
    Path marker = dataDir.resolve("marker.txt");
    Files.write(marker, content.getBytes(UTF_8));
    return marker;
  }

  private Properties loadPortState(Path dataDir) throws IOException {
    Properties properties = new Properties();
    try (InputStream input = Files.newInputStream(portStateFile(dataDir))) {
      properties.load(input);
    }
    return properties;
  }

  private void assertPositivePort(Properties properties, String key) {
    assertTrue(Integer.parseInt(properties.getProperty(key)) > 0, key);
  }

  private Path metadataDir(Path dataDir) {
    return dataDir.resolve(METADATA_DIR_NAME);
  }

  private Path portStateFile(Path dataDir) {
    return dataDir.resolve(PORTS_STATE_FILE_NAME);
  }
}
