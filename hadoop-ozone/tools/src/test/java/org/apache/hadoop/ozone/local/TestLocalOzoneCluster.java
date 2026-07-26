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
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_CONTAINER_RATIS_ENABLED_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY;
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
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SafeModeRuleStatusProto;
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
  void prepareConfigurationExposesPreparedPorts() throws Exception {
    Path dataDir = tempDir.resolve("local-ozone");
    LocalOzoneClusterConfig config =
        LocalOzoneClusterConfig.builder(dataDir).build();

    try (LocalOzoneCluster cluster = newCluster(config)) {
      LocalOzoneCluster.PreparedConfiguration prepared =
          cluster.prepareConfiguration();

      assertTrue(Files.isDirectory(metadataDir(dataDir)));
      assertTrue(Files.isRegularFile(portStateFile(dataDir)));
      assertTrue(prepared.getScmPort() > 0);
      assertTrue(prepared.getOmPort() > 0);
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

  @Test
  void prepareConfigurationCreatesDatanodeConfigurations() throws Exception {
    Path dataDir = tempDir.resolve("local-ozone");
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(dataDir)
        .setDatanodes(2)
        .build();

    LocalOzoneCluster.PreparedConfiguration prepared = prepare(config);

    assertEquals(2, prepared.getDatanodeConfigurations().size());
    for (int index = 0; index < 2; index++) {
      OzoneConfiguration dnConf = prepared.getDatanodeConfigurations().get(index);
      Path datanodeDir = dataDir.resolve("datanode-" + index);
      assertTrue(Files.isDirectory(datanodeDir.resolve("ozone-metadata")));
      assertTrue(Files.isDirectory(datanodeDir.resolve("data")));
      assertEquals(datanodeDir.resolve("ozone-metadata").toString(),
          dnConf.get(OZONE_METADATA_DIRS));
      assertEquals(datanodeDir.resolve("data").toString(),
          dnConf.get(HDDS_DATANODE_DIR_KEY));
      assertTrue(dnConf.getInt(HDDS_CONTAINER_IPC_PORT, 0) > 0);
    }
    assertNotEquals(
        prepared.getDatanodeConfigurations().get(0)
            .getInt(HDDS_CONTAINER_IPC_PORT, 0),
        prepared.getDatanodeConfigurations().get(1)
            .getInt(HDDS_CONTAINER_IPC_PORT, 0));
  }

  @Test
  void prepareConfigurationRejectsTooManyDatanodes() throws Exception {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(
            tempDir.resolve("local-ozone"))
        .setDatanodes(LocalOzoneCluster.MAX_DATANODES + 1)
        .build();

    IOException error = assertPrepareFails(config);

    assertEquals("Datanode count " + (LocalOzoneCluster.MAX_DATANODES + 1)
        + " exceeds the local maximum of " + LocalOzoneCluster.MAX_DATANODES
        + "; each datanode reserves 8 local ports.", error.getMessage());
  }

  /** A local cluster with no datanodes is unusable and would otherwise time out during readiness. */
  @Test
  void zeroDatanodesIsRejected() throws Exception {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(
            tempDir.resolve("local-ozone"))
        .setDatanodes(0)
        .build();

    IOException error = assertPrepareFails(config);

    assertMessageContains(error, "Datanode count 0");
  }

  /**
   * A key unset after being configured still reports a source with no value behind it, so
   * setLocalOverride has to treat it as unconfigured rather than as a conflict.
   */
  @Test
  void keyUnsetAfterBeingConfiguredIsNotRejected() throws Exception {
    OzoneConfiguration seed = new OzoneConfiguration();
    seed.set(OZONE_REPLICATION, ReplicationFactor.THREE.name(), "test-ozone-site.xml");
    seed.unset(OZONE_REPLICATION);

    assertEquals(ReplicationFactor.ONE.name(), prepared(seed).get(OZONE_REPLICATION));
  }

  @Test
  void configuredMetadataDirIsReplacedByTheLocalDataDir() throws Exception {
    OzoneConfiguration seed = new OzoneConfiguration();
    seed.set(OZONE_METADATA_DIRS, "/real/cluster/metadata", "test-ozone-site.xml");

    assertEquals(metadataDir(tempDir.resolve("local-ozone")).toString(),
        prepared(seed).get(OZONE_METADATA_DIRS));
  }

  /**
   * A file named with --conf is the user's choice however it is called, so a path ending in
   * ozone-default.xml stays a conflict rather than counting as a shipped default.
   */
  @Test
  void defaultsFileNamedByPathCountsAsUserConfig() {
    OzoneConfiguration seed = new OzoneConfiguration();
    seed.set(OZONE_REPLICATION, ReplicationFactor.THREE.name(), "/home/me/ozone-default.xml");

    IOException error = assertPrepareFails(seed);

    assertMessageContains(error, OZONE_REPLICATION);
  }

  @Test
  void tooManyDatanodesIsRejectedBeforeFormatDeletesDataDir() throws Exception {
    Path dataDir = tempDir.resolve("local-ozone");
    Path marker = writeMarker(dataDir, "keep me");
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(dataDir)
        .setFormatMode(LocalOzoneClusterConfig.FormatMode.ALWAYS)
        .setDatanodes(LocalOzoneCluster.MAX_DATANODES + 1)
        .build();

    assertPrepareFails(config);

    assertTrue(Files.exists(marker),
        "format ALWAYS must not delete the data dir for a run that cannot start");
  }

  /**
   * The rejection message has to carry enough for the user to locate and remove the value.
   */
  @Test
  void conflictingUserConfigIsRejected() {
    OzoneConfiguration seed = new OzoneConfiguration();
    seed.set(OZONE_REPLICATION, ReplicationFactor.THREE.name(), "test-ozone-site.xml");

    IOException error = assertPrepareFails(seed);

    assertMessageContains(error, OZONE_REPLICATION);
    assertMessageContains(error, ReplicationFactor.ONE.name());
    assertMessageContains(error, ReplicationFactor.THREE.name());
    assertMessageContains(error, "test-ozone-site.xml");
  }

  @Test
  void userConfigMatchingTheLocalRequirementIsAccepted() throws Exception {
    OzoneConfiguration seed = new OzoneConfiguration();
    seed.set(OZONE_REPLICATION, ReplicationFactor.ONE.name(), "test-ozone-site.xml");

    assertEquals(ReplicationFactor.ONE.name(), prepared(seed).get(OZONE_REPLICATION));
  }

  @Test
  void conflictingUserConfigIsRejectedBeforeFormatDeletesDataDir() throws Exception {
    Path dataDir = tempDir.resolve("local-ozone");
    Path marker = writeMarker(dataDir, "keep me");
    OzoneConfiguration seed = new OzoneConfiguration();
    seed.set(OZONE_REPLICATION, ReplicationFactor.THREE.name(), "test-ozone-site.xml");
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(dataDir)
        .setFormatMode(LocalOzoneClusterConfig.FormatMode.ALWAYS)
        .build();

    assertPrepareFails(config, seed);

    assertTrue(Files.exists(marker),
        "format ALWAYS must not delete the data dir for a run that cannot start");
  }

  /**
   * A duplicate configured port is a user-input error PortAllocator detects deterministically, so
   * it must be rejected before format mode ALWAYS deletes the data dir.
   */
  @Test
  void duplicateConfiguredPortsAreRejectedBeforeFormatDeletesDataDir() throws Exception {
    Path dataDir = tempDir.resolve("local-ozone");
    Path marker = writeMarker(dataDir, "keep me");
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(dataDir)
        .setFormatMode(LocalOzoneClusterConfig.FormatMode.ALWAYS)
        .setScmPort(9860)
        .setOmPort(9860)
        .build();

    IOException error = assertPrepareFails(config);

    assertMessageContains(error, "more than once");
    assertTrue(Files.exists(marker),
        "format ALWAYS must not delete the data dir for a run that cannot start");
  }

  /**
   * Hadoop never trims XML values on load and the services read these keys through getTrimmed(),
   * so a padded value that names the required setting already means what the runtime requires.
   */
  @Test
  void paddedValueMatchingTheLocalRequirementIsAccepted() throws Exception {
    OzoneConfiguration seed = new OzoneConfiguration();
    seed.set(OZONE_REPLICATION_TYPE, "\n  STAND_ALONE\n", "test-ozone-site.xml");

    assertEquals(ReplicationType.STAND_ALONE.name(), prepared(seed).get(OZONE_REPLICATION_TYPE));
  }

  /** Duration#toNanos overflows for very long timeouts, so the wait must not read it. */
  @Test
  void readinessAcceptsTimeoutBeyondNanoTimeRange() throws Exception {
    LocalOzoneCluster.waitForReadiness(() -> null, "Ozone cluster",
        Duration.ofDays(999_999_999L));
  }

  /**
   * The same interval written in another unit means what the runtime requires, so comparing as a
   * duration rather than as text has to accept it instead of refusing to start.
   */
  @Test
  void equivalentDurationSpellingIsAccepted() throws Exception {
    OzoneConfiguration seed = new OzoneConfiguration();
    seed.set(HDDS_HEARTBEAT_INTERVAL, "1000ms", "test-ozone-site.xml");
    seed.set(OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY, "1000ms", "test-ozone-site.xml");
    seed.set(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "3000ms", "test-ozone-site.xml");

    OzoneConfiguration conf = prepared(seed);

    assertEquals("1s", conf.get(HDDS_HEARTBEAT_INTERVAL));
    assertEquals("1s", conf.get(OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY));
    assertEquals("3s", conf.get(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT));
  }

  /** ozone local requires explicit units even though the service accessors accept bare numbers. */
  @Test
  void unitlessConfigurationDurationsAreRejected() {
    assertUnitlessDurationRejected(HDDS_HEARTBEAT_INTERVAL, "1000");
    assertUnitlessDurationRejected(OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY, "1");
    assertUnitlessDurationRejected(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "3000");
  }

  @Test
  void equivalentBooleanSpellingIsAccepted() throws Exception {
    OzoneConfiguration seed = new OzoneConfiguration();
    seed.set(HDDS_CONTAINER_RATIS_ENABLED_KEY, "FALSE", "test-ozone-site.xml");

    assertFalse(prepared(seed).getBoolean(HDDS_CONTAINER_RATIS_ENABLED_KEY, true));
  }

  /**
   * ReplicationConfig.parse() reads both "1" and "ONE", and compose environments in this repo
   * write the numeric form, so it has to be accepted as the value the runtime requires.
   */
  @Test
  void numericReplicationIsAccepted() throws Exception {
    OzoneConfiguration seed = new OzoneConfiguration();
    seed.set(OZONE_SERVER_DEFAULT_REPLICATION_KEY, "1", "test-ozone-site.xml");

    assertEquals(ReplicationFactor.ONE.name(),
        prepared(seed).get(OZONE_SERVER_DEFAULT_REPLICATION_KEY));
  }

  /**
   * Comparing durations by value must not swallow a genuine conflict.
   */
  @Test
  void conflictingDurationIsRejected() {
    OzoneConfiguration seed = new OzoneConfiguration();
    seed.set(HDDS_HEARTBEAT_INTERVAL, "30s", "test-ozone-site.xml");

    IOException error = assertPrepareFails(seed);

    assertMessageContains(error, HDDS_HEARTBEAT_INTERVAL);
    assertMessageContains(error, "30s");
  }

  /**
   * A value the accessor cannot parse is a conflict, reported by the same message rather than
   * escaping as a NumberFormatException from the comparison itself.
   */
  @Test
  void unparseableValueIsRejected() {
    OzoneConfiguration seed = new OzoneConfiguration();
    seed.set(HDDS_HEARTBEAT_INTERVAL, "banana", "test-ozone-site.xml");

    IOException error = assertPrepareFails(seed);

    assertMessageContains(error, HDDS_HEARTBEAT_INTERVAL);
    assertMessageContains(error, "banana");
  }

  /**
   * Regression guard: ozone-default.xml ships a value for most keys the local runtime requires
   * (hdds.heartbeat.interval=30s, and so on) and is always on the classpath, so counting a shipped
   * default as a user choice would reject every run.
   */
  @Test
  void shippedDefaultIsNotTreatedAsUserConfig() throws Exception {
    assertEquals("1s", prepared(new OzoneConfiguration()).get(HDDS_HEARTBEAT_INTERVAL));
    assertNotEquals("1s", new OzoneConfiguration().get(HDDS_HEARTBEAT_INTERVAL),
        "the shipped default must differ from the local value, or this guards nothing");
  }

  @Test
  void generatedShippedDefaultIsNotTreatedAsUserConfig() throws Exception {
    OzoneConfiguration seed = new OzoneConfiguration();
    seed.set(OZONE_REPLICATION, ReplicationFactor.THREE.name(), "hdds-server-scm-default.xml");

    assertEquals(ReplicationFactor.ONE.name(), prepared(seed).get(OZONE_REPLICATION));
  }

  @Test
  void safeModeRuleBlockerListsOnlyUnmetRules() {
    String blocker = LocalOzoneCluster.formatSafeModeRuleBlocker(Arrays.asList(
        safeModeRule("registered datanodes", true, "registered"),
        safeModeRule("healthy containers", false, "1 of 3 reported")));

    assertEquals("healthy containers: 1 of 3 reported", blocker);
  }

  @Test
  void safeModeRuleBlockerFallsBackToSatisfiedRuleStatuses() {
    String blocker = LocalOzoneCluster.formatSafeModeRuleBlocker(Collections.singletonList(
        safeModeRule("registered datanodes", true, "registered")));

    assertEquals("all rules currently report satisfied: registered datanodes: registered", blocker);
  }

  @Test
  void safeModeRuleBlockerExplainsUnavailableRuleStatus() {
    assertEquals("safe-mode rule status is not available yet",
        LocalOzoneCluster.formatSafeModeRuleBlocker(Collections.emptyList()));
  }

  @Test
  void readinessTimeoutNamesTheUnmetCondition() {
    TimeoutException error = assertThrows(TimeoutException.class,
        () -> LocalOzoneCluster.waitForReadiness(() -> "only 1 of 3 datanodes have registered",
            "Ozone cluster", Duration.ZERO));

    assertMessageContains(error, "only 1 of 3 datanodes have registered");
    assertMessageContains(error, "Ozone cluster");
  }

  @Test
  void readinessReturnsOnceBlockerReportsReady() throws Exception {
    AtomicInteger attempts = new AtomicInteger();

    LocalOzoneCluster.waitForReadiness(
        () -> attempts.incrementAndGet() < 2 ? "not yet" : null, "Ozone cluster",
        Duration.ofSeconds(30));

    assertEquals(2, attempts.get());
  }

  @Test
  void persistedPortFileContainsDatanodePorts() throws Exception {
    Path dataDir = tempDir.resolve("local-ozone");
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(dataDir)
        .setDatanodes(1)
        .build();

    prepare(config);

    Properties properties = loadPortState(dataDir);
    assertPositivePort(properties, "dn.0.http");
    assertPositivePort(properties, "dn.0.client");
    assertPositivePort(properties, "dn.0.container.ipc");
    assertPositivePort(properties, "dn.0.ratis.ipc");
    assertPositivePort(properties, "dn.0.ratis.admin");
    assertPositivePort(properties, "dn.0.ratis.server");
    assertPositivePort(properties, "dn.0.ratis.datastream");
    assertPositivePort(properties, "dn.0.replication");
  }

  @Test
  void prepareConfigurationPersistsDatanodePortsAcrossInstances()
      throws Exception {
    Path dataDir = tempDir.resolve("local-ozone");
    LocalOzoneClusterConfig config =
        LocalOzoneClusterConfig.builder(dataDir).build();

    LocalOzoneCluster.PreparedConfiguration first = prepare(config);
    LocalOzoneCluster.PreparedConfiguration second = prepare(config);

    assertEquals(
        first.getDatanodeConfigurations().get(0)
            .getInt(HDDS_CONTAINER_IPC_PORT, 0),
        second.getDatanodeConfigurations().get(0)
            .getInt(HDDS_CONTAINER_IPC_PORT, 0));
  }

  @Test
  void formatNeverRejectsPortStateMissingDatanodePorts() throws Exception {
    Path dataDir = tempDir.resolve("local-ozone");
    prepare(LocalOzoneClusterConfig.builder(dataDir).setDatanodes(1).build());
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(dataDir)
        .setFormatMode(LocalOzoneClusterConfig.FormatMode.NEVER)
        .setDatanodes(2)
        .build();

    IOException error = assertPrepareFails(config);

    assertMessageContains(error, "dn.1.");
  }

  @Test
  void getDatanodeCountReturnsZeroBeforeStart() throws Exception {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(
            tempDir.resolve("local-ozone"))
        .setDatanodes(3)
        .build();

    try (LocalOzoneCluster cluster = newCluster(config)) {
      cluster.prepareConfiguration();

      assertEquals(0, cluster.getDatanodeCount());
    }
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

  /** Prepares a default-config cluster over {@code seed} and returns the prepared configuration. */
  private OzoneConfiguration prepared(OzoneConfiguration seed) throws IOException {
    LocalOzoneClusterConfig config = defaultConfig();
    try (LocalOzoneCluster cluster = new LocalOzoneCluster(config, seed)) {
      return cluster.prepareConfiguration().getConfiguration();
    }
  }

  private LocalOzoneClusterConfig defaultConfig() {
    return LocalOzoneClusterConfig.builder(tempDir.resolve("local-ozone")).build();
  }

  private IOException assertPrepareFails(LocalOzoneClusterConfig config) {
    return assertPrepareFails(config, new OzoneConfiguration());
  }

  private IOException assertPrepareFails(OzoneConfiguration seed) {
    return assertPrepareFails(defaultConfig(), seed);
  }

  private IOException assertPrepareFails(LocalOzoneClusterConfig config,
      OzoneConfiguration seed) {
    return assertThrows(IOException.class, () -> {
      try (LocalOzoneCluster cluster = new LocalOzoneCluster(config, seed)) {
        cluster.prepareConfiguration();
      }
    });
  }

  private void assertUnitlessDurationRejected(String key, String value) {
    OzoneConfiguration seed = new OzoneConfiguration();
    seed.set(key, value, "test-ozone-site.xml");

    IOException error = assertPrepareFails(seed);

    assertMessageContains(error, key);
    assertMessageContains(error, value);
    assertMessageContains(error, "test-ozone-site.xml");
  }

  private void assertMessageContains(Exception error, String expectedText) {
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

  private static SafeModeRuleStatusProto safeModeRule(String name, boolean validated, String status) {
    return SafeModeRuleStatusProto.newBuilder()
        .setRuleName(name)
        .setValidate(validated)
        .setStatusText(status)
        .build();
  }

  private Path metadataDir(Path dataDir) {
    return dataDir.resolve(METADATA_DIR_NAME);
  }

  private Path portStateFile(Path dataDir) {
    return dataDir.resolve(PORTS_STATE_FILE_NAME);
  }
}
