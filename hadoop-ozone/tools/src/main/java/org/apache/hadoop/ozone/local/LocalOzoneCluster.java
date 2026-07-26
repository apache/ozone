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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_CLIENT_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_CONTAINER_RATIS_ENABLED_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DB_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_GRPC_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HA_RATIS_SERVER_RPC_FIRST_ELECTION_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_DIR;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HA_RATIS_STORAGE_DIR;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTPS_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.server.http.BaseHttpServer.SERVER_DIR;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_ADMIN_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_SERVER_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_HTTP_BASEDIR;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_RATIS_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.common.Storage.StorageState.INITIALIZED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTPS_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_PORT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_STORAGE_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_DB_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.TimeDurationUtil;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SafeModeRuleStatusProto;
import org.apache.hadoop.hdds.scm.proxy.SCMClientConfig;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.replication.ReplicationServer;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts the SCM, OM, and datanode portion of the {@code ozone local} runtime.
 *
 * <p>S3 Gateway and Recon are added by later local runtime tickets.</p>
 */
public final class LocalOzoneCluster implements LocalOzoneRuntime {

  private static final Logger LOG =
      LoggerFactory.getLogger(LocalOzoneCluster.class);

  static final String PORTS_STATE_FILE_NAME = "ports.properties";

  private static final String METADATA_DIR_NAME = "metadata";
  private static final String SCM_DIR_NAME = "scm";
  private static final String OM_DIR_NAME = "om";
  private static final String OZONE_METADATA_DIR_NAME = "ozone-metadata";
  private static final String DATA_DIR_NAME = "data";
  private static final String RATIS_DIR_NAME = "ratis";
  private static final String DATANODE_DIR_PREFIX = "datanode-";
  private static final String SCM_CLIENT_PORT_KEY = "scm.client";
  private static final String SCM_BLOCK_PORT_KEY = "scm.block";
  private static final String SCM_DATANODE_PORT_KEY = "scm.datanode";
  private static final String SCM_SECURITY_PORT_KEY = "scm.security";
  private static final String SCM_HTTP_PORT_KEY = "scm.http";
  private static final String SCM_HTTPS_PORT_KEY = "scm.https";
  private static final String SCM_RATIS_PORT_KEY = "scm.ratis";
  private static final String SCM_GRPC_PORT_KEY = "scm.grpc";
  private static final String OM_RPC_PORT_KEY = "om.rpc";
  private static final String OM_HTTP_PORT_KEY = "om.http";
  private static final String OM_HTTPS_PORT_KEY = "om.https";
  private static final String OM_RATIS_PORT_KEY = "om.ratis";
  private static final String DATANODE_PORT_KEY_PREFIX = "dn.";
  private static final String DATANODE_HTTP_PORT_KEY_SUFFIX = "http";
  private static final String DATANODE_CLIENT_PORT_KEY_SUFFIX = "client";
  private static final String DATANODE_CONTAINER_IPC_PORT_KEY_SUFFIX = "container.ipc";
  private static final String DATANODE_RATIS_IPC_PORT_KEY_SUFFIX = "ratis.ipc";
  private static final String DATANODE_RATIS_ADMIN_PORT_KEY_SUFFIX = "ratis.admin";
  private static final String DATANODE_RATIS_SERVER_PORT_KEY_SUFFIX = "ratis.server";
  private static final String DATANODE_RATIS_DATASTREAM_PORT_KEY_SUFFIX = "ratis.datastream";
  private static final String DATANODE_REPLICATION_PORT_KEY_SUFFIX = "replication";
  private static final int LOCAL_RATIS_RPC_TIMEOUT_SECONDS = 1;
  private static final long SCM_CLIENT_MAX_RETRY_TIMEOUT_MILLIS = 30_000;
  private static final long READINESS_POLL_INTERVAL_MILLIS = 500;
  private static final long READINESS_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(5);

  private static final int MAX_PORT = 65_535;

  private static final String[] REQUIRED_PERSISTED_PORT_KEYS = {
      SCM_CLIENT_PORT_KEY,
      SCM_BLOCK_PORT_KEY,
      SCM_DATANODE_PORT_KEY,
      SCM_SECURITY_PORT_KEY,
      SCM_HTTP_PORT_KEY,
      SCM_HTTPS_PORT_KEY,
      SCM_RATIS_PORT_KEY,
      SCM_GRPC_PORT_KEY,
      OM_RPC_PORT_KEY,
      OM_HTTP_PORT_KEY,
      OM_HTTPS_PORT_KEY,
      OM_RATIS_PORT_KEY
  };

  private static final String[] DATANODE_PORT_KEY_SUFFIXES = {
      DATANODE_HTTP_PORT_KEY_SUFFIX,
      DATANODE_CLIENT_PORT_KEY_SUFFIX,
      DATANODE_CONTAINER_IPC_PORT_KEY_SUFFIX,
      DATANODE_RATIS_IPC_PORT_KEY_SUFFIX,
      DATANODE_RATIS_ADMIN_PORT_KEY_SUFFIX,
      DATANODE_RATIS_SERVER_PORT_KEY_SUFFIX,
      DATANODE_RATIS_DATASTREAM_PORT_KEY_SUFFIX,
      DATANODE_REPLICATION_PORT_KEY_SUFFIX
  };

  // Every datanode runs in this JVM and reserves DATANODE_PORT_KEY_SUFFIXES.length
  // local ports, so an unbounded count would exhaust local ports; cap it.
  static final int MAX_DATANODES = 20;

  private static final Set<String> SHIPPED_DEFAULT_RESOURCES = Collections.unmodifiableSet(
      OzoneConfiguration.getConfigurationResourceFiles().stream()
          .filter(resource -> resource.endsWith("-default.xml"))
          .collect(Collectors.toSet()));

  private static final String[] NO_ARGS = new String[0];

  private final LocalOzoneClusterConfig config;
  private final OzoneConfiguration seedConfiguration;
  private boolean closed;

  private PreparedConfiguration preparedConfiguration;
  private StorageContainerManager scm;
  private OzoneManager om;
  private final List<HddsDatanodeService> datanodes = new ArrayList<>();
  private boolean previousMetricsMiniClusterMode;
  private boolean metricsMiniClusterModeEnabled;

  public LocalOzoneCluster(LocalOzoneClusterConfig config,
      OzoneConfiguration seedConfiguration) {
    this.config = Objects.requireNonNull(config, "config");
    this.seedConfiguration = new OzoneConfiguration(
        Objects.requireNonNull(seedConfiguration, "seedConfiguration"));
  }

  @Override
  public void start() throws Exception {
    if (closed) {
      throw new IOException("Local Ozone cluster is already closed.");
    }
    if (scm != null && om != null) {
      return;
    }
    if (scm != null || om != null) {
      throw new IOException("Local Ozone cluster is partially started.");
    }

    try {
      enableSameJvmMetricsMode();
      PreparedConfiguration prepared = prepareConfiguration();
      initializeStorage(prepared.getConfiguration());
      startScm(prepared.getConfiguration());
      startOm(prepared.getConfiguration());
      startDatanodes(prepared.getDatanodeConfigurations());
      waitForClusterReadiness(config.getStartupTimeout());
    } catch (Exception ex) {
      // Roll back without latching closed: the caller's close() still owns the
      // ephemeral data dir lifecycle.
      stopServices();
      throw ex;
    }
  }

  PreparedConfiguration prepareConfiguration() throws IOException {
    if (preparedConfiguration != null) {
      return preparedConfiguration;
    }

    // Every rejection of user input runs before prepareStorageLayout(), which deletes the data
    // dir in format mode ALWAYS: a run that cannot start must not destroy local state first. The
    // configure* steps only compute configuration and validate ports; directories are created
    // afterwards by createServiceDirectories().
    requireSupportedDatanodeCount();
    validateStorageLayout();
    OzoneConfiguration conf = new OzoneConfiguration(seedConfiguration);
    configureLocalDefaults(conf);

    PersistedPortState persistedPorts = loadPersistedPortState();
    PortAllocator portAllocator = new PortAllocator();
    int scmPort = configureScm(conf, persistedPorts, portAllocator);
    int omPort = configureOm(conf, persistedPorts, portAllocator);
    List<OzoneConfiguration> datanodeConfigurations =
        configureDatanodes(conf, persistedPorts, portAllocator);

    prepareStorageLayout();
    createServiceDirectories();
    persistedPorts.store();
    preparedConfiguration = new PreparedConfiguration(conf, scmPort, omPort,
        datanodeConfigurations);
    return preparedConfiguration;
  }

  @Override
  public String getDisplayHost() {
    return LocalOzoneClusterConfig.DEFAULT_BIND_HOST.equals(config.getHost())
        ? LocalOzoneClusterConfig.DEFAULT_HOST : config.getHost();
  }

  @Override
  public int getScmPort() {
    return scm.getClientRpcAddress().getPort();
  }

  @Override
  public int getOmPort() {
    return om.getOmRpcServerAddr().getPort();
  }

  /**
   * Returns the number of running datanodes.
   */
  public int getDatanodeCount() {
    return datanodes.size();
  }

  @Override
  public int getS3gPort() {
    return -1;
  }

  @Override
  public String getS3Endpoint() {
    return "";
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;

    try {
      stopServices();
    } finally {
      // Ephemeral mode owns the data directory lifecycle for short-lived runs.
      if (config.isEphemeral()) {
        try {
          deleteDirectory(config.getDataDir());
        } catch (IOException ex) {
          LOG.warn("Failed to delete local Ozone data dir {} during shutdown.",
              config.getDataDir(), ex);
        }
      }
    }
  }

  private void stopServices() {
    try {
      // Shutdown is best-effort so one failed service cannot leak the others,
      // but the failures are logged: stopServices() also runs as start()
      // rollback, where a silent failure hides leaked threads and ports.
      IOUtils.close(LOG, this::stopDatanodes, this::stopOm, this::stopScm);
    } finally {
      restoreSameJvmMetricsMode();
    }
  }

  private void stopDatanodes() {
    List<AutoCloseable> stoppers = new ArrayList<>();
    for (int i = datanodes.size() - 1; i >= 0; i--) {
      HddsDatanodeService service = datanodes.get(i);
      stoppers.add(() -> {
        service.stop();
        service.join();
      });
    }
    datanodes.clear();
    IOUtils.close(LOG, stoppers);
  }

  private void stopOm() {
    OzoneManager service = om;
    om = null;
    if (service != null && service.stop()) {
      service.join();
    }
  }

  private void stopScm() {
    StorageContainerManager service = scm;
    scm = null;
    if (service != null) {
      service.stop();
      service.join();
    }
  }

  private void configureLocalDefaults(OzoneConfiguration conf) throws IOException {
    conf.set(OZONE_METADATA_DIRS, metadataDir().toString());
    // SCM and OM share one configuration and JVM, so the Jetty base dir is a
    // single service-neutral location; Jetty keeps per-context temp dirs.
    conf.setIfUnset(OZONE_HTTP_BASEDIR, metadataDir() + SERVER_DIR);
    setLocalOverrideReplication(conf, OZONE_REPLICATION, ReplicationFactor.ONE);
    setLocalOverride(conf, OZONE_REPLICATION_TYPE, ReplicationType.STAND_ALONE.name());
    setLocalOverrideReplication(conf, OZONE_SERVER_DEFAULT_REPLICATION_KEY, ReplicationFactor.ONE);
    setLocalOverride(conf, OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY,
        ReplicationType.STAND_ALONE.name());
    setLocalOverride(conf, HDDS_CONTAINER_RATIS_ENABLED_KEY, false);
    // A single-node local cluster can heartbeat aggressively; this speeds
    // datanode registration and safe-mode exit.
    setLocalOverrideDuration(conf, HDDS_HEARTBEAT_INTERVAL, "1s");
    setLocalOverride(conf, HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);
    setLocalOverride(conf, HDDS_SCM_SAFEMODE_MIN_DATANODE, config.getDatanodes());
    setLocalOverrideDuration(conf, OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY,
        LOCAL_RATIS_RPC_TIMEOUT_SECONDS + "s");
    setLocalOverrideDuration(conf, HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "3s");
    conf.setIfUnset(OZONE_SCM_HA_RATIS_SERVER_RPC_FIRST_ELECTION_TIMEOUT,
        "1s");

    SCMClientConfig scmClientConfig = conf.getObject(SCMClientConfig.class);
    scmClientConfig.setMaxRetryTimeout(SCM_CLIENT_MAX_RETRY_TIMEOUT_MILLIS);
    conf.setFromObject(scmClientConfig);
  }

  /**
   * Applies a value the local runtime requires. Set rather than {@code setIfUnset} because
   * ozone-default.xml would otherwise win.
   *
   * <p>This overload compares text. The typed overloads compare through the accessor the services
   * read the key with, so a value that already means what the runtime requires is kept.</p>
   *
   * @throws IOException if the user configured {@code key} with a value other than {@code value}
   */
  private void setLocalOverride(OzoneConfiguration conf, String key, String value)
      throws IOException {
    // Configuration#unset() leaves the key in updatingResource, so a source can outlive its
    // value; there is nothing to reject when no value is configured. The comparison trims:
    // Hadoop never trims XML values on load and the services read these keys through
    // getTrimmed(), so a padded value already means what the runtime requires.
    String configured = conf.get(key);
    if (configured != null && !value.equals(configured.trim())) {
      rejectUserConfigured(conf, key, value);
    }
    conf.set(key, value);
  }

  private void setLocalOverride(OzoneConfiguration conf, String key, boolean value)
      throws IOException {
    // Defaulting to the negation keeps a value getBoolean() cannot read from matching by accident.
    String configured = conf.get(key);
    if (configured != null && conf.getBoolean(key, !value) != value) {
      rejectUserConfigured(conf, key, String.valueOf(value));
    }
    conf.setBoolean(key, value);
  }

  private void setLocalOverride(OzoneConfiguration conf, String key, int value)
      throws IOException {
    String configured = conf.get(key);
    if (configured != null && !matchesInt(conf, key, value)) {
      rejectUserConfigured(conf, key, String.valueOf(value));
    }
    conf.setInt(key, value);
  }

  /**
   * Compares the configured value as a duration rather than as text, so the same length written
   * in another unit is not treated as a conflict. An explicit unit is required to avoid silently
   * interpreting a bare number differently from the user intended.
   *
   * @throws IOException if the user configured {@code key} with a different duration
   */
  private void setLocalOverrideDuration(OzoneConfiguration conf, String key, String value)
      throws IOException {
    long requiredMillis = TimeDurationUtil.getTimeDurationHelper(key, value, TimeUnit.MILLISECONDS);
    String configured = conf.get(key);
    if (configured != null && !matchesDuration(conf, key, configured, requiredMillis)) {
      rejectUserConfigured(conf, key, value);
    }
    conf.set(key, value);
  }

  /**
   * Applies a replication factor the local runtime requires, reading the configured value the way
   * {@link org.apache.hadoop.hdds.client.ReplicationConfig#parse} does, which accepts both the
   * numeric and the named spelling.
   *
   * @throws IOException if the user configured {@code key} with a different factor
   */
  private void setLocalOverrideReplication(OzoneConfiguration conf, String key,
      ReplicationFactor value) throws IOException {
    String configured = conf.get(key);
    if (configured != null && parseReplicationFactor(configured) != value) {
      rejectUserConfigured(conf, key, value.name());
    }
    conf.set(key, value.name());
  }

  /**
   * Throws when the value {@code conf} carries for {@code key} is the user's choice rather than a
   * shipped default. The message names the source because the user has to find the value to
   * remove it.
   */
  private static void rejectUserConfigured(OzoneConfiguration conf, String key, String required)
      throws IOException {
    String source = userConfiguredSource(conf, key);
    if (source == null) {
      return;
    }
    throw new IOException("ozone local requires " + key + "=" + required
        + ", but it is set to " + conf.get(key) + " (source: " + source
        + "). Remove or change that setting.");
  }

  private static boolean matchesInt(OzoneConfiguration conf, String key, int value) {
    try {
      return conf.getInt(key, value) == value;
    } catch (NumberFormatException unreadable) {
      // A value the accessor cannot read is a conflict; the caller reports it by key.
      return false;
    }
  }

  private static boolean matchesDuration(OzoneConfiguration conf, String key, String configured,
      long requiredMillis) {
    if (lacksTimeUnit(configured)) {
      return false;
    }
    try {
      return conf.getTimeDuration(key, requiredMillis, TimeUnit.MILLISECONDS) == requiredMillis;
    } catch (NumberFormatException unreadable) {
      return false;
    }
  }

  /**
   * {@link TimeDurationUtil} only warns about a missing unit and then assumes the caller's,
   * silently reinterpreting a bare number (for example "120" as 120 milliseconds). Every unit
   * suffix it accepts ends in a letter, so a trailing digit means the unit is missing.
   */
  static boolean lacksTimeUnit(String value) {
    String trimmed = value.trim();
    return !trimmed.isEmpty() && Character.isDigit(trimmed.charAt(trimmed.length() - 1));
  }

  /** Returns the factor {@code value} names in either spelling, or null if it names neither. */
  private static ReplicationFactor parseReplicationFactor(String value) {
    String trimmed = value.trim();
    try {
      return ReplicationFactor.valueOf(Integer.parseInt(trimmed));
    } catch (IllegalArgumentException notNumeric) {
      try {
        return ReplicationFactor.valueOf(trimmed);
      } catch (IllegalArgumentException notNamed) {
        return null;
      }
    }
  }

  /**
   * Returns where {@code key} got the value the user chose, or null if the user chose none. A
   * value whose last source is one of Ozone's shipped {@code *-default.xml} resources is a default,
   * not a user choice. The comparison is exact: Configuration records a classpath resource by its
   * bare name, while {@link OzoneLocal} qualifies a file named with {@code --conf} so it stays a
   * user choice however that file is called.
   */
  private static String userConfiguredSource(OzoneConfiguration conf, String key) {
    String[] sources = conf.getPropertySources(key);
    if (sources == null || sources.length == 0) {
      return null;
    }
    String source = sources[sources.length - 1];
    return SHIPPED_DEFAULT_RESOURCES.contains(source) ? null : source;
  }

  private int configureScm(OzoneConfiguration conf,
      PersistedPortState persistedPorts, PortAllocator portAllocator)
      throws IOException {
    int scmClientPort = reservePort(portAllocator, persistedPorts,
        SCM_CLIENT_PORT_KEY, config.getScmPort());
    int scmBlockPort = reservePort(portAllocator, persistedPorts,
        SCM_BLOCK_PORT_KEY, 0);
    int scmDatanodePort = reservePort(portAllocator, persistedPorts,
        SCM_DATANODE_PORT_KEY, 0);
    int scmSecurityPort = reservePort(portAllocator, persistedPorts,
        SCM_SECURITY_PORT_KEY, 0);
    int scmHttpPort = reservePort(portAllocator, persistedPorts,
        SCM_HTTP_PORT_KEY, 0);
    int scmHttpsPort = reservePort(portAllocator, persistedPorts,
        SCM_HTTPS_PORT_KEY, 0);
    int scmRatisPort = reservePort(portAllocator, persistedPorts,
        SCM_RATIS_PORT_KEY, 0);
    int scmGrpcPort = reservePort(portAllocator, persistedPorts,
        SCM_GRPC_PORT_KEY, 0);

    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY,
        address(config.getHost(), scmClientPort));
    conf.set(OZONE_SCM_CLIENT_BIND_HOST_KEY, config.getBindHost());
    conf.set(OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
        address(config.getHost(), scmBlockPort));
    conf.set(OZONE_SCM_BLOCK_CLIENT_BIND_HOST_KEY, config.getBindHost());
    conf.set(OZONE_SCM_DATANODE_ADDRESS_KEY,
        address(config.getHost(), scmDatanodePort));
    conf.set(OZONE_SCM_DATANODE_BIND_HOST_KEY, config.getBindHost());
    conf.set(OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY,
        address(config.getHost(), scmSecurityPort));
    conf.set(OZONE_SCM_SECURITY_SERVICE_BIND_HOST_KEY, config.getBindHost());
    conf.set(OZONE_SCM_HTTP_ADDRESS_KEY,
        address(config.getHost(), scmHttpPort));
    conf.set(OZONE_SCM_HTTP_BIND_HOST_KEY, config.getBindHost());
    conf.set(OZONE_SCM_HTTPS_ADDRESS_KEY,
        address(config.getHost(), scmHttpsPort));
    conf.set(OZONE_SCM_HTTPS_BIND_HOST_KEY, config.getBindHost());
    conf.setInt(OZONE_SCM_RATIS_PORT_KEY, scmRatisPort);
    conf.setInt(OZONE_SCM_GRPC_PORT_KEY, scmGrpcPort);
    conf.setStrings(OZONE_SCM_NAMES,
        address(config.getHost(), scmDatanodePort));
    configureScmStorage(conf);
    return scmClientPort;
  }

  private void configureScmStorage(OzoneConfiguration conf) {
    Path scmDir = config.getDataDir().resolve(SCM_DIR_NAME);
    Path scmMetadataDir = scmDir.resolve(OZONE_METADATA_DIR_NAME);

    conf.setIfUnset(OZONE_SCM_DB_DIRS,
        scmDir.resolve(DATA_DIR_NAME).toString());
    conf.setIfUnset(OZONE_SCM_HA_RATIS_STORAGE_DIR,
        scmDir.resolve(RATIS_DIR_NAME).toString());
    conf.setIfUnset(OZONE_SCM_HA_RATIS_SNAPSHOT_DIR,
        scmMetadataDir.resolve(OZONE_RATIS_SNAPSHOT_DIR).toString());
  }

  private int configureOm(OzoneConfiguration conf,
      PersistedPortState persistedPorts, PortAllocator portAllocator)
      throws IOException {
    int omRpcPort = reservePort(portAllocator, persistedPorts, OM_RPC_PORT_KEY,
        config.getOmPort());
    int omHttpPort = reservePort(portAllocator, persistedPorts,
        OM_HTTP_PORT_KEY, 0);
    int omHttpsPort = reservePort(portAllocator, persistedPorts,
        OM_HTTPS_PORT_KEY, 0);
    int omRatisPort = reservePort(portAllocator, persistedPorts,
        OM_RATIS_PORT_KEY, 0);

    conf.set(OZONE_OM_ADDRESS_KEY, address(config.getHost(), omRpcPort));
    conf.set(OZONE_OM_HTTP_ADDRESS_KEY, address(config.getHost(), omHttpPort));
    conf.set(OZONE_OM_HTTP_BIND_HOST_KEY, config.getBindHost());
    conf.set(OZONE_OM_HTTPS_ADDRESS_KEY,
        address(config.getHost(), omHttpsPort));
    conf.set(OZONE_OM_HTTPS_BIND_HOST_KEY, config.getBindHost());
    conf.setInt(OZONE_OM_RATIS_PORT_KEY, omRatisPort);
    configureOmStorage(conf);
    return omRpcPort;
  }

  private void configureOmStorage(OzoneConfiguration conf) {
    Path omDir = config.getDataDir().resolve(OM_DIR_NAME);
    Path omMetadataDir = omDir.resolve(OZONE_METADATA_DIR_NAME);

    conf.setIfUnset(OZONE_OM_DB_DIRS, omDir.resolve(DATA_DIR_NAME).toString());
    conf.setIfUnset(OZONE_OM_RATIS_STORAGE_DIR,
        omDir.resolve(RATIS_DIR_NAME).toString());
    conf.setIfUnset(OZONE_OM_RATIS_SNAPSHOT_DIR,
        omMetadataDir.resolve(OZONE_RATIS_SNAPSHOT_DIR).toString());
    conf.setIfUnset(OZONE_OM_SNAPSHOT_DIFF_DB_DIR,
        omMetadataDir.toString());
  }

  private List<OzoneConfiguration> configureDatanodes(OzoneConfiguration conf,
      PersistedPortState persistedPorts, PortAllocator portAllocator)
      throws IOException {
    List<OzoneConfiguration> datanodeConfigurations =
        new ArrayList<>(config.getDatanodes());
    for (int index = 0; index < config.getDatanodes(); index++) {
      datanodeConfigurations.add(
          configureDatanode(conf, index, persistedPorts, portAllocator));
    }
    return datanodeConfigurations;
  }

  private OzoneConfiguration configureDatanode(OzoneConfiguration conf,
      int index, PersistedPortState persistedPorts,
      PortAllocator portAllocator) throws IOException {
    OzoneConfiguration dnConf = new OzoneConfiguration(conf);
    configureDatanodeStorage(dnConf, index);

    dnConf.set(HDDS_DATANODE_HTTP_ADDRESS_KEY, address(config.getHost(),
        reserveDatanodePort(portAllocator, persistedPorts, index,
            DATANODE_HTTP_PORT_KEY_SUFFIX)));
    dnConf.set(HDDS_DATANODE_HTTP_BIND_HOST_KEY, config.getBindHost());
    dnConf.set(HDDS_DATANODE_CLIENT_ADDRESS_KEY, address(config.getHost(),
        reserveDatanodePort(portAllocator, persistedPorts, index,
            DATANODE_CLIENT_PORT_KEY_SUFFIX)));
    dnConf.set(HDDS_DATANODE_CLIENT_BIND_HOST_KEY, config.getBindHost());
    dnConf.setInt(HDDS_CONTAINER_IPC_PORT,
        reserveDatanodePort(portAllocator, persistedPorts, index,
            DATANODE_CONTAINER_IPC_PORT_KEY_SUFFIX));
    dnConf.setInt(HDDS_CONTAINER_RATIS_IPC_PORT,
        reserveDatanodePort(portAllocator, persistedPorts, index,
            DATANODE_RATIS_IPC_PORT_KEY_SUFFIX));
    dnConf.setInt(HDDS_CONTAINER_RATIS_ADMIN_PORT,
        reserveDatanodePort(portAllocator, persistedPorts, index,
            DATANODE_RATIS_ADMIN_PORT_KEY_SUFFIX));
    dnConf.setInt(HDDS_CONTAINER_RATIS_SERVER_PORT,
        reserveDatanodePort(portAllocator, persistedPorts, index,
            DATANODE_RATIS_SERVER_PORT_KEY_SUFFIX));
    dnConf.setInt(HDDS_CONTAINER_RATIS_DATASTREAM_PORT,
        reserveDatanodePort(portAllocator, persistedPorts, index,
            DATANODE_RATIS_DATASTREAM_PORT_KEY_SUFFIX));

    ReplicationServer.ReplicationConfig replicationConfig =
        dnConf.getObject(ReplicationServer.ReplicationConfig.class);
    replicationConfig.setPort(reserveDatanodePort(portAllocator,
        persistedPorts, index, DATANODE_REPLICATION_PORT_KEY_SUFFIX));
    dnConf.setFromObject(replicationConfig);
    return dnConf;
  }

  private void configureDatanodeStorage(OzoneConfiguration dnConf, int index) {
    Path datanodeDir = config.getDataDir()
        .resolve(DATANODE_DIR_PREFIX + index);
    Path datanodeMetadataDir = datanodeDir.resolve(OZONE_METADATA_DIR_NAME);

    dnConf.set(OZONE_METADATA_DIRS, datanodeMetadataDir.toString());
    // Each datanode gets its own Jetty base dir so same-JVM HTTP servers do
    // not share unpacked web resources.
    dnConf.set(OZONE_HTTP_BASEDIR, datanodeMetadataDir + SERVER_DIR);
    dnConf.set(HDDS_DATANODE_DIR_KEY,
        datanodeDir.resolve(DATA_DIR_NAME).toString());
    dnConf.set(HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR,
        datanodeDir.resolve(RATIS_DIR_NAME).toString());
  }

  private void initializeStorage(OzoneConfiguration conf) throws IOException {
    SCMStorageConfig scmStorage = new SCMStorageConfig(conf);
    OMStorage omStorage = new OMStorage(conf);
    String clusterId = resolveClusterId(scmStorage, omStorage);
    String scmId = initializeScmStorage(conf, scmStorage, clusterId);
    initializeOmStorage(conf, omStorage, clusterId, scmId);
  }

  private String resolveClusterId(SCMStorageConfig scmStorage,
      OMStorage omStorage) throws IOException {
    String scmClusterId = initializedClusterId(scmStorage);
    String omClusterId = initializedClusterId(omStorage);
    if (scmClusterId != null && omClusterId != null
        && !scmClusterId.equals(omClusterId)) {
      throw new IOException("Local Ozone SCM cluster ID " + scmClusterId
          + " does not match OM cluster ID " + omClusterId + ".");
    }
    // Reuse an initialized component's cluster ID so local metadata survives
    // restarting with a partially formatted data directory.
    if (scmClusterId != null) {
      return scmClusterId;
    }
    if (omClusterId != null) {
      return omClusterId;
    }
    return UUID.randomUUID().toString();
  }

  private String initializeScmStorage(OzoneConfiguration conf,
      SCMStorageConfig scmStorage, String clusterId) throws IOException {
    if (scmStorage.getState() != INITIALIZED) {
      requireStorageFormatting("SCM");
    }
    // scmInit formats storage before Ratis so a crash between the two is
    // recoverable, and re-initializes Ratis for a half-formatted directory.
    if (!StorageContainerManager.scmInit(conf, clusterId)) {
      throw new IOException("SCM initialization failed for local Ozone; see the SCM log for the cause.");
    }
    return new SCMStorageConfig(conf).getScmId();
  }

  private void initializeOmStorage(OzoneConfiguration conf, OMStorage omStorage,
      String clusterId, String scmId) throws IOException {
    if (omStorage.getState() == INITIALIZED) {
      if (!clusterId.equals(omStorage.getClusterID())) {
        throw new IOException("Local Ozone OM cluster ID "
            + omStorage.getClusterID() + " does not match SCM cluster ID "
            + clusterId + ".");
      }
      return;
    }

    requireStorageFormatting("OM");
    omStorage.setClusterId(clusterId);
    omStorage.setOmId(UUID.randomUUID().toString());
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      OzoneManager.initializeSecurity(conf, omStorage, scmId);
    }
    omStorage.initialize();
  }

  private void requireStorageFormatting(String component) throws IOException {
    if (config.getFormatMode() == LocalOzoneClusterConfig.FormatMode.NEVER) {
      throw new IOException(component + " storage is not initialized. "
          + "Format mode NEVER requires existing SCM and OM storage.");
    }
  }

  private void startScm(OzoneConfiguration conf) throws Exception {
    // Assign the field before start() so a failed start can still be rolled
    // back by stopServices().
    scm = StorageContainerManager.createSCM(conf);
    scm.start();
  }

  private void startOm(OzoneConfiguration conf) throws Exception {
    om = OzoneManager.createOm(conf);
    om.start();
  }

  private void startDatanodes(List<OzoneConfiguration> datanodeConfigurations) {
    for (OzoneConfiguration dnConf : datanodeConfigurations) {
      // Track the datanode before start() so a failed start can still be
      // rolled back by stopServices().
      HddsDatanodeService datanode = new HddsDatanodeService(NO_ARGS);
      datanodes.add(datanode);
      datanode.start(dnConf);
    }
  }

  private void waitForClusterReadiness(Duration timeout) throws Exception {
    waitForReadiness(this::clusterReadinessBlocker, "Ozone cluster", timeout);
  }

  /**
   * Polls {@code blocker} until it reports ready. {@code blocker} returns why {@code subject} is
   * not ready yet, so the wait can name the unmet condition instead of reporting a bare timeout
   * that cannot distinguish a slow start from a stuck one.
   */
  static void waitForReadiness(Supplier<String> blocker, String subject, Duration timeout)
      throws InterruptedException, TimeoutException {
    long startNanos = System.nanoTime();
    long nextLogNanos = startNanos + READINESS_LOG_INTERVAL_NANOS;
    while (true) {
      String reason = blocker.get();
      if (reason == null) {
        return;
      }
      long now = System.nanoTime();
      // Compared as elapsed Duration: timeout.toNanos() throws for very long timeouts, and an
      // absolute nano deadline can wrap negative on a long-uptime host.
      if (Duration.ofNanos(now - startNanos).compareTo(timeout) >= 0) {
        throw new TimeoutException("Timed out waiting " + timeout + " for the local " + subject
            + " to become ready: " + reason + ".");
      }
      if (now >= nextLogNanos) {
        LOG.info("Waiting for the local {} to become ready: {}.", subject, reason);
        nextLogNanos = now + READINESS_LOG_INTERVAL_NANOS;
      }
      Thread.sleep(READINESS_POLL_INTERVAL_MILLIS);
    }
  }

  /**
   * Returns why the cluster is not usable yet, or null once it is ready.
   */
  private String clusterReadinessBlocker() {
    if (!scm.checkLeader()) {
      return "SCM has no Ratis leader yet";
    }
    if (!om.isLeaderReady()) {
      return "OM is not leader-ready yet";
    }
    int registered = scm.getScmNodeManager().getAllNodeCount();
    if (registered < config.getDatanodes()) {
      return "only " + registered + " of " + config.getDatanodes()
          + " datanodes have registered with SCM";
    }
    // Registration alone is not enough: SCM refuses block allocation until it leaves safe mode.
    if (scm.isInSafeMode()) {
      return "SCM is still in safe mode ("
          + formatSafeModeRuleBlocker(scm.getRuleStatus()) + ")";
    }
    return null;
  }

  static String formatSafeModeRuleBlocker(List<SafeModeRuleStatusProto> rules) {
    if (rules.isEmpty()) {
      return "safe-mode rule status is not available yet";
    }
    String unmetRules = joinSafeModeRules(rules.stream().filter(rule -> !rule.getValidate()));
    return unmetRules.isEmpty()
        ? "all rules currently report satisfied: " + joinSafeModeRules(rules.stream())
        : unmetRules;
  }

  private static String joinSafeModeRules(Stream<SafeModeRuleStatusProto> rules) {
    return rules.map(rule -> rule.getRuleName() + ": " + rule.getStatusText())
        .collect(Collectors.joining("; "));
  }

  private void enableSameJvmMetricsMode() {
    if (!metricsMiniClusterModeEnabled) {
      previousMetricsMiniClusterMode = DefaultMetricsSystem.inMiniClusterMode();
      DefaultMetricsSystem.setMiniClusterMode(true);
      metricsMiniClusterModeEnabled = true;
    }
  }

  private void restoreSameJvmMetricsMode() {
    if (metricsMiniClusterModeEnabled) {
      DefaultMetricsSystem.setMiniClusterMode(previousMetricsMiniClusterMode);
      metricsMiniClusterModeEnabled = false;
    }
  }

  private void requireSupportedDatanodeCount() throws IOException {
    int datanodeCount = config.getDatanodes();
    if (datanodeCount < 1) {
      throw new IOException("Datanode count " + datanodeCount
          + " is below the local minimum of 1; a local cluster requires at least one datanode.");
    }
    if (datanodeCount > MAX_DATANODES) {
      throw new IOException("Datanode count " + datanodeCount
          + " exceeds the local maximum of " + MAX_DATANODES
          + "; each datanode reserves " + DATANODE_PORT_KEY_SUFFIXES.length
          + " local ports.");
    }
  }

  /** The read-only half of the layout checks, run before any user input can be rejected. */
  private void validateStorageLayout() throws IOException {
    Path dataDir = config.getDataDir();
    if (Files.exists(dataDir) && !Files.isDirectory(dataDir)) {
      throw new IOException("Local Ozone data dir " + dataDir
          + " is not a directory.");
    }
    if (config.getFormatMode() == LocalOzoneClusterConfig.FormatMode.NEVER) {
      requireExistingLayout();
    }
  }

  private void prepareStorageLayout() throws IOException {
    switch (config.getFormatMode()) {
    case ALWAYS:
      LOG.info("Removing local Ozone data dir {} (format mode ALWAYS).", config.getDataDir());
      deleteDirectory(config.getDataDir());
      createBaseLayout();
      break;
    case NEVER:
      // validateStorageLayout() already required the existing layout.
      break;
    case IF_NEEDED:
      createBaseLayout();
      break;
    default:
      throw new IOException("Unsupported format mode "
          + config.getFormatMode() + ".");
    }
  }

  private void createServiceDirectories() throws IOException {
    Path dataDir = config.getDataDir();
    Files.createDirectories(dataDir.resolve(SCM_DIR_NAME).resolve(DATA_DIR_NAME));
    Files.createDirectories(dataDir.resolve(OM_DIR_NAME).resolve(DATA_DIR_NAME));
    for (int index = 0; index < config.getDatanodes(); index++) {
      Path datanodeDir = dataDir.resolve(DATANODE_DIR_PREFIX + index);
      Files.createDirectories(datanodeDir.resolve(OZONE_METADATA_DIR_NAME));
      Files.createDirectories(datanodeDir.resolve(DATA_DIR_NAME));
    }
  }

  private void createBaseLayout() throws IOException {
    Files.createDirectories(config.getDataDir());
    Files.createDirectories(metadataDir());
  }

  /**
   * {@link LocalOzoneClusterConfig.FormatMode#NEVER} is a strict reuse mode:
   * the command must not initialize missing local state on behalf of the user.
   */
  private void requireExistingLayout() throws IOException {
    Path dataDir = config.getDataDir();
    if (!Files.exists(dataDir)) {
      throw new IOException("Local Ozone data dir " + dataDir
          + " does not exist.");
    }
    if (!Files.isDirectory(metadataDir())) {
      throw new IOException("Local Ozone metadata dir " + metadataDir()
          + " does not exist.");
    }
    if (!Files.isRegularFile(portStateFile())) {
      throw new IOException("Local Ozone port state file " + portStateFile()
          + " does not exist.");
    }
  }

  private PersistedPortState loadPersistedPortState() throws IOException {
    if (config.getFormatMode() == LocalOzoneClusterConfig.FormatMode.ALWAYS) {
      // prepareStorageLayout() deletes the data dir after the ports are validated, so the
      // persisted ports are stale; start from an empty state without reading the doomed file.
      return PersistedPortState.empty(portStateFile());
    }
    PersistedPortState persistedPorts =
        PersistedPortState.load(portStateFile());
    if (config.getFormatMode() == LocalOzoneClusterConfig.FormatMode.NEVER) {
      persistedPorts.requireKeys(requiredPersistedPortKeys());
    }
    return persistedPorts;
  }

  private String[] requiredPersistedPortKeys() {
    List<String> keys =
        new ArrayList<>(Arrays.asList(REQUIRED_PERSISTED_PORT_KEYS));
    for (int index = 0; index < config.getDatanodes(); index++) {
      for (String suffix : DATANODE_PORT_KEY_SUFFIXES) {
        keys.add(datanodePortKey(index, suffix));
      }
    }
    return keys.toArray(new String[0]);
  }

  private int reservePort(PortAllocator allocator,
      PersistedPortState persistedPorts, String key, int configuredPort)
      throws IOException {
    int preferredPort = configuredPort > 0 ? configuredPort
        : persistedPorts.get(key);
    int port = allocator.reserve(preferredPort);
    persistedPorts.set(key, port);
    return port;
  }

  private int reserveDatanodePort(PortAllocator allocator,
      PersistedPortState persistedPorts, int index, String suffix)
      throws IOException {
    return reservePort(allocator, persistedPorts,
        datanodePortKey(index, suffix), 0);
  }

  private static String datanodePortKey(int index, String suffix) {
    return DATANODE_PORT_KEY_PREFIX + index + "." + suffix;
  }

  private Path metadataDir() {
    return config.getDataDir().resolve(METADATA_DIR_NAME);
  }

  private Path portStateFile() {
    return config.getDataDir().resolve(PORTS_STATE_FILE_NAME);
  }

  private static String address(String host, int port) {
    return host + ":" + port;
  }

  private static String initializedClusterId(Storage storage) {
    return storage.getState() == INITIALIZED ? storage.getClusterID() : null;
  }

  private static void deleteDirectory(Path directory) throws IOException {
    if (!Files.exists(directory)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(directory)) {
      Iterable<Path> deleteOrder =
          () -> paths.sorted(Comparator.reverseOrder()).iterator();
      for (Path path : deleteOrder) {
        Files.deleteIfExists(path);
      }
    }
  }

  static final class PreparedConfiguration {
    private final OzoneConfiguration configuration;
    private final int scmPort;
    private final int omPort;
    private final List<OzoneConfiguration> datanodeConfigurations;

    PreparedConfiguration(OzoneConfiguration configuration, int scmPort,
        int omPort, List<OzoneConfiguration> datanodeConfigurations) {
      this.configuration = Objects.requireNonNull(configuration,
          "configuration");
      this.scmPort = scmPort;
      this.omPort = omPort;
      this.datanodeConfigurations = Collections.unmodifiableList(
          new ArrayList<>(Objects.requireNonNull(datanodeConfigurations,
              "datanodeConfigurations")));
    }

    OzoneConfiguration getConfiguration() {
      return configuration;
    }

    int getScmPort() {
      return scmPort;
    }

    int getOmPort() {
      return omPort;
    }

    List<OzoneConfiguration> getDatanodeConfigurations() {
      return datanodeConfigurations;
    }
  }

  /**
   * Allocates distinct local ports for the configuration being prepared.
   */
  static final class PortAllocator {
    private final Set<Integer> reserved = new HashSet<>();

    int reserve(int preferredPort) throws IOException {
      if (preferredPort > 0) {
        return reserveConfiguredPort(preferredPort);
      }

      while (true) {
        int candidate = nextFreePort();
        if (reserved.add(candidate)) {
          return candidate;
        }
      }
    }

    private int reserveConfiguredPort(int port) throws IOException {
      if (port > MAX_PORT) {
        throw new IOException("Port " + port + " is outside the valid range.");
      }
      if (!reserved.add(port)) {
        throw new IOException("Port " + port
            + " is configured more than once.");
      }
      return port;
    }

    private static int nextFreePort() throws IOException {
      try (ServerSocket socket = new ServerSocket(0)) {
        socket.setReuseAddress(false);
        return socket.getLocalPort();
      }
    }
  }

  /**
   * Persists dynamic port choices so repeated local starts keep stable
   * client-facing endpoints until the user explicitly formats storage.
   */
  static final class PersistedPortState {
    private final Path path;
    private final Properties properties = new Properties();
    private boolean dirty;

    private PersistedPortState(Path path) {
      this.path = path;
    }

    static PersistedPortState empty(Path path) {
      return new PersistedPortState(path);
    }

    static PersistedPortState load(Path path) throws IOException {
      PersistedPortState state = new PersistedPortState(path);
      if (!Files.exists(path)) {
        return state;
      }
      if (!Files.isRegularFile(path)) {
        throw new IOException("Local Ozone port state file " + path
            + " is not a regular file.");
      }
      try (InputStream input = Files.newInputStream(path)) {
        state.properties.load(input);
      }
      return state;
    }

    int get(String key) throws IOException {
      String value = properties.getProperty(key);
      if (value == null) {
        return 0;
      }
      String trimmedValue = value.trim();
      if (trimmedValue.isEmpty()) {
        return 0;
      }
      try {
        int port = Integer.parseInt(trimmedValue);
        if (port < 0 || port > MAX_PORT) {
          throw invalidPortValue(key, value);
        }
        return port;
      } catch (NumberFormatException ex) {
        throw invalidPortValue(key, value);
      }
    }

    void requireKeys(String[] keys) throws IOException {
      for (String key : keys) {
        if (get(key) <= 0) {
          throw new IOException("Local Ozone port state file " + path
              + " is missing required port key " + key + ".");
        }
      }
    }

    void set(String key, int port) {
      String value = Integer.toString(port);
      if (!value.equals(properties.getProperty(key))) {
        properties.setProperty(key, value);
        dirty = true;
      }
    }

    void store() throws IOException {
      if (!dirty && Files.exists(path)) {
        return;
      }
      try (OutputStream output = Files.newOutputStream(path)) {
        properties.store(output, "Local Ozone reserved ports");
      }
      dirty = false;
    }

    private static IOException invalidPortValue(String key, String value) {
      return new IOException("Invalid port value for " + key + ": " + value);
    }
  }
}
