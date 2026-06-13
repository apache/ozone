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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_CONTAINER_RATIS_ENABLED_KEY;
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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMHANodeDetails;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServerImpl;
import org.apache.hadoop.hdds.scm.proxy.SCMClientConfig;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts the SCM and OM portion of the {@code ozone local} runtime.
 *
 * <p>Datanodes, S3 Gateway, Recon, and end-to-end key writes are added by
 * later local runtime tickets.</p>
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
  private static final int LOCAL_RATIS_RPC_TIMEOUT_SECONDS = 1;
  private static final long SCM_CLIENT_MAX_RETRY_TIMEOUT_MILLIS = 30_000;
  private static final long READINESS_POLL_INTERVAL_MILLIS = 500;
  private static final int PORT_CONNECT_TIMEOUT_MILLIS = 1_000;

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

  private final LocalOzoneClusterConfig config;
  private final OzoneConfiguration seedConfiguration;
  private final AtomicBoolean closed = new AtomicBoolean();

  private PreparedConfiguration preparedConfiguration;
  private StorageContainerManager scm;
  private OzoneManager om;
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
    if (closed.get()) {
      throw new IOException("Local Ozone cluster is already closed.");
    }
    if (scm != null && om != null) {
      return;
    }
    if (scm != null || om != null) {
      throw new IOException("Local Ozone cluster is partially started.");
    }

    enableSameJvmMetricsMode();
    PreparedConfiguration prepared = prepareConfiguration();
    try {
      initializeStorage(prepared.getConfiguration());
      scm = startScm(prepared.getConfiguration());
      om = startOm(prepared.getConfiguration());
      waitForScmAndOmReadiness(config.getStartupTimeout());
    } catch (Exception ex) {
      try {
        close();
      } catch (Exception closeEx) {
        ex.addSuppressed(closeEx);
      }
      throw ex;
    }
  }

  PreparedConfiguration prepareConfiguration() throws IOException {
    if (preparedConfiguration != null) {
      return preparedConfiguration;
    }

    prepareStorageLayout();

    OzoneConfiguration conf = new OzoneConfiguration(seedConfiguration);
    configureLocalDefaults(conf);

    PersistedPortState persistedPorts = loadPersistedPortState();
    PortAllocator portAllocator = new PortAllocator();
    int scmPort = configureScm(conf, persistedPorts, portAllocator);
    int omPort = configureOm(conf, persistedPorts, portAllocator);

    persistedPorts.store();
    preparedConfiguration = new PreparedConfiguration(conf, scmPort, omPort);
    return preparedConfiguration;
  }

  @Override
  public String getDisplayHost() {
    return LocalOzoneClusterConfig.DEFAULT_BIND_HOST.equals(config.getHost())
        ? LocalOzoneClusterConfig.DEFAULT_HOST : config.getHost();
  }

  @Override
  public int getScmPort() {
    if (scm != null) {
      return scm.getClientRpcAddress().getPort();
    }
    return preparedConfiguration == null ? config.getScmPort()
        : preparedConfiguration.getScmPort();
  }

  @Override
  public int getOmPort() {
    if (om != null) {
      return om.getOmRpcServerAddr().getPort();
    }
    return preparedConfiguration == null ? config.getOmPort()
        : preparedConfiguration.getOmPort();
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
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    try {
      // Shutdown is best-effort so one failed service cannot leak the other.
      stopQuietly("OM", om, OzoneManager::stop);
      om = null;
      stopQuietly("SCM", scm, StorageContainerManager::stop);
      scm = null;
    } finally {
      restoreSameJvmMetricsMode();
      // Ephemeral mode owns the data directory lifecycle for short-lived runs.
      if (config.isEphemeral()) {
        deleteDirectory(config.getDataDir());
      }
    }
  }

  private void configureLocalDefaults(OzoneConfiguration conf) {
    conf.set(OZONE_METADATA_DIRS, metadataDir().toString());
    conf.set(OZONE_REPLICATION, ReplicationFactor.ONE.name());
    conf.set(OZONE_REPLICATION_TYPE, ReplicationType.STAND_ALONE.name());
    conf.set(OZONE_SERVER_DEFAULT_REPLICATION_KEY, ReplicationFactor.ONE.name());
    conf.set(OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY,
        ReplicationType.STAND_ALONE.name());
    conf.setBoolean(HDDS_CONTAINER_RATIS_ENABLED_KEY, false);
    conf.setBoolean(HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);
    conf.setInt(HDDS_SCM_SAFEMODE_MIN_DATANODE,
        Math.max(1, config.getDatanodes()));
    conf.setTimeDuration(OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY,
        LOCAL_RATIS_RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    conf.set(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "3s");
    conf.setIfUnset(OZONE_SCM_HA_RATIS_SERVER_RPC_FIRST_ELECTION_TIMEOUT,
        "1s");

    SCMClientConfig scmClientConfig = conf.getObject(SCMClientConfig.class);
    scmClientConfig.setMaxRetryTimeout(SCM_CLIENT_MAX_RETRY_TIMEOUT_MILLIS);
    conf.setFromObject(scmClientConfig);
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

  private void configureScmStorage(OzoneConfiguration conf) throws IOException {
    Path scmDir = config.getDataDir().resolve(SCM_DIR_NAME);
    Path scmMetadataDir = scmDir.resolve(OZONE_METADATA_DIR_NAME);
    Files.createDirectories(scmDir.resolve(DATA_DIR_NAME));

    conf.setIfUnset(OZONE_SCM_DB_DIRS,
        scmDir.resolve(DATA_DIR_NAME).toString());
    conf.setIfUnset(OZONE_SCM_HA_RATIS_STORAGE_DIR,
        scmDir.resolve(RATIS_DIR_NAME).toString());
    conf.setIfUnset(OZONE_SCM_HA_RATIS_SNAPSHOT_DIR,
        scmMetadataDir.resolve(OZONE_RATIS_SNAPSHOT_DIR).toString());
    conf.setIfUnset(OZONE_HTTP_BASEDIR, scmMetadataDir + SERVER_DIR);
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

  private void configureOmStorage(OzoneConfiguration conf) throws IOException {
    Path omDir = config.getDataDir().resolve(OM_DIR_NAME);
    Path omMetadataDir = omDir.resolve(OZONE_METADATA_DIR_NAME);
    Files.createDirectories(omDir.resolve(DATA_DIR_NAME));

    conf.setIfUnset(OZONE_OM_DB_DIRS, omDir.resolve(DATA_DIR_NAME).toString());
    conf.setIfUnset(OZONE_OM_RATIS_STORAGE_DIR,
        omDir.resolve(RATIS_DIR_NAME).toString());
    conf.setIfUnset(OZONE_OM_RATIS_SNAPSHOT_DIR,
        omMetadataDir.resolve(OZONE_RATIS_SNAPSHOT_DIR).toString());
    conf.setIfUnset(OZONE_OM_SNAPSHOT_DIFF_DB_DIR,
        omMetadataDir.toString());
    conf.setIfUnset(OZONE_HTTP_BASEDIR, omMetadataDir + SERVER_DIR);
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
    if (scmStorage.getState() == INITIALIZED) {
      return scmStorage.getScmId();
    }

    requireStorageFormatting("SCM");
    String scmId = UUID.randomUUID().toString();
    scmStorage.setClusterId(clusterId);
    scmStorage.setScmId(scmId);
    scmStorage.setPrimaryScmNodeId(scmId);
    scmStorage.initialize();
    scmStorage.setSCMHAFlag(true);
    scmStorage.persistCurrentState();

    SCMRatisServerImpl.initialize(clusterId, scmId,
        SCMHANodeDetails.loadSCMHAConfig(conf, scmStorage)
            .getLocalNodeDetails(), conf);
    HddsUtils.createDir(SCMHAUtils.getSCMRatisSnapshotDirectory(conf));
    return scmId;
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

  private StorageContainerManager startScm(OzoneConfiguration conf)
      throws Exception {
    StorageContainerManager manager = StorageContainerManager.createSCM(conf);
    manager.start();
    return manager;
  }

  private OzoneManager startOm(OzoneConfiguration conf) throws Exception {
    OzoneManager manager = OzoneManager.createOm(conf);
    manager.start();
    return manager;
  }

  private void waitForScmAndOmReadiness(Duration timeout) throws Exception {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadlineNanos) {
      // Readiness is intentionally scoped to SCM leadership and RPC reachability
      // until datanode and full safe-mode readiness are added in later tickets.
      if (scm.checkLeader()
          && isPortOpen(getDisplayHost(), getScmPort())
          && isPortOpen(getDisplayHost(), getOmPort())) {
        return;
      }
      Thread.sleep(READINESS_POLL_INTERVAL_MILLIS);
    }
    throw new TimeoutException("Timed out waiting " + timeout
        + " for local SCM leadership and SCM/OM RPC ports.");
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

  private void prepareStorageLayout() throws IOException {
    Path dataDir = config.getDataDir();
    if (Files.exists(dataDir) && !Files.isDirectory(dataDir)) {
      throw new IOException("Local Ozone data dir " + dataDir
          + " is not a directory.");
    }

    switch (config.getFormatMode()) {
    case ALWAYS:
      deleteDirectory(dataDir);
      createBaseLayout();
      break;
    case NEVER:
      requireExistingLayout();
      break;
    case IF_NEEDED:
      createBaseLayout();
      break;
    default:
      throw new IOException("Unsupported format mode "
          + config.getFormatMode() + ".");
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
    PersistedPortState persistedPorts =
        PersistedPortState.load(portStateFile());
    if (config.getFormatMode() == LocalOzoneClusterConfig.FormatMode.NEVER) {
      persistedPorts.requireKeys(REQUIRED_PERSISTED_PORT_KEYS);
    }
    return persistedPorts;
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

  private static boolean isPortOpen(String host, int port) {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(host, port),
          PORT_CONNECT_TIMEOUT_MILLIS);
      return true;
    } catch (IOException ex) {
      return false;
    }
  }

  private static <T> void stopQuietly(String serviceName, T service,
      ServiceStopper<T> stopper) {
    if (service == null) {
      return;
    }
    try {
      stopper.stop(service);
    } catch (Exception ex) {
      LOG.warn("Failed to stop local {}. Continuing shutdown.", serviceName,
          ex);
    }
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

  @FunctionalInterface
  private interface ServiceStopper<T> {
    void stop(T service) throws Exception;
  }

  static final class PreparedConfiguration {
    private final OzoneConfiguration configuration;
    private final int scmPort;
    private final int omPort;

    PreparedConfiguration(OzoneConfiguration configuration, int scmPort,
        int omPort) {
      this.configuration = Objects.requireNonNull(configuration,
          "configuration");
      this.scmPort = scmPort;
      this.omPort = omPort;
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
      if (port > 65_535) {
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
        if (port < 0 || port > 65_535) {
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
