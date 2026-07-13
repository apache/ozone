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
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_DATANODE_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_TASK_SAFEMODE_WAIT_THRESHOLD;
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
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_HTTPS_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_DB_DIR;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_HTTPS_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_HTTP_ENABLED_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTPS_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTP_BIND_HOST_KEY;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URL;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.TimeDurationUtil;
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
import org.apache.hadoop.ozone.recon.ConfigurationProvider;
import org.apache.hadoop.ozone.recon.ReconServer;
import org.apache.hadoop.ozone.recon.ReconSqlDbConfig;
import org.apache.hadoop.ozone.s3.Gateway;
import org.apache.hadoop.ozone.s3.OzoneConfigurationHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts the SCM, OM, datanode, and optional S3 Gateway and Recon portion of the {@code ozone local}
 * runtime.
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
  private static final String S3G_HTTP_PORT_KEY = "s3g.http";
  private static final String S3G_HTTPS_PORT_KEY = "s3g.https";
  private static final String S3G_WEBADMIN_HTTP_PORT_KEY = "s3g.web.http";
  private static final String S3G_WEBADMIN_HTTPS_PORT_KEY = "s3g.web.https";
  private static final String RECON_HTTP_PORT_KEY = "recon.http";
  private static final String RECON_HTTPS_PORT_KEY = "recon.https";
  private static final String RECON_DATANODE_PORT_KEY = "recon.datanode";
  private static final String RECON_DIR_NAME = "recon";
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

  // OzoneConfiguration loads ozone-default.xml plus a generated <module>-default.xml for every
  // module (see OzoneConfiguration.getConfigurationResourceFiles), so match the suffix: Recon's
  // default for OZONE_RECON_TASK_SAFEMODE_WAIT_THRESHOLD ships in ozone-recon-default.xml.
  private static final String DEFAULT_XML_SUFFIX = "-default.xml";

  private static final String[] NO_ARGS = new String[0];

  private final LocalOzoneClusterConfig config;
  private final OzoneConfiguration seedConfiguration;
  private boolean closed;

  private PreparedConfiguration preparedConfiguration;
  private StorageContainerManager scm;
  private OzoneManager om;
  private Gateway s3Gateway;
  private ReconServer reconServer;
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
      if (config.isReconEnabled()) {
        startRecon(prepared.getConfiguration());
        waitForHttpEndpointReadiness(getReconEndpoint(), "Recon",
            config.getStartupTimeout());
      }
      if (config.isS3gEnabled()) {
        startS3Gateway(prepared.getConfiguration());
      }
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

    // Both checks reject the run without touching the filesystem, and both run before
    // prepareStorageLayout(), which deletes the data dir in format mode ALWAYS: a run that cannot
    // start must not destroy local state first.
    requireSupportedDatanodeCount();
    OzoneConfiguration conf = new OzoneConfiguration(seedConfiguration);
    configureLocalDefaults(conf);

    prepareStorageLayout();

    PersistedPortState persistedPorts = loadPersistedPortState();
    PortAllocator portAllocator = new PortAllocator();
    int scmPort = configureScm(conf, persistedPorts, portAllocator);
    int omPort = configureOm(conf, persistedPorts, portAllocator);
    int s3gPort = configureS3Gateway(conf, persistedPorts, portAllocator);
    int reconPort = configureRecon(conf, persistedPorts, portAllocator);
    List<OzoneConfiguration> datanodeConfigurations =
        configureDatanodes(conf, persistedPorts, portAllocator);

    persistedPorts.store();
    preparedConfiguration = new PreparedConfiguration(conf, scmPort, omPort,
        s3gPort, reconPort, datanodeConfigurations);
    return preparedConfiguration;
  }

  @Override
  public String getDisplayHost() {
    return LocalOzoneClusterConfig.WILDCARD_HOST.equals(config.getHost())
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
    InetSocketAddress address = getS3gBoundAddress();
    return address != null ? address.getPort() : -1;
  }

  @Override
  public String getS3Endpoint() {
    int port = getS3gPort();
    return port > 0 ? "http://" + getDisplayHost() + ":" + port : "";
  }

  /**
   * Returns the address the S3 Gateway HTTP listener is bound to, or null when there is no
   * listener to report: before {@link #start()}, after {@link #close()}, and when the HTTP
   * policy leaves the connector off, since {@code BaseHttpServer} assigns its address only for
   * a server it enables. Keyed off the field rather than off {@code isS3gEnabled()}, which says
   * what was asked for rather than what is running.
   */
  InetSocketAddress getS3gBoundAddress() {
    return s3Gateway != null ? s3Gateway.getHttpAddress() : null;
  }

  @Override
  public int getReconPort() {
    if (!config.isReconEnabled()) {
      return -1;
    }
    return preparedConfiguration == null ? config.getReconPort()
        : preparedConfiguration.getReconPort();
  }

  @Override
  public String getReconEndpoint() {
    if (!config.isReconEnabled()) {
      return "";
    }
    return "http://" + getDisplayHost() + ":" + getReconPort();
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
      IOUtils.close(LOG, this::stopS3Gateway, this::stopRecon, this::stopDatanodes, this::stopOm, this::stopScm);
    } finally {
      restoreSameJvmMetricsMode();
    }
  }

  private void stopS3Gateway() throws Exception {
    Gateway service = s3Gateway;
    s3Gateway = null;
    if (service != null) {
      service.stop();
    }
  }

  private void stopRecon() {
    ReconServer service = reconServer;
    reconServer = null;
    if (service != null) {
      service.stop();
      service.join();
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
    setLocalOverride(conf, OZONE_METADATA_DIRS, metadataDir().toString());
    // SCM, OM, and the S3 Gateway share one configuration and JVM, so the Jetty base dir is a
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
    setLocalOverride(conf, HDDS_SCM_SAFEMODE_MIN_DATANODE, Math.max(1, config.getDatanodes()));
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
   * Applies a value the local runtime requires, rejecting a conflicting one the user configured.
   * These keys are set rather than {@code setIfUnset} because ozone-default.xml would otherwise
   * win; a user value is refused rather than replaced, so the cluster never behaves differently
   * from the configuration the user is reading. Rejecting here, at the point of the override,
   * keeps a later override from being added without the same check.
   *
   * <p>This overload compares text, for keys whose value carries no other spelling. The typed
   * overloads below compare through the accessor the services read the key with, so a value that
   * already means what the runtime requires is kept rather than rejected.</p>
   *
   * @throws IOException if the user configured {@code key} with a value other than {@code value}
   */
  private void setLocalOverride(OzoneConfiguration conf, String key, String value)
      throws IOException {
    // Configuration#unset() leaves the key in updatingResource, so a source can outlive its
    // value; there is nothing to reject when no value is configured.
    if (conf.get(key) != null && !value.equals(conf.get(key))) {
      rejectUserConfigured(conf, key, value);
    }
    conf.set(key, value);
  }

  private void setLocalOverride(OzoneConfiguration conf, String key, boolean value)
      throws IOException {
    // Defaulting to the negation keeps a value getBoolean() cannot read from matching by accident.
    if (conf.get(key) != null && conf.getBoolean(key, !value) != value) {
      rejectUserConfigured(conf, key, String.valueOf(value));
    }
    conf.setBoolean(key, value);
  }

  private void setLocalOverride(OzoneConfiguration conf, String key, int value)
      throws IOException {
    if (conf.get(key) != null && !matchesInt(conf, key, value)) {
      rejectUserConfigured(conf, key, String.valueOf(value));
    }
    conf.setInt(key, value);
  }

  /**
   * Applies a duration the local runtime requires. The configured value is compared as a duration
   * rather than as text, so the same length written in another unit is not treated as a conflict.
   *
   * @throws IOException if the user configured {@code key} with a different duration
   */
  private void setLocalOverrideDuration(OzoneConfiguration conf, String key, String value)
      throws IOException {
    long requiredMillis = TimeDurationUtil.getTimeDurationHelper(key, value, TimeUnit.MILLISECONDS);
    if (conf.get(key) != null && !matchesDuration(conf, key, requiredMillis)) {
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
    if (source != null) {
      throw new IOException("ozone local requires " + key + "=" + required
          + ", but the configuration sets " + conf.get(key) + " (source: " + source
          + "). Remove that value, or run with a configuration directory (OZONE_CONF_DIR)"
          + " that does not set it.");
    }
  }

  private static boolean matchesInt(OzoneConfiguration conf, String key, int value) {
    try {
      return conf.getInt(key, value) == value;
    } catch (NumberFormatException unreadable) {
      // A value the accessor cannot read is a conflict; the caller reports it by key.
      return false;
    }
  }

  private static boolean matchesDuration(OzoneConfiguration conf, String key,
      long requiredMillis) {
    try {
      return conf.getTimeDuration(key, requiredMillis, TimeUnit.MILLISECONDS) == requiredMillis;
    } catch (NumberFormatException unreadable) {
      return false;
    }
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
   * value whose last source is one of the shipped {@code *-default.xml} resources is a default,
   * not a user choice.
   */
  private static String userConfiguredSource(OzoneConfiguration conf, String key) {
    String[] sources = conf.getPropertySources(key);
    if (sources == null || sources.length == 0) {
      return null;
    }
    String source = sources[sources.length - 1];
    return isShippedDefault(source) ? null : source;
  }

  /**
   * Whether {@code source} names a shipped {@code *-default.xml} resource, ozone-default.xml or
   * ozone-recon-default.xml. Configuration records a classpath resource by its bare name, so a
   * file the user named with {@code --conf} keeps its path and stays a user choice however that
   * file happens to be called.
   */
  private static boolean isShippedDefault(String source) {
    return !source.contains("/") && !source.contains("\\")
        && source.endsWith(DEFAULT_XML_SUFFIX);
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
  }

  private int configureS3Gateway(OzoneConfiguration conf,
      PersistedPortState persistedPorts, PortAllocator portAllocator)
      throws IOException {
    if (!config.isS3gEnabled()) {
      return -1;
    }
    // The runtime advertises an http:// endpoint and reads the bound port back off this
    // listener, so a configuration that switches it off leaves nothing to report.
    setLocalOverride(conf, OZONE_S3G_HTTP_ENABLED_KEY, true);

    int s3gHttpPort = reservePort(portAllocator, persistedPorts,
        S3G_HTTP_PORT_KEY, config.getS3gPort());
    int s3gWebHttpPort = reservePort(portAllocator, persistedPorts,
        S3G_WEBADMIN_HTTP_PORT_KEY, 0);
    // Port 0, not a reservation: the local runtime runs the default HTTP-only policy, so these
    // connectors never bind. A reserved port is only a hint until the listener claims it, so
    // reserving one for a listener that never starts adds a window for another process to take
    // it without ever making an endpoint stable.
    int s3gHttpsPort = 0;
    int s3gWebHttpsPort = 0;

    conf.set(OZONE_S3G_HTTP_ADDRESS_KEY,
        address(config.getHost(), s3gHttpPort));
    conf.set(OZONE_S3G_HTTP_BIND_HOST_KEY, config.getBindHost());
    conf.set(OZONE_S3G_HTTPS_ADDRESS_KEY,
        address(config.getHost(), s3gHttpsPort));
    conf.set(OZONE_S3G_HTTPS_BIND_HOST_KEY, config.getBindHost());
    conf.set(OZONE_S3G_WEBADMIN_HTTP_ADDRESS_KEY,
        address(config.getHost(), s3gWebHttpPort));
    conf.set(OZONE_S3G_WEBADMIN_HTTP_BIND_HOST_KEY, config.getBindHost());
    conf.set(OZONE_S3G_WEBADMIN_HTTPS_ADDRESS_KEY,
        address(config.getHost(), s3gWebHttpsPort));
    conf.set(OZONE_S3G_WEBADMIN_HTTPS_BIND_HOST_KEY, config.getBindHost());
    return s3gHttpPort;
  }

  private int configureRecon(OzoneConfiguration conf,
      PersistedPortState persistedPorts, PortAllocator portAllocator)
      throws IOException {
    if (!config.isReconEnabled()) {
      return -1;
    }
    Path reconDir = config.getDataDir().resolve(RECON_DIR_NAME);
    Files.createDirectories(reconDir);
    conf.setIfUnset(OZONE_RECON_DB_DIR, reconDir.toString());
    conf.setIfUnset(OZONE_RECON_OM_SNAPSHOT_DB_DIR, reconDir.toString());
    conf.setIfUnset(OZONE_RECON_SCM_DB_DIR, reconDir.toString());

    ReconSqlDbConfig dbConfig = conf.getObject(ReconSqlDbConfig.class);
    dbConfig.setJdbcUrl("jdbc:derby:"
        + reconDir.resolve("ozone_recon_derby.db"));
    conf.setFromObject(dbConfig);

    int reconHttpPort = reservePort(portAllocator, persistedPorts,
        RECON_HTTP_PORT_KEY, config.getReconPort());
    int reconHttpsPort = reservePort(portAllocator, persistedPorts,
        RECON_HTTPS_PORT_KEY, 0);
    int reconDatanodePort = reservePort(portAllocator, persistedPorts,
        RECON_DATANODE_PORT_KEY, 0);

    conf.set(OZONE_RECON_ADDRESS_KEY,
        address(config.getHost(), reconDatanodePort));
    conf.set(OZONE_RECON_DATANODE_ADDRESS_KEY,
        address(config.getHost(), reconDatanodePort));
    conf.set(OZONE_RECON_DATANODE_BIND_HOST_KEY, config.getBindHost());
    conf.set(OZONE_RECON_HTTP_ADDRESS_KEY,
        address(config.getHost(), reconHttpPort));
    conf.set(OZONE_RECON_HTTP_BIND_HOST_KEY, config.getBindHost());
    conf.set(OZONE_RECON_HTTPS_ADDRESS_KEY,
        address(config.getHost(), reconHttpsPort));
    conf.set(OZONE_RECON_HTTPS_BIND_HOST_KEY, config.getBindHost());
    // Recon's start-up blocks until it leaves safe mode, and an empty local
    // cluster never leaves it on its own, so keep the fallback wait short.
    // Use set(), not setIfUnset(): the generated ozone-recon-default.xml
    // already supplies the 300s default, so setIfUnset() would do nothing here
    // and Recon would block start-up for 5 minutes.
    setLocalOverrideDuration(conf, OZONE_RECON_TASK_SAFEMODE_WAIT_THRESHOLD, "10s");
    return reconHttpPort;
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

  private void configureDatanodeStorage(OzoneConfiguration dnConf, int index)
      throws IOException {
    Path datanodeDir = config.getDataDir()
        .resolve(DATANODE_DIR_PREFIX + index);
    Path datanodeMetadataDir = datanodeDir.resolve(OZONE_METADATA_DIR_NAME);
    Files.createDirectories(datanodeMetadataDir);
    Files.createDirectories(datanodeDir.resolve(DATA_DIR_NAME));

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

  private void startS3Gateway(OzoneConfiguration conf) throws Exception {
    // Gateway reads its configuration from the static holder, like MiniOzoneCluster does when
    // running S3 Gateway in the same JVM.
    OzoneConfigurationHolder.resetConfiguration();
    OzoneConfigurationHolder.setConfiguration(new OzoneConfiguration(conf));
    Gateway gateway = new Gateway();
    // Start through call(), not execute(): GenericCli's execution-exception handler reduces the
    // startup failure cause to a bare exit code and can even System.exit the JVM on an
    // AccessControlException. parseArgs() primes the picocli state that Gateway.start() reads.
    gateway.getCmd().parseArgs();
    // call() returns only after Jetty has bound the gateway's ports and serves requests, so no
    // readiness poll follows. The field holds only a fully started gateway: a failed or timed-out
    // startup is stopped by executeWithinStartupTimeout, never by the rollback in stopServices(),
    // which could otherwise race a startup attempt that is still running.
    executeWithinStartupTimeout("S3 Gateway", gateway::call, gateway::stop, config.getStartupTimeout());
    s3Gateway = gateway;
  }

  /**
   * Runs a blocking in-JVM service startup under {@code timeout}, so the launcher fails fast and
   * rolls back instead of hanging if a startup ever blocks. {@code startupCleanup} stops whatever
   * a startup that did not finish cleanly managed to bring up: it runs in place when the startup
   * throws, and on a timeout it is queued behind the startup on the same single thread, because a
   * timed-out startup can ignore the interrupt and still finish after the rollback in
   * {@link #stopServices()} has run. The queued cleanup is that abandoned attempt's only stopper,
   * and the shared thread runs it after every write the attempt made.
   */
  static <V> V executeWithinStartupTimeout(String serviceName, Callable<V> startup,
      AutoCloseable startupCleanup, Duration timeout) throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
      Thread thread = new Thread(runnable, "local-startup-" + serviceName);
      thread.setDaemon(true);
      return thread;
    });
    Future<V> future = executor.submit(startup);
    try {
      return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException ex) {
      future.cancel(true);
      executor.submit(() -> IOUtils.close(LOG, startupCleanup));
      throw new IOException("Local " + serviceName + " did not start within " + timeout + ".", ex);
    } catch (InterruptedException ex) {
      // Same shape as the timeout path, and for the same reason: the startup may ignore the
      // interrupt and finish afterwards, so its cleanup has to be queued behind it rather than
      // run here. Without this the attempt keeps its ports with nothing left holding a
      // reference to stop it, since s3Gateway is never assigned.
      future.cancel(true);
      executor.submit(() -> IOUtils.close(LOG, startupCleanup));
      Thread.currentThread().interrupt();
      throw ex;
    } catch (ExecutionException ex) {
      IOUtils.close(LOG, startupCleanup);
      Throwable cause = ex.getCause();
      throw cause instanceof Exception ? (Exception) cause : ex;
    } finally {
      // shutdown(), not shutdownNow(): the timeout path queues the cleanup, which shutdownNow()
      // would discard. The worker is a daemon thread, so it cannot keep the JVM alive.
      executor.shutdown();
    }
  }

  private void startRecon(OzoneConfiguration conf) throws Exception {
    // ReconServer reads its configuration from the static provider, like
    // MiniOzoneCluster does when running Recon in the same JVM.
    ConfigurationProvider.resetConfiguration();
    ConfigurationProvider.setConfiguration(new OzoneConfiguration(conf));
    ReconServer recon = new ReconServer();
    // The field holds only a successfully launched server: a failed or timed-out launch is
    // stopped by executeWithinStartupTimeout, never by the rollback in stopServices(), which
    // could otherwise race a launch that is still running.
    int exitCode = executeWithinStartupTimeout("Recon", () -> recon.execute(NO_ARGS),
        recon::stop, config.getStartupTimeout());
    if (exitCode != 0) {
      // execute() has returned, so stopping the partially started server cannot race it.
      IOUtils.close(LOG, recon::stop);
      throw new IOException("Failed to start local Recon. Exit code "
          + exitCode + ".");
    }
    reconServer = recon;
  }

  private void waitForHttpEndpointReadiness(String endpoint,
      String serviceName, Duration timeout) throws Exception {
    waitForReadiness(() -> httpEndpointBlocker(endpoint, serviceName),
        serviceName, timeout);
  }

  /**
   * Returns why {@code endpoint} is not serving yet, or null once it is. Unlike SCM and OM, Recon
   * exposes no in-process readiness API to this package, and ReconServer even reports success
   * while its initialization failed; an HTTP response from the endpoint is the service-level
   * signal that requests are being served. A failed probe is expected while the service starts,
   * but the reason is kept: a refused connection, a wrong port and a hung listener are otherwise
   * indistinguishable once the wait times out.
   */
  private static String httpEndpointBlocker(String endpoint, String serviceName) {
    HttpURLConnection connection = null;
    try {
      connection = (HttpURLConnection) new URL(endpoint).openConnection();
      connection.setConnectTimeout((int) READINESS_POLL_INTERVAL_MILLIS);
      connection.setReadTimeout((int) READINESS_POLL_INTERVAL_MILLIS);
      connection.getResponseCode();
      return null;
    } catch (IOException ex) {
      LOG.debug("{} readiness probe of {} failed.", serviceName, endpoint, ex);
      return endpoint + " is not answering (" + ex + ")";
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
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
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    long nextLogNanos = System.nanoTime() + READINESS_LOG_INTERVAL_NANOS;
    while (true) {
      String reason = blocker.get();
      if (reason == null) {
        return;
      }
      if (System.nanoTime() >= deadlineNanos) {
        throw new TimeoutException("Timed out waiting " + timeout + " for the local " + subject
            + " to become ready: " + reason + ".");
      }
      if (System.nanoTime() >= nextLogNanos) {
        LOG.info("Waiting for the local {} to become ready: {}.", subject, reason);
        nextLogNanos = System.nanoTime() + READINESS_LOG_INTERVAL_NANOS;
      }
      Thread.sleep(READINESS_POLL_INTERVAL_MILLIS);
    }
  }

  /**
   * Returns why the cluster is not usable yet, or null once it is ready. The cluster is usable
   * once SCM and OM are leader-ready, every datanode has registered with SCM, and SCM has left
   * safe mode.
   */
  private String clusterReadinessBlocker() {
    if (!scm.checkLeader()) {
      return "SCM has no Ratis leader yet";
    }
    if (!om.isLeaderReady()) {
      return "OM is not leader-ready yet";
    }
    int registered = scm.getScmNodeManager().getAllNodes().size();
    if (registered < config.getDatanodes()) {
      return "only " + registered + " of " + config.getDatanodes()
          + " datanodes have registered with SCM";
    }
    // Registration alone is not enough: SCM refuses block allocation until it leaves safe mode.
    if (scm.isInSafeMode()) {
      return "SCM is still in safe mode (" + unmetSafeModeRules() + ")";
    }
    return null;
  }

  private String unmetSafeModeRules() {
    return scm.getScmSafeModeManager().getRuleStatus().entrySet().stream()
        .filter(rule -> !rule.getValue().getLeft())
        .map(rule -> rule.getKey() + ": " + rule.getValue().getRight())
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

  /**
   * Rejects a datanode count this host cannot serve, or that the cluster cannot start with.
   */
  private void requireSupportedDatanodeCount() throws IOException {
    int datanodeCount = config.getDatanodes();
    if (datanodeCount < 1) {
      // configureLocalDefaults() requires one datanode for safe mode, so a smaller count leaves
      // SCM in safe mode until the readiness wait gives up.
      throw new IOException("Datanode count " + datanodeCount
          + " is below the local minimum of 1; SCM stays in safe mode without a datanode.");
    }
    if (datanodeCount > MAX_DATANODES) {
      throw new IOException("Datanode count " + datanodeCount
          + " exceeds the local maximum of " + MAX_DATANODES
          + "; each datanode reserves " + DATANODE_PORT_KEY_SUFFIXES.length
          + " local ports.");
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
      LOG.info("Removing local Ozone data dir {} (format mode ALWAYS).", dataDir);
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
      persistedPorts.requireKeys(requiredPersistedPortKeys());
    }
    return persistedPorts;
  }

  private String[] requiredPersistedPortKeys() {
    List<String> keys =
        new ArrayList<>(Arrays.asList(REQUIRED_PERSISTED_PORT_KEYS));
    if (config.isS3gEnabled()) {
      keys.add(S3G_HTTP_PORT_KEY);
      keys.add(S3G_WEBADMIN_HTTP_PORT_KEY);
    }
    if (config.isReconEnabled()) {
      keys.add(RECON_HTTP_PORT_KEY);
      keys.add(RECON_HTTPS_PORT_KEY);
      keys.add(RECON_DATANODE_PORT_KEY);
    }
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
    private final int s3gPort;
    private final int reconPort;
    private final List<OzoneConfiguration> datanodeConfigurations;

    PreparedConfiguration(OzoneConfiguration configuration, int scmPort, int omPort, int s3gPort, int reconPort,
        List<OzoneConfiguration> datanodeConfigurations) {
      this.configuration = Objects.requireNonNull(configuration,
          "configuration");
      this.scmPort = scmPort;
      this.omPort = omPort;
      this.s3gPort = s3gPort;
      this.reconPort = reconPort;
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

    int getS3gPort() {
      return s3gPort;
    }

    int getReconPort() {
      return reconPort;
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
