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
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_CONTAINER_RATIS_ENABLED_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_ADMIN_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_SERVER_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.container.replication.ReplicationServer;
import org.apache.hadoop.ozone.local.LocalOzoneClusterConfig.FormatMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts a local in-process Ozone cluster for development and testing.
 *
 * <p>This implementation manages the lifecycle of datanodes (and eventually
 * SCM, OM, S3G) running within a single JVM process.</p>
 */
public final class LocalOzoneCluster implements LocalOzoneRuntime {

  private static final Logger LOG =
      LoggerFactory.getLogger(LocalOzoneCluster.class);

  private static final String[] NO_ARGS = new String[0];
  private static final String PORTS_STATE_FILE = "ports.properties";

  private final LocalOzoneClusterConfig config;
  private final OzoneConfiguration seedConfiguration;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final List<HddsDatanodeService> datanodes = new ArrayList<>();

  private boolean previousMetricsMiniClusterMode;
  private boolean metricsMiniClusterModeEnabled;

  /**
   * Creates a new local Ozone cluster.
   *
   * @param config the cluster configuration
   * @param seedConfiguration the base Ozone configuration
   */
  public LocalOzoneCluster(LocalOzoneClusterConfig config,
      OzoneConfiguration seedConfiguration) {
    this.config = Objects.requireNonNull(config, "config");
    this.seedConfiguration = new OzoneConfiguration(
        Objects.requireNonNull(seedConfiguration, "seedConfiguration"));
  }

  @Override
  public void start() throws Exception {
    enableMiniClusterMetricsMode();
    prepareDataDirectory();

    OzoneConfiguration baseConf = prepareBaseConfiguration();
    startDatanodes(baseConf);

    // TODO: Add SCM/OM startup and waitForClusterToBeReady() in future tasks
    LOG.info("Local Ozone cluster started with {} datanode(s)",
        datanodes.size());
  }

  @Override
  public String getDisplayHost() {
    return "0.0.0.0".equals(config.getHost())
        ? LocalOzoneClusterConfig.DEFAULT_HOST
        : config.getHost();
  }

  /**
   * Returns the number of running datanodes.
   */
  public int getDatanodeCount() {
    return datanodes.size();
  }

  @Override
  public int getScmPort() {
    // TODO: Return actual SCM port when SCM is implemented
    return -1;
  }

  @Override
  public int getOmPort() {
    // TODO: Return actual OM port when OM is implemented
    return -1;
  }

  @Override
  public int getS3gPort() {
    // TODO: Return actual S3G port when S3G is implemented
    return -1;
  }

  @Override
  public String getS3Endpoint() {
    // TODO: Return actual S3 endpoint when S3G is implemented
    return "";
  }

  @Override
  public void close() throws Exception {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    try {
      stopDatanodes();

      if (config.isEphemeral()) {
        deleteDirectory(config.getDataDir());
        LOG.info("Deleted ephemeral data directory: {}", config.getDataDir());
      }
    } finally {
      restoreMetricsMode();
    }
  }

  /**
   * Prepares the data directory, formatting if needed.
   */
  private void prepareDataDirectory() throws IOException {
    if (config.getFormatMode() == FormatMode.ALWAYS) {
      deleteDirectory(config.getDataDir());
    }
    Files.createDirectories(config.getDataDir());
  }

  /**
   * Prepares the base Ozone configuration with local-safe defaults.
   */
  OzoneConfiguration prepareBaseConfiguration() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration(seedConfiguration);

    // Local-safe replication defaults: single replica, no Ratis
    conf.set(OZONE_REPLICATION, ReplicationFactor.ONE.name());
    conf.set(OZONE_REPLICATION_TYPE, ReplicationType.STAND_ALONE.name());
    conf.setBoolean(HDDS_CONTAINER_RATIS_ENABLED_KEY, false);

    // Root metadata directory
    Path metadataDir = Files.createDirectories(
        config.getDataDir().resolve("metadata"));
    conf.set(OZONE_METADATA_DIRS, metadataDir.toString());

    return conf;
  }

  /**
   * Starts the configured number of datanodes.
   */
  private void startDatanodes(OzoneConfiguration baseConf) throws IOException {
    PersistedPorts persistedPorts = PersistedPorts.load(
        config.getDataDir().resolve(PORTS_STATE_FILE));
    PortAllocator ports = new PortAllocator();

    for (int index = 0; index < config.getDatanodes(); index++) {
      OzoneConfiguration dnConf = createDatanodeConfiguration(
          baseConf, index, ports, persistedPorts);
      HddsDatanodeService datanode = startDatanode(dnConf);
      datanodes.add(datanode);
      LOG.info("Started datanode {} of {}", index + 1, config.getDatanodes());
    }

    persistedPorts.store();
  }

  /**
   * Creates isolated configuration for a single datanode.
   */
  private OzoneConfiguration createDatanodeConfiguration(
      OzoneConfiguration baseConf, int index, PortAllocator ports,
      PersistedPorts persistedPorts) throws IOException {

    OzoneConfiguration dnConf = new OzoneConfiguration(baseConf);

    // Create isolated directories for this datanode
    Path datanodeDir = Files.createDirectories(
        config.getDataDir().resolve("datanode-" + (index + 1)));
    Path metaDir = Files.createDirectories(datanodeDir.resolve("metadata"));
    Path dataDir = Files.createDirectories(datanodeDir.resolve("data-0"));
    Path ratisDir = Files.createDirectories(datanodeDir.resolve("ratis"));

    dnConf.set(OZONE_METADATA_DIRS, metaDir.toString());
    dnConf.set(HDDS_DATANODE_DIR_KEY, dataDir.toString());
    dnConf.set(HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR, ratisDir.toString());

    // Allocate unique ports for this datanode
    String prefix = "dn." + index;

    dnConf.set(HDDS_DATANODE_HTTP_ADDRESS_KEY,
        address(config.getHost(), reservePort(ports, persistedPorts,
            prefix + ".http")));
    dnConf.set(HDDS_DATANODE_HTTP_BIND_HOST_KEY, config.getBindHost());

    dnConf.set(HDDS_DATANODE_CLIENT_ADDRESS_KEY,
        address(config.getHost(), reservePort(ports, persistedPorts,
            prefix + ".client")));
    dnConf.set(HDDS_DATANODE_CLIENT_BIND_HOST_KEY, config.getBindHost());

    dnConf.setInt(HDDS_CONTAINER_IPC_PORT,
        reservePort(ports, persistedPorts, prefix + ".container.ipc"));
    dnConf.setInt(HDDS_CONTAINER_RATIS_IPC_PORT,
        reservePort(ports, persistedPorts, prefix + ".ratis.ipc"));
    dnConf.setInt(HDDS_CONTAINER_RATIS_ADMIN_PORT,
        reservePort(ports, persistedPorts, prefix + ".ratis.admin"));
    dnConf.setInt(HDDS_CONTAINER_RATIS_SERVER_PORT,
        reservePort(ports, persistedPorts, prefix + ".ratis.server"));
    dnConf.setInt(HDDS_CONTAINER_RATIS_DATASTREAM_PORT,
        reservePort(ports, persistedPorts, prefix + ".ratis.datastream"));

    dnConf.setFromObject(new ReplicationServer.ReplicationConfig()
        .setPort(reservePort(ports, persistedPorts, prefix + ".replication")));

    return dnConf;
  }

  /**
   * Starts a single datanode with the given configuration.
   */
  private HddsDatanodeService startDatanode(OzoneConfiguration conf)
      throws IOException {
    HddsDatanodeService datanode = new HddsDatanodeService(NO_ARGS);
    datanode.setConfiguration(conf);
    datanode.start(conf);
    return datanode;
  }

  /**
   * Stops all running datanodes in reverse order.
   */
  private void stopDatanodes() {
    for (int i = datanodes.size() - 1; i >= 0; i--) {
      HddsDatanodeService datanode = datanodes.get(i);
      try {
        datanode.stop();
        datanode.join();
        LOG.info("Stopped datanode {}", i + 1);
      } catch (Exception ex) {
        LOG.warn("Failed to stop datanode {}", i + 1, ex);
      }
    }
    datanodes.clear();
  }

  /**
   * Reserves a port, preferring a previously persisted value.
   */
  private int reservePort(PortAllocator allocator,
      PersistedPorts persistedPorts, String key) throws IOException {
    int preferredPort = persistedPorts.get(key);
    int port = allocator.reserve(preferredPort);
    persistedPorts.set(key, port);
    return port;
  }

  /**
   * Enables mini-cluster mode for metrics to allow multiple services
   * in the same JVM.
   */
  private void enableMiniClusterMetricsMode() {
    if (!metricsMiniClusterModeEnabled) {
      previousMetricsMiniClusterMode = DefaultMetricsSystem.inMiniClusterMode();
      DefaultMetricsSystem.setMiniClusterMode(true);
      metricsMiniClusterModeEnabled = true;
    }
  }

  /**
   * Restores the previous metrics mode.
   */
  private void restoreMetricsMode() {
    if (metricsMiniClusterModeEnabled) {
      DefaultMetricsSystem.setMiniClusterMode(previousMetricsMiniClusterMode);
      metricsMiniClusterModeEnabled = false;
    }
  }

  private static String address(String host, int port) {
    return host + ":" + port;
  }

  private static void deleteDirectory(Path directory) throws IOException {
    if (!Files.exists(directory)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(directory)) {
      for (Path path : (Iterable<Path>) paths
          .sorted(Comparator.reverseOrder())::iterator) {
        Files.deleteIfExists(path);
      }
    }
  }
}
