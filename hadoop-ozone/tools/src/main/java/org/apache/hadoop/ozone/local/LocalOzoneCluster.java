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
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_CONTAINER_RATIS_ENABLED_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_GRPC_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTPS_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_PORT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * Prepares local filesystem state and configuration for {@code ozone local}.
 *
 * <p>This runtime intentionally stops at filesystem and configuration
 * preparation. Starting SCM, OM, datanodes, and S3 Gateway belongs to later
 * HDDS-15084/HDDS-15086 work.</p>
 */
public final class LocalOzoneCluster implements LocalOzoneRuntime {

  static final String PORTS_STATE_FILE_NAME = "ports.properties";

  private static final String METADATA_DIR_NAME = "metadata";
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
  private static final String OM_RATIS_PORT_KEY = "om.ratis";

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
      OM_RATIS_PORT_KEY
  };

  private final LocalOzoneClusterConfig config;
  private final OzoneConfiguration seedConfiguration;

  private PreparedConfiguration preparedConfiguration;

  public LocalOzoneCluster(LocalOzoneClusterConfig config,
      OzoneConfiguration seedConfiguration) {
    this.config = Objects.requireNonNull(config, "config");
    this.seedConfiguration = new OzoneConfiguration(
        Objects.requireNonNull(seedConfiguration, "seedConfiguration"));
  }

  @Override
  public void start() throws IOException {
    prepareConfiguration();
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
    return preparedConfiguration == null ? config.getScmPort()
        : preparedConfiguration.getScmPort();
  }

  @Override
  public int getOmPort() {
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
    // Ephemeral mode owns the data directory lifecycle for short-lived runs.
    if (config.isEphemeral()) {
      deleteDirectory(config.getDataDir());
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
    return scmClientPort;
  }

  private int configureOm(OzoneConfiguration conf,
      PersistedPortState persistedPorts, PortAllocator portAllocator)
      throws IOException {
    int omRpcPort = reservePort(portAllocator, persistedPorts, OM_RPC_PORT_KEY,
        config.getOmPort());
    int omHttpPort = reservePort(portAllocator, persistedPorts,
        OM_HTTP_PORT_KEY, 0);
    int omRatisPort = reservePort(portAllocator, persistedPorts,
        OM_RATIS_PORT_KEY, 0);

    conf.set(OZONE_OM_ADDRESS_KEY, address(config.getHost(), omRpcPort));
    conf.set(OZONE_OM_HTTP_ADDRESS_KEY, address(config.getHost(), omHttpPort));
    conf.set(OZONE_OM_HTTP_BIND_HOST_KEY, config.getBindHost());
    conf.setInt(OZONE_OM_RATIS_PORT_KEY, omRatisPort);
    return omRpcPort;
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
