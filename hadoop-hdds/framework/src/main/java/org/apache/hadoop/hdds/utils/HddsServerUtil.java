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

package org.apache.hadoop.hdds.utils;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_INITIAL_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_INITIAL_HEARTBEAT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_RECON_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_RECON_HEARTBEAT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_RECON_INITIAL_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_RECON_INITIAL_HEARTBEAT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdds.HddsUtils.getHostName;
import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.getHostPort;
import static org.apache.hadoop.hdds.HddsUtils.getPortNumberFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.getScmServiceId;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_LOG_WARN_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_LOG_WARN_INTERVAL_COUNT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_RPC_RETRY_COUNT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_RPC_RETRY_COUNT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_RPC_RETRY_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_RPC_RETRY_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_RPC_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_RPC_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdds.server.ServerUtils.sanitizeUserArgs;
import static org.apache.hadoop.hdds.utils.Archiver.includeFile;
import static org.apache.hadoop.hdds.utils.Archiver.tar;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_DATANODE_CONTAINER_DB_DIR;

import com.google.common.base.Strings;
import com.google.protobuf.BlockingService;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.protocol.SecretKeyProtocolScm;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.protocolPB.SecretKeyProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.protocolPB.SecretKeyProtocolDatanodePB;
import org.apache.hadoop.hdds.protocolPB.SecretKeyProtocolOmPB;
import org.apache.hadoop.hdds.protocolPB.SecretKeyProtocolScmPB;
import org.apache.hadoop.hdds.ratis.ServerNotLeaderException;
import org.apache.hadoop.hdds.recon.ReconConfigKeys;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.proxy.SCMClientConfig;
import org.apache.hadoop.hdds.scm.proxy.SCMSecurityProtocolFailoverProxyProvider;
import org.apache.hadoop.hdds.scm.proxy.SecretKeyProtocolFailoverProxyProvider;
import org.apache.hadoop.hdds.scm.proxy.SingleSecretKeyProtocolProxyProvider;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hdds stateless helper functions for server side components.
 */
public final class HddsServerUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HddsServerUtil.class);

  private static final int SHUTDOWN_HOOK_PRIORITY = 0;

  public static final String OZONE_RATIS_SNAPSHOT_COMPLETE_FLAG_NAME =
      "OZONE_RATIS_SNAPSHOT_COMPLETE";

  private HddsServerUtil() {
  }

  /**
   * Add protobuf-based protocol to the {@link org.apache.hadoop.ipc_.RPC.Server}.
   * @param conf configuration
   * @param protocol Protocol interface
   * @param service service that implements the protocol
   * @param server RPC server to which the protocol and implementation is added to
   */
  public static void addPBProtocol(Configuration conf, Class<?> protocol,
      BlockingService service, RPC.Server server) {
    RPC.setProtocolEngine(conf, protocol, ProtobufRpcEngine.class);
    server.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, protocol, service);
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the SCM.
   *
   * @param conf
   * @return Target {@code InetSocketAddress} for the SCM client endpoint.
   */
  public static InetSocketAddress getScmClientBindAddress(
      ConfigurationSource conf) {
    final String host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_KEY)
        .orElse(ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_DEFAULT);

    final int port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY)
        .orElse(conf.getInt(ScmConfigKeys.OZONE_SCM_CLIENT_PORT_KEY,
            ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT));

    return NetUtils.createSocketAddr(host + ":" + port);
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the SCM Block service.
   *
   * @param conf
   * @return Target {@code InetSocketAddress} for the SCM block client endpoint.
   */
  public static InetSocketAddress getScmBlockClientBindAddress(
      ConfigurationSource conf) {
    final String host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_BIND_HOST_KEY)
        .orElse(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_BIND_HOST_DEFAULT);

    final int port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY)
        .orElse(conf.getInt(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_KEY,
            ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT));

    return NetUtils.createSocketAddr(host + ":" + port);
  }

  /**
   * Retrieve the socket address that should be used by scm security server to
   * service clients.
   *
   * @param conf
   * @return Target {@code InetSocketAddress} for the SCM security service.
   */
  public static InetSocketAddress getScmSecurityInetAddress(
      ConfigurationSource conf) {
    final String host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_BIND_HOST_KEY)
        .orElse(ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_BIND_HOST_DEFAULT);

    final OptionalInt port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY);

    return NetUtils.createSocketAddr(
        host
            + ":" + port
            .orElse(conf.getInt(ScmConfigKeys
                    .OZONE_SCM_SECURITY_SERVICE_PORT_KEY,
                ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_DEFAULT)));
  }

  /**
   * Retrieve the socket address that should be used by DataNodes to connect
   * to the SCM.
   *
   * @param conf
   * @return Target {@code InetSocketAddress} for the SCM service endpoint.
   */
  public static InetSocketAddress getScmDataNodeBindAddress(
      ConfigurationSource conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_KEY);

    final OptionalInt port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY);

    return NetUtils.createSocketAddr(
        host.orElse(ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_DEFAULT) + ":" +
            port.orElse(conf.getInt(OZONE_SCM_DATANODE_PORT_KEY,
                ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT)));
  }

  /**
   * Retrieve the socket address that should be used by DataNodes to connect
   * to Recon.
   *
   * @param conf
   * @return Target {@code InetSocketAddress} for the SCM service endpoint.
   */
  public static InetSocketAddress getReconDataNodeBindAddress(
      ConfigurationSource conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        ReconConfigKeys.OZONE_RECON_DATANODE_BIND_HOST_KEY);

    final OptionalInt port = getPortNumberFromConfigKeys(conf,
        ReconConfigKeys.OZONE_RECON_DATANODE_ADDRESS_KEY);

    return NetUtils.createSocketAddr(
        host.orElse(
            ReconConfigKeys.OZONE_RECON_DATANODE_BIND_HOST_DEFAULT) + ":" +
            port.orElse(ReconConfigKeys.OZONE_RECON_DATANODE_PORT_DEFAULT));
  }

  /**
   * Returns the interval in which the heartbeat processor thread runs.
   *
   * @param conf - Configuration
   * @return long in Milliseconds.
   */
  public static long getScmheartbeatCheckerInterval(ConfigurationSource conf) {
    return conf.getTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Heartbeat Interval - Defines the heartbeat frequency from a datanode to
   * SCM.
   *
   * @param conf - Ozone Config
   * @return - HB interval in milli seconds.
   */
  public static long getScmHeartbeatInterval(ConfigurationSource conf) {
    return conf.getTimeDuration(HDDS_HEARTBEAT_INTERVAL,
        HDDS_HEARTBEAT_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
  }

  /**
   * Heartbeat Interval - Defines the initial heartbeat frequency from a datanode to
   * SCM.
   *
   * @param conf - Ozone Config
   * @return - HB interval in milli seconds.
   */
  public static long getScmInitialHeartbeatInterval(ConfigurationSource conf) {
    return conf.getTimeDuration(HDDS_INITIAL_HEARTBEAT_INTERVAL,
        HDDS_INITIAL_HEARTBEAT_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
  }

  /**
   * Heartbeat Interval - Defines the heartbeat frequency from a datanode to
   * Recon.
   *
   * @param conf - Ozone Config
   * @return - HB interval in milli seconds.
   */
  public static long getReconHeartbeatInterval(ConfigurationSource conf) {
    return conf.getTimeDuration(HDDS_RECON_HEARTBEAT_INTERVAL,
        HDDS_RECON_HEARTBEAT_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
  }

  /**
   * Heartbeat Interval - Defines the initial heartbeat frequency from a datanode to
   * Recon.
   *
   * @param conf - Ozone Config
   * @return - HB interval in milli seconds.
   */
  public static long getInitialReconHeartbeatInterval(ConfigurationSource conf) {
    return conf.getTimeDuration(HDDS_RECON_INITIAL_HEARTBEAT_INTERVAL,
        HDDS_RECON_INITIAL_HEARTBEAT_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
  }

  /**
   * Get the Stale Node interval, which is used by SCM to flag a datanode as
   * stale, if the heartbeat from that node has been missing for this duration.
   *
   * @param conf - Configuration.
   * @return - Long, Milliseconds to wait before flagging a node as stale.
   */
  public static long getStaleNodeInterval(ConfigurationSource conf) {

    long staleNodeIntervalMs =
        conf.getTimeDuration(OZONE_SCM_STALENODE_INTERVAL,
            OZONE_SCM_STALENODE_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);

    long heartbeatThreadFrequencyMs = getScmheartbeatCheckerInterval(conf);

    long heartbeatIntervalMs = getScmHeartbeatInterval(conf);


    // Make sure that StaleNodeInterval is configured way above the frequency
    // at which we run the heartbeat thread.
    //
    // Here we check that staleNodeInterval is at least five times more than the
    // frequency at which the accounting thread is going to run.
    staleNodeIntervalMs = sanitizeUserArgs(OZONE_SCM_STALENODE_INTERVAL,
        staleNodeIntervalMs, OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        heartbeatThreadFrequencyMs, 5, 1000);

    // Make sure that stale node value is greater than configured value that
    // datanodes are going to send HBs.
    staleNodeIntervalMs = sanitizeUserArgs(OZONE_SCM_STALENODE_INTERVAL,
        staleNodeIntervalMs, HDDS_HEARTBEAT_INTERVAL, heartbeatIntervalMs, 3,
        1000);
    return staleNodeIntervalMs;
  }

  /**
   * Gets the interval for dead node flagging. This has to be a value that is
   * greater than stale node value,  and by transitive relation we also know
   * that this value is greater than heartbeat interval and heartbeatProcess
   * Interval.
   *
   * @param conf - Configuration.
   * @return - the interval for dead node flagging.
   */
  public static long getDeadNodeInterval(ConfigurationSource conf) {
    long staleNodeIntervalMs = getStaleNodeInterval(conf);
    long deadNodeIntervalMs = conf.getTimeDuration(OZONE_SCM_DEADNODE_INTERVAL,
        OZONE_SCM_DEADNODE_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);

    // Make sure that dead nodes Ms is at least twice the time for staleNodes
    // with a max of 1000 times the staleNodes.
    return sanitizeUserArgs(OZONE_SCM_DEADNODE_INTERVAL, deadNodeIntervalMs,
        OZONE_SCM_STALENODE_INTERVAL, staleNodeIntervalMs, 2, 1000);
  }

  /**
   * Timeout value for the RPC from Datanode to SCM, primarily used for
   * Heartbeats and container reports.
   *
   * @param conf - Ozone Config
   * @return - Rpc timeout in Milliseconds.
   */
  public static long getScmRpcTimeOutInMilliseconds(ConfigurationSource conf) {
    return conf.getTimeDuration(OZONE_SCM_HEARTBEAT_RPC_TIMEOUT,
        OZONE_SCM_HEARTBEAT_RPC_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
  }

  /**
   * Max retry count of rpcProxy for EndpointStateMachine of SCM.
   *
   * @param conf - Ozone Config
   * @return - Max retry count.
   */
  public static int getScmRpcRetryCount(ConfigurationSource conf) {
    return conf.getInt(OZONE_SCM_HEARTBEAT_RPC_RETRY_COUNT,
        OZONE_SCM_HEARTBEAT_RPC_RETRY_COUNT_DEFAULT);
  }

  /**
   * Fixed datanode rpc retry interval, which is used by datanode to connect
   * the SCM.
   *
   * @param conf - Ozone Config
   * @return - Rpc retry interval.
   */
  public static long getScmRpcRetryInterval(ConfigurationSource conf) {
    return conf.getTimeDuration(OZONE_SCM_HEARTBEAT_RPC_RETRY_INTERVAL,
        OZONE_SCM_HEARTBEAT_RPC_RETRY_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
  }

  /**
   * Log Warn interval.
   *
   * @param conf - Ozone Config
   * @return - Log warn interval.
   */
  public static int getLogWarnInterval(ConfigurationSource conf) {
    return conf.getInt(OZONE_SCM_HEARTBEAT_LOG_WARN_INTERVAL_COUNT,
        OZONE_SCM_HEARTBEAT_LOG_WARN_DEFAULT);
  }

  /**
   * returns the Container port.
   * @param conf - Conf
   * @return port number.
   */
  public static int getContainerPort(ConfigurationSource conf) {
    return conf.getInt(OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT,
        OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT_DEFAULT);
  }

  public static Collection<String> getOzoneDatanodeRatisDirectory(
      ConfigurationSource conf) {
    Collection<String> rawLocations = conf.getTrimmedStringCollection(
            OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR);

    if (rawLocations.isEmpty()) {
      rawLocations = new ArrayList<>(1);
      rawLocations.add(ServerUtils.getDefaultRatisDirectory(conf, NodeType.DATANODE));
    }
    return rawLocations;
  }

  public static long requiredReplicationSpace(long defaultContainerSize) {
    // During container import it requires double the container size to hold container in tmp and dest directory
    return 2 * defaultContainerSize;
  }

  public static Collection<String> getDatanodeStorageDirs(ConfigurationSource conf) {
    Collection<String> rawLocations = conf.getTrimmedStringCollection(HDDS_DATANODE_DIR_KEY);
    if (rawLocations.isEmpty()) {
      throw new IllegalArgumentException("No location configured in " + HDDS_DATANODE_DIR_KEY);
    }
    return rawLocations;
  }

  public static Collection<String> getDatanodeDbDirs(
      ConfigurationSource conf) {
    // No fallback here, since this config is optional.
    return conf.getTrimmedStringCollection(HDDS_DATANODE_CONTAINER_DB_DIR);
  }

  /**
   * Get the path for datanode id file.
   *
   * @param conf - Configuration
   * @return the path of datanode id as string
   */
  public static String getDatanodeIdFilePath(ConfigurationSource conf) {
    String dataNodeIDDirPath =
        conf.getTrimmed(ScmConfigKeys.OZONE_SCM_DATANODE_ID_DIR);
    if (Strings.isNullOrEmpty(dataNodeIDDirPath)) {
      File metaDirPath = ServerUtils.getOzoneMetaDirPath(conf);
      if (metaDirPath == null) {
        // this means meta data is not found, in theory should not happen at
        // this point because should've failed earlier.
        throw new IllegalArgumentException("Unable to locate meta data" +
            "directory when getting datanode id path");
      }
      dataNodeIDDirPath = metaDirPath.toString();
    }
    // Use default datanode id file name for file path
    return new File(dataNodeIDDirPath,
        OzoneConsts.OZONE_SCM_DATANODE_ID_FILE_DEFAULT).toString();
  }

  /**
   * Create a scm security client.
   * @param conf    - Ozone configuration.
   *
   * @return {@link SCMSecurityProtocol}
   * @throws IOException
   */
  public static SCMSecurityProtocolClientSideTranslatorPB getScmSecurityClient(
      ConfigurationSource conf) throws IOException {
    SecurityConfig.initSecurityProvider(conf);
    return new SCMSecurityProtocolClientSideTranslatorPB(
        new SCMSecurityProtocolFailoverProxyProvider(conf,
            UserGroupInformation.getCurrentUser()));
  }

  public static SCMSecurityProtocolClientSideTranslatorPB
      getScmSecurityClientWithMaxRetry(OzoneConfiguration conf,
      UserGroupInformation ugi) throws IOException {
    // Certificate from SCM is required for DN startup to succeed, so retry
    // for ever. In this way DN start up is resilient to SCM service running
    // status.
    OzoneConfiguration configuration = new OzoneConfiguration(conf);
    SecurityConfig.initSecurityProvider(configuration);
    SCMClientConfig scmClientConfig =
        conf.getObject(SCMClientConfig.class);
    int retryCount = Integer.MAX_VALUE;
    scmClientConfig.setRetryCount(retryCount);
    configuration.setFromObject(scmClientConfig);

    return new SCMSecurityProtocolClientSideTranslatorPB(
        new SCMSecurityProtocolFailoverProxyProvider(configuration,
            ugi == null ? UserGroupInformation.getCurrentUser() : ugi));
  }

  /**
   * Create a scm block client, used by putKey() and getKey().
   *
   * @return {@link ScmBlockLocationProtocol}
   * @throws IOException
   */
  public static SCMSecurityProtocolClientSideTranslatorPB getScmSecurityClient(
      OzoneConfiguration conf, UserGroupInformation ugi) throws IOException {
    SecurityConfig.initSecurityProvider(conf);
    SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient =
        new SCMSecurityProtocolClientSideTranslatorPB(
            new SCMSecurityProtocolFailoverProxyProvider(conf, ugi));
    return TracingUtil.createProxy(scmSecurityClient,
        SCMSecurityProtocolClientSideTranslatorPB.class, conf);
  }

  /**
   * Creates a {@link org.apache.hadoop.hdds.protocol.SecretKeyProtocolScm}
   * intended to be used by clients under the SCM identity.
   *
   * @param conf - Ozone configuration
   * @return {@link org.apache.hadoop.hdds.protocol.SecretKeyProtocolScm}
   * @throws IOException
   */
  public static SecretKeyProtocolScm getSecretKeyClientForSCM(
      ConfigurationSource conf) throws IOException {
    SecretKeyProtocolClientSideTranslatorPB scmSecretClient =
        new SecretKeyProtocolClientSideTranslatorPB(
            new SecretKeyProtocolFailoverProxyProvider(conf,
                UserGroupInformation.getCurrentUser(),
                SecretKeyProtocolScmPB.class), SecretKeyProtocolScmPB.class);

    return TracingUtil.createProxy(scmSecretClient,
        SecretKeyProtocolScm.class, conf);
  }

  /**
   * Create a {@link org.apache.hadoop.hdds.protocol.SecretKeyProtocol} for
   * datanode service, should be use only if user is the Datanode identity.
   */
  public static SecretKeyProtocolClientSideTranslatorPB
      getSecretKeyClientForDatanode(ConfigurationSource conf)
      throws IOException {
    return new SecretKeyProtocolClientSideTranslatorPB(
        new SecretKeyProtocolFailoverProxyProvider(conf,
            UserGroupInformation.getCurrentUser(),
            SecretKeyProtocolDatanodePB.class),
        SecretKeyProtocolDatanodePB.class);
  }

  /**
   * Create a {@link org.apache.hadoop.hdds.protocol.SecretKeyProtocol} for
   * OM service, should be use only if user is the OM identity.
   */
  public static SecretKeyProtocolClientSideTranslatorPB
      getSecretKeyClientForOm(ConfigurationSource conf) throws IOException {
    return new SecretKeyProtocolClientSideTranslatorPB(
        new SecretKeyProtocolFailoverProxyProvider(conf,
            UserGroupInformation.getCurrentUser(),
            SecretKeyProtocolOmPB.class),
        SecretKeyProtocolOmPB.class);
  }

  public static SecretKeyProtocolClientSideTranslatorPB
      getSecretKeyClientForDatanode(ConfigurationSource conf,
      UserGroupInformation ugi) {
    return new SecretKeyProtocolClientSideTranslatorPB(
        new SecretKeyProtocolFailoverProxyProvider(conf, ugi,
            SecretKeyProtocolDatanodePB.class),
        SecretKeyProtocolDatanodePB.class);
  }

  /**
   * Create a {@link org.apache.hadoop.hdds.protocol.SecretKeyProtocol} for
   * SCM service, should be use only if user is the Datanode identity.
   *
   * The protocol returned by this method only target a single destination
   * SCM node.
   */
  public static SecretKeyProtocolClientSideTranslatorPB
      getSecretKeyClientForScm(ConfigurationSource conf,
      String scmNodeId, UserGroupInformation ugi) {
    return new SecretKeyProtocolClientSideTranslatorPB(
        new SingleSecretKeyProtocolProxyProvider(conf, ugi,
            SecretKeyProtocolScmPB.class, scmNodeId),
        SecretKeyProtocolScmPB.class);
  }

  /**
   * Initialize hadoop metrics system for Ozone servers.
   * @param configuration OzoneConfiguration to use.
   * @param serverName    The logical name of the server components.
   */
  public static MetricsSystem initializeMetrics(
      OzoneConfiguration configuration, String serverName) {
    MetricsSystem metricsSystem = DefaultMetricsSystem.initialize(serverName);
    try {
      JvmMetrics.create(serverName,
          configuration.get(HddsConfigKeys.HDDS_METRICS_SESSION_ID_KEY),
          DefaultMetricsSystem.instance());
      CpuMetrics.create();
    } catch (MetricsException e) {
      LOG.info("Metrics source JvmMetrics already added to DataNode.");
    }
    return metricsSystem;
  }

  /**
   * Write DB Checkpoint to an output stream as a compressed file (tar).
   *
   * @param checkpoint    checkpoint file
   * @param destination   destination output stream.
   * @param toExcludeList the files to be excluded
   * @throws IOException
   */
  public static void writeDBCheckpointToStream(
      DBCheckpoint checkpoint,
      OutputStream destination,
      Set<String> toExcludeList)
      throws IOException {
    try (ArchiveOutputStream<TarArchiveEntry> archiveOutputStream = tar(destination);
        Stream<Path> files =
            Files.list(checkpoint.getCheckpointLocation())) {
      for (Path path : files.collect(Collectors.toList())) {
        if (path != null) {
          Path fileNamePath = path.getFileName();
          if (fileNamePath != null) {
            String fileName = fileNamePath.toString();
            if (!toExcludeList.contains(fileName)) {
              includeFile(path.toFile(), fileName, archiveOutputStream);
            }
          }
        }
      }
      includeRatisSnapshotCompleteFlag(archiveOutputStream);
    }
  }

  // Mark tarball completed.
  public static void includeRatisSnapshotCompleteFlag(
      ArchiveOutputStream<TarArchiveEntry> archiveOutput) throws IOException {
    File file = File.createTempFile(
        OZONE_RATIS_SNAPSHOT_COMPLETE_FLAG_NAME, "");
    includeFile(file, OZONE_RATIS_SNAPSHOT_COMPLETE_FLAG_NAME, archiveOutput);
  }

  static boolean ratisSnapshotComplete(Path dir) {
    return new File(dir.toString(),
        OZONE_RATIS_SNAPSHOT_COMPLETE_FLAG_NAME).exists();
  }

  // optimize ugi lookup for RPC operations to avoid a trip through
  // UGI.getCurrentUser which is synch'ed
  public static UserGroupInformation getRemoteUser() throws IOException {
    UserGroupInformation ugi = Server.getRemoteUser();
    return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
  }

  /**
   * Add exception classes which server won't log at all.
   * @param server
   */
  public static void addSuppressedLoggingExceptions(RPC.Server server) {
    server.addSuppressedLoggingExceptions(ServerNotLeaderException.class);
  }

  public static void startupShutdownMessage(VersionInfo versionInfo,
      Class<?> clazz, String[] args, Logger log, OzoneConfiguration conf) {
    final String hostname = NetUtils.getHostname();
    final String className = clazz.getSimpleName();

    if (log.isInfoEnabled()) {
      log.info(createStartupMessage(versionInfo, className, hostname,
          args, HddsUtils.processForLogging(conf)));
    }

    if (SystemUtils.IS_OS_UNIX) {
      try {
        SignalLogger.INSTANCE.register(log);
      } catch (Throwable t) {
        log.warn("failed to register any UNIX signal loggers: ", t);
      }
    }
    ShutdownHookManager.get().addShutdownHook(
        () -> log.info(toStartupShutdownString("SHUTDOWN_MSG: ",
            "Shutting down " + className + " at " + hostname)),
        SHUTDOWN_HOOK_PRIORITY);
  }

  /**
   * Return a message for logging.
   * @param prefix prefix keyword for the message
   * @param msg content of the message
   * @return a message for logging
   */
  public static String toStartupShutdownString(String prefix, String... msg) {
    StringBuilder b = new StringBuilder(prefix);
    b.append("\n/************************************************************");
    for (String s : msg) {
      b.append('\n').append(prefix).append(s);
    }
    b.append("\n************************************************************/");
    return b.toString();
  }

  /**
   * Generate the text for the startup/shutdown message of processes.
   * @param className short name of the class
   * @param hostname hostname
   * @param args Command arguments
   * @return a string to log.
   */
  private static String createStartupMessage(VersionInfo versionInfo,
      String className, String hostname, String[] args,
      Map<String, String> conf) {
    return toStartupShutdownString("STARTUP_MSG: ",
        "Starting " + className,
        "       host = " + hostname,
        "    version = " + versionInfo.getVersion(),
        "      build = " + versionInfo.getUrl() + "/" + versionInfo.getRevision(),
        "       java = " + System.getProperty("java.version"),
        "       args = " + (args != null ? Arrays.asList(args) : new ArrayList<>()),
        "  classpath = " + System.getProperty("java.class.path"),
        "       conf = " + conf);
  }

  public static void setPoolSize(ThreadPoolExecutor executor, int size, Logger logger) {
    Preconditions.assertTrue(size > 0, () -> "Pool size must be positive: " + size);

    int currentCorePoolSize = executor.getCorePoolSize();

    // In ThreadPoolExecutor, maximumPoolSize must always be greater than or
    // equal to the corePoolSize. We must make sure this invariant holds when
    // changing the pool size. Therefore, we take into account whether the
    // new size is greater or smaller than the current core pool size.
    String change = "unchanged";
    if (size > currentCorePoolSize) {
      change = "increased";
      executor.setMaximumPoolSize(size);
      executor.setCorePoolSize(size);
    } else if (size < currentCorePoolSize) {
      change = "decreased";
      executor.setCorePoolSize(size);
      executor.setMaximumPoolSize(size);
    }
    if (logger != null) {
      logger.info("pool size {} from {} to {}", change, currentCorePoolSize, size);
    }
  }

  /**
   * Retrieve the socket addresses of all storage container managers.
   *
   * @return A collection of SCM addresses
   * @throws IllegalArgumentException If the configuration is invalid
   */
  public static Collection<InetSocketAddress> getSCMAddressForDatanodes(
      ConfigurationSource conf) {

    // First check HA style config, if not defined fall back to OZONE_SCM_NAMES

    if (getScmServiceId(conf) != null) {
      List<SCMNodeInfo> scmNodeInfoList = SCMNodeInfo.buildNodeInfo(conf);
      Collection<InetSocketAddress> scmAddressList =
          new HashSet<>(scmNodeInfoList.size());
      for (SCMNodeInfo scmNodeInfo : scmNodeInfoList) {
        scmAddressList.add(
            NetUtils.createSocketAddr(scmNodeInfo.getScmDatanodeAddress()));
      }
      return scmAddressList;
    } else {
      // fall back to OZONE_SCM_NAMES.
      Collection<String> names =
          conf.getTrimmedStringCollection(ScmConfigKeys.OZONE_SCM_NAMES);
      if (names.isEmpty()) {
        throw new IllegalArgumentException(ScmConfigKeys.OZONE_SCM_NAMES
            + " need to be a set of valid DNS names or IP addresses."
            + " Empty address list found.");
      }

      Collection<InetSocketAddress> addresses = new HashSet<>(names.size());
      for (String address : names) {
        Optional<String> hostname = getHostName(address);
        if (!hostname.isPresent()) {
          throw new IllegalArgumentException("Invalid hostname for SCM: "
              + address);
        }
        int port = getHostPort(address)
            .orElse(conf.getInt(OZONE_SCM_DATANODE_PORT_KEY,
                OZONE_SCM_DATANODE_PORT_DEFAULT));
        InetSocketAddress addr = NetUtils.createSocketAddr(hostname.get(),
            port);
        addresses.add(addr);
      }

      if (addresses.size() > 1) {
        LOG.warn("When SCM HA is configured, configure {} appended with " +
                "serviceId and nodeId. {} is deprecated.", OZONE_SCM_ADDRESS_KEY,
            OZONE_SCM_NAMES);
      }
      return addresses;
    }
  }

  /**
   * Returns the SCM address for datanodes based on the service ID and the SCM addresses.
   * @param conf Configuration
   * @param scmServiceId SCM service ID
   * @param scmNodeIds Requested SCM node IDs
   * @return A collection with addresses of the request SCM node IDs.
   * Null if there is any wrongly configured SCM address. Note that the returned collection
   * might not be ordered the same way as the requested SCM node IDs
   */
  public static Collection<Pair<String, InetSocketAddress>> getSCMAddressForDatanodes(
      ConfigurationSource conf, String scmServiceId, Set<String> scmNodeIds) {
    Collection<Pair<String, InetSocketAddress>> scmNodeAddress = new HashSet<>(scmNodeIds.size());
    for (String scmNodeId : scmNodeIds) {
      String addressKey = ConfUtils.addKeySuffixes(
          OZONE_SCM_ADDRESS_KEY, scmServiceId, scmNodeId);
      String scmAddress = conf.get(addressKey);
      if (scmAddress == null) {
        LOG.warn("The SCM address configuration {} is not defined, return nothing", addressKey);
        return null;
      }

      int scmDatanodePort = SCMNodeInfo.getPort(conf, scmServiceId, scmNodeId,
          OZONE_SCM_DATANODE_ADDRESS_KEY, OZONE_SCM_DATANODE_PORT_KEY,
          OZONE_SCM_DATANODE_PORT_DEFAULT);

      String scmDatanodeAddressStr = SCMNodeInfo.buildAddress(scmAddress, scmDatanodePort);
      InetSocketAddress scmDatanodeAddress = NetUtils.createSocketAddr(scmDatanodeAddressStr);
      scmNodeAddress.add(Pair.of(scmNodeId, scmDatanodeAddress));
    }
    return scmNodeAddress;
  }

}
