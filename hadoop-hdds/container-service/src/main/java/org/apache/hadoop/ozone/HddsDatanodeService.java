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

package org.apache.hadoop.ozone;

import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.HTTP;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.HTTPS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NODES_KEY;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getRemoteUser;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmSecurityClientWithMaxRetry;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_DATANODE_PLUGINS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_WORKERS;
import static org.apache.hadoop.ozone.common.Storage.StorageState.INITIALIZED;
import static org.apache.hadoop.ozone.conf.OzoneServiceConfig.DEFAULT_SHUTDOWN_HOOK_PRIORITY;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.HDDS_DATANODE_BLOCK_DELETE_THREAD_MAX;
import static org.apache.hadoop.ozone.container.replication.ReplicationServer.ReplicationConfig.REPLICATION_STREAMS_LIMIT_KEY;
import static org.apache.hadoop.security.UserGroupInformation.getCurrentUser;
import static org.apache.hadoop.util.ExitUtil.terminate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.management.ObjectName;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hdds.DatanodeVersion;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.DiskBalancerProtocol;
import org.apache.hadoop.hdds.protocol.SecretKeyProtocol;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.symmetric.DefaultSecretKeyClient;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.DNCertificateClient;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.hdds.server.http.RatisDropwizardExports;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.HddsVersionInfo;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine.DatanodeStates;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.DeleteBlocksCommandHandler;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerProtocolServer;
import org.apache.hadoop.ozone.util.OzoneNetUtils;
import org.apache.hadoop.ozone.util.ShutdownHookManager;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;

/**
 * Datanode service plugin to start the HDDS container services.
 */

@Command(name = "ozone datanode",
    hidden = true, description = "Start the datanode for ozone",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class HddsDatanodeService extends GenericCli implements Callable<Void>, ServicePlugin {

  private static final Logger LOG = LoggerFactory.getLogger(
      HddsDatanodeService.class);

  public static final String TESTING_DATANODE_VERSION_INITIAL = "testing.hdds.datanode.version.initial";
  public static final String TESTING_DATANODE_VERSION_CURRENT = "testing.hdds.datanode.version.current";

  private OzoneConfiguration conf;
  private SecurityConfig secConf;
  private DatanodeDetails datanodeDetails;
  private DatanodeStateMachine datanodeStateMachine;
  private List<ServicePlugin> plugins;
  private CertificateClient dnCertClient;
  private SecretKeyClient secretKeyClient;
  private String component;
  private HddsDatanodeHttpServer httpServer;
  private boolean printBanner;
  private String[] args;
  private final AtomicBoolean isStopped = new AtomicBoolean(false);
  private final Map<String, RatisDropwizardExports> ratisMetricsMap =
      new ConcurrentHashMap<>();
  private List<RatisDropwizardExports.MetricReporter> ratisReporterList = null;
  private DNMXBeanImpl serviceRuntimeInfo;
  private ObjectName dnInfoBeanName;
  private HddsDatanodeClientProtocolServer clientProtocolServer;
  private OzoneAdmins admins;
  private ReconfigurationHandler reconfigurationHandler;
  private String scmServiceId;

  //Constructor for DataNode PluginService
  public HddsDatanodeService() { }

  /**
   * Create a Datanode instance based on the supplied command-line arguments.
   * <p>
   * This method is intended for unit tests only. It suppresses the
   * startup/shutdown message and skips registering Unix signal handlers.
   *
   * @param args      command line arguments.
   */
  @VisibleForTesting
  public HddsDatanodeService(String[] args) {
    this(false, args);
  }

  /**
   * Create a Datanode instance based on the supplied command-line arguments.
   *
   * @param args        command line arguments.
   * @param printBanner if true, then log a verbose startup message.
   */
  private HddsDatanodeService(boolean printBanner, String[] args) {
    this.printBanner = printBanner;
    this.args = args != null ? Arrays.copyOf(args, args.length) : null;
  }

  public static void main(String[] args) {
    try {
      OzoneNetUtils.disableJvmNetworkAddressCacheIfRequired(
              new OzoneConfiguration());
      HddsDatanodeService hddsDatanodeService =
          new HddsDatanodeService(true, args);
      hddsDatanodeService.run(args);
    } catch (Throwable e) {
      LOG.error("Exception in HddsDatanodeService.", e);
      terminate(1, e);
    }
  }

  @Override
  public Void call() throws Exception {
    OzoneConfiguration configuration = getOzoneConf();
    if (printBanner) {
      HddsServerUtil.startupShutdownMessage(HddsVersionInfo.HDDS_VERSION_INFO,
          HddsDatanodeService.class, args, LOG, configuration);
    }
    start(configuration);
    ShutdownHookManager.get().addShutdownHook(() -> {
      try {
        stop();
        join();
      } catch (Exception e) {
        LOG.error("Error during stop Ozone Datanode.", e);
      }
    }, DEFAULT_SHUTDOWN_HOOK_PRIORITY);
    return null;
  }

  public void setConfiguration(OzoneConfiguration configuration) {
    this.conf = configuration;
  }

  /**
   * Starts HddsDatanode services.
   *
   * @param service The service instance invoking this method
   */
  @Override
  public void start(Object service) {
    if (service instanceof Configurable) {
      start(new OzoneConfiguration(((Configurable) service).getConf()));
    } else {
      start(new OzoneConfiguration());
    }
  }

  public void start(OzoneConfiguration configuration) {
    setConfiguration(configuration);
    start();
  }

  @SuppressWarnings("methodlength")
  public void start() {
    serviceRuntimeInfo = new DNMXBeanImpl(HddsVersionInfo.HDDS_VERSION_INFO) {
      @Override
      public String getNamespace() {
        return HddsUtils.getScmServiceId(conf);
      }
    };
    serviceRuntimeInfo.setStartTime();

    ratisReporterList = RatisDropwizardExports
        .registerRatisMetricReporters(ratisMetricsMap, () -> isStopped.get());

    OzoneConfiguration.activate();
    HddsServerUtil.initializeMetrics(conf, "HddsDatanode");
    try {
      String hostname = HddsUtils.getHostName(conf);
      datanodeDetails = initializeDatanodeDetails();
      datanodeDetails.setHostName(hostname);
      serviceRuntimeInfo.setHostName(hostname);
      datanodeDetails.validateDatanodeIpAddress();
      datanodeDetails.setVersion(
          HddsVersionInfo.HDDS_VERSION_INFO.getVersion());
      datanodeDetails.setSetupTime(Time.now());
      datanodeDetails.setRevision(
          HddsVersionInfo.HDDS_VERSION_INFO.getRevision());
      TracingUtil.initTracing(
          "HddsDatanodeService." + datanodeDetails.getID(), conf);
      LOG.info("HddsDatanodeService {}", datanodeDetails);
      // Authenticate Hdds Datanode service if security is enabled
      if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
        component = "dn-" + datanodeDetails.getID();
        secConf = new SecurityConfig(conf);

        if (SecurityUtil.getAuthenticationMethod(conf).equals(
            UserGroupInformation.AuthenticationMethod.KERBEROS)) {
          LOG.info("Ozone security is enabled. Attempting login for Hdds " +
                  "Datanode user. Principal: {},keytab: {}", conf.get(
                  HddsConfigKeys.HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY),
              conf.get(
                  HddsConfigKeys.HDDS_DATANODE_KERBEROS_KEYTAB_FILE_KEY));

          UserGroupInformation.setConfiguration(conf);

          SecurityUtil
              .login(conf,
                  HddsConfigKeys.HDDS_DATANODE_KERBEROS_KEYTAB_FILE_KEY,
                  HddsConfigKeys.HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY,
                  hostname);
        } else {
          throw new AuthenticationException(SecurityUtil.
              getAuthenticationMethod(conf) + " authentication method not " +
              "supported. Datanode user" + " login " + "failed.");
        }
        LOG.info("Hdds Datanode login successful.");
      }

      DatanodeLayoutStorage layoutStorage = new DatanodeLayoutStorage(conf,
          datanodeDetails.getUuidString());
      if (layoutStorage.getState() != INITIALIZED) {
        layoutStorage.initialize();
      }

      if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
        dnCertClient = initializeCertificateClient(dnCertClient);

        if (secConf.isTokenEnabled()) {
          SecretKeyProtocol secretKeyProtocol =
              HddsServerUtil.getSecretKeyClientForDatanode(conf);
          secretKeyClient = DefaultSecretKeyClient.create(
              conf, secretKeyProtocol, "");
          secretKeyClient.start(conf);
        }
      }

      reconfigurationHandler =
          new ReconfigurationHandler("DN", conf, this::checkAdminPrivilege)
              .register(HDDS_DATANODE_BLOCK_DELETE_THREAD_MAX,
                  this::reconfigBlockDeleteThreadMax)
              .register(OZONE_BLOCK_DELETING_SERVICE_WORKERS,
                  this::reconfigDeletingServiceWorkers)
              .register(OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
                  this::reconfigBlockDeletingServiceInterval)
              .register(OZONE_BLOCK_DELETING_SERVICE_TIMEOUT,
                  this::reconfigBlockDeletingServiceTimeout)
              .register(REPLICATION_STREAMS_LIMIT_KEY,
                  this::reconfigReplicationStreamsLimit);

      scmServiceId = HddsUtils.getScmServiceId(conf);
      if (scmServiceId != null) {
        reconfigurationHandler.register(OZONE_SCM_NODES_KEY + "." + scmServiceId,
            this::reconfigScmNodes);
      }

      reconfigurationHandler.setReconfigurationCompleteCallback(reconfigurationHandler.defaultLoggingCallback());

      datanodeStateMachine = new DatanodeStateMachine(this, datanodeDetails, conf,
          dnCertClient, secretKeyClient, this::terminateDatanode,
          reconfigurationHandler);
      try {
        httpServer = new HddsDatanodeHttpServer(conf);
        httpServer.start();
        HttpConfig.Policy policy = HttpConfig.getHttpPolicy(conf);

        if (policy.isHttpEnabled()) {
          int httpPort = httpServer.getHttpAddress().getPort();
          datanodeDetails.setPort(DatanodeDetails.newPort(HTTP, httpPort));
          serviceRuntimeInfo.setHttpPort(String.valueOf(httpPort));
        }

        if (policy.isHttpsEnabled()) {
          int httpsPort = httpServer.getHttpsAddress().getPort();
          datanodeDetails.setPort(DatanodeDetails.newPort(HTTPS, httpsPort));
          serviceRuntimeInfo.setHttpsPort(String.valueOf(httpsPort));
        }

      } catch (Exception ex) {
        LOG.error("HttpServer failed to start.", ex);
      }

      DiskBalancerProtocol diskBalancerProtocol =
          new DiskBalancerProtocolServer(datanodeStateMachine,
              this::checkAdminPrivilege);
      clientProtocolServer = new HddsDatanodeClientProtocolServer(
          datanodeDetails, conf, HddsVersionInfo.HDDS_VERSION_INFO,
          reconfigurationHandler, diskBalancerProtocol);

      int clientRpcport = clientProtocolServer.getClientRpcAddress().getPort();
      serviceRuntimeInfo.setClientRpcPort(String.valueOf(clientRpcport));

      // Get admin list
      String starterUser =
          UserGroupInformation.getCurrentUser().getShortUserName();
      admins = OzoneAdmins.getOzoneAdmins(starterUser, conf);
      LOG.info("Datanode start with admins: {}", admins.getAdminUsernames());

      clientProtocolServer.start();
      startPlugins();
      // Starting HDDS Daemons
      datanodeStateMachine.startDaemon();

      //for standalone, follower only test we can start the datanode (==raft
      // rings)
      //manually. In normal case it's handled by the initial SCM handshake.

      if ("follower"
          .equalsIgnoreCase(System.getenv("OZONE_DATANODE_STANDALONE_TEST"))) {
        startRatisForTest();
      }
      registerMXBean();
    } catch (IOException e) {
      throw new RuntimeException("Can't start the HDDS datanode plugin", e);
    } catch (AuthenticationException ex) {
      throw new RuntimeException("Fail to authentication when starting" +
          " HDDS datanode plugin", ex);
    }
  }

  @VisibleForTesting
  SCMSecurityProtocolClientSideTranslatorPB createScmSecurityClient()
      throws IOException {
    return getScmSecurityClientWithMaxRetry(conf, getCurrentUser());
  }

  /**
   * Initialize and start Ratis server.
   * <p>
   * In normal case this initialization is done after the SCM registration.
   * In can be forced to make it possible to test one, single, isolated
   * datanode.
   */
  private void startRatisForTest() throws IOException {
    String clusterId = "clusterId";
    datanodeStateMachine.getContainer().start(clusterId);
    MutableVolumeSet volumeSet =
        getDatanodeStateMachine().getContainer().getVolumeSet();

    Map<String, StorageVolume> volumeMap = volumeSet.getVolumeMap();

    for (Map.Entry<String, StorageVolume> entry : volumeMap.entrySet()) {
      HddsVolume hddsVolume = (HddsVolume) entry.getValue();
      boolean result = StorageVolumeUtil.checkVolume(hddsVolume, clusterId,
          clusterId, conf, LOG, null);
      if (!result) {
        volumeSet.failVolume(hddsVolume.getHddsRootDir().getPath());
      }
    }
  }

  /**
   * Initializes secure Datanode.
   * */
  @VisibleForTesting
  public CertificateClient initializeCertificateClient(
      CertificateClient certClient) throws IOException {
    LOG.info("Initializing secure Datanode.");

    if (certClient == null) {
      dnCertClient = new DNCertificateClient(secConf,
          createScmSecurityClient(),
          datanodeDetails,
          datanodeDetails.getCertSerialId(), this::saveNewCertId,
          this::terminateDatanode);
      certClient = dnCertClient;
    }
    certClient.initWithRecovery();
    return certClient;
  }

  private void registerMXBean() {
    Map<String, String> jmxProperties = new HashMap<>();
    jmxProperties.put("component", "ServerRuntime");
    this.dnInfoBeanName = HddsUtils.registerWithJmxProperties(
        "HddsDatanodeService",
        "HddsDatanodeServiceInfo", jmxProperties, this.serviceRuntimeInfo);
  }

  private void unregisterMXBean() {
    if (this.dnInfoBeanName != null) {
      MBeans.unregister(this.dnInfoBeanName);
      this.dnInfoBeanName = null;
    }
  }

  /**
   * Returns DatanodeDetails or null in case of Error.
   *
   * @return DatanodeDetails
   */
  private DatanodeDetails initializeDatanodeDetails()
      throws IOException {
    String idFilePath = HddsServerUtil.getDatanodeIdFilePath(conf);
    Objects.requireNonNull(idFilePath, "idFilePath == null");
    File idFile = new File(idFilePath);
    DatanodeDetails details;
    if (idFile.exists()) {
      details = ContainerUtils.readDatanodeDetailsFrom(idFile);
    } else {
      // There is no datanode.id file, this might be the first time datanode
      // is started.
      details = DatanodeDetails.newBuilder().setID(DatanodeID.randomID()).build();
      details.setInitialVersion(getInitialVersion());
    }
    // Current version is always overridden to the latest
    details.setCurrentVersion(getCurrentVersion());
    return details;
  }

  /**
   * Persist DatanodeDetails to file system.
   * @param dnDetails
   *
   * @return DatanodeDetails
   */
  private void persistDatanodeDetails(DatanodeDetails dnDetails)
      throws IOException {
    String idFilePath = HddsServerUtil.getDatanodeIdFilePath(conf);
    Objects.requireNonNull(idFilePath,  "idFilePath == null");
    File idFile = new File(idFilePath);
    ContainerUtils.writeDatanodeDetailsTo(dnDetails, idFile, conf);
  }

  /**
   * Starts all the service plugins which are configured using
   * OzoneConfigKeys.HDDS_DATANODE_PLUGINS_KEY.
   */
  private void startPlugins() {
    try {
      plugins = conf.getInstances(HDDS_DATANODE_PLUGINS_KEY,
          ServicePlugin.class);
    } catch (RuntimeException e) {
      String pluginsValue = conf.get(HDDS_DATANODE_PLUGINS_KEY);
      LOG.error("Unable to load HDDS DataNode plugins. " +
              "Specified list of plugins: {}",
          pluginsValue, e);
      throw e;
    }
    for (ServicePlugin plugin : plugins) {
      try {
        plugin.start(this);
        LOG.info("Started plug-in {}", plugin);
      } catch (Throwable t) {
        LOG.warn("ServicePlugin {} could not be started", plugin, t);
      }
    }
  }

  /**
   * Returns the OzoneConfiguration used by this HddsDatanodeService.
   *
   * @return OzoneConfiguration
   */
  public OzoneConfiguration getConf() {
    return conf;
  }

  /**
   * Return DatanodeDetails if set, return null otherwise.
   *
   * @return DatanodeDetails
   */
  @VisibleForTesting
  public DatanodeDetails getDatanodeDetails() {
    return datanodeDetails;
  }

  @VisibleForTesting
  public DatanodeStateMachine getDatanodeStateMachine() {
    return datanodeStateMachine;
  }

  public HddsDatanodeClientProtocolServer getClientProtocolServer() {
    return clientProtocolServer;
  }

  public void join() {
    if (datanodeStateMachine != null) {
      try {
        datanodeStateMachine.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.info("Interrupted during StorageContainerManager join.");
      }
    }
    if (getClientProtocolServer() != null) {
      try {
        getClientProtocolServer().join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.info("Interrupted during HddsDatanodeClientProtocolServer join.");
      }
    }
  }

  public void terminateDatanode() {
    stop();
    terminate(1);
  }

  @Override
  public void stop() {
    if (!isStopped.getAndSet(true)) {
      if (plugins != null) {
        for (ServicePlugin plugin : plugins) {
          try {
            plugin.stop();
            LOG.info("Stopped plug-in {}", plugin);
          } catch (Throwable t) {
            LOG.warn("ServicePlugin {} could not be stopped", plugin, t);
          }
        }
      }
      if (datanodeStateMachine != null) {
        datanodeStateMachine.stopDaemon();
      }
      if (httpServer != null) {
        try {
          httpServer.stop();
        } catch (Exception e) {
          LOG.error("Stopping HttpServer is failed.", e);
        }
      }
      if (getClientProtocolServer() != null) {
        getClientProtocolServer().stop();
      }
      unregisterMXBean();
      RatisDropwizardExports.clear(ratisMetricsMap, ratisReporterList);

      if (secretKeyClient != null) {
        secretKeyClient.stop();
      }
    }
  }

  @Override
  public void close() {
    if (plugins != null) {
      for (ServicePlugin plugin : plugins) {
        try {
          plugin.close();
        } catch (Throwable t) {
          LOG.warn("ServicePlugin {} could not be closed", plugin, t);
        }
      }
    }

    if (dnCertClient != null) {
      try {
        dnCertClient.close();
      } catch (IOException e) {
        LOG.warn("Certificate client could not be closed", e);
      }
    }
  }

  @VisibleForTesting
  public String getComponent() {
    return component;
  }

  public CertificateClient getCertificateClient() {
    return dnCertClient;
  }

  @VisibleForTesting
  public void setCertificateClient(CertificateClient client)
      throws IOException {
    if (dnCertClient != null) {
      dnCertClient.close();
    }
    dnCertClient = client;
  }

  @VisibleForTesting
  public void setSecretKeyClient(SecretKeyClient client) {
    this.secretKeyClient = client;
  }

  @Override
  public void printError(Throwable error) {
    LOG.error("Exception in HddsDatanodeService.", error);
  }

  public void saveNewCertId(String newCertId) {
    // save new certificate Id to VERSION file
    datanodeDetails.setCertSerialId(newCertId);
    try {
      persistDatanodeDetails(datanodeDetails);
    } catch (IOException ex) {
      // New cert ID cannot be persisted into VERSION file.
      String msg = "Failed to persist new cert ID " + newCertId +
          "to VERSION file. Terminating datanode...";
      LOG.error(msg, ex);
      terminateDatanode();
    }
  }

  public boolean isStopped() {
    return isStopped.get();
  }

  /**
   * Check ozone admin privilege, throws exception if not admin.
   */
  private void checkAdminPrivilege(String operation)
      throws IOException {
    final UserGroupInformation ugi = getRemoteUser();
    admins.checkAdminUserPrivilege(ugi);
  }

  @VisibleForTesting
  public ReconfigurationHandler getReconfigurationHandler() {
    return reconfigurationHandler;
  }

  private String reconfigBlockDeleteThreadMax(String value) {
    DeleteBlocksCommandHandler handler =
        (DeleteBlocksCommandHandler) getDatanodeStateMachine()
            .getCommandDispatcher().getDeleteBlocksCommandHandler();
    handler.setPoolSize(Integer.parseInt(value));
    return value;
  }

  private String reconfigDeletingServiceWorkers(String value) {
    Preconditions.checkArgument(Integer.parseInt(value) >= 0,
        OZONE_BLOCK_DELETING_SERVICE_WORKERS + " cannot be negative.");
    return value;
  }

  private String reconfigReplicationStreamsLimit(String value) {
    getDatanodeStateMachine().getContainer().getReplicationServer()
        .setPoolSize(Integer.parseInt(value));
    return value;
  }

  private String reconfigBlockDeletingServiceInterval(String value) {
    return value;
  }

  private String reconfigBlockDeletingServiceTimeout(String value) {
    return value;
  }

  /**
   * Reconfigure the SCM nodes configuration which will trigger the creation and removal of
   * SCM connections based on the difference between the old and the new SCM nodes configuration.
   * <p>
   * The assumption is that the SCM node address configurations exists for all the involved node IDs
   * This is because reconfiguration can only support one configuration field at a time
   * @param value The new configuration value for "ozone.scm.nodes.SERVICEID"
   * @return new configuration for "ozone.scm.nodes.SERVICEID" which reflects the SCMs that the datanode has
   *         is not connected to.
   */
  private String reconfigScmNodes(String value) {
    if (StringUtils.isBlank(value)) {
      throw new IllegalArgumentException("Reconfiguration failed since setting the empty SCM nodes " +
          "configuration is not allowed");
    }
    Set<String> previousNodeIds = new HashSet<>(HddsUtils.getSCMNodeIds(getConf(), scmServiceId));
    Set<String> newScmNodeIds = Stream.of(ConfigurationSource.getTrimmedStringsFromValue(value))
        .collect(Collectors.toSet());

    if (newScmNodeIds.isEmpty()) {
      throw new IllegalArgumentException("Reconfiguration failed since setting the empty SCM nodes " +
          "configuration is not allowed");
    }

    Set<String> scmNodesIdsToAdd = Sets.difference(newScmNodeIds, previousNodeIds);
    Set<String> scmNodesIdsToRemove = Sets.difference(previousNodeIds, newScmNodeIds);

    // We should only update configuration with the SCMs that are actually added / removed
    // If there is partial reconfiguration (e.g. one successful add and one failed add),
    // we want to be able to retry on the failed node reconfiguration.
    // If we don't handle this, the subsequent reconfiguration will not work since the node
    // configuration is already exists / removed.
    Set<String> effectiveScmNodeIds = new HashSet<>(previousNodeIds);

    LOG.info("Reconfiguring SCM nodes for service ID {} with new SCM nodes {} and remove SCM nodes {}",
        scmServiceId, scmNodesIdsToAdd, scmNodesIdsToRemove);

    Collection<Pair<String, InetSocketAddress>> scmToAdd = HddsUtils.getSCMAddressForDatanodes(
        getConf(), scmServiceId, scmNodesIdsToAdd);
    if (scmToAdd == null) {
      throw new IllegalStateException("Reconfiguration failed to get SCM address to add due to wrong configuration");
    }
    Collection<Pair<String, InetSocketAddress>> scmToRemove = HddsUtils.getSCMAddressForDatanodes(
        getConf(), scmServiceId, scmNodesIdsToRemove);
    if (scmToRemove == null) {
      throw new IllegalArgumentException(
          "Reconfiguration failed to get SCM address to remove due to wrong configuration");
    }

    StateContext context = datanodeStateMachine.getContext();
    SCMConnectionManager connectionManager = datanodeStateMachine.getConnectionManager();

    // Assert that the datanode is in RUNNING state since
    // 1. If the datanode state is INIT, there might be concurrent connection manager operations
    //    that might cause unpredictable behaviors
    // 2. If the datanode state is SHUTDOWN, it means that datanode is shutting down and there is no need
    //    to reconfigure the connections.
    if (!DatanodeStates.RUNNING.equals(context.getState())) {
      throw new IllegalStateException("Reconfiguration failed since the datanode the current state" +
          context.getState().toString() + " is not in RUNNING state");
    }

    // Add the new SCM servers
    for (Pair<String, InetSocketAddress> pair : scmToAdd) {
      String scmNodeId = pair.getLeft();
      InetSocketAddress scmAddress = pair.getRight();
      if (scmAddress.isUnresolved()) {
        LOG.warn("Reconfiguration failed to add SCM address {} for SCM service {} since it can't " +
            "be resolved, skipping", scmAddress, scmServiceId);
        continue;
      }
      try {
        connectionManager.addSCMServer(scmAddress, context.getThreadNamePrefix());
        context.addEndpoint(scmAddress);
        effectiveScmNodeIds.add(scmNodeId);
        LOG.info("Reconfiguration successfully add SCM address {} for SCM service {}", scmAddress, scmServiceId);
      } catch (IOException e) {
        LOG.error("Reconfiguration failed to add SCM address {} for SCM service {}", scmAddress, scmServiceId, e);
      }
    }

    // Remove the old SCM server
    for (Pair<String, InetSocketAddress> pair : scmToRemove) {
      String scmNodeId = pair.getLeft();
      InetSocketAddress scmAddress = pair.getRight();
      try {
        connectionManager.removeSCMServer(scmAddress);
        context.removeEndpoint(scmAddress);
        effectiveScmNodeIds.remove(scmNodeId);
        LOG.info("Reconfiguration successfully remove SCM address {} for SCM service {}",
            scmAddress, scmServiceId);
      } catch (IOException e) {
        LOG.error("Reconfiguration failed to remove SCM address {} for SCM service {}", scmAddress, scmServiceId, e);
      }
    }

    // Resize the executor pool size to (number of SCMs + 1 Recon)
    // Refer to DatanodeStateMachine#getEndPointTaskThreadPoolSize
    datanodeStateMachine.resizeExecutor(connectionManager.getNumOfConnections());

    // TODO: In the future, we might also do some assertions on the SCM
    //  - The SCM cannot be a leader since this causes the datanode to disappear
    //  - The SCM should be decommissioned
    return String.join(",", effectiveScmNodeIds);
  }

  /**
   * Returns the initial version of the datanode.
   */
  private int getInitialVersion() {
    return conf.getInt(TESTING_DATANODE_VERSION_INITIAL, DatanodeVersion.CURRENT_VERSION);
  }

  /**
   * Returns the current version of the datanode.
   */
  private int getCurrentVersion() {
    return conf.getInt(TESTING_DATANODE_VERSION_CURRENT, DatanodeVersion.CURRENT_VERSION);
  }
}
