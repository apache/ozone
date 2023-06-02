/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.DatanodeVersion;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.datanode.metadata.DatanodeCRLStore;
import org.apache.hadoop.hdds.datanode.metadata.DatanodeCRLStoreImpl;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.DNCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.hdds.server.http.RatisDropwizardExports;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.HddsVersionInfo;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.util.OzoneNetUtils;
import org.apache.hadoop.ozone.util.ShutdownHookManager;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.HTTP;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.HTTPS;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getRemoteUser;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_DATANODE_PLUGINS_KEY;
import static org.apache.hadoop.ozone.conf.OzoneServiceConfig.DEFAULT_SHUTDOWN_HOOK_PRIORITY;
import static org.apache.hadoop.ozone.common.Storage.StorageState.INITIALIZED;
import static org.apache.hadoop.util.ExitUtil.terminate;

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
public class HddsDatanodeService extends GenericCli implements ServicePlugin {

  private static final Logger LOG = LoggerFactory.getLogger(
      HddsDatanodeService.class);

  private OzoneConfiguration conf;
  private SecurityConfig secConf;
  private DatanodeDetails datanodeDetails;
  private DatanodeStateMachine datanodeStateMachine;
  private List<ServicePlugin> plugins;
  private CertificateClient dnCertClient;
  private String component;
  private HddsDatanodeHttpServer httpServer;
  private boolean printBanner;
  private String[] args;
  private final AtomicBoolean isStopped = new AtomicBoolean(false);
  private final Map<String, RatisDropwizardExports> ratisMetricsMap =
      new ConcurrentHashMap<>();
  private List<RatisDropwizardExports.MetricReporter> ratisReporterList = null;
  private DNMXBeanImpl serviceRuntimeInfo =
      new DNMXBeanImpl(HddsVersionInfo.HDDS_VERSION_INFO) { };
  private ObjectName dnInfoBeanName;
  private DatanodeCRLStore dnCRLStore;
  private HddsDatanodeClientProtocolServer clientProtocolServer;
  private OzoneAdmins admins;
  private ReconfigurationHandler reconfigurationHandler;

  //Constructor for DataNode PluginService
  public HddsDatanodeService() { }

  public HddsDatanodeService(boolean printBanner, String[] args) {
    this.printBanner = printBanner;
    this.args = args != null ? Arrays.copyOf(args, args.length) : null;
  }

  private void cleanTmpDir() {
    if (datanodeStateMachine != null) {
      MutableVolumeSet volumeSet =
          datanodeStateMachine.getContainer().getVolumeSet();
      for (StorageVolume volume : volumeSet.getVolumesList()) {
        if (volume instanceof HddsVolume) {
          HddsVolume hddsVolume = (HddsVolume) volume;
          try {
            KeyValueContainerUtil.ContainerDeleteDirectory
                .cleanTmpDir(hddsVolume);
          } catch (IOException ex) {
            LOG.error("Error while cleaning tmp delete directory " +
                "under {}", hddsVolume.getWorkingDir(), ex);
          }
        }
      }
    }
  }

  /**
   * Create a Datanode instance based on the supplied command-line arguments.
   * <p>
   * This method is intended for unit tests only. It suppresses the
   * startup/shutdown message and skips registering Unix signal handlers.
   *
   * @param args      command line arguments.
   * @return Datanode instance
   */
  @VisibleForTesting
  public static HddsDatanodeService createHddsDatanodeService(
      String[] args) {
    return createHddsDatanodeService(args, false);
  }

  /**
   * Create a Datanode instance based on the supplied command-line arguments.
   *
   * @param args        command line arguments.
   * @param printBanner if true, then log a verbose startup message.
   * @return Datanode instance
   */
  private static HddsDatanodeService createHddsDatanodeService(
      String[] args, boolean printBanner) {
    return new HddsDatanodeService(printBanner, args);
  }

  public static void main(String[] args) {
    try {
      OzoneNetUtils.disableJvmNetworkAddressCacheIfRequired(
              new OzoneConfiguration());
      HddsDatanodeService hddsDatanodeService =
          createHddsDatanodeService(args, true);
      hddsDatanodeService.run(args);
    } catch (Throwable e) {
      LOG.error("Exception in HddsDatanodeService.", e);
      terminate(1, e);
    }
  }

  public static Logger getLogger() {
    return LOG;
  }

  @Override
  public Void call() throws Exception {
    OzoneConfiguration configuration = createOzoneConfiguration();
    if (printBanner) {
      StringUtils.startupShutdownMessage(HddsVersionInfo.HDDS_VERSION_INFO,
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

  public void start() {
    serviceRuntimeInfo.setStartTime();

    ratisReporterList = RatisDropwizardExports
        .registerRatisMetricReporters(ratisMetricsMap, () -> isStopped.get());

    OzoneConfiguration.activate();
    HddsServerUtil.initializeMetrics(conf, "HddsDatanode");
    try {
      String hostname = HddsUtils.getHostName(conf);
      String ip = InetAddress.getByName(hostname).getHostAddress();
      datanodeDetails = initializeDatanodeDetails();
      datanodeDetails.setHostName(hostname);
      datanodeDetails.setIpAddress(ip);
      datanodeDetails.setVersion(
          HddsVersionInfo.HDDS_VERSION_INFO.getVersion());
      datanodeDetails.setSetupTime(Time.now());
      datanodeDetails.setRevision(
          HddsVersionInfo.HDDS_VERSION_INFO.getRevision());
      datanodeDetails.setBuildDate(HddsVersionInfo.HDDS_VERSION_INFO.getDate());
      datanodeDetails.setCurrentVersion(DatanodeVersion.CURRENT_VERSION);
      TracingUtil.initTracing(
          "HddsDatanodeService." + datanodeDetails.getUuidString()
              .substring(0, 8), conf);
      LOG.info("HddsDatanodeService host:{} ip:{}", hostname, ip);
      // Authenticate Hdds Datanode service if security is enabled
      if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
        component = "dn-" + datanodeDetails.getUuidString();

        secConf = new SecurityConfig(conf);
        dnCertClient = new DNCertificateClient(secConf, datanodeDetails,
            datanodeDetails.getCertSerialId(), this::saveNewCertId,
            this::terminateDatanode);

        if (SecurityUtil.getAuthenticationMethod(conf).equals(
            UserGroupInformation.AuthenticationMethod.KERBEROS)) {
          LOG.info("Ozone security is enabled. Attempting login for Hdds " +
                  "Datanode user. Principal: {},keytab: {}", conf.get(
              DFSConfigKeysLegacy.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY),
              conf.get(
                  DFSConfigKeysLegacy.DFS_DATANODE_KERBEROS_KEYTAB_FILE_KEY));

          UserGroupInformation.setConfiguration(conf);

          SecurityUtil
              .login(conf,
                  DFSConfigKeysLegacy.DFS_DATANODE_KERBEROS_KEYTAB_FILE_KEY,
                  DFSConfigKeysLegacy.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY,
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

      // initialize datanode CRL store
      dnCRLStore = new DatanodeCRLStoreImpl(conf);

      if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
        dnCertClient = initializeCertificateClient(dnCertClient);
      }
      datanodeStateMachine = new DatanodeStateMachine(datanodeDetails, conf,
          dnCertClient, this::terminateDatanode, dnCRLStore);
      try {
        httpServer = new HddsDatanodeHttpServer(conf);
        httpServer.start();
        HttpConfig.Policy policy = HttpConfig.getHttpPolicy(conf);
        if (policy.isHttpEnabled()) {
          datanodeDetails.setPort(DatanodeDetails.newPort(HTTP,
                  httpServer.getHttpAddress().getPort()));
        }
        if (policy.isHttpsEnabled()) {
          datanodeDetails.setPort(DatanodeDetails.newPort(HTTPS,
                  httpServer.getHttpsAddress().getPort()));
        }
      } catch (Exception ex) {
        LOG.error("HttpServer failed to start.", ex);
      }

      reconfigurationHandler =
          new ReconfigurationHandler("DN", conf, this::checkAdminPrivilege);

      clientProtocolServer = new HddsDatanodeClientProtocolServer(
          datanodeDetails, conf, HddsVersionInfo.HDDS_VERSION_INFO,
          reconfigurationHandler);

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

    CertificateClient.InitResponse response = certClient.init();
    if (response.equals(CertificateClient.InitResponse.REINIT)) {
      certClient.close();
      LOG.info("Re-initialize certificate client.");
      certClient = new DNCertificateClient(secConf, datanodeDetails, null,
          this::saveNewCertId, this::terminateDatanode);
      response = certClient.init();
    }
    LOG.info("Init response: {}", response);
    switch (response) {
    case SUCCESS:
      LOG.info("Initialization successful, case:{}.", response);
      break;
    case GETCERT:
      CertificateSignRequest.Builder csrBuilder = certClient.getCSRBuilder();
      String dnCertSerialId =
          certClient.signAndStoreCertificate(csrBuilder.build());
      // persist cert ID to VERSION file
      datanodeDetails.setCertSerialId(dnCertSerialId);
      persistDatanodeDetails(datanodeDetails);
      LOG.info("Successfully stored SCM signed certificate, case:{}.",
          response);
      break;
    case FAILURE:
      LOG.error("DN security initialization failed, case:{}.", response);
      throw new RuntimeException("DN security initialization failed.");
    case RECOVER:
      LOG.error("DN security initialization failed, case:{}. OM certificate " +
          "is missing.", response);
      throw new RuntimeException("DN security initialization failed.");
    default:
      LOG.error("DN security initialization failed. Init response: {}",
          response);
      throw new RuntimeException("DN security initialization failed.");
    }

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
    Preconditions.checkNotNull(idFilePath);
    File idFile = new File(idFilePath);
    if (idFile.exists()) {
      return ContainerUtils.readDatanodeDetailsFrom(idFile);
    } else {
      // There is no datanode.id file, this might be the first time datanode
      // is started.
      DatanodeDetails details = DatanodeDetails.newBuilder()
          .setUuid(UUID.randomUUID()).build();
      details.setInitialVersion(DatanodeVersion.CURRENT_VERSION);
      details.setCurrentVersion(DatanodeVersion.CURRENT_VERSION);
      return details;
    }
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
    Preconditions.checkNotNull(idFilePath);
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

  @VisibleForTesting
  public DatanodeCRLStore getCRLStore() {
    return dnCRLStore;
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
      // Clean <HddsVolume>/tmp/container_delete_service dir.
      cleanTmpDir();
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
      // stop dn crl store
      try {
        if (dnCRLStore != null) {
          dnCRLStore.stop();
        }
      } catch (Exception ex) {
        LOG.error("Datanode CRL store stop failed", ex);
      }
      RatisDropwizardExports.clear(ratisMetricsMap, ratisReporterList);
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
}
