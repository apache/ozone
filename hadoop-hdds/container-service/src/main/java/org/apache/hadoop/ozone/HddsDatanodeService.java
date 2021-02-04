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
import java.security.KeyPair;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.DNCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.server.http.RatisDropwizardExports;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.HddsVersionInfo;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.sun.jmx.mbeanserver.Introspector;
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec.getX509Certificate;
import static org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest.getEncodedString;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_DATANODE_PLUGINS_KEY;
import static org.apache.hadoop.util.ExitUtil.terminate;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
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
  private DNMXBeanImpl serviceRuntimeInfo =
      new DNMXBeanImpl(HddsVersionInfo.HDDS_VERSION_INFO) {};
  private ObjectName dnInfoBeanName;

  //Constructor for DataNode PluginService
  public HddsDatanodeService(){}

  public HddsDatanodeService(boolean printBanner, String[] args) {
    this.printBanner = printBanner;
    this.args = args != null ? Arrays.copyOf(args, args.length) : null;
  }

  /**
   * Create an Datanode instance based on the supplied command-line arguments.
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
   * Create an Datanode instance based on the supplied command-line arguments.
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
      Introspector.checkCompliance(DNMXBeanImpl.class);
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
    if (printBanner) {
      StringUtils.startupShutdownMessage(HddsVersionInfo.HDDS_VERSION_INFO,
          HddsDatanodeService.class, args, LOG);
    }
    start(createOzoneConfiguration());
    join();
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

    RatisDropwizardExports.
        registerRatisMetricReporters(ratisMetricsMap);

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
      TracingUtil.initTracing(
          "HddsDatanodeService." + datanodeDetails.getUuidString()
              .substring(0, 8), conf);
      LOG.info("HddsDatanodeService host:{} ip:{}", hostname, ip);
      // Authenticate Hdds Datanode service if security is enabled
      if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
        component = "dn-" + datanodeDetails.getUuidString();

        dnCertClient = new DNCertificateClient(new SecurityConfig(conf),
            datanodeDetails.getCertSerialId());

        if (SecurityUtil.getAuthenticationMethod(conf).equals(
            UserGroupInformation.AuthenticationMethod.KERBEROS)) {
          LOG.info("Ozone security is enabled. Attempting login for Hdds " +
                  "Datanode user. Principal: {},keytab: {}", conf.get(
              DFSConfigKeysLegacy.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY),
              conf.get(DFSConfigKeysLegacy.DFS_DATANODE_KEYTAB_FILE_KEY));

          UserGroupInformation.setConfiguration(conf);

          SecurityUtil
              .login(conf, DFSConfigKeysLegacy.DFS_DATANODE_KEYTAB_FILE_KEY,
                  DFSConfigKeysLegacy.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY,
                  hostname);
        } else {
          throw new AuthenticationException(SecurityUtil.
              getAuthenticationMethod(conf) + " authentication method not " +
              "supported. Datanode user" + " login " + "failed.");
        }
        LOG.info("Hdds Datanode login successful.");
      }
      if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
        initializeCertificateClient(conf);
      }
      datanodeStateMachine = new DatanodeStateMachine(datanodeDetails, conf,
          dnCertClient, this::terminateDatanode);
      try {
        httpServer = new HddsDatanodeHttpServer(conf);
        httpServer.start();
      } catch (Exception ex) {
        LOG.error("HttpServer failed to start.", ex);
      }
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
    String scmId = "scm-01";
    String clusterId = "clusterId";
    datanodeStateMachine.getContainer().start(scmId);
    MutableVolumeSet volumeSet =
        getDatanodeStateMachine().getContainer().getVolumeSet();

    Map<String, HddsVolume> volumeMap = volumeSet.getVolumeMap();

    for (Map.Entry<String, HddsVolume> entry : volumeMap.entrySet()) {
      HddsVolume hddsVolume = entry.getValue();
      boolean result = HddsVolumeUtil.checkVolume(hddsVolume, scmId,
          clusterId, LOG);
      if (!result) {
        volumeSet.failVolume(hddsVolume.getHddsRootDir().getPath());
      }
    }
  }

  /**
   * Initializes secure Datanode.
   * */
  @VisibleForTesting
  public void initializeCertificateClient(OzoneConfiguration config)
      throws IOException {
    LOG.info("Initializing secure Datanode.");

    CertificateClient.InitResponse response = dnCertClient.init();
    LOG.info("Init response: {}", response);
    switch (response) {
    case SUCCESS:
      LOG.info("Initialization successful, case:{}.", response);
      break;
    case GETCERT:
      getSCMSignedCert(config);
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
  }

  /**
   * Get SCM signed certificate and store it using certificate client.
   * @param config
   * */
  private void getSCMSignedCert(OzoneConfiguration config) {
    try {
      PKCS10CertificationRequest csr = getCSR(config);
      // TODO: For SCM CA we should fetch certificate from multiple SCMs.
      SCMSecurityProtocolClientSideTranslatorPB secureScmClient =
          HddsServerUtil.getScmSecurityClient(config);
      SCMGetCertResponseProto response = secureScmClient.
          getDataNodeCertificateChain(
              datanodeDetails.getProtoBufMessage(),
              getEncodedString(csr));
      // Persist certificates.
      if(response.hasX509CACertificate()) {
        String pemEncodedCert = response.getX509Certificate();
        dnCertClient.storeCertificate(pemEncodedCert, true);
        dnCertClient.storeCertificate(response.getX509CACertificate(), true,
            true);
        String dnCertSerialId = getX509Certificate(pemEncodedCert).
            getSerialNumber().toString();
        datanodeDetails.setCertSerialId(dnCertSerialId);
        persistDatanodeDetails(datanodeDetails);
        // Rebuild dnCertClient with the new CSR result so that the default
        // certSerialId and the x509Certificate can be updated.
        dnCertClient = new DNCertificateClient(
            new SecurityConfig(config), dnCertSerialId);

      } else {
        throw new RuntimeException("Unable to retrieve datanode certificate " +
            "chain");
      }
    } catch (IOException | CertificateException e) {
      LOG.error("Error while storing SCM signed certificate.", e);
      throw new RuntimeException(e);
    }
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
   * Creates CSR for DN.
   * @param config
   * */
  @VisibleForTesting
  public PKCS10CertificationRequest getCSR(ConfigurationSource config)
      throws IOException {
    CertificateSignRequest.Builder builder = dnCertClient.getCSRBuilder();
    KeyPair keyPair = new KeyPair(dnCertClient.getPublicKey(),
        dnCertClient.getPrivateKey());

    String hostname = InetAddress.getLocalHost().getCanonicalHostName();
    String subject = UserGroupInformation.getCurrentUser()
        .getShortUserName() + "@" + hostname;

    builder.setCA(false)
        .setKey(keyPair)
        .setConfiguration(config)
        .setSubject(subject);

    LOG.info("Creating csr for DN-> subject:{}", subject);
    return builder.build();
  }

  /**
   * Returns DatanodeDetails or null in case of Error.
   *
   * @return DatanodeDetails
   */
  private DatanodeDetails initializeDatanodeDetails()
      throws IOException {
    String idFilePath = HddsServerUtil.getDatanodeIdFilePath(conf);
    if (idFilePath == null || idFilePath.isEmpty()) {
      LOG.error("A valid path is needed for config setting {}",
          ScmConfigKeys.OZONE_SCM_DATANODE_ID_DIR);
      throw new IllegalArgumentException(
          ScmConfigKeys.OZONE_SCM_DATANODE_ID_DIR +
          " must be defined. See" +
          " https://wiki.apache.org/hadoop/Ozone#Configuration" +
          " for details on configuring Ozone.");
    }

    Preconditions.checkNotNull(idFilePath);
    File idFile = new File(idFilePath);
    if (idFile.exists()) {
      return ContainerUtils.readDatanodeDetailsFrom(idFile);
    } else {
      // There is no datanode.id file, this might be the first time datanode
      // is started.
      return DatanodeDetails.newBuilder().setUuid(UUID.randomUUID()).build();
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
    if (idFilePath == null || idFilePath.isEmpty()) {
      LOG.error("A valid path is needed for config setting {}",
          ScmConfigKeys.OZONE_SCM_DATANODE_ID_DIR);
      throw new IllegalArgumentException(
          ScmConfigKeys.OZONE_SCM_DATANODE_ID_DIR +
          " must be defined. See" +
          " https://wiki.apache.org/hadoop/Ozone#Configuration" +
          " for details on configuring Ozone.");
    }

    Preconditions.checkNotNull(idFilePath);
    File idFile = new File(idFilePath);
    ContainerUtils.writeDatanodeDetailsTo(dnDetails, idFile);
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

  public void join() {
    if (datanodeStateMachine != null) {
      try {
        datanodeStateMachine.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.info("Interrupted during StorageContainerManager join.");
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
      unregisterMXBean();
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
  }

  @VisibleForTesting
  public String getComponent() {
    return component;
  }

  public CertificateClient getCertificateClient() {
    return dnCertClient;
  }

  @VisibleForTesting
  public void setCertificateClient(CertificateClient client) {
    dnCertClient = client;
  }
}
