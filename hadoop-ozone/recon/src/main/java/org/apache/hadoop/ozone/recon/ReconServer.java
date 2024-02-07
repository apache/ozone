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

package org.apache.hadoop.ozone.recon;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.recon.ReconConfig;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.ozone.recon.api.types.FeatureProvider;
import org.apache.hadoop.ozone.recon.security.ReconCertificateClient;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.recon.scm.ReconSafeModeManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageConfig;
import org.apache.hadoop.ozone.recon.metrics.ReconTaskStatusMetrics;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider;
import org.apache.hadoop.ozone.util.OzoneNetUtils;
import org.apache.hadoop.ozone.util.OzoneVersionInfo;
import org.apache.hadoop.ozone.util.ShutdownHookManager;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ratis.util.JvmPauseMonitor;
import org.hadoop.ozone.recon.codegen.ReconSchemaGenerationModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.apache.hadoop.hdds.ratis.RatisHelper.newJvmPauseMonitor;
import static org.apache.hadoop.hdds.recon.ReconConfig.ConfigStrings.OZONE_RECON_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfig.ConfigStrings.OZONE_RECON_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmSecurityClientWithMaxRetry;
import static org.apache.hadoop.ozone.common.Storage.StorageState.INITIALIZED;
import static org.apache.hadoop.ozone.conf.OzoneServiceConfig.DEFAULT_SHUTDOWN_HOOK_PRIORITY;
import static org.apache.hadoop.security.UserGroupInformation.getCurrentUser;
import static org.apache.hadoop.util.ExitUtil.terminate;

/**
 * Recon server main class that stops and starts recon services.
 */
public class ReconServer extends GenericCli {

  private static final Logger LOG = LoggerFactory.getLogger(ReconServer.class);
  private Injector injector;

  private JvmPauseMonitor jvmPauseMonitor;
  private ReconHttpServer httpServer;
  private ReconContainerMetadataManager reconContainerMetadataManager;
  private OzoneManagerServiceProvider ozoneManagerServiceProvider;
  private ReconDBProvider reconDBProvider;
  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private OzoneStorageContainerManager reconStorageContainerManager;
  private ReconSafeModeManager reconSafeModeMgr;
  private OzoneConfiguration configuration;
  private ReconStorageConfig reconStorage;
  private CertificateClient certClient;
  private ReconTaskStatusMetrics reconTaskStatusMetrics;

  private volatile boolean isStarted = false;

  public static void main(String[] args) {
    OzoneNetUtils.disableJvmNetworkAddressCacheIfRequired(
            new OzoneConfiguration());
    new ReconServer().run(args);
  }

  @Override
  public Void call() throws Exception {
    String[] originalArgs = getCmd().getParseResult().originalArgs()
        .toArray(new String[0]);

    configuration = createOzoneConfiguration();
    StringUtils.startupShutdownMessage(OzoneVersionInfo.OZONE_VERSION_INFO,
            ReconServer.class, originalArgs, LOG, configuration);
    ConfigurationProvider.setConfiguration(configuration);

    injector = Guice.createInjector(new ReconControllerModule(),
        new ReconRestServletModule(configuration),
        new ReconSchemaGenerationModule());

    //Pass on injector to listener that does the Guice - Jersey HK2 bridging.
    ReconGuiceServletContextListener.setInjector(injector);

    reconStorage = injector.getInstance(ReconStorageConfig.class);

    LOG.info("Initializing Recon server...");
    try {
      loginReconUserIfSecurityEnabled(configuration);
      try {
        if (reconStorage.getState() != INITIALIZED) {
          reconStorage.initialize();
        }
        if (OzoneSecurityUtil.isSecurityEnabled(configuration)) {
          LOG.info("ReconStorageConfig initialized." +
              "Initializing certificate.");
          initializeCertificateClient();
        }
      } catch (Exception e) {
        LOG.error("Error during initializing Recon certificate", e);
      }
      jvmPauseMonitor = newJvmPauseMonitor("Recon");
      this.reconDBProvider = injector.getInstance(ReconDBProvider.class);
      this.reconContainerMetadataManager =
          injector.getInstance(ReconContainerMetadataManager.class);
      this.reconNamespaceSummaryManager =
          injector.getInstance(ReconNamespaceSummaryManager.class);

      ReconSchemaManager reconSchemaManager =
          injector.getInstance(ReconSchemaManager.class);
      LOG.info("Creating Recon Schema.");
      reconSchemaManager.createReconSchema();

      this.reconSafeModeMgr = injector.getInstance(ReconSafeModeManager.class);
      this.reconSafeModeMgr.setInSafeMode(true);
      httpServer = injector.getInstance(ReconHttpServer.class);
      this.ozoneManagerServiceProvider =
          injector.getInstance(OzoneManagerServiceProvider.class);
      this.reconStorageContainerManager =
          injector.getInstance(OzoneStorageContainerManager.class);

      this.reconTaskStatusMetrics =
          injector.getInstance(ReconTaskStatusMetrics.class);

      LOG.info("Initializing support of Recon Features...");
      FeatureProvider.initFeatureSupport(configuration);
      LOG.info("Recon server initialized successfully!");
    } catch (Exception e) {
      LOG.error("Error during initializing Recon server.", e);
    }
    // Start all services
    start();
    isStarted = true;

    ShutdownHookManager.get().addShutdownHook(() -> {
      try {
        stop();
        join();
      } catch (Exception e) {
        LOG.error("Error during stop Recon server", e);
      }
    }, DEFAULT_SHUTDOWN_HOOK_PRIORITY);
    return null;
  }

  /**
   * Initializes secure Recon.
   * */
  private void initializeCertificateClient()
      throws IOException {
    LOG.info("Initializing secure Recon.");
    SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient =
        getScmSecurityClientWithMaxRetry(configuration, getCurrentUser());
    SecurityConfig secConf = new SecurityConfig(configuration);
    certClient = new ReconCertificateClient(secConf, scmSecurityClient,
        reconStorage, this::saveNewCertId, this::terminateRecon);
    certClient.initWithRecovery();
  }

  public void saveNewCertId(String newCertId) {
    try {
      reconStorage.setReconCertSerialId(newCertId);
      reconStorage.persistCurrentState();
    } catch (IOException ex) {
      // New cert ID cannot be persisted into VERSION file.
      LOG.error("Failed to persist new cert ID {} to VERSION file." +
          "Terminating OzoneManager...", newCertId, ex);
      terminateRecon();
    }
  }

  public void terminateRecon() {
    stop();
    terminate(1);
  }

  /**
   * Need a way to restart services from tests.
   */
  public void start() throws Exception {
    if (!isStarted) {
      LOG.info("Starting Recon server");
      isStarted = true;
      // Initialize metrics for Recon
      HddsServerUtil.initializeMetrics(configuration, "Recon");
      if (reconTaskStatusMetrics != null) {
        reconTaskStatusMetrics.register();
      }
      if (httpServer != null) {
        httpServer.start();
      }
      if (ozoneManagerServiceProvider != null) {
        ozoneManagerServiceProvider.start();
      }
      if (reconStorageContainerManager != null) {
        reconStorageContainerManager.start();
      }
      if (jvmPauseMonitor != null) {
        jvmPauseMonitor.start();
      }
    }
  }

  public void stop() {
    if (isStarted) {
      LOG.info("Stopping Recon server");
      if (httpServer != null) {
        try {
          httpServer.stop();
        } catch (Exception e) {
          LOG.error("Stopping HttpServer is failed.", e);
        }
      }

      if (reconStorageContainerManager != null) {
        reconStorageContainerManager.stop();
      }
      if (ozoneManagerServiceProvider != null) {
        try {
          ozoneManagerServiceProvider.stop();
        } catch (Exception e) {
          LOG.error("Stopping ozoneManagerServiceProvider is failed.", e);
        }
      }
      if (reconTaskStatusMetrics != null) {
        reconTaskStatusMetrics.unregister();
      }
      isStarted = false;
      if (reconDBProvider != null) {
        try {
          LOG.info("Closing Recon Container Key DB.");
          reconDBProvider.close();
        } catch (Exception ex) {
          LOG.error("Recon Container Key DB close failed", ex);
        }
      }
    }
    if (certClient != null) {
      try {
        certClient.close();
      } catch (IOException ioe) {
        LOG.error("Failed to close certificate client.", ioe);
      }
    }
    if (jvmPauseMonitor != null) {
      jvmPauseMonitor.stop();
    }
  }

  public void join() {
    if (reconStorageContainerManager != null) {
      reconStorageContainerManager.join();
    }
  }

  /**
   * Logs in the Recon user if security is enabled in the configuration.
   *
   * @param conf OzoneConfiguration
   */
  private static void loginReconUserIfSecurityEnabled(
      OzoneConfiguration  conf) {
    try {
      if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
        loginReconUser(conf);
      }
    } catch (Exception ex) {
      LOG.error("Error login in as Recon service. ", ex);
    }
  }

  /**
   * Login Recon service user if security is enabled.
   *
   * @param  conf OzoneConfiguration
   * @throws IOException, AuthenticationException
   */
  private static void loginReconUser(OzoneConfiguration conf)
      throws IOException, AuthenticationException {

    if (SecurityUtil.getAuthenticationMethod(conf).equals(
        UserGroupInformation.AuthenticationMethod.KERBEROS)) {
      ReconConfig reconConfig = conf.getObject(ReconConfig.class);
      LOG.info("Ozone security is enabled. Attempting login for Recon service. "
              + "Principal: {}, keytab: {}",
          reconConfig.getKerberosPrincipal(),
          reconConfig.getKerberosKeytab());
      UserGroupInformation.setConfiguration(conf);
      InetSocketAddress socAddr = HddsUtils.getReconAddresses(conf);
      SecurityUtil.login(conf,
          OZONE_RECON_KERBEROS_KEYTAB_FILE_KEY,
          OZONE_RECON_KERBEROS_PRINCIPAL_KEY,
          socAddr.getHostName());
    } else {
      throw new AuthenticationException(SecurityUtil.getAuthenticationMethod(
          conf) + " authentication method not supported. "
          + "Recon service login failed.");
    }
    LOG.info("Recon login successful.");
  }

  @VisibleForTesting
  public OzoneManagerServiceProvider getOzoneManagerServiceProvider() {
    return ozoneManagerServiceProvider;
  }

  @VisibleForTesting
  public OzoneStorageContainerManager getReconStorageContainerManager() {
    return reconStorageContainerManager;
  }

  @VisibleForTesting
  public StorageContainerServiceProvider getStorageContainerServiceProvider() {
    return injector.getInstance(StorageContainerServiceProvider.class);
  }

  @VisibleForTesting
  public ReconContainerMetadataManager getReconContainerMetadataManager() {
    return reconContainerMetadataManager;
  }

  @VisibleForTesting
  public ReconNamespaceSummaryManager getReconNamespaceSummaryManager() {
    return reconNamespaceSummaryManager;
  }

  @VisibleForTesting
  ReconHttpServer getHttpServer() {
    return httpServer;
  }
}
