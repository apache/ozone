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

import static org.apache.hadoop.hdds.recon.ReconConfig.ConfigStrings.OZONE_RECON_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfig.ConfigStrings.OZONE_RECON_KERBEROS_PRINCIPAL_KEY;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfig;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.util.OzoneVersionInfo;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.hadoop.ozone.recon.codegen.ReconSchemaGenerationModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Guice;
import com.google.inject.Injector;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Recon server main class that stops and starts recon services.
 */
public class ReconServer extends GenericCli {

  private static final Logger LOG = LoggerFactory.getLogger(ReconServer.class);
  private Injector injector;

  private ReconHttpServer httpServer;
  private ContainerDBServiceProvider containerDBServiceProvider;
  private OzoneManagerServiceProvider ozoneManagerServiceProvider;
  private OzoneStorageContainerManager reconStorageContainerManager;
  private OzoneConfiguration configuration;

  private volatile boolean isStarted = false;

  public static void main(String[] args) {
    new ReconServer().run(args);
  }

  @Override
  public Void call() throws Exception {
    String[] originalArgs = getCmd().getParseResult().originalArgs()
        .toArray(new String[0]);
    StringUtils.startupShutdownMessage(OzoneVersionInfo.OZONE_VERSION_INFO,
        ReconServer.class, originalArgs, LOG);

    configuration = createOzoneConfiguration();
    ConfigurationProvider.setConfiguration(configuration);

    injector =  Guice.createInjector(new
        ReconControllerModule(),
        new ReconRestServletModule() {
          @Override
          protected void configureServlets() {
            rest("/api/v1/*")
              .packages("org.apache.hadoop.ozone.recon.api");
          }
        }, new ReconSchemaGenerationModule());

    //Pass on injector to listener that does the Guice - Jersey HK2 bridging.
    ReconGuiceServletContextListener.setInjector(injector);

    LOG.info("Initializing Recon server...");
    try {
      loginReconUserIfSecurityEnabled(configuration);
      this.containerDBServiceProvider =
          injector.getInstance(ContainerDBServiceProvider.class);

      ReconSchemaManager reconSchemaManager =
          injector.getInstance(ReconSchemaManager.class);
      LOG.info("Creating Recon Schema.");
      reconSchemaManager.createReconSchema();

      httpServer = injector.getInstance(ReconHttpServer.class);
      this.ozoneManagerServiceProvider =
          injector.getInstance(OzoneManagerServiceProvider.class);
      this.reconStorageContainerManager =
          injector.getInstance(OzoneStorageContainerManager.class);
      LOG.info("Recon server initialized successfully!");

    } catch (Exception e) {
      LOG.error("Error during initializing Recon server.", e);
    }
    // Start all services
    start();
    isStarted = true;

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        stop();
        join();
      } catch (Exception e) {
        LOG.error("Error during stop Recon server", e);
      }
    }));
    return null;
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
      if (httpServer != null) {
        httpServer.start();
      }
      if (ozoneManagerServiceProvider != null) {
        ozoneManagerServiceProvider.start();
      }
      if (reconStorageContainerManager != null) {
        reconStorageContainerManager.start();
      }
    }
  }

  public void stop() throws Exception {
    if (isStarted) {
      LOG.info("Stopping Recon server");
      if (httpServer != null) {
        httpServer.stop();
      }
      if (reconStorageContainerManager != null) {
        reconStorageContainerManager.stop();
      }
      if (ozoneManagerServiceProvider != null) {
        ozoneManagerServiceProvider.stop();
      }
      if (containerDBServiceProvider != null) {
        containerDBServiceProvider.close();
      }
      isStarted = false;
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
  public ContainerDBServiceProvider getContainerDBServiceProvider() {
    return containerDBServiceProvider;
  }

  @VisibleForTesting
  ReconHttpServer getHttpServer() {
    return httpServer;
  }
}
