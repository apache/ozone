/**
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

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManager;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.hadoop.ozone.recon.codegen.ReconSchemaGenerationModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;


/**
 * Recon server main class that stops and starts recon services.
 */
public class ReconServer extends GenericCli {

  private static final Logger LOG = LoggerFactory.getLogger(ReconServer.class);
  private Injector injector;

  private ReconHttpServer httpServer;

  public static void main(String[] args) {
    new ReconServer().run(args);
  }

  @Override
  public Void call() throws Exception {
    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();
    ConfigurationProvider.setConfiguration(ozoneConfiguration);

    injector =  Guice.createInjector(new
        ReconControllerModule(),
        new ReconRestServletModule() {
          @Override
          protected void configureServlets() {
            rest("/api/*")
              .packages("org.apache.hadoop.ozone.recon.api");
          }
        },
        new ReconSchemaGenerationModule(),
        new ReconTaskBindingModule());

    //Pass on injector to listener that does the Guice - Jersey HK2 bridging.
    ReconGuiceServletContextListener.setInjector(injector);

    LOG.info("Initializing Recon server...");
    try {

      LOG.info("Creating Recon Schema.");
      ReconSchemaManager reconSchemaManager = injector.getInstance(
          ReconSchemaManager.class);
      reconSchemaManager.createReconSchema();

      LOG.info("Recon server initialized successfully!");

      httpServer = injector.getInstance(ReconHttpServer.class);
      LOG.info("Starting Recon server");
      httpServer.start();

      getOzoneManagerServiceProvider().start();
      getReconStorageContainerManager().start();

    } catch (Exception e) {
      LOG.error("Error during initializing Recon server.", e);
      stop();
    }

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        stop();
      } catch (Exception e) {
        LOG.error("Error during stop Recon server", e);
      }
    }));
    return null;
  }

  void stop() throws Exception {
    LOG.info("Stopping Recon server");
    if (httpServer != null) {
      httpServer.stop();
    }
    OzoneManagerServiceProvider ozoneManagerServiceProvider = injector
        .getInstance(OzoneManagerServiceProvider.class);
    ozoneManagerServiceProvider.stop();
  }

  private OzoneManagerServiceProvider getOzoneManagerServiceProvider() {
    return injector.getInstance(OzoneManagerServiceProvider.class);
  }

  private ReconStorageContainerManager getReconStorageContainerManager() {
    return injector.getInstance(ReconStorageContainerManager.class);
  }

}
