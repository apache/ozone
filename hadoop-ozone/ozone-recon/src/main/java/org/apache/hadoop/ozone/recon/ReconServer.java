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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.hadoop.ozone.recon.schema.ReconInternalSchemaDefinition;
import org.hadoop.ozone.recon.schema.StatsSchemaDefinition;
import org.hadoop.ozone.recon.schema.UtilizationSchemaDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;


/**
 * Recon server main class that stops and starts recon services.
 */
public class ReconServer extends GenericCli {

  private static final Logger LOG = LoggerFactory.getLogger(ReconServer.class);
  private final ScheduledExecutorService scheduler =
      Executors.newScheduledThreadPool(1);
  private Injector injector;

  @Inject
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
        new ReconTaskBindingModule());

    //Pass on injector to listener that does the Guice - Jersey HK2 bridging.
    ReconGuiceServletContextListener.setInjector(injector);

    LOG.info("Initializing Recon server...");
    try {
      StatsSchemaDefinition statsSchemaDefinition = injector.getInstance(
          StatsSchemaDefinition.class);
      statsSchemaDefinition.initializeSchema();

      UtilizationSchemaDefinition utilizationSchemaDefinition =
          injector.getInstance(UtilizationSchemaDefinition.class);
      utilizationSchemaDefinition.initializeSchema();

      ReconInternalSchemaDefinition reconInternalSchemaDefinition =
          injector.getInstance(ReconInternalSchemaDefinition.class);
      reconInternalSchemaDefinition.initializeSchema();

      LOG.info("Recon server initialized successfully!");

      httpServer = injector.getInstance(ReconHttpServer.class);
      LOG.info("Starting Recon server");
      httpServer.start();

      //Start Ozone Manager Service that pulls data from OM.
      OzoneManagerServiceProvider ozoneManagerServiceProvider = injector
          .getInstance(OzoneManagerServiceProvider.class);
      ozoneManagerServiceProvider.start();
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
    httpServer.stop();
  }
}
