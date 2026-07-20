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

package org.apache.hadoop.ozone.recon;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.servlet.ServletModule;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.UriBuilder;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.recon.api.filters.ReconAdminFilter;
import org.apache.hadoop.ozone.recon.api.filters.ReconAuthFilter;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.internal.inject.InjectionManager;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spi.Container;
import org.glassfish.jersey.server.spi.ContainerLifecycleListener;
import org.glassfish.jersey.servlet.ServletContainer;
import org.jvnet.hk2.guice.bridge.api.GuiceBridge;
import org.jvnet.hk2.guice.bridge.api.GuiceIntoHK2Bridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to scan API Service classes and bind them to the injector.
 */
public class ReconRestServletModule extends ServletModule {

  public static final String BASE_API_PATH = "/api/v1";
  public static final String API_PACKAGE = "org.apache.hadoop.ozone.recon.api";

  public static final String CHATBOT_API_PACKAGE = "org.apache.hadoop.ozone.recon.chatbot.api";

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconRestServletModule.class);

  private final ConfigurationSource conf;

  public ReconRestServletModule(ConfigurationSource conf) {
    this.conf = conf;
  }

  @Override
  protected void configureServlets() {
    if (conf instanceof OzoneConfiguration
        && ChatbotConfigKeys.isChatbotEnabled((OzoneConfiguration) conf)) {
      configureApi(API_PACKAGE, CHATBOT_API_PACKAGE);
    } else {
      configureApi(API_PACKAGE);
    }
  }

  private void configureApi(String... packages) {
    StringBuilder sb = new StringBuilder();

    for (String pkg : packages) {
      if (sb.length() > 0) {
        sb.append(',');
      }
      checkIfPackageExistsAndLog(pkg);
      sb.append(pkg);
    }
    Map<String, String> params = new HashMap<>();
    params.put("javax.ws.rs.Application",
        GuiceResourceConfig.class.getCanonicalName());
    if (sb.length() > 0) {
      params.put("jersey.config.server.provider.packages", sb.toString());
    }
    bind(ServletContainer.class).in(Scopes.SINGLETON);

    String allApiPath = UriBuilder.fromPath(BASE_API_PATH).path("*").build().toString();
    serve(allApiPath).with(ServletContainer.class, params);

    if (OzoneSecurityUtil.isHttpSecurityEnabled(conf)) {
      filter(allApiPath).through(ReconAuthFilter.class);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Added authentication filter to path {}", allApiPath);
      }

      boolean authorizationEnabled = OzoneSecurityUtil.isAuthorizationEnabled(conf);
      if (authorizationEnabled) {
        filter(allApiPath).through(ReconAdminFilter.class);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Added admin filter to path {}", allApiPath);
        }
      }
    }
  }

  private void checkIfPackageExistsAndLog(String pkg) {
    String resourcePath = pkg.replace(".", "/");
    URL resource = getClass().getClassLoader().getResource(resourcePath);
    if (resource != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using API endpoints from package {} for paths under {}.",
            pkg, BASE_API_PATH);
      }
    } else {
      LOG.warn("No Beans in '{}' found. Requests {} will fail.", pkg, BASE_API_PATH);
    }
  }
}

/**
 * Class to bridge Guice bindings to Jersey hk2 bindings.
 */
class GuiceResourceConfig extends ResourceConfig {
  GuiceResourceConfig() {
    register(new ContainerLifecycleListener() {

      @Override
      public void onStartup(Container container) {
        ServletContainer servletContainer = (ServletContainer) container;
        InjectionManager injectionManager = container.getApplicationHandler()
            .getInjectionManager();
        ServiceLocator serviceLocator = injectionManager
            .getInstance(ServiceLocator.class);
        GuiceBridge.getGuiceBridge().initializeGuiceBridge(serviceLocator);
        GuiceIntoHK2Bridge guiceBridge = serviceLocator
            .getService(GuiceIntoHK2Bridge.class);
        Injector injector = (Injector) servletContainer.getServletContext()
            .getAttribute(Injector.class.getName());
        guiceBridge.bridgeGuiceInjector(injector);
      }

      @Override
      public void onReload(Container container) {
      }

      @Override
      public void onShutdown(Container container) {
      }
    });
  }
}
