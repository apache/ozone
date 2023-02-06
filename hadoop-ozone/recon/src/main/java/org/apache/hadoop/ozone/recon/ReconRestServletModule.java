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

import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.servlet.ServletModule;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.recon.api.AdminOnly;
import org.apache.hadoop.ozone.recon.api.filters.ReconAdminFilter;
import org.apache.hadoop.ozone.recon.api.filters.ReconAuthFilter;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.internal.inject.InjectionManager;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spi.Container;
import org.glassfish.jersey.server.spi.ContainerLifecycleListener;
import org.glassfish.jersey.servlet.ServletContainer;
import org.jvnet.hk2.guice.bridge.api.GuiceBridge;
import org.jvnet.hk2.guice.bridge.api.GuiceIntoHK2Bridge;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED_DEFAULT;

/**
 * Class to scan API Service classes and bind them to the injector.
 */
public class ReconRestServletModule extends ServletModule {

  public static final String BASE_API_PATH = UriBuilder.fromPath("/api").path(
      "v1").build().toString();
  public static final String API_PACKAGE = "org.apache.hadoop.ozone.recon.api";

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconRestServletModule.class);

  private final ConfigurationSource conf;

  public ReconRestServletModule(ConfigurationSource conf) {
    this.conf = conf;
  }

  @Override
  protected void configureServlets() {
    configureApi(BASE_API_PATH, API_PACKAGE);
  }

  private void configureApi(String baseApiPath, String... packages) {
    StringBuilder sb = new StringBuilder();
    Set<String> adminEndpoints = new HashSet<>();

    for (String pkg : packages) {
      if (sb.length() > 0) {
        sb.append(',');
      }
      checkIfPackageExistsAndLog(pkg, baseApiPath);
      sb.append(pkg);
      // Check for classes marked as admin only that will need an extra
      // filter applied to their path.
      Reflections reflections = new Reflections(pkg,
          new TypeAnnotationsScanner(), new SubTypesScanner());
      Set<Class<?>> adminEndpointClasses =
          reflections.getTypesAnnotatedWith(AdminOnly.class);
      adminEndpointClasses.stream()
          .map(clss -> UriBuilder.fromResource(clss).build().toString())
          .forEachOrdered(adminEndpoints::add);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Registered the following endpoint classes as admin only: {}",
            adminEndpointClasses);
      }
    }
    Map<String, String> params = new HashMap<>();
    params.put("javax.ws.rs.Application",
        GuiceResourceConfig.class.getCanonicalName());
    if (sb.length() > 0) {
      params.put("jersey.config.server.provider.packages", sb.toString());
    }
    bind(ServletContainer.class).in(Scopes.SINGLETON);

    String allApiPath =
        UriBuilder.fromPath(baseApiPath).path("*").build().toString();
    serve(allApiPath).with(ServletContainer.class, params);
    addFilters(baseApiPath, adminEndpoints);
  }

  private void addFilters(String basePath, Set<String> adminSubPaths) {
    if (OzoneSecurityUtil.isHttpSecurityEnabled(conf)) {
      String authPath =
          UriBuilder.fromPath(basePath).path("*").build().toString();
      filter(authPath).through(ReconAuthFilter.class);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Added authentication filter to path {}", authPath);
      }

      boolean aclEnabled = conf.getBoolean(OZONE_ACL_ENABLED,
          OZONE_ACL_ENABLED_DEFAULT);
      if (aclEnabled) {
        for (String path: adminSubPaths) {
          String adminPath =
              UriBuilder.fromPath(basePath).path(path + "*").build().toString();
          filter(adminPath).through(ReconAdminFilter.class);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Added admin filter to path {}", adminPath);
          }
        }
      }
    }
  }

  private void checkIfPackageExistsAndLog(String pkg, String path) {
    String resourcePath = pkg.replace(".", "/");
    URL resource = getClass().getClassLoader().getResource(resourcePath);
    if (resource != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using API endpoints from package {} for paths under {}.",
            pkg, path);
      }
    } else {
      LOG.warn("No Beans in '{}' found. Requests {} will fail.", pkg, path);
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
