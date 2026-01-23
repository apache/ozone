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

package org.apache.ozone.fs.http.server;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.ozone.fs.http.server.metrics.HttpFSServerMetrics;
import org.apache.ozone.lib.server.ServerException;
import org.apache.ozone.lib.service.FileSystemAccess;
import org.apache.ozone.lib.servlet.ServerWebApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bootstrap class that manages the initialization and destruction of the
 * HttpFSServer server, it is a <code>javax.servlet.ServletContextListener
 * </code> implementation that is wired in HttpFSServer's WAR
 * <code>WEB-INF/web.xml</code>.
 * <p>
 * It provides access to the server context via the singleton {@link #get}.
 * <p>
 * All the configuration is loaded from configuration properties prefixed
 * with <code>httpfs.</code>.
 */
@InterfaceAudience.Private
public class HttpFSServerWebApp extends ServerWebApp {
  private static final Logger LOG
      = LoggerFactory.getLogger(HttpFSServerWebApp.class);

  /**
   * Server name and prefix for all configuration properties.
   */
  public static final String NAME = "httpfs";

  /**
   * Configuration property that defines HttpFSServer admin group.
   */
  public static final String CONF_ADMIN_GROUP = "admin.group";

  private static final AtomicReference<HttpFSServerWebApp> SERVER = new AtomicReference<>();
  private static final AtomicReference<HttpFSServerMetrics> METRICS = new AtomicReference<>();

  private String adminGroup;

  /**
   * Default constructor.
   *
   * @throws IOException thrown if the home/conf/log/temp directory paths
   * could not be resolved.
   */
  public HttpFSServerWebApp() throws IOException {
    super(NAME);
  }

  /**
   * Initializes the HttpFSServer server, loads configuration and required
   * services.
   *
   * @throws ServerException thrown if HttpFSServer server could not be
   * initialized.
   */
  @Override
  public void init() throws ServerException {
    if (!SERVER.compareAndSet(null, this)) {
      throw new RuntimeException("HttpFSServer server already initialized");
    }
    super.init();
    adminGroup = getConfig().get(getPrefixedName(CONF_ADMIN_GROUP), "admin");
    LOG.info("Connects to FileSystem [{}]",
             get().get(FileSystemAccess.class).getFileSystemConfiguration().
               get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY));
    setMetrics(getConfig());
  }

  /**
   * Shutdowns all running services.
   */
  @Override
  public void destroy() {
    SERVER.set(null);
    HttpFSServerMetrics metrics = METRICS.getAndSet(null);
    if (metrics != null) {
      metrics.shutdown();
    }
    super.destroy();
  }

  private static void setMetrics(Configuration config) {
    LOG.info("Initializing HttpFSServerMetrics");
    METRICS.updateAndGet(prev -> prev != null ? prev : HttpFSServerMetrics.create(config, "HttpFSServer"));
    JvmPauseMonitor pauseMonitor = new JvmPauseMonitor();
    pauseMonitor.init(config);
    pauseMonitor.start();
    METRICS.get().getJvmMetrics().setPauseMonitor(pauseMonitor);
    FSOperations.setBufferSize(config);
    DefaultMetricsSystem.initialize("HttpFSServer");
  }

  /**
   * Returns HttpFSServer server singleton, configuration and services are
   * accessible through it.
   *
   * @return the HttpFSServer server singleton.
   */
  public static HttpFSServerWebApp get() {
    return SERVER.get();
  }

  /**
   * gets the HttpFSServerMetrics instance.
   * @return the HttpFSServerMetrics singleton.
   */
  public static HttpFSServerMetrics getMetrics() {
    return METRICS.get();
  }

  /**
   * Returns HttpFSServer admin group.
   *
   * @return httpfs admin group.
   */
  public String getAdminGroup() {
    return adminGroup;
  }

}
