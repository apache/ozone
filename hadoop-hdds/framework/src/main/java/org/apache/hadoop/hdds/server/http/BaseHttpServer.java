/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.server.http;

import javax.servlet.http.HttpServlet;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.HddsConfServlet;
import org.apache.hadoop.hdds.conf.HddsPrometheusConfig;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;

import org.apache.commons.lang3.StringUtils;
import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.getPortNumberFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.createDir;
import static org.apache.hadoop.hdds.server.http.HttpConfig.getHttpPolicy;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_GROUPS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_HTTPS_NEED_AUTH_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_HTTPS_NEED_AUTH_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_HTTP_SECURITY_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_HTTP_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SERVER_HTTPS_KEYPASSWORD_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SERVER_HTTPS_KEYSTORE_PASSWORD_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SERVER_HTTPS_TRUSTSTORE_PASSWORD_KEY;

import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for HTTP server of the Ozone related components.
 */
public abstract class BaseHttpServer {

  private static final Logger LOG =
      LoggerFactory.getLogger(BaseHttpServer.class);
  static final String PROMETHEUS_SINK = "PROMETHEUS_SINK";
  private static final String JETTY_BASETMPDIR =
      "org.eclipse.jetty.webapp.basetempdir";

  private HttpServer2 httpServer;
  private final MutableConfigurationSource conf;

  private InetSocketAddress httpAddress;
  private InetSocketAddress httpsAddress;

  private HttpConfig.Policy policy;

  private String name;
  private PrometheusMetricsSink prometheusMetricsSink;

  private boolean prometheusSupport;

  private boolean profilerSupport;

  public BaseHttpServer(MutableConfigurationSource conf, String name)
      throws IOException {
    this.name = name;
    this.conf = conf;
    policy = getHttpPolicy(conf);
    if (isEnabled()) {
      this.httpAddress = getHttpBindAddress();
      this.httpsAddress = getHttpsBindAddress();

      // Avoid registering o.a.h.http.PrometheusServlet in HttpServer2.
      // TODO: Replace "hadoop.prometheus.endpoint.enabled" with
      // CommonConfigurationKeysPublic.HADOOP_PROMETHEUS_ENABLED when possible.
      conf.set("hadoop.prometheus.endpoint.enabled", "false");

      HttpServer2.Builder builder = newHttpServer2BuilderForOzone(
          conf, httpAddress, httpsAddress, name);

      boolean isSecurityEnabled = UserGroupInformation.isSecurityEnabled() &&
          OzoneSecurityUtil.isHttpSecurityEnabled(conf);
      LOG.info("Hadoop Security Enabled: {} " +
              "Ozone Security Enabled: {} " +
              "Ozone HTTP Security Enabled: {} ",
          UserGroupInformation.isSecurityEnabled(),
          conf.getBoolean(OZONE_SECURITY_ENABLED_KEY,
              OZONE_SECURITY_ENABLED_DEFAULT),
          conf.getBoolean(OZONE_HTTP_SECURITY_ENABLED_KEY,
              OZONE_HTTP_SECURITY_ENABLED_DEFAULT));

      if (isSecurityEnabled) {
        String httpAuthType = conf.get(getHttpAuthType(), "simple");
        LOG.info("HttpAuthType: {} = {}", getHttpAuthType(), httpAuthType);
        // Ozone config prefix must be set to avoid AuthenticationFilter
        // fall back to default one form hadoop.http.authentication.
        builder.authFilterConfigurationPrefix(getHttpAuthConfigPrefix());
        if (httpAuthType.equals("kerberos")) {
          builder.setSecurityEnabled(true);
          builder.setUsernameConfKey(getSpnegoPrincipal());
          builder.setKeytabConfKey(getKeytabFile());
        }
      }

      final boolean xFrameEnabled = conf.getBoolean(
          DFSConfigKeysLegacy.DFS_XFRAME_OPTION_ENABLED,
          DFSConfigKeysLegacy.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

      final String xFrameOptionValue = conf.getTrimmed(
          DFSConfigKeysLegacy.DFS_XFRAME_OPTION_VALUE,
          DFSConfigKeysLegacy.DFS_XFRAME_OPTION_VALUE_DEFAULT);

      builder.configureXFrame(xFrameEnabled).setXFrameOption(xFrameOptionValue);

      httpServer = builder.build();
      httpServer.addServlet("conf", "/conf", HddsConfServlet.class);

      httpServer.addServlet("logstream", "/logstream", LogStreamServlet.class);
      prometheusSupport =
          conf.getBoolean(HddsConfigKeys.HDDS_PROMETHEUS_ENABLED, true);

      profilerSupport =
          conf.getBoolean(HddsConfigKeys.HDDS_PROFILER_ENABLED, false);

      if (prometheusSupport) {
        prometheusMetricsSink = new PrometheusMetricsSink();
        httpServer.getWebAppContext().getServletContext()
            .setAttribute(PROMETHEUS_SINK, prometheusMetricsSink);
        HddsPrometheusConfig prometheusConfig =
            conf.getObject(HddsPrometheusConfig.class);
        String token = prometheusConfig.getPrometheusEndpointToken();
        if (StringUtils.isNotEmpty(token)) {
          httpServer.getWebAppContext().getServletContext()
              .setAttribute(PrometheusServlet.SECURITY_TOKEN, token);
          // Adding as internal servlet since we want to have token based
          // auth and hence SPNEGO should be disabled if security is enabled.
          httpServer.addInternalServlet("prometheus", "/prom",
              PrometheusServlet.class);
        } else {
          // If token is not configured, keeping as regular servlet and not
          // internal servlet since we do not want to expose /prom endpoint
          // without authentication in a secure cluster.
          httpServer.addServlet("prometheus", "/prom",
              PrometheusServlet.class);
        }
      }

      if (profilerSupport) {
        LOG.warn(
            "/prof java profiling servlet is activated. Not safe for "
                + "production!");
        httpServer.addServlet("profile", "/prof", ProfileServlet.class);
      }

      String baseDir = conf.get(OzoneConfigKeys.OZONE_HTTP_BASEDIR);
      if (!StringUtils.isEmpty(baseDir)) {
        createDir(baseDir);
        httpServer.getWebAppContext().setAttribute(JETTY_BASETMPDIR, baseDir);
        LOG.info("HTTP server of {} uses base directory {}", name, baseDir);
      }
    }
  }

  /**
   * Return a HttpServer.Builder that the OzoneManager/SCM/Datanode/S3Gateway/
   * Recon to initialize their HTTP / HTTPS server.
   */
  public static HttpServer2.Builder newHttpServer2BuilderForOzone(
      MutableConfigurationSource conf, final InetSocketAddress httpAddr,
      final InetSocketAddress httpsAddr, String name) throws IOException {
    HttpConfig.Policy policy = getHttpPolicy(conf);

    String userString = conf.get(OZONE_ADMINISTRATORS, "");
    String groupString = conf.get(OZONE_ADMINISTRATORS_GROUPS, "");

    HttpServer2.Builder builder = new HttpServer2.Builder().setName(name)
        .setConf(conf)
        .setACL(new AccessControlList(userString, groupString));

    // initialize the webserver for uploading/downloading files.
    if (policy.isHttpEnabled()) {
      if (httpAddr.getPort() == 0) {
        builder.setFindPort(true);
      }

      URI uri = URI.create("http://" + NetUtils.getHostPortString(httpAddr));
      builder.addEndpoint(uri);
      LOG.info("Starting Web-server for {} at: {}", name, uri);
    }

    if (policy.isHttpsEnabled() && httpsAddr != null) {
      ConfigurationSource sslConf = loadSslConfiguration(conf);
      loadSslConfToHttpServerBuilder(builder, sslConf);

      if (httpsAddr.getPort() == 0) {
        builder.setFindPort(true);
      }

      URI uri = URI.create("https://" + NetUtils.getHostPortString(httpsAddr));
      builder.addEndpoint(uri);
      LOG.info("Starting Web-server for {} at: {}", name, uri);
    }
    return builder;
  }


  /**
   * Add a servlet to BaseHttpServer.
   *
   * @param servletName The name of the servlet
   * @param pathSpec    The path spec for the servlet
   * @param clazz       The servlet class
   */
  protected void addServlet(String servletName, String pathSpec,
      Class<? extends HttpServlet> clazz) {
    httpServer.addServlet(servletName, pathSpec, clazz);
  }

  protected void addInternalServlet(String servletName, String pathSpec,
      Class<? extends HttpServlet> clazz) {
    httpServer.addInternalServlet(servletName, pathSpec, clazz);
  }


  /**
   * Returns the WebAppContext associated with this HttpServer.
   *
   * @return WebAppContext
   */
  protected WebAppContext getWebAppContext() {
    return httpServer.getWebAppContext();
  }

  protected InetSocketAddress getBindAddress(String bindHostKey,
      String addressKey, String bindHostDefault, int bindPortdefault) {
    final Optional<String> bindHost =
        getHostNameFromConfigKeys(conf, bindHostKey);

    final OptionalInt addressPort =
        getPortNumberFromConfigKeys(conf, addressKey);

    final Optional<String> addressHost =
        getHostNameFromConfigKeys(conf, addressKey);

    String hostName = bindHost.orElse(addressHost.orElse(bindHostDefault));

    return NetUtils.createSocketAddr(
        hostName + ":" + addressPort.orElse(bindPortdefault));
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the  HTTPS web interface.
   *
   * @return Target InetSocketAddress for the Ozone HTTPS endpoint.
   */
  public InetSocketAddress getHttpsBindAddress() {
    return getBindAddress(getHttpsBindHostKey(), getHttpsAddressKey(),
        getBindHostDefault(), getHttpsBindPortDefault());
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the  HTTP web interface.
   * <p>
   * * @return Target InetSocketAddress for the Ozone HTTP endpoint.
   */
  public InetSocketAddress getHttpBindAddress() {
    return getBindAddress(getHttpBindHostKey(), getHttpAddressKey(),
        getBindHostDefault(), getHttpBindPortDefault());

  }

  public void start() throws IOException {
    if (httpServer != null && isEnabled()) {
      httpServer.start();
      if (prometheusSupport) {
        DefaultMetricsSystem.instance()
            .register("prometheus", "Hadoop metrics prometheus exporter",
                prometheusMetricsSink);
      }
      updateConnectorAddress();
    }

  }

  private boolean isEnabled() {
    return conf.getBoolean(getEnabledKey(), true);
  }

  public void stop() throws Exception {
    if (httpServer != null) {
      httpServer.stop();
    }
  }

  /**
   * Update the configured listen address based on the real port
   * <p>
   * (eg. replace :0 with real port)
   */
  public void updateConnectorAddress() {
    int connIdx = 0;
    if (policy.isHttpEnabled()) {
      httpAddress = httpServer.getConnectorAddress(connIdx++);
      String realAddress = NetUtils.getHostPortString(httpAddress);
      conf.set(getHttpAddressKey(), realAddress);
      LOG.info("HTTP server of {} listening at http://{}", name, realAddress);
    }

    if (policy.isHttpsEnabled()) {
      httpsAddress = httpServer.getConnectorAddress(connIdx);
      String realAddress = NetUtils.getHostPortString(httpsAddress);
      conf.set(getHttpsAddressKey(), realAddress);
      LOG.info("HTTPS server of {} listening at https://{}", name, realAddress);
    }
  }


  public static HttpServer2.Builder loadSslConfToHttpServerBuilder(
      HttpServer2.Builder builder, ConfigurationSource sslConf) {
    return builder
        .needsClientAuth(
            sslConf.getBoolean(OZONE_CLIENT_HTTPS_NEED_AUTH_KEY,
                OZONE_CLIENT_HTTPS_NEED_AUTH_DEFAULT))
        .keyPassword(getPassword(sslConf, OZONE_SERVER_HTTPS_KEYPASSWORD_KEY))
        .keyStore(sslConf.get("ssl.server.keystore.location"),
            getPassword(sslConf, OZONE_SERVER_HTTPS_KEYSTORE_PASSWORD_KEY),
            sslConf.get("ssl.server.keystore.type", "jks"))
        .trustStore(sslConf.get("ssl.server.truststore.location"),
            getPassword(sslConf, OZONE_SERVER_HTTPS_TRUSTSTORE_PASSWORD_KEY),
            sslConf.get("ssl.server.truststore.type", "jks"))
        .excludeCiphers(
            sslConf.get("ssl.server.exclude.cipher.list"));
  }

  /**
   * Leverages the Configuration.getPassword method to attempt to get
   * passwords from the CredentialProvider API before falling back to
   * clear text in config - if falling back is allowed.
   *
   * @param conf  Configuration instance
   * @param alias name of the credential to retrieve
   * @return String credential value or null
   */
  static String getPassword(ConfigurationSource conf, String alias) {
    String password = null;
    try {
      char[] passchars = conf.getPassword(alias);
      if (passchars != null) {
        password = new String(passchars);
      }
    } catch (IOException ioe) {
      LOG.warn("Setting password to null since IOException is caught"
          + " when getting password", ioe);

      password = null;
    }
    return password;
  }
  /**
   * Load HTTPS-related configuration.
   */
  public static ConfigurationSource loadSslConfiguration(
      ConfigurationSource conf) {
    Configuration sslConf =
        new Configuration(false);

    sslConf.addResource(conf.get(
        OzoneConfigKeys.OZONE_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        OzoneConfigKeys.OZONE_SERVER_HTTPS_KEYSTORE_RESOURCE_DEFAULT));

    final String[] reqSslProps = {
        OzoneConfigKeys.OZONE_SERVER_HTTPS_TRUSTSTORE_LOCATION_KEY,
        OzoneConfigKeys.OZONE_SERVER_HTTPS_KEYSTORE_LOCATION_KEY,
        OzoneConfigKeys.OZONE_SERVER_HTTPS_KEYSTORE_PASSWORD_KEY,
        OzoneConfigKeys.OZONE_SERVER_HTTPS_KEYPASSWORD_KEY
    };

    // Check if the required properties are included
    for (String sslProp : reqSslProps) {
      if (sslConf.get(sslProp) == null) {
        LOG.warn("SSL config {} is missing. If {} is specified, make sure it "
                + "is a relative path", sslProp,
                OzoneConfigKeys.OZONE_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY);
      }
    }

    boolean requireClientAuth = conf.getBoolean(
        OZONE_CLIENT_HTTPS_NEED_AUTH_KEY, OZONE_CLIENT_HTTPS_NEED_AUTH_DEFAULT);
    sslConf.setBoolean(OZONE_CLIENT_HTTPS_NEED_AUTH_KEY, requireClientAuth);
    return new LegacyHadoopConfigurationSource(sslConf);
  }

  public InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  public InetSocketAddress getHttpsAddress() {
    return httpsAddress;
  }

  protected abstract String getHttpAddressKey();

  protected abstract String getHttpsAddressKey();

  protected abstract String getHttpBindHostKey();

  protected abstract String getHttpsBindHostKey();

  protected abstract String getBindHostDefault();

  protected abstract int getHttpBindPortDefault();

  protected abstract int getHttpsBindPortDefault();

  protected abstract String getKeytabFile();

  protected abstract String getSpnegoPrincipal();

  protected abstract String getEnabledKey();

  protected abstract String getHttpAuthType();

  protected abstract String getHttpAuthConfigPrefix();

}
