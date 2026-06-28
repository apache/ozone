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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_HTTP_AUTH_TYPE;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_HTTP_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_HTTP_AUTH_TYPE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_HTTP_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.DN_LOGGER;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.OM_LOGGER;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.SCM_LOGGER;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.SET_LEVEL;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.assertGetLogLevelResponse;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.assertSetLogLevelResponse;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.enableSpnegoOnHttpClient;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.getDnHttpAddress;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.getKerberosHttpAddress;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.getLogLevelWithSpnego;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.getScmHttpAddress;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.openUnauthenticatedConnection;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.restoreSystemProperty;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.setLogLevelWithSpnego;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_AUTH_TYPE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration tests for the /logLevel HTTP endpoint in a secure cluster.
 *
 * <p>After changing {@code HttpServer2}, run with {@code -am} or install
 * {@code hadoop-hdds/framework} so tests use the updated server code.
 */
public class TestLogLevelEndpointSecure {

  private static final String SCM_SERVICE_ID = "loglevel-scm";

  @TempDir
  private static java.io.File workDir;

  private static MiniKdc miniKdc;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration secureConf;
  private static UserGroupInformation adminUgi;
  private static String omHttpAddress;
  private static String scmHttpAddress;
  private static String dnHttpAddress;
  private static String adminPrincipal;
  private static String host;

  @BeforeAll
  static void startSecureCluster() throws Exception {
    ExitUtils.disableSystemExit();
    DefaultMetricsSystem.setMiniClusterMode(true);
    secureConf = new OzoneConfiguration();
    secureConf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "localhost");

    startMiniKdc();
    configureSecureCluster();
    createCredentialsInKdc();

    UserGroupInformation.setConfiguration(secureConf);
    adminUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        adminPrincipal, secureConf.get(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY));
    secureConf.set(OZONE_ADMINISTRATORS, adminUgi.getShortUserName());
    UserGroupInformation.setLoginUser(adminUgi);

    OzoneManager.setTestSecureOmFlag(true);
    cluster = MiniOzoneCluster.newHABuilder(secureConf)
        .setSCMServiceId(SCM_SERVICE_ID)
        .setNumOfStorageContainerManagers(3)
        .setNumOfOzoneManagers(1)
        .setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();

    omHttpAddress = getKerberosHttpAddress(host, cluster.getOzoneManager());
    scmHttpAddress = getKerberosHttpAddress(host, getScmHttpAddress(cluster));
    dnHttpAddress = getKerberosHttpAddress(host, getDnHttpAddress(cluster));
  }

  @AfterAll
  static void stopSecureCluster() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
    if (miniKdc != null) {
      miniKdc.stop();
    }
  }

  @ParameterizedTest(name = "admin GET /logLevel on {0}")
  @MethodSource("secureHttpEndpoints")
  void testGetLogLevelForAdminWithSpnego(
      String serviceName, String httpAddress, String logger) throws Exception {
    String response = runAsAdmin(
        () -> getLogLevelWithSpnego(httpAddress, logger));

    assertGetLogLevelResponse(response, serviceName);
    assertFalse(response.contains("Unauthenticated users are not authorized"),
        serviceName + " authenticated admin must not be rejected");
  }

  @ParameterizedTest(name = "admin SET /logLevel on {0}")
  @MethodSource("secureHttpEndpoints")
  void testSetLogLevelForAdminWithSpnego(
      String serviceName, String httpAddress, String logger) throws Exception {
    String setResponse = runAsAdmin(
        () -> setLogLevelWithSpnego(httpAddress, logger, SET_LEVEL));
    assertSetLogLevelResponse(setResponse, serviceName, SET_LEVEL);

    String getResponse = runAsAdmin(
        () -> getLogLevelWithSpnego(httpAddress, logger));
    assertGetLogLevelResponse(getResponse, serviceName);
    LogLevelEndpointTestUtil.assertEffectiveLevel(getResponse, SET_LEVEL,
        serviceName);
  }

  @ParameterizedTest(name = "reject unauthenticated GET /logLevel on {0}")
  @MethodSource("secureHttpEndpoints")
  void testLogLevelRejectsUnauthenticatedRequest(
      String serviceName, String httpAddress, String logger) throws Exception {
    UserGroupInformation.setConfiguration(secureConf);
    UserGroupInformation previousLoginUser = UserGroupInformation.getLoginUser();
    String previousDisabledSchemes =
        System.getProperty("jdk.http.auth.tunneling.disabledSchemes");
    String previousDisabledProxySchemes =
        System.getProperty("jdk.http.auth.proxying.disabledSchemes");
    try {
      UserGroupInformation.setLoginUser(
          UserGroupInformation.createRemoteUser("nobody"));
      System.setProperty("jdk.http.auth.tunneling.disabledSchemes",
          "Basic,Negotiate,Kerberos");
      System.setProperty("jdk.http.auth.proxying.disabledSchemes",
          "Basic,Negotiate,Kerberos");

      HttpURLConnection connection =
          openUnauthenticatedConnection(httpAddress, logger);
      int responseCode = connection.getResponseCode();
      assertNotEquals(HttpURLConnection.HTTP_OK, responseCode,
          serviceName + " must not allow unauthenticated /logLevel access");
      assertTrue(responseCode == HttpURLConnection.HTTP_UNAUTHORIZED
              || responseCode == HttpURLConnection.HTTP_FORBIDDEN,
          serviceName + " must reject unauthenticated /logLevel, got "
              + responseCode);
    } finally {
      UserGroupInformation.setLoginUser(previousLoginUser);
      restoreSystemProperty("jdk.http.auth.tunneling.disabledSchemes",
          previousDisabledSchemes);
      restoreSystemProperty("jdk.http.auth.proxying.disabledSchemes",
          previousDisabledProxySchemes);
    }
  }

  static Stream<Arguments> secureHttpEndpoints() {
    return Stream.of(
        Arguments.of("OM", omHttpAddress, OM_LOGGER),
        Arguments.of("SCM", scmHttpAddress, SCM_LOGGER),
        Arguments.of("DN", dnHttpAddress, DN_LOGGER));
  }

  private static String runAsAdmin(PrivilegedExceptionAction<String> action)
      throws Exception {
    UserGroupInformation.setConfiguration(secureConf);
    UserGroupInformation.setLoginUser(adminUgi);
    adminUgi.setAuthenticationMethod(KERBEROS);
    enableSpnegoOnHttpClient();
    return adminUgi.doAs(action);
  }

  private static void startMiniKdc() throws Exception {
    Properties securityProperties = MiniKdc.createConf();
    miniKdc = new MiniKdc(securityProperties, workDir);
    miniKdc.start();
  }

  private static void configureSecureCluster() throws Exception {
    secureConf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    secureConf.setBoolean(HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED, false);
    secureConf.setBoolean(OZONE_HTTP_SECURITY_ENABLED_KEY, true);
    secureConf.setBoolean(HADOOP_SECURITY_AUTHORIZATION, true);
    secureConf.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS.name());
    secureConf.set(OZONE_OM_HTTP_AUTH_TYPE, "kerberos");
    secureConf.set(HDDS_SCM_HTTP_AUTH_TYPE, "kerberos");
    secureConf.set(HDDS_DATANODE_HTTP_AUTH_TYPE, "kerberos");

    host = InetAddress.getLocalHost().getCanonicalHostName()
        .toLowerCase();
    String realm = miniKdc.getRealm();
    String hostAndRealm = host + "@" + realm;
    String httpSpnegoPrincipal = "HTTP/_HOST@" + realm;
    adminPrincipal = "om/" + hostAndRealm;

    String scmPrincipal = "scm/" + hostAndRealm;
    secureConf.set(HDDS_SCM_KERBEROS_PRINCIPAL_KEY, scmPrincipal);
    secureConf.set(HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY, httpSpnegoPrincipal);
    secureConf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY, scmPrincipal);
    secureConf.set(OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY, httpSpnegoPrincipal);
    secureConf.set(HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY, scmPrincipal);
    secureConf.set(HDDS_DATANODE_HTTP_KERBEROS_PRINCIPAL_KEY,
        httpSpnegoPrincipal);

    File omKeytab = new File(workDir, "om.keytab");
    File spnegoKeytab = new File(workDir, "http.keytab");
    secureConf.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY,
        omKeytab.getAbsolutePath());
    secureConf.set(OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE,
        spnegoKeytab.getAbsolutePath());
    secureConf.set(HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY,
        omKeytab.getAbsolutePath());
    secureConf.set(HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY,
        spnegoKeytab.getAbsolutePath());
    secureConf.set(HDDS_DATANODE_KERBEROS_KEYTAB_FILE_KEY,
        omKeytab.getAbsolutePath());
    secureConf.set(HDDS_DATANODE_HTTP_KERBEROS_KEYTAB_FILE_KEY,
        spnegoKeytab.getAbsolutePath());
  }

  private static void createCredentialsInKdc() throws Exception {
    String realm = miniKdc.getRealm();
    String hostAndRealm = host + "@" + realm;
    String httpPrincipal = "HTTP/" + hostAndRealm;
    String scmPrincipal = "scm/" + hostAndRealm;
    File omKeytab = new File(secureConf.get(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY));
    File spnegoKeytab =
        new File(secureConf.get(OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE));
    miniKdc.createPrincipal(omKeytab, adminPrincipal, scmPrincipal);
    miniKdc.createPrincipal(spnegoKeytab, httpPrincipal);
  }
}
