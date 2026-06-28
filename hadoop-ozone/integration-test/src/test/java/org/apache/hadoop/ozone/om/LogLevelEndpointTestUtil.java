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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;

/**
 * Shared helpers for /logLevel HTTP endpoint integration tests.
 */
final class LogLevelEndpointTestUtil {

  static final String MARKER = "<!-- OUTPUT -->";
  static final Pattern TAG = Pattern.compile("<[^>]*>");
  static final String EFFECTIVE_LEVEL = "Effective Level";
  static final String SET_LEVEL = "DEBUG";

  static final String OM_LOGGER = OzoneManager.class.getName();
  static final String SCM_LOGGER = StorageContainerManager.class.getName();
  static final String DN_LOGGER = HddsDatanodeService.class.getName();

  private LogLevelEndpointTestUtil() {
  }

  /** OM HTTP address from a running {@link MiniOzoneCluster}. */
  static String getOmHttpAddress(MiniOzoneCluster cluster) {
    return getOmHttpAddress(cluster.getOzoneManager());
  }

  /** OM HTTP address from a running {@link OzoneManager} (non-Kerberos). */
  static String getOmHttpAddress(OzoneManager om) {
    return toConnectHostPort(om.getHttpServer().getHttpAddress());
  }

  /**
   * Kerberos HTTP address for OM using the canonical hostname registered in the
   * KDC SPNEGO principal.
   */
  static String getKerberosHttpAddress(String host, OzoneManager om) {
    return host + ":" + om.getHttpServer().getHttpAddress().getPort();
  }

  /**
   * Kerberos HTTP address using the canonical hostname and a {@code host:port}
   * value from configuration.
   */
  static String getKerberosHttpAddress(String host, String configHostPort) {
    int port = NetUtils.createSocketAddr(configHostPort).getPort();
    return host + ":" + port;
  }

  /**
   * SCM HTTP {@code host:port} from a running {@link MiniOzoneCluster}.
   * SCM does not expose {@code getHttpServer()}, so the bound address is read
   * from cluster configuration (HA uses service- and node-suffixed keys).
   */
  static String getScmHttpAddress(MiniOzoneCluster cluster) {
    OzoneConfiguration conf = cluster.getConf();
    String key = ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY;
    String scmServiceId = HddsUtils.getScmServiceId(conf);
    if (scmServiceId != null) {
      String scmNodeId = conf.getTrimmed(ScmConfigKeys.OZONE_SCM_PRIMORDIAL_NODE_ID_KEY);
      key = ConfUtils.addKeySuffixes(key, scmServiceId, scmNodeId);
    }
    return toConnectHostPort(NetUtils.createSocketAddr(conf.get(key)));
  }

  /**
   * DN HTTP {@code host:port} from a running {@link MiniOzoneCluster}.
   * Datanodes do not expose {@code getHttpServer()}, so the bound address is
   * read from the datanode configuration after startup.
   */
  static String getDnHttpAddress(MiniOzoneCluster cluster) {
    HddsDatanodeService datanode = cluster.getHddsDatanodes().get(0);
    return toConnectHostPort(NetUtils.createSocketAddr(
        datanode.getConf().get(HddsConfigKeys.HDDS_DATANODE_HTTP_ADDRESS_KEY)));
  }

  private static String toConnectHostPort(InetSocketAddress address) {
    return NetUtils.getHostPortString(NetUtils.getConnectAddress(address));
  }

  static HttpURLConnection openUnauthenticatedConnection(String address,
      String logger) throws Exception {
    return openUnauthenticatedConnection(address, logger, null);
  }

  static HttpURLConnection openUnauthenticatedConnection(String address,
      String logger, String level) throws Exception {
    URL url = new URL(buildLogLevelUrl(address, logger, level));
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.connect();
    return connection;
  }

  static String getLogLevel(String address, String logger) throws Exception {
    HttpURLConnection connection =
        openUnauthenticatedConnection(address, logger);
    assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    return readResponse(connection);
  }

  static String setLogLevel(String address, String logger, String level)
      throws Exception {
    HttpURLConnection connection =
        openUnauthenticatedConnection(address, logger, level);
    assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    return readResponse(connection);
  }

  static String getLogLevelWithSpnego(String address, String logger)
      throws Exception {
    return fetchLogLevelWithSpnego(address, logger, null);
  }

  static String setLogLevelWithSpnego(String address, String logger,
      String level) throws Exception {
    return fetchLogLevelWithSpnego(address, logger, level);
  }

  static String fetchLogLevelWithSpnego(String address, String logger)
      throws Exception {
    return fetchLogLevelWithSpnego(address, logger, null);
  }

  private static String fetchLogLevelWithSpnego(String address, String logger,
      String level) throws Exception {
    URL url = new URL(buildLogLevelUrl(address, logger, level));
    AuthenticatedURL authenticatedUrl =
        new AuthenticatedURL(new KerberosAuthenticator());
    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    HttpURLConnection connection = (HttpURLConnection) authenticatedUrl
        .openConnection(url, token);
    connection.connect();
    assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode(),
        "Authenticated admin must receive /logLevel content");
    return readResponse(connection);
  }

  static void assertGetLogLevelResponse(String response, String serviceName) {
    assertTrue(response.contains(EFFECTIVE_LEVEL),
        serviceName + " GET /logLevel must return effective level");
    assertTrue(!response.contains("Bad Level"),
        serviceName + " GET /logLevel must not report bad level");
  }

  static void assertSetLogLevelResponse(String response, String serviceName,
      String level) {
    assertTrue(response.contains("Setting Level to " + level),
        serviceName + " SET /logLevel must confirm level change");
    assertEffectiveLevel(response, level, serviceName);
  }

  static void assertEffectiveLevel(String response, String level,
      String serviceName) {
    assertTrue(response.contains(EFFECTIVE_LEVEL + ": " + level),
        serviceName + " response must show effective level " + level
            + ", got: " + response);
  }

  static void enableSpnegoOnHttpClient() {
    System.clearProperty("jdk.http.auth.tunneling.disabledSchemes");
    System.clearProperty("jdk.http.auth.proxying.disabledSchemes");
  }

  static String readResponse(URLConnection connection) throws Exception {
    StringBuilder body = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith(MARKER)) {
          body.append(TAG.matcher(line).replaceAll(""));
        }
      }
    }
    return body.toString();
  }

  static void restoreSystemProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  private static String buildLogLevelUrl(String address, String logger,
      String level) {
    StringBuilder url = new StringBuilder("http://")
        .append(address)
        .append("/logLevel?log=")
        .append(logger);
    if (level != null) {
      url.append("&level=").append(level);
    }
    return url.toString();
  }
}
