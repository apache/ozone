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

package org.apache.hadoop.hdds.server.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.http.UriCompliance;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Testing HttpServer2.
 */
public class TestHttpServer2 {

  /**
   * Test hadoop.http.idle_timeout.ms correctly loaded, and not being default
   * value from core-default.xml of hadoop-common.
   *
   * @throws Exception
   */
  @Test
  public void testIdleTimeout() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    URI uri = URI.create("https://example.com/");

    HttpServer2 srv = new HttpServer2.Builder()
            .setConf(conf)
            .setName("test")
            .addEndpoint(uri)
            .build();
    for (ServerConnector server : srv.getListeners()) {
      // Check default value in ozone-default.xml
      assertEquals(60000, server.getIdleTimeout());
    }
  }

  /**
   * By default ambiguous URIs (e.g. empty path segments from "//") are rejected
   * with a 400: the connector uses Jetty's strict URI compliance and the
   * servlet layer does not decode ambiguous URIs.
   */
  @Test
  public void testUriComplianceStrictByDefault() throws Exception {
    HttpServer2 srv = buildServer(false);
    assertSame(UriCompliance.DEFAULT, uriComplianceOf(srv));
    assertFalse(srv.getWebAppContext().getServletHandler()
        .isDecodeAmbiguousURIs());
  }

  /**
   * With allowAmbiguousUri the connector uses the LEGACY compliance mode and
   * the servlet layer decodes ambiguous URIs, accepting empty path segments as
   * Jetty 9.4 did. The S3 Gateway needs this for object keys containing "//".
   */
  @Test
  public void testUriComplianceLegacyWhenAmbiguousAllowed() throws Exception {
    HttpServer2 srv = buildServer(true);
    assertSame(UriCompliance.LEGACY, uriComplianceOf(srv));
    assertTrue(srv.getWebAppContext().getServletHandler()
        .isDecodeAmbiguousURIs());
  }

  /**
   * By default the "/logs" context serves symlinked entries, matching Jetty 9.4:
   * Jetty 12's ServletContextHandler installs a symlink alias checker, so the
   * alias-check list is non-empty.
   */
  @Test
  public void testLogsContextServesAliasesByDefault(@TempDir Path logDir)
      throws Exception {
    ServletContextHandler logs = buildLogsContext(logDir, new OzoneConfiguration());
    assertNotNull(logs, "expected a /logs context");
    assertFalse(logs.getAliasChecks().isEmpty(),
        "symlinked log entries should be served by default");
  }

  /**
   * With hadoop.jetty.logs.serve.aliases=false the opt-out must actually take
   * effect: the alias checks are cleared so aliased (e.g. symlinked) entries
   * under the log directory are denied rather than served.
   */
  @Test
  public void testLogsContextOptOutClearsAliasChecks(@TempDir Path logDir)
      throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean("hadoop.jetty.logs.serve.aliases", false);
    ServletContextHandler logs = buildLogsContext(logDir, conf);
    assertNotNull(logs, "expected a /logs context");
    assertTrue(logs.getAliasChecks().isEmpty(),
        "opt-out must deny aliased (symlinked) log entries");
  }

  /**
   * When hadoop.log.dir points at a directory that does not yet exist, the
   * server creates it and serves "/logs" from it (preserving auto-create).
   */
  @Test
  public void testLogsDirectoryAutoCreatedWhenAbsent(@TempDir Path parent)
      throws Exception {
    Path logDir = parent.resolve("created-logs");
    assertFalse(Files.exists(logDir));
    ServletContextHandler logs = buildLogsContext(logDir, new OzoneConfiguration());
    assertNotNull(logs, "expected a /logs context");
    assertTrue(Files.isDirectory(logDir), "log directory should be created");
  }

  /**
   * When hadoop.log.dir cannot be used as a directory (here: a regular file is
   * in the way), the server must not fail to start; it skips "/logs" gracefully,
   * as the sibling "/static" context does when its base resource is absent.
   */
  @Test
  public void testLogsContextSkippedWhenDirectoryUnavailable(@TempDir Path parent)
      throws Exception {
    Path logFile = parent.resolve("logs-is-a-file");
    Files.createFile(logFile);
    ServletContextHandler logs = buildLogsContext(logFile, new OzoneConfiguration());
    assertNull(logs, "/logs must be skipped when the log directory is unavailable");
  }

  @Test
  public void testStaticGuardServesExistingBaseResource(@TempDir Path dir) {
    assertTrue(HttpServer2.baseResourceExists(dir.toUri().toString()));
  }

  @Test
  public void testStaticGuardSkipsMissingBaseResource(@TempDir Path dir) {
    assertFalse(HttpServer2.baseResourceExists(
        dir.resolve("does-not-exist").toUri().toString()));
  }

  @Test
  public void testStaticGuardAssumesNonFileResourcePresent() {
    // Non-file resources (e.g. inside a packaged jar) are assumed present.
    assertTrue(HttpServer2.baseResourceExists("http://host/webapps/static"));
  }

  @Test
  public void testStaticGuardSkipsMalformedResourceUrl() {
    assertFalse(HttpServer2.baseResourceExists("http://exa mple/static"));
  }

  /**
   * Drives hadoop's real {@code AuthenticationFilter} through the
   * {@link org.apache.hadoop.hdds.server.http.servletbridge.JavaxFilterBridge}
   * on an embedded Jetty EE10 server, exercising the javax->jakarta adaptation
   * ({@code JakartaToJavaxFilterConfig}, {@code JakartaToJavaxServletContext})
   * that the synthetic bridge unit tests do not cover. Uses simple/pseudo auth
   * with anonymous access disallowed: a request without a user is refused with
   * 401, and one carrying a user reaches the servlet with {@code getRemoteUser()}
   * bridged back onto the jakarta request.
   */
  @Test
  public void testHadoopAuthFilterRunsThroughBridge() throws Exception {
    HttpServer2 server = new HttpServer2.Builder()
        .setConf(new OzoneConfiguration())
        .setName("test")
        .addEndpoint(URI.create("http://localhost:0"))
        .build();

    Map<String, String> params = new HashMap<>();
    params.put("type", "simple");
    params.put("simple.anonymous.allowed", "false");
    params.put("signature.secret", "bridge-test-secret");
    server.addGlobalFilter("auth",
        "org.apache.hadoop.security.authentication.server.AuthenticationFilter",
        params);
    server.addServlet("whoami", "/whoami", RemoteUserServlet.class);
    server.start();
    try {
      int port = server.getConnectorAddress(0).getPort();
      String base = "http://localhost:" + port + "/whoami";

      // No user and anonymous disallowed: the filter refuses with a 401.
      assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, statusOf(base));

      // A user carried through pseudo auth reaches the servlet, and the
      // authenticated principal is bridged back onto the jakarta request.
      HttpURLConnection accepted =
          (HttpURLConnection) new URL(base + "?user.name=alice").openConnection();
      accepted.setConnectTimeout(5000);
      accepted.setReadTimeout(5000);
      assertEquals(HttpURLConnection.HTTP_OK, accepted.getResponseCode());
      assertEquals("alice", readBody(accepted));
    } finally {
      server.stop();
    }
  }

  private static int statusOf(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(5000);
    try {
      return conn.getResponseCode();
    } finally {
      conn.disconnect();
    }
  }

  private static String readBody(HttpURLConnection conn) throws IOException {
    try (InputStream in = conn.getInputStream()) {
      return IOUtils.toString(in, StandardCharsets.UTF_8).trim();
    }
  }

  /** Servlet that echoes the authenticated remote user for the bridge test. */
  public static class RemoteUserServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws IOException {
      resp.setContentType("text/plain");
      String user = req.getRemoteUser();
      resp.getWriter().write(user == null ? "" : user);
    }
  }

  private static HttpServer2 buildServer(boolean allowAmbiguousUri)
      throws Exception {
    return new HttpServer2.Builder()
        .setConf(new OzoneConfiguration())
        .setName("test")
        .addEndpoint(URI.create("http://example.com/"))
        .allowAmbiguousUri(allowAmbiguousUri)
        .build();
  }

  private static ServletContextHandler buildLogsContext(
      Path logDir, OzoneConfiguration conf) throws Exception {
    String previous = System.getProperty("hadoop.log.dir");
    System.setProperty("hadoop.log.dir", logDir.toString());
    try {
      HttpServer2 srv = new HttpServer2.Builder()
          .setConf(conf)
          .setName("test")
          .addEndpoint(URI.create("http://example.com/"))
          .build();
      return findLogsContext(srv.getWebAppContext().getServer().getHandler());
    } finally {
      if (previous == null) {
        System.clearProperty("hadoop.log.dir");
      } else {
        System.setProperty("hadoop.log.dir", previous);
      }
    }
  }

  private static ServletContextHandler findLogsContext(Handler handler) {
    if (handler instanceof ServletContextHandler
        && "/logs".equals(((ServletContextHandler) handler).getContextPath())) {
      return (ServletContextHandler) handler;
    }
    if (handler instanceof Handler.Container) {
      for (Handler child : ((Handler.Container) handler).getHandlers()) {
        ServletContextHandler found = findLogsContext(child);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  private static UriCompliance uriComplianceOf(HttpServer2 srv) {
    ServerConnector connector = srv.getListeners().get(0);
    return connector.getConnectionFactory(HttpConnectionFactory.class)
        .getHttpConfiguration().getUriCompliance();
  }
}
