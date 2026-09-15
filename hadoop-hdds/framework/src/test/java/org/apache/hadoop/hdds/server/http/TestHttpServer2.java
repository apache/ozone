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
import java.util.EnumSet;
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
   * With allowAmbiguousUri the connector relaxes only the ambiguous empty
   * segments ("//"), percent encodings ("%25") and encoded path separators that
   * S3 object keys and WebHDFS paths need, and the servlet layer decodes
   * ambiguous URIs. Unlike Jetty's LEGACY mode it must not re-admit %2e/%2e%2e
   * path traversal, UTF-16 or truncated UTF-8 encodings, suspicious path
   * characters or userinfo.
   */
  @Test
  public void testUriComplianceRelaxedWhenAmbiguousAllowed() throws Exception {
    HttpServer2 srv = buildServer(true);
    assertEquals(EnumSet.of(
        UriCompliance.Violation.AMBIGUOUS_EMPTY_SEGMENT,
        UriCompliance.Violation.AMBIGUOUS_PATH_ENCODING,
        UriCompliance.Violation.AMBIGUOUS_PATH_SEPARATOR),
        uriComplianceOf(srv).getAllowed());
    assertTrue(srv.getWebAppContext().getServletHandler()
        .isDecodeAmbiguousURIs());
  }

  /**
   * The relaxed compliance mode is enforced at the wire level on a running
   * server: with allowAmbiguousUri the S3/WebHDFS use case (empty segments and
   * percent encodings) reaches the servlet, while %2e%2e path traversal and %u
   * UTF-16 encodings -- which Jetty's LEGACY mode would re-admit -- are still
   * rejected with a 400.
   */
  @Test
  public void testAmbiguousUriHardeningStillRejectsTraversal() throws Exception {
    HttpServer2 server = new HttpServer2.Builder()
        .setConf(new OzoneConfiguration())
        .setName("test")
        .addEndpoint(URI.create("http://localhost:0"))
        .allowAmbiguousUri(true)
        .build();
    server.addServlet("echo", "/echo/*", OkServlet.class);
    server.start();
    try {
      String base = "http://localhost:" + server.getConnectorAddress(0).getPort();
      // Empty path segments ("//") and percent encodings ("%25") are the
      // S3/WebHDFS use case: admitted and delivered to the servlet.
      assertEquals(HttpURLConnection.HTTP_OK, statusOf(base + "/echo/a//b%25c"));
      // Path traversal and UTF-16 encodings stay outside that use case: 400.
      assertEquals(HttpURLConnection.HTTP_BAD_REQUEST,
          statusOf(base + "/echo/%2e%2e/x"));
      assertEquals(HttpURLConnection.HTTP_BAD_REQUEST,
          statusOf(base + "/echo/%u002e"));
    } finally {
      server.stop();
    }
  }

  /**
   * The wire-level mirror of {@link #testAmbiguousUriHardeningStillRejectsTraversal}:
   * without allowAmbiguousUri the very same S3/WebHDFS URL (empty segments plus a
   * percent encoding) that the relaxed server admits is rejected by the connector
   * with a 400 before it can reach the servlet -- which is why the opt-in knob
   * exists.
   */
  @Test
  public void testAmbiguousUriRejectedWhenNotAllowed() throws Exception {
    HttpServer2 server = new HttpServer2.Builder()
        .setConf(new OzoneConfiguration())
        .setName("test")
        .addEndpoint(URI.create("http://localhost:0"))
        .build();
    server.addServlet("echo", "/echo/*", OkServlet.class);
    server.start();
    try {
      String base = "http://localhost:" + server.getConnectorAddress(0).getPort();
      assertEquals(HttpURLConnection.HTTP_BAD_REQUEST,
          statusOf(base + "/echo/a//b%25c"));
    } finally {
      server.stop();
    }
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
   *
   * <p>hadoop-auth authenticates once and then relies on the signed
   * {@code hadoop.auth} cookie it sets. Replaying only that cookie -- with no
   * {@code user.name} -- must re-authenticate as the same user, which drives the
   * bridge's {@link org.apache.hadoop.hdds.server.http.servletbridge.JakartaToJavaxRequest#getCookies()}
   * read path on a real, Jetty-parsed cookie (the sole non-trivial conversion in
   * the request bridge). A reserved-name cookie sent alongside it, which the
   * javax {@code Cookie} constructor rejects, must be skipped in the same call
   * without failing the request.
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

      // Replay only the signed hadoop.auth cookie, with no user.name: the filter
      // must re-authenticate as alice off the cookie, which flows through the
      // request bridge's getCookies() on a real Jetty-parsed cookie. A reserved
      // "$Version" cookie sent alongside it must be skipped, not fail the request.
      String authCookie = authCookieOf(accepted);
      assertNotNull(authCookie, "expected a hadoop.auth Set-Cookie");
      HttpURLConnection replay =
          (HttpURLConnection) new URL(base).openConnection();
      replay.setConnectTimeout(5000);
      replay.setReadTimeout(5000);
      replay.setRequestProperty("Cookie", authCookie + "; $Version=1");
      assertEquals(HttpURLConnection.HTTP_OK, replay.getResponseCode());
      assertEquals("alice", readBody(replay));
    } finally {
      server.stop();
    }
  }

  /**
   * A client may send cookies whose names (e.g. {@code $Version}, {@code Path})
   * are delivered as ordinary cookies by Jetty 12's default RFC6265 parsing and
   * accepted by jakarta.servlet 6, but rejected by the stricter javax.servlet
   * 3.1 {@code Cookie} constructor the bridge converts into. The bridge must
   * skip such cookies rather than let hadoop-auth's {@code getCookies()} throw
   * and end the request in a 500, matching Jetty 9.4's lenient cookie handling.
   */
  @Test
  public void testReservedCookieNamesDoNotFailBridgedRequest() throws Exception {
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
      HttpURLConnection accepted = (HttpURLConnection)
          new URL("http://localhost:" + port + "/whoami?user.name=alice").openConnection();
      accepted.setConnectTimeout(5000);
      accepted.setReadTimeout(5000);
      // Reserved cookie names that the javax Cookie constructor rejects. Without
      // the bridge skipping them, getCookies() would throw and the request would
      // fail with a 500 before pseudo auth ever runs.
      accepted.setRequestProperty("Cookie", "$Version=1; Path=/");
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

  /**
   * Returns the {@code hadoop.auth="..."} name=value pair from the response's
   * Set-Cookie headers (dropping attributes such as Path/Expires), or null if
   * none was set.
   */
  private static String authCookieOf(HttpURLConnection conn) {
    for (int i = 0; ; i++) {
      String key = conn.getHeaderFieldKey(i);
      String value = conn.getHeaderField(i);
      if (key == null && value == null) {
        return null;
      }
      if ("Set-Cookie".equalsIgnoreCase(key) && value != null
          && value.startsWith("hadoop.auth=")) {
        return value.split(";", 2)[0];
      }
    }
  }

  /** Servlet that returns 200 for any request, used to probe URI admission. */
  public static class OkServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
      resp.setStatus(HttpServletResponse.SC_OK);
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
