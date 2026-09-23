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
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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
   * With allowAmbiguousUri the connector relaxes the ambiguous empty segments
   * ("//"), percent encodings ("%25") and encoded path separators that S3 object
   * keys and WebHDFS paths need, plus the suspicious path characters (backslash,
   * DEL, C0 controls) that Jetty 9.4 passed through, and the servlet layer
   * decodes ambiguous URIs. Unlike Jetty's LEGACY mode it must not re-admit
   * %2e/%2e%2e path traversal, UTF-16 or truncated UTF-8 encodings or userinfo.
   */
  @Test
  public void testUriComplianceRelaxedWhenAmbiguousAllowed() throws Exception {
    HttpServer2 srv = buildServer(true);
    assertEquals(EnumSet.of(
        UriCompliance.Violation.AMBIGUOUS_EMPTY_SEGMENT,
        UriCompliance.Violation.AMBIGUOUS_PATH_ENCODING,
        UriCompliance.Violation.AMBIGUOUS_PATH_SEPARATOR,
        UriCompliance.Violation.SUSPICIOUS_PATH_CHARACTERS),
        uriComplianceOf(srv).getAllowed());
    assertTrue(srv.getWebAppContext().getServletHandler()
        .isDecodeAmbiguousURIs());
  }

  /**
   * The relaxed compliance mode is enforced at the wire level on a running
   * server: with allowAmbiguousUri the S3/WebHDFS use case (empty segments,
   * percent encodings and the suspicious path characters -- an encoded backslash
   * such as an S3 key "dir\file", a C0 control and DEL) reaches the servlet, while
   * %2e%2e path traversal and %u UTF-16 encodings -- which Jetty's LEGACY mode
   * would re-admit -- are still rejected with a 400.
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
      // The suspicious path characters (SUSPICIOUS_PATH_CHARACTERS) that Jetty 9.4
      // passed through -- an encoded backslash such as an S3 key "dir\file", a C0
      // control (%01) and DEL (%7F) -- are admitted and delivered to the servlet.
      assertEquals(HttpURLConnection.HTTP_OK, statusOf(base + "/echo/a%5Cb"));
      assertEquals(HttpURLConnection.HTTP_OK, statusOf(base + "/echo/a%01b"));
      assertEquals(HttpURLConnection.HTTP_OK, statusOf(base + "/echo/a%7Fb"));
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
   * By default the "/logs" context serves symlinked entries, matching Jetty 9.4.
   * Drive it over the wire on a running server with a symlink that escapes the
   * log directory (the case the opt-out below rejects): it is served with a 200,
   * rather than only asserting Jetty's alias-check list is non-empty. A symlink
   * whose target stays inside the log directory is served in both modes -- the
   * DefaultServlet re-installs an in-base AllowedResourceAliasChecker at startup
   * -- so only an escaping symlink distinguishes the opt-out.
   */
  @Test
  public void testLogsContextServesSymlinkByDefault(@TempDir Path tmp)
      throws Exception {
    Path logDir = Files.createDirectory(tmp.resolve("logs"));
    Files.write(logDir.resolve("real.log"), "log line".getBytes(StandardCharsets.UTF_8));
    Path outside = Files.createDirectory(tmp.resolve("outside"));
    Path target = Files.write(outside.resolve("secret.log"),
        "outside".getBytes(StandardCharsets.UTF_8));
    Files.createSymbolicLink(logDir.resolve("link.log"), target);
    HttpServer2 server = startLogsServer(logDir, new OzoneConfiguration());
    try {
      String base = "http://localhost:"
          + server.getConnectorAddress(0).getPort() + "/logs/";
      assertEquals(HttpURLConnection.HTTP_OK, statusOf(base + "real.log"),
          "a regular log file must be served");
      assertEquals(HttpURLConnection.HTTP_OK, statusOf(base + "link.log"),
          "symlinked log entries should be served by default");
    } finally {
      server.stop();
    }
  }

  /**
   * With hadoop.jetty.logs.serve.aliases=false the opt-out must actually take
   * effect: on a running server a symlink that escapes the log directory is
   * denied with a 404 while the regular file beside it is still served, rather
   * than only asserting Jetty's alias-check list is empty.
   */
  @Test
  public void testLogsContextOptOutDeniesSymlink(@TempDir Path tmp)
      throws Exception {
    Path logDir = Files.createDirectory(tmp.resolve("logs"));
    Files.write(logDir.resolve("real.log"), "log line".getBytes(StandardCharsets.UTF_8));
    Path outside = Files.createDirectory(tmp.resolve("outside"));
    Path target = Files.write(outside.resolve("secret.log"),
        "outside".getBytes(StandardCharsets.UTF_8));
    Files.createSymbolicLink(logDir.resolve("link.log"), target);
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean("hadoop.jetty.logs.serve.aliases", false);
    HttpServer2 server = startLogsServer(logDir, conf);
    try {
      String base = "http://localhost:"
          + server.getConnectorAddress(0).getPort() + "/logs/";
      assertEquals(HttpURLConnection.HTTP_OK, statusOf(base + "real.log"),
          "a regular log file must still be served with the opt-out");
      assertEquals(HttpURLConnection.HTTP_NOT_FOUND, statusOf(base + "link.log"),
          "opt-out must deny symlinked log entries that escape the log dir");
    } finally {
      server.stop();
    }
  }

  /**
   * When hadoop.log.dir points at a directory that does not yet exist, the
   * server creates it and serves "/logs" from it. This is new behavior added
   * because Jetty 12 refuses to start a context whose base resource is missing;
   * Jetty 9.4 silently tolerated it.
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
   * When hadoop.log.dir is itself a symlink to an existing directory -- a common packaging layout,
   * e.g. /opt/ozone/logs -> /var/log/ozone -- "/logs" must still be served. Before JDK 20
   * (JDK-8294193) Files.createDirectories throws FileAlreadyExistsException for a symlink to a
   * directory, so creating unconditionally would drop the context on the JDK 17 runtime the server
   * supports, while a JDK 21+ runtime would hide it.
   */
  @Test
  public void testLogsContextServedWhenLogDirIsSymlink(@TempDir Path parent)
      throws Exception {
    Path realLogDir = Files.createDirectory(parent.resolve("real-logs"));
    Path linkedLogDir = Files.createSymbolicLink(parent.resolve("logs"), realLogDir);
    ServletContextHandler logs = buildLogsContext(linkedLogDir, new OzoneConfiguration());
    assertNotNull(logs, "/logs must be served when hadoop.log.dir is a symlink to a directory");
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

  /**
   * Exercises the {@code /logLevel} servlet end to end on an embedded server.
   * The servlet was reimplemented with inlined ServletUtil helpers during the
   * jakarta port, so a broken registration, content type or parameter trimming
   * would otherwise go unnoticed. A GET with a logger and level returns 200
   * text/html, reports the new effective level and actually reconfigures the
   * log4j logger; a blank (whitespace-only) log parameter is trimmed to nothing
   * by the inlined getParameter helper, so the form is returned without changing
   * any logger.
   */
  @Test
  public void testLogLevelServletGetsAndSetsLevel() throws Exception {
    String probe = "org.apache.hadoop.hdds.server.http.LogLevelServletProbe";
    Logger probeLogger = Logger.getLogger(probe);
    Level original = probeLogger.getLevel();
    HttpServer2 server = new HttpServer2.Builder()
        .setConf(new OzoneConfiguration())
        .setName("test")
        .addEndpoint(URI.create("http://localhost:0"))
        .build();
    server.start();
    try {
      String base = "http://localhost:" + server.getConnectorAddress(0).getPort()
          + "/logLevel";

      // Set the level through the servlet: 200 text/html, the effective level is
      // reported back and the log4j logger is actually reconfigured.
      probeLogger.setLevel(Level.INFO);
      HttpURLConnection set = (HttpURLConnection)
          new URL(base + "?log=" + probe + "&level=DEBUG").openConnection();
      set.setConnectTimeout(5000);
      set.setReadTimeout(5000);
      assertEquals(HttpURLConnection.HTTP_OK, set.getResponseCode());
      assertTrue(set.getContentType().startsWith("text/html"),
          "log level page must be served as HTML");
      String body = readBody(set);
      assertTrue(body.contains("Setting Level to DEBUG"),
          "servlet must report the level change");
      assertTrue(body.contains("Effective Level: <b>DEBUG</b>"),
          "servlet must report the new effective level");
      assertEquals(Level.DEBUG, probeLogger.getEffectiveLevel(),
          "the log4j logger must actually be reconfigured");

      // A blank (whitespace-only) log parameter is trimmed to null by the inlined
      // getParameter helper: the form is returned and no logger is changed.
      probeLogger.setLevel(Level.INFO);
      HttpURLConnection blank = (HttpURLConnection)
          new URL(base + "?log=%20").openConnection();
      blank.setConnectTimeout(5000);
      blank.setReadTimeout(5000);
      assertEquals(HttpURLConnection.HTTP_OK, blank.getResponseCode());
      assertTrue(blank.getContentType().startsWith("text/html"));
      String form = readBody(blank);
      assertTrue(form.contains("Get Log Level"),
          "blank request must return the get/set form");
      assertFalse(form.contains("Results"),
          "blank request must not run the get/set path");
      assertEquals(Level.INFO, probeLogger.getEffectiveLevel(),
          "a blank log parameter must not change any logger");
    } finally {
      probeLogger.setLevel(original);
      server.stop();
    }
  }

  /**
   * An operator-configured {@code ozone.http.filter.initializers} entry that
   * registers a javax.servlet.Filter outside the set Ozone can bridge into Jetty
   * EE10 must abort server construction with an
   * {@link HttpServerConfigurationException} rather than being silently dropped.
   * This proves the exception {@code ServletElementsFactory} throws propagates
   * all the way out of {@code HttpServer2.Builder.build()} through the filter
   * initializer path, which is what lets OM, SCM and DN fail fast on such a
   * misconfiguration instead of coming up with a degraded web server.
   */
  @Test
  public void testNonBridgeableFilterInitializerFailsBuild() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
        NonBridgeableFilterInitializer.class.getName());
    HttpServer2.Builder builder = new HttpServer2.Builder()
        .setConf(conf)
        .setName("test")
        .addEndpoint(URI.create("http://localhost:0"));
    assertThrows(HttpServerConfigurationException.class, builder::build);
  }

  /**
   * "/conf" is registered by {@link HttpServer2#addDefaultServlets()}, so no service-specific code
   * puts it on the server any more and nothing but a wire request proves it is still reachable --
   * a dropped or duplicated registration would otherwise fail no test. Driving it on a running
   * server also pins the redaction of sensitive values end to end, on the XML branch in particular,
   * which emitted them in clear text until {@code HddsConfServlet} was given a redactor.
   *
   * <p>The format comes from the {@code Accept} header rather than a query parameter, and an
   * absent header means XML, so both branches are reachable from here.
   */
  @Test
  public void servesConfOverTheWireWithSecretsRedacted() throws Exception {
    final String knownKey = "ozone.test.wire.key";
    final String knownValue = "wire-value";
    final String secretKey = "test.ssl.keystore.password";
    final String secretValue = "must-not-appear-on-the-wire";
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(knownKey, knownValue);
    conf.set(secretKey, secretValue);

    HttpServer2 server = new HttpServer2.Builder()
        .setConf(conf)
        .setName("test")
        .addEndpoint(URI.create("http://localhost:0"))
        .configureXFrame(true)
        .build();
    server.start();
    try {
      String confUrl =
          "http://localhost:" + server.getConnectorAddress(0).getPort() + "/conf";

      for (String accept : new String[] {null, "application/json"}) {
        String what = accept == null ? "xml (no Accept header)" : accept;
        HttpURLConnection conn = connectTo(confUrl, accept);
        assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode(),
            "/conf must be reachable on a running server: " + what);
        assertNotNull(conn.getHeaderField("X-FRAME-OPTIONS"),
            "configureXFrame(true) must set the header on /conf responses");
        String body = readBody(conn);
        assertTrue(body.contains(knownKey) && body.contains(knownValue),
            "/conf must report a configured key: " + what);
        assertTrue(body.contains(secretKey),
            "/conf must still list the sensitive key: " + what);
        assertFalse(body.contains(secretValue),
            "/conf must not expose the sensitive value: " + what);
      }
    } finally {
      server.stop();
    }
  }

  private static HttpURLConnection connectTo(String url, String accept) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(5000);
    if (accept != null) {
      conn.setRequestProperty("Accept", accept);
    }
    return conn;
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

  /**
   * FilterInitializer that registers a non-bridgeable javax filter, standing in
   * for an operator-configured ozone.http.filter.initializers entry.
   */
  public static class NonBridgeableFilterInitializer extends FilterInitializer {
    @Override
    public void initFilter(FilterContainer container, Configuration conf) {
      container.addFilter("nonbridgeable",
          NonBridgeableJavaxFilter.class.getName(), new HashMap<>());
    }
  }

  /**
   * A plain javax.servlet.Filter outside the set ServletElementsFactory can
   * bridge into Jetty EE10, so registering it must be rejected.
   */
  public static class NonBridgeableJavaxFilter implements javax.servlet.Filter {
    @Override
    public void init(javax.servlet.FilterConfig filterConfig) {
    }

    @Override
    public void doFilter(javax.servlet.ServletRequest request,
        javax.servlet.ServletResponse response, javax.servlet.FilterChain chain)
        throws IOException, javax.servlet.ServletException {
      chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
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

  /**
   * Builds and starts a server whose "/logs" context is backed by the given log
   * directory. hadoop.log.dir must be set before build() so addDefaultApps picks
   * it up; the running context keeps its base resource once started, so the
   * property is restored immediately afterwards.
   */
  private static HttpServer2 startLogsServer(
      Path logDir, OzoneConfiguration conf) throws Exception {
    String previous = System.getProperty("hadoop.log.dir");
    System.setProperty("hadoop.log.dir", logDir.toString());
    try {
      HttpServer2 server = new HttpServer2.Builder()
          .setConf(conf)
          .setName("test")
          .addEndpoint(URI.create("http://localhost:0"))
          .build();
      server.start();
      return server;
    } finally {
      if (previous == null) {
        System.clearProperty("hadoop.log.dir");
      } else {
        System.setProperty("hadoop.log.dir", previous);
      }
    }
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
