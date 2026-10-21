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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.eclipse.jetty.http.UriCompliance;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
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
   * When ozone.http.basedir is not set, the base dir must be created under the
   * system temporary directory (java.io.tmpdir), not the process working
   * directory, so startup does not fail when the CWD is not writable.
   */
  @Test
  public void testSetHttpBaseDirUsesSystemTempDir() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.unset(OzoneConfigKeys.OZONE_HTTP_BASEDIR);

    HttpServer2.setHttpBaseDir(conf);

    String baseDir = conf.get(OzoneConfigKeys.OZONE_HTTP_BASEDIR);
    assertThat(baseDir).isNotEmpty();
    Path baseDirPath = Paths.get(baseDir).toAbsolutePath().normalize();
    assertThat(baseDirPath).exists();
    Path systemTmpDir = Paths.get(System.getProperty("java.io.tmpdir"))
        .toAbsolutePath().normalize();
    assertThat(baseDirPath.startsWith(systemTmpDir)).isTrue();
  }

  /**
   * The base dir must be owner-only (rwx------) so that only the user running
   * the service can access it, since java.io.tmpdir is shared.
   */
  @Test
  public void testSetHttpBaseDirIsOwnerOnly() throws Exception {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.unset(OzoneConfigKeys.OZONE_HTTP_BASEDIR);

    HttpServer2.setHttpBaseDir(conf);

    Path baseDirPath = Paths.get(conf.get(OzoneConfigKeys.OZONE_HTTP_BASEDIR));
    assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(baseDirPath))).isEqualTo("rwx------");
  }

  /**
   * By default ambiguous URIs (e.g. empty path segments from "//") are rejected
   * with a 400: the connector uses Jetty's strict URI compliance.
   */
  @Test
  public void testUriComplianceStrictByDefault() throws Exception {
    HttpServer2 srv = buildServer(false);
    assertSame(UriCompliance.DEFAULT, uriComplianceOf(srv));
  }

  /**
   * With allowAmbiguousUri the connector relaxes the ambiguous empty segments
   * ("//"), percent encodings ("%25") and encoded path separators that S3 object
   * keys and WebHDFS paths need, plus the suspicious path characters (backslash,
   * DEL, C0 controls) that Jetty 9.4 passed through. Unlike Jetty's LEGACY mode
   * it must not re-admit %2e/%2e%2e path traversal, UTF-16 or truncated UTF-8
   * encodings or userinfo.
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
  }

  /**
   * The S3 Gateway additionally needs percent-encoded "." and ".." segments,
   * which an object key may contain. Its mode is the relaxed mode plus
   * AMBIGUOUS_PATH_SEGMENT; the HttpFS mode above stays without it.
   */
  @Test
  public void testUriComplianceAllowsEncodedDotSegmentsForS3()
      throws Exception {
    HttpServer2 srv = buildServer(true, true);
    assertEquals(EnumSet.of(
        UriCompliance.Violation.AMBIGUOUS_EMPTY_SEGMENT,
        UriCompliance.Violation.AMBIGUOUS_PATH_ENCODING,
        UriCompliance.Violation.AMBIGUOUS_PATH_SEPARATOR,
        UriCompliance.Violation.SUSPICIOUS_PATH_CHARACTERS,
        UriCompliance.Violation.AMBIGUOUS_PATH_SEGMENT),
        uriComplianceOf(srv).getAllowed());
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
   * An unencoded "#" in the request target is rejected with a 400 rather than
   * silently truncating the object key. Jetty 12.1 added
   * {@link UriCompliance.Violation#FRAGMENT}, which the relaxed Ozone modes
   * deliberately do not allow: before it existed, Jetty split such a target and
   * handed the servlet only the part before the "#", so a request for the S3 key
   * "my#key" operated on "my" with a 200. Rejecting the malformed target is the
   * safer behavior, so this is pinned rather than relaxed. A conforming client
   * percent-encodes the character, and "%23" still reaches the servlet.
   */
  @Test
  public void testUnencodedFragmentRejectedRatherThanTruncatingKey()
      throws Exception {
    HttpServer2 server = new HttpServer2.Builder()
        .setConf(new OzoneConfiguration())
        .setName("test")
        .addEndpoint(URI.create("http://localhost:0"))
        .allowAmbiguousUri(true)
        .allowEncodedDotSegments(true)
        .build();
    server.addServlet("echo", "/echo/*", OkServlet.class);
    server.start();
    try {
      int port = server.getConnectorAddress(0).getPort();
      assertEquals(HttpURLConnection.HTTP_BAD_REQUEST,
          rawStatusOf(port, "/echo/my#key"));
      assertEquals(HttpURLConnection.HTTP_OK,
          rawStatusOf(port, "/echo/my%23key"));
    } finally {
      server.stop();
    }
  }

  /**
   * With allowEncodedDotSegments -- the S3 Gateway mode -- the encoded "." and
   * ".." segments an object key may contain reach the servlet instead of being
   * rejected with a 400, as Jetty 9.4 (pre-migration) accepted them. The %u
   * UTF-16 encodings that Jetty's LEGACY mode would re-admit stay rejected.
   */
  @Test
  public void testEncodedDotSegmentsAdmittedWhenAllowed() throws Exception {
    HttpServer2 server = new HttpServer2.Builder()
        .setConf(new OzoneConfiguration())
        .setName("test")
        .addEndpoint(URI.create("http://localhost:0"))
        .allowAmbiguousUri(true)
        .allowEncodedDotSegments(true)
        .build();
    server.addServlet("echo", "/echo/*", OkServlet.class);
    server.start();
    try {
      String base = "http://localhost:" + server.getConnectorAddress(0).getPort();
      assertEquals(HttpURLConnection.HTTP_OK,
          statusOf(base + "/echo/dir/%2e/file.txt"));
      assertEquals(HttpURLConnection.HTTP_OK,
          statusOf(base + "/echo/dir/%2e%2e/file.txt"));
      assertEquals(HttpURLConnection.HTTP_BAD_REQUEST,
          statusOf(base + "/echo/%u002e"));
    } finally {
      server.stop();
    }
  }

  /**
   * Every default servlet must answer on a running server. hadoop-common's
   * JMXJsonServlet and ConfServlet route their admin access check through
   * org.apache.hadoop.http.HttpServer2, which cannot be loaded on a Jetty 12
   * classpath because it references the removed
   * org.eclipse.jetty.servlet.ServletContextHandler, so "/jmx" and "/conf" would
   * answer 500 unless Ozone's own servlets are registered instead.
   */
  @Test
  public void testDefaultServletsAnswer() throws Exception {
    HttpServer2 server = new HttpServer2.Builder()
        .setConf(new OzoneConfiguration())
        .setName("test")
        .addEndpoint(URI.create("http://localhost:0"))
        .build();
    server.start();
    try {
      String base = "http://localhost:" + server.getConnectorAddress(0).getPort();
      assertEquals(HttpURLConnection.HTTP_OK, statusOf(base + "/jmx"));
      assertEquals(HttpURLConnection.HTTP_OK, statusOf(base + "/conf"));
      assertEquals(HttpURLConnection.HTTP_OK, statusOf(base + "/stacks"));
      assertEquals(HttpURLConnection.HTTP_OK, statusOf(base + "/logLevel"));
    } finally {
      server.stop();
    }
  }

  /**
   * The "/logs" context serves symlinked entries, matching Jetty 9.4. Drive it
   * over the wire on a running server with a symlink that escapes the log
   * directory, rather than only asserting Jetty's alias-check list is non-empty.
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
    ContextHandler logs = buildLogsContext(logDir, new OzoneConfiguration());
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
    ContextHandler logs = buildLogsContext(linkedLogDir, new OzoneConfiguration());
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
    ContextHandler logs = buildLogsContext(logFile, new OzoneConfiguration());
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

  /**
   * Sends the request target on a raw socket, so that characters a URL-based
   * client would rewrite or strip -- notably an unencoded "#", which
   * {@link URL} treats as a fragment and never puts on the wire -- reach the
   * connector verbatim.
   */
  private static int rawStatusOf(int port, String target) throws IOException {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", port), 5000);
      socket.setSoTimeout(5000);
      socket.getOutputStream().write(("GET " + target + " HTTP/1.1\r\n"
          + "Host: localhost\r\nConnection: close\r\n\r\n")
          .getBytes(StandardCharsets.ISO_8859_1));
      socket.getOutputStream().flush();
      BufferedReader reader = new BufferedReader(new InputStreamReader(
          socket.getInputStream(), StandardCharsets.ISO_8859_1));
      String statusLine = reader.readLine();
      assertNotNull(statusLine, "no response for " + target);
      return Integer.parseInt(statusLine.split(" ")[1]);
    }
  }

  /** Servlet that returns 200 for any request, used to probe URI admission. */
  public static class OkServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
      resp.setStatus(HttpServletResponse.SC_OK);
    }
  }

  private static HttpServer2 buildServer(boolean allowAmbiguousUri)
      throws Exception {
    return buildServer(allowAmbiguousUri, false);
  }

  private static HttpServer2 buildServer(boolean allowAmbiguousUri,
      boolean allowEncodedDotSegments) throws Exception {
    return new HttpServer2.Builder()
        .setConf(new OzoneConfiguration())
        .setName("test")
        .addEndpoint(URI.create("http://example.com/"))
        .allowAmbiguousUri(allowAmbiguousUri)
        .allowEncodedDotSegments(allowEncodedDotSegments)
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

  private static ContextHandler buildLogsContext(
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

  /**
   * Finds the "/logs" context in the core handler tree. An EE8 context is not a
   * core Handler itself: it contributes a core ContextHandler, which is what the
   * tree holds and what this matches on.
   */
  private static ContextHandler findLogsContext(Handler handler) {
    if (handler instanceof ContextHandler
        && "/logs".equals(((ContextHandler) handler).getContextPath())) {
      return (ContextHandler) handler;
    }
    if (handler instanceof Handler.Container) {
      for (Handler child : ((Handler.Container) handler).getHandlers()) {
        ContextHandler found = findLogsContext(child);
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
