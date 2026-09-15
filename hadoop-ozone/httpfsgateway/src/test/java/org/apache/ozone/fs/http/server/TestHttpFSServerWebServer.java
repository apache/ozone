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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.http.HttpServer2;
import org.eclipse.jetty.http.UriCompliance;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link HttpFSServerWebServer}.
 */
public class TestHttpFSServerWebServer {

  /**
   * WebHDFS paths (/webhdfs/v1/&lt;path&gt;) carry user file names that can
   * contain characters ('%' arrives as %25) or empty segments ("//") which
   * Jetty 12 rejects with 400 by default. The HttpFS web server must allow such
   * ambiguous URIs, which relaxes the connector's URI compliance to admit
   * empty path segments, percent encodings and encoded path separators (but not
   * %2e path traversal or UTF-16 encodings). {@code TestHttpServer2} covers that
   * this mode also decodes ambiguous URIs in the servlet layer.
   */
  @Test
  public void allowsAmbiguousUris() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    HttpFSServerWebServer webServer =
        new HttpFSServerWebServer(conf, new Configuration(false));

    ServerConnector connector = (ServerConnector) jettyServer(webServer)
        .getConnectors()[0];
    assertEquals(EnumSet.of(
        UriCompliance.Violation.AMBIGUOUS_EMPTY_SEGMENT,
        UriCompliance.Violation.AMBIGUOUS_PATH_ENCODING,
        UriCompliance.Violation.AMBIGUOUS_PATH_SEPARATOR),
        connector.getConnectionFactory(HttpConnectionFactory.class)
            .getHttpConfiguration().getUriCompliance().getAllowed());
  }

  /**
   * Boots the full {@link HttpFSServerWebServer} with simple (pseudo) auth and a
   * signature secret, then drives real HTTP requests through the bridged
   * {@link HttpFSAuthenticationFilter}. HttpFS wraps hadoop-auth's
   * {@code javax.servlet} {@code DelegationTokenAuthenticationFilter} in the
   * {@code JavaxFilterBridge}; nothing else in this module starts the web app or
   * sends a request through it, so this exercises the bridged filter end to end:
   * an anonymous request (anonymous access disallowed) is refused with 401
   * before reaching the WebHDFS servlet; a request carrying {@code user.name}
   * passes the bridged filter and reaches a WebHDFS operation (GETFILESTATUS on
   * "/", served by the default {@code file:///} file system); and a
   * GETDELEGATIONTOKEN request, handled inside the bridged delegation-token
   * filter, issues a token for the authenticated user while an anonymous one is
   * refused.
   */
  @Test
  public void bridgedAuthFilterGuardsEmbeddedServer(@TempDir Path baseDir)
      throws Exception {
    Map<String, String> saved = new HashMap<>();
    HttpFSServerWebServer webServer = configureWebServer(baseDir, false, saved);
    try {
      webServer.start();
      String base = webServer.getUrl().toString() + "/v1/";

      // Anonymous with anonymous access disallowed: the bridged filter refuses
      // it with 401 before it can reach the WebHDFS servlet.
      assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED,
          statusOf(base + "?op=GETFILESTATUS"));

      // user.name=alice passes the bridged filter and reaches a WebHDFS
      // operation (GETFILESTATUS on the file:/// root).
      HttpURLConnection status =
          openConnection(base + "?op=GETFILESTATUS&user.name=alice");
      assertEquals(HttpURLConnection.HTTP_OK, status.getResponseCode());
      assertTrue(readBody(status).contains("FileStatus"),
          "expected a WebHDFS FileStatus response");

      // GETDELEGATIONTOKEN is handled inside the bridged delegation-token
      // filter: anonymous is refused (403, since the token operation requires
      // real credentials), the authenticated user gets a token.
      assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
          statusOf(base + "?op=GETDELEGATIONTOKEN"));
      HttpURLConnection token =
          openConnection(base + "?op=GETDELEGATIONTOKEN&user.name=alice");
      assertEquals(HttpURLConnection.HTTP_OK, token.getResponseCode());
      assertTrue(readBody(token).contains("Token"),
          "expected a delegation token response");
    } finally {
      webServer.stop();
      restore(saved);
    }
  }

  /**
   * Request-level counterpart to {@link #allowsAmbiguousUris} (which only checks
   * the connector's configured compliance): drives real WebHDFS requests through
   * the running HttpFS server. An empty path segment ("//", the WebHDFS analogue
   * of an S3 "//dir1" key) and a "%25" percent encoding -- which Jetty 12 rejects
   * with 400 by default -- are admitted by the relaxed connector and delivered to
   * the WebHDFS servlet, which resolves them to a real (here non-existent) file
   * and answers 404 rather than the connector's 400. "%2e%2e" path traversal,
   * which the relaxed mode still does not admit, is rejected at the connector with
   * a 400 before it can reach the servlet.
   */
  @Test
  public void admitsAmbiguousUriPathsButRejectsTraversal(@TempDir Path baseDir)
      throws Exception {
    Map<String, String> saved = new HashMap<>();
    HttpFSServerWebServer webServer = configureWebServer(baseDir, false, saved);
    try {
      webServer.start();
      String base = webServer.getUrl().toString() + "/v1";

      // Empty segment ("//") and percent encoding ("%25") reach the WebHDFS
      // servlet, which resolves them ("/a//b" -> "/a/b", "/a%25b" -> "/a%b") and
      // reports the file does not exist (404). A strict connector would reject
      // both with 400 before the servlet.
      assertEquals(HttpURLConnection.HTTP_NOT_FOUND,
          statusOf(base + "/a//b?op=GETFILESTATUS&user.name=alice"));
      assertEquals(HttpURLConnection.HTTP_NOT_FOUND,
          statusOf(base + "/a%25b?op=GETFILESTATUS&user.name=alice"));

      // "%2e%2e" path traversal is outside the S3/WebHDFS use case and stays
      // rejected at the connector with a 400, before auth or the servlet run.
      assertEquals(HttpURLConnection.HTTP_BAD_REQUEST,
          statusOf(base + "/%2e%2e/x?op=GETFILESTATUS&user.name=alice"));
    } finally {
      webServer.stop();
      restore(saved);
    }
  }

  /**
   * Writes the HttpFS config (simple auth, a signature secret, anonymous access
   * per {@code anonymousAllowed}) and the {@code httpfs.*} system properties the
   * web app requires, then builds an {@link HttpFSServerWebServer} bound to an
   * ephemeral local port. The caller starts it and must {@link #restore} the
   * saved system properties in a finally block.
   */
  private static HttpFSServerWebServer configureWebServer(Path baseDir,
      boolean anonymousAllowed, Map<String, String> saved) throws Exception {
    Path confDir = Files.createDirectories(baseDir.resolve("conf"));
    Path secretFile = confDir.resolve("httpfs-signature.secret");
    Files.write(secretFile,
        "embedded-test-secret".getBytes(StandardCharsets.UTF_8));
    Files.write(confDir.resolve("httpfs-site.xml"), (
        "<?xml version=\"1.0\"?>\n"
        + "<configuration>\n"
        + "  <property>\n"
        + "    <name>hadoop.http.authentication.simple.anonymous.allowed</name>\n"
        + "    <value>" + anonymousAllowed + "</value>\n"
        + "  </property>\n"
        + "  <property>\n"
        + "    <name>hadoop.http.authentication.signature.secret.file</name>\n"
        + "    <value>" + secretFile + "</value>\n"
        + "  </property>\n"
        + "</configuration>\n").getBytes(StandardCharsets.UTF_8));

    setProperty(saved, "httpfs.home.dir", baseDir.toString());
    setProperty(saved, "httpfs.config.dir", confDir.toString());
    setProperty(saved, "httpfs.log.dir",
        Files.createDirectories(baseDir.resolve("log")).toString());
    setProperty(saved, "httpfs.temp.dir",
        Files.createDirectories(baseDir.resolve("temp")).toString());

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("httpfs.http.hostname", "localhost");
    conf.setInt("httpfs.http.port", 0);
    return new HttpFSServerWebServer(conf, new Configuration(false));
  }

  private static HttpURLConnection openConnection(String url)
      throws IOException {
    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(5000);
    return conn;
  }

  private static int statusOf(String url) throws IOException {
    HttpURLConnection conn = openConnection(url);
    try {
      return conn.getResponseCode();
    } finally {
      conn.disconnect();
    }
  }

  private static String readBody(HttpURLConnection conn) throws IOException {
    try (InputStream in = conn.getInputStream()) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buffer = new byte[1024];
      int read = in.read(buffer);
      while (read > -1) {
        out.write(buffer, 0, read);
        read = in.read(buffer);
      }
      return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
  }

  private static void setProperty(Map<String, String> saved, String key,
      String value) {
    saved.put(key, System.getProperty(key));
    System.setProperty(key, value);
  }

  private static void restore(Map<String, String> saved) {
    for (Map.Entry<String, String> entry : saved.entrySet()) {
      if (entry.getValue() == null) {
        System.clearProperty(entry.getKey());
      } else {
        System.setProperty(entry.getKey(), entry.getValue());
      }
    }
  }

  private static Server jettyServer(HttpFSServerWebServer webServer)
      throws ReflectiveOperationException {
    Field httpServerField =
        HttpFSServerWebServer.class.getDeclaredField("httpServer");
    httpServerField.setAccessible(true);
    HttpServer2 httpServer = (HttpServer2) httpServerField.get(webServer);

    Field webServerField = HttpServer2.class.getDeclaredField("webServer");
    webServerField.setAccessible(true);
    return (Server) webServerField.get(httpServer);
  }
}
