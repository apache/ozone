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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests the JSON/query contract of the forked {@link JMXJsonServlet}.
 *
 * <p>Ported from hadoop-common's {@code TestJMXJsonServlet} so the qry/get
 * parameter handling and the servlet's error handling are covered by the unit
 * gate; previously only acceptance robots parsed the /jmx body. The servlet
 * streams and flushes the JSON body before it calls setStatus, so a bad or
 * unknown get request is already committed at HTTP 200: its error surfaces as
 * {@code "result":"ERROR"} in the body rather than as a 400/404 wire status,
 * and that is what these tests assert. A malformed {@code qry} does not get even
 * that far -- it fails before the beans array is written, so the body carries no
 * error member at all. The JSONP {@code callback} parameter of
 * the original servlet is not present in the fork, so it is not exercised
 * here.</p>
 */
public class TestJMXJsonServlet {

  private static HttpServer2 server;
  private static URL baseUrl;

  @BeforeAll
  static void setUp() throws Exception {
    server = new HttpServer2.Builder()
        .setConf(new OzoneConfiguration())
        .setName("test")
        .addEndpoint(URI.create("http://localhost:0"))
        .build();
    server.start();
    baseUrl = new URL("http://localhost:"
        + server.getConnectorAddress(0).getPort() + "/");
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  void testQuery() throws Exception {
    String result = readBody(get("/jmx?qry=java.lang:type=Runtime"));
    assertReFind("\"name\"\\s*:\\s*\"java.lang:type=Runtime\"", result);
    assertReFind("\"modelerType\"", result);

    result = readBody(get("/jmx?qry=java.lang:type=Memory"));
    assertReFind("\"name\"\\s*:\\s*\"java.lang:type=Memory\"", result);
    assertReFind("\"modelerType\"", result);

    result = readBody(get("/jmx"));
    assertReFind("\"beans\"\\s*:", result);
  }

  @Test
  void testGetAttribute() throws Exception {
    HttpURLConnection conn = get("/jmx?get=java.lang:type=Memory::HeapMemoryUsage");
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    assertReFind("\"committed\"\\s*:", readBody(conn));
    assertEquals("GET",
        conn.getHeaderField(JMXJsonServlet.ACCESS_CONTROL_ALLOW_METHODS));
    assertNotNull(
        conn.getHeaderField(JMXJsonServlet.ACCESS_CONTROL_ALLOW_ORIGIN));
  }

  @Test
  void testGetWithBadFormat() throws Exception {
    // "::" is stripped as a trailing separator, leaving a single token, so the
    // get value does not split into name::attribute and is reported as an error.
    // The servlet streams and flushes the JSON body before it calls setStatus,
    // so the response is already committed at 200 and the error surfaces only in
    // the body, not the wire status.
    HttpURLConnection conn = get("/jmx?get=java.lang:type=Memory::");
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    assertReFind("\"result\"\\s*:\\s*\"ERROR\"", readBody(conn));
  }

  @Test
  void testGetUnknownAttribute() throws Exception {
    // A missing attribute is likewise reported as an error in the body; the
    // status stays 200 because the beans array is already streamed before the
    // servlet attempts setStatus (see testGetWithBadFormat).
    HttpURLConnection conn =
        get("/jmx?get=java.lang:type=Memory::NoSuchAttribute");
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    assertReFind("\"result\"\\s*:\\s*\"ERROR\"", readBody(conn));
  }

  @Test
  void testQueryWithBadFormat() throws Exception {
    // The other half of the 400 the servlet sets and never sends: an ObjectName
    // that does not parse throws before any bean is written, so the response is
    // committed at 200 with an empty JSON object -- there is not even a
    // "result":"ERROR" member to inspect. The class Javadoc documents this, so
    // pin it.
    HttpURLConnection conn = get("/jmx?qry=java.lang:type");
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    String body = readBody(conn);
    assertReNotFind("\"beans\"\\s*:", body);
    assertReNotFind("\"result\"\\s*:", body);
    assertEquals("{ }", body.trim(), "malformed qry must yield an empty JSON object");
  }

  @Test
  void testTraceRequest() throws Exception {
    HttpURLConnection conn = (HttpURLConnection)
        new URL(baseUrl, "/jmx").openConnection();
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(5000);
    conn.setRequestMethod("TRACE");
    assertEquals(HttpURLConnection.HTTP_BAD_METHOD, conn.getResponseCode());
  }

  /**
   * The servlet gates every response on
   * {@code hadoop.security.instrumentation.requires.admin}, which is off by default and so is left
   * permissive by every other test here. Its own server, because the shared one above must stay
   * permissive for them.
   *
   * <p>/jmx exposes the daemon's full MBean surface, so a fork that dropped or inverted the guard
   * would publish it to any caller while all the body-shape tests above still passed. Simple
   * (pseudo) auth stands in for kerberos -- it selects the user per request with no KDC -- and
   * anonymous access is left on so all three outcomes of the guard are reachable from one server:
   * an unauthenticated caller, an authenticated caller outside the admin ACL, and an admin.
   */
  @Test
  void testInstrumentationRequiresAdmin() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN, true);
    // Without this the admin check short-circuits to "everybody is an admin".
    conf.setBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true);
    HttpServer2 adminOnly = new HttpServer2.Builder()
        .setConf(conf)
        .setName("test")
        .addEndpoint(URI.create("http://localhost:0"))
        .setACL(new AccessControlList("jmxadmin"))
        .build();

    Map<String, String> params = new HashMap<>();
    params.put("type", "simple");
    params.put("simple.anonymous.allowed", "true");
    params.put("signature.secret", "jmx-admin-test-secret");
    adminOnly.addGlobalFilter("auth",
        "org.apache.hadoop.security.authentication.server.AuthenticationFilter",
        params);
    adminOnly.start();
    try {
      URL adminOnlyUrl = new URL("http://localhost:"
          + adminOnly.getConnectorAddress(0).getPort() + "/");

      HttpURLConnection anonymous = get(adminOnlyUrl, "/jmx");
      assertEquals(HttpURLConnection.HTTP_FORBIDDEN, anonymous.getResponseCode(),
          "an unauthenticated caller must not reach the MBean dump");
      assertReNotFind("\"beans\"\\s*:", readBody(anonymous));

      assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
          get(adminOnlyUrl, "/jmx?user.name=intruder").getResponseCode(),
          "a caller outside the admin ACL must not reach the MBean dump");

      HttpURLConnection admin = get(adminOnlyUrl, "/jmx?user.name=jmxadmin");
      assertEquals(HttpURLConnection.HTTP_OK, admin.getResponseCode());
      assertReFind("\"beans\"\\s*:", readBody(admin));
    } finally {
      adminOnly.stop();
    }
  }

  private static HttpURLConnection get(String path) throws IOException {
    return get(baseUrl, path);
  }

  private static HttpURLConnection get(URL base, String path) throws IOException {
    HttpURLConnection conn =
        (HttpURLConnection) new URL(base, path).openConnection();
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(5000);
    return conn;
  }

  private static String readBody(HttpURLConnection conn) throws IOException {
    InputStream in = conn.getResponseCode() < HttpURLConnection.HTTP_BAD_REQUEST
        ? conn.getInputStream() : conn.getErrorStream();
    if (in == null) {
      return "";
    }
    try {
      return IOUtils.toString(in, StandardCharsets.UTF_8);
    } finally {
      in.close();
    }
  }

  private static void assertReFind(String re, String value) {
    assertTrue(Pattern.compile(re).matcher(value).find(),
        "'" + re + "' does not match " + value);
  }

  private static void assertReNotFind(String re, String value) {
    assertFalse(Pattern.compile(re).matcher(value).find(),
        "'" + re + "' unexpectedly matches " + value);
  }
}
