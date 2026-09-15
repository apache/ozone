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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests the JSON/query contract of the forked {@link JMXJsonServlet}.
 *
 * <p>Ported from hadoop-common's {@code TestJMXJsonServlet} so the qry/get
 * parameter handling and the 400/404 paths of the forked servlet are covered by
 * the unit gate; previously only acceptance robots parsed the /jmx body. The
 * JSONP {@code callback} parameter of the original servlet is not present in the
 * fork, so it is not exercised here.</p>
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
  void testTraceRequest() throws Exception {
    HttpURLConnection conn = (HttpURLConnection)
        new URL(baseUrl, "/jmx").openConnection();
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(5000);
    conn.setRequestMethod("TRACE");
    assertEquals(HttpURLConnection.HTTP_BAD_METHOD, conn.getResponseCode());
  }

  private static HttpURLConnection get(String path) throws IOException {
    HttpURLConnection conn =
        (HttpURLConnection) new URL(baseUrl, path).openConnection();
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
}
