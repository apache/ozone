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

package org.apache.hadoop.ozone.s3;

import static org.apache.ozone.test.GenericTestUtils.PortAllocator.localhostWithFreePort;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.ozone.test.GenericTestUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration test class to verify the routing and direct access functionality
 * of the ProxyServer.
 */
public class ProxyServerIntegrationTest {
  private static final int NUM_SERVERS = 3;
  private static final String SERVICE_PATH = "/service-name";

  private static final List<Server> SERVERS = new ArrayList<>();
  private static final List<String> ENDPOINTS = new ArrayList<>();
  private static ProxyServer proxy;
  private static String proxyUrl;

  @BeforeAll
  public static void setup() throws Exception {
    for (int i = 0; i < NUM_SERVERS; i++) {
      startMockServer("server-" + i);
    }
    waitForMockServersStarted();
    startProxy();
    waitForProxyReady();
  }

  @AfterAll
  public static void cleanup() throws Exception {
    if (proxy != null) {
      proxy.stop();
    }
    for (Server server : SERVERS) {
      server.stop();
    }
  }

  private static void startMockServer(String name) throws Exception {
    String address = localhostWithFreePort();
    int port = Integer.parseInt(address.split(":")[1]);

    Server server = new Server(port);
    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/");
    context.addServlet(new ServletHolder(new ServiceServlet(name)), SERVICE_PATH);
    server.setHandler(context);
    server.start();

    SERVERS.add(server);
    ENDPOINTS.add("http://" + address);
  }

  private static void startProxy() throws Exception {
    String address = localhostWithFreePort();
    String[] hostPort = address.split(":");
    proxy = new ProxyServer(ENDPOINTS, hostPort[0], Integer.parseInt(hostPort[1]));
    proxy.start();
    proxyUrl = "http://" + address + SERVICE_PATH;
  }

  private static void waitForMockServersStarted() throws TimeoutException, InterruptedException {
    for (Server server : SERVERS) {
      GenericTestUtils.waitFor(server::isStarted, 100, 3000);
    }
  }

  private static void waitForProxyReady() throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      try {
        return proxy.isStarted();
      } catch (Exception e) {
        return false;
      }
    }, 50, 3000);
  }

  @Test
  public void testRouting() throws IOException {
    for (int i = 0; i < NUM_SERVERS * 2; i++) {
      String response = sendRequest(proxyUrl);
      assertThat(response).isEqualTo("server-" + i % NUM_SERVERS);
    }
  }

  @Test
  public void testDirectAccess() throws IOException {
    for (int i = 0; i < ENDPOINTS.size(); i++) {
      String response = sendRequest(ENDPOINTS.get(i) + SERVICE_PATH);
      assertThat(response).isEqualTo("server-" + i);
    }
  }

  private static String sendRequest(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(5000);

    if (conn.getResponseCode() != 200) {
      throw new IOException("HTTP " + conn.getResponseCode() + " from " + url);
    }

    try (InputStream is = conn.getInputStream();
         Scanner scanner = new Scanner(is, StandardCharsets.UTF_8.name())) {
      return scanner.useDelimiter("\\A").hasNext() ? scanner.next() : "";
    }
  }

  /**
   * A simple servlet that returns the name of the server.
   */
  public static class ServiceServlet extends HttpServlet {
    private final String name;

    public ServiceServlet(String name) {
      this.name = name;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      resp.setContentType("text/plain");
      resp.getWriter().write(name);
    }
  }
}
