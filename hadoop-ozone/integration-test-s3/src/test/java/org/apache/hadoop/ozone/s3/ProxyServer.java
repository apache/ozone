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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProxyServer acts as a reverse proxy for S3G endpoints.
 * It forwards requests to the selected S3G endpoint based on the load balancing strategy.
 */
public class ProxyServer {
  private static final Logger LOG = LoggerFactory.getLogger(ProxyServer.class);
  private final List<String> s3gEndpoints;
  private final LoadBalanceStrategy loadBalanceStrategy;
  private final HttpServer server;
  private final String host;
  private final int port;
  private final ExecutorService executor;

  // Add timeout configurations for better handling of large file operations
  private static final int CONNECT_TIMEOUT_MS = 60000; // 60 seconds
  private static final int READ_TIMEOUT_MS = 300000;   // 5 minutes

  private static final int BUFFER_SIZE = 64 * 1024; // 64KB
  private static final ThreadLocal<byte[]> IO_BUFFER = ThreadLocal.withInitial(() -> new byte[BUFFER_SIZE]);

  public ProxyServer(List<String> s3gEndpoints, String host, int proxyPort) throws Exception {
    this(s3gEndpoints, host, proxyPort, new RoundRobinStrategy());
  }

  public ProxyServer(List<String> s3gEndpoints, String host, int proxyPort,
                     LoadBalanceStrategy loadBalanceStrategy) throws Exception {
    this.s3gEndpoints = s3gEndpoints;
    this.loadBalanceStrategy = loadBalanceStrategy;
    this.host = host;
    this.port = proxyPort;
    server = HttpServer.create(new InetSocketAddress(host, proxyPort), 0);
    server.createContext("/", new ProxyHandler());

    this.executor = Executors.newCachedThreadPool();
    server.setExecutor(executor);

    LOG.info("ProxyServer initialized with endpoints: {}", s3gEndpoints);
    LOG.info("Load balance strategy: {}", loadBalanceStrategy.getClass().getSimpleName());
    LOG.info("Timeout settings - Connect: {}ms, Read: {}ms", CONNECT_TIMEOUT_MS, READ_TIMEOUT_MS);
  }

  public void start() {
    server.start();
    LOG.info("Proxy started on http://{}:{}", host, port);

  }

  public void stop() {
    server.stop(0);
    executor.shutdownNow();
    LOG.info("Proxy stopped on http://{}:{}", host, port);
  }

  private class ProxyHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      HttpURLConnection conn = null;
      try {
        String target = loadBalanceStrategy.selectEndpoint(s3gEndpoints) + exchange.getRequestURI().toString();
        conn = createConnection(exchange, target);
        String requestMethod = conn.getRequestMethod();

        LOG.info("Received request and forwarding request to [{}] {}", requestMethod, target);

        copyRequestHeaders(exchange, conn);
        addForwardedHeaders(exchange, conn);
        copyRequestBody(exchange, conn);

        int responseCode = conn.getResponseCode();
        copyResponseHeaders(exchange, conn);

        if (requestMethod.equals("HEAD") || responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
          exchange.sendResponseHeaders(responseCode, 0);
          return;
        }

        copyResponseBody(exchange, conn, responseCode);

      } catch (Exception e) {
        LOG.error("Error forwarding request to S3G endpoint", e);
        sendErrorResponse(exchange, e);
      } finally {
        closeResources(conn, exchange);
      }
    }

    private void sendErrorResponse(HttpExchange exchange, Exception e) {
      try {
        exchange.sendResponseHeaders(502, 0);
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(("Proxy error: " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
        }
      } catch (IOException ioException) {
        LOG.error("Failed to send error response", ioException);
      }
    }

    private void closeResources(HttpURLConnection conn, HttpExchange exchange) {
      if (conn != null) {
        conn.disconnect();
      }
      exchange.close();
    }

    private void addForwardedHeaders(HttpExchange exchange, HttpURLConnection conn) {
      String remoteAddr = exchange.getRemoteAddress().getAddress().getHostAddress();
      String xff = exchange.getRequestHeaders().getFirst("X-Forwarded-For");

      if (StringUtils.isNotBlank(xff)) {
        conn.setRequestProperty("X-Forwarded-For", xff + ", " + remoteAddr);
      } else {
        conn.setRequestProperty("X-Forwarded-For", remoteAddr);
      }

      conn.setRequestProperty("X-Forwarded-Proto", "http");
    }

    private void copyResponseBody(HttpExchange exchange, HttpURLConnection conn, int responseCode) throws IOException {
      String transferEncoding = conn.getHeaderField("Transfer-Encoding");
      String contentLengthStr = conn.getHeaderField("Content-Length");

      if (transferEncoding != null && transferEncoding.equalsIgnoreCase("chunked")) {
        exchange.sendResponseHeaders(responseCode, 0);
      } else {
        long contentLength = Optional.ofNullable(contentLengthStr).map(Long::parseLong).orElse(0L);
        exchange.sendResponseHeaders(responseCode, contentLength);
      }

      try (InputStream is = responseCode >= 400 ? conn.getErrorStream() : conn.getInputStream();
           OutputStream os = exchange.getResponseBody()) {
        if (is != null) {
          IOUtils.copyLarge(is, os, IO_BUFFER.get());
          os.flush();
        }
      }
    }

    private HttpURLConnection createConnection(HttpExchange exchange, String target) throws IOException {
      HttpURLConnection conn = (HttpURLConnection) new URL(target).openConnection();
      String requestMethod = exchange.getRequestMethod();
      conn.setRequestMethod(requestMethod);
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      return conn;
    }

    private void copyRequestBody(HttpExchange exchange, HttpURLConnection conn) throws IOException {
      String method = exchange.getRequestMethod().toLowerCase();
      if (method.equals("post") || method.equals("put") || method.equals("patch") || method.equals("delete")) {
        String contentLength = exchange.getRequestHeaders().getFirst("Content-Length");
        conn.setDoOutput(true);
        if (StringUtils.isNotBlank(contentLength)) {
          conn.setFixedLengthStreamingMode(Long.parseLong(contentLength));
        } else {
          conn.setChunkedStreamingMode(0);
        }
        try (InputStream reqBody = exchange.getRequestBody();
             OutputStream os = conn.getOutputStream()) {
          IOUtils.copy(reqBody, os);
        }
      }
    }

    private void copyRequestHeaders(HttpExchange exchange, HttpURLConnection conn) {
      for (String headerName : exchange.getRequestHeaders().keySet()) {
        if (isHopByHopHeader(headerName)) {
          continue;
        }

        // Skip Expect header to avoid 100-continue issue
        if ("Expect".equalsIgnoreCase(headerName)) {
          continue;
        }

        List<String> headerValues = exchange.getRequestHeaders().get(headerName);
        if (headerValues == null || headerValues.isEmpty()) {
          continue;
        }

        for (String value : headerValues) {
          // Convert header names to lowercase because HttpServer capitalizes the first letter,
          // but s3g expects lowercase header names.
          conn.addRequestProperty(headerName.toLowerCase(), value);
        }
      }
    }
  }

  private void copyResponseHeaders(HttpExchange exchange, HttpURLConnection conn) {
    for (String headerName : conn.getHeaderFields().keySet()) {
      if (headerName == null || isHopByHopHeader(headerName)) {
        continue;
      }

      List<String> headerValues = conn.getHeaderFields().get(headerName);
      if (headerValues == null) {
        continue;
      }

      for (String headerValue : headerValues) {
        exchange.getResponseHeaders().add(headerName, headerValue);
      }
    }
  }

  private boolean isHopByHopHeader(String header) {
    String lowerName = header.toLowerCase();
    return "connection".equals(lowerName) || "keep-alive".equals(lowerName)
        || "proxy-authenticate".equals(lowerName) || "proxy-authorization".equals(lowerName)
        || "te".equals(lowerName) || "trailers".equals(lowerName)
        || "transfer-encoding".equals(lowerName) || "upgrade".equals(lowerName);
  }
}
