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

import com.google.common.net.HttpHeaders;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.proxy.ProxyServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
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
  private final Server server;
  private final String host;
  private final int port;

  public ProxyServer(List<String> s3gEndpoints, String host, int proxyPort) throws Exception {
    this(s3gEndpoints, host, proxyPort, new RoundRobinStrategy());
  }

  public ProxyServer(List<String> s3gEndpoints, String host, int proxyPort,
                     LoadBalanceStrategy loadBalanceStrategy) throws Exception {
    this.s3gEndpoints = s3gEndpoints;
    this.loadBalanceStrategy = loadBalanceStrategy;
    this.host = host;
    this.port = proxyPort;

    server = new Server(proxyPort);
    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/");

    ProxyHandler proxyHandler = new ProxyHandler();
    ServletHolder proxyHolder = new ServletHolder(proxyHandler);

    proxyHolder.setInitParameter("proxyTo", "");


    context.addServlet(proxyHolder, "/*");
    server.setHandler(context);

    LOG.info("ProxyServer initialized with endpoints: {}", s3gEndpoints);
    LOG.info("Load balance strategy: {}", loadBalanceStrategy.getClass().getSimpleName());
  }

  public void start() throws Exception {
    server.start();
    LOG.info("Proxy started on http://{}:{}", host, port);
  }

  public void stop() throws Exception {
    server.stop();
    LOG.info("Proxy stopped on http://{}:{}", host, port);
  }

  public boolean isStarted() {
    return server.isStarted();
  }

  /**
   * ProxyHandler is a subclass of Jetty's ProxyServlet.Transparent.
   * It implements logic for request rewriting, service handling,
   * proxy response failure, and rewrite failure handling.
   * This handler is mainly used to forward requests to different S3G endpoints
   * based on the load balancing strategy, handle special HTTP headers (such as Expect),
   * and manage exceptions during the proxy process.
   */
  public class ProxyHandler extends ProxyServlet.Transparent {

    @Override
    public void init() throws ServletException {
      super.init();

      LOG.info("MyProxyHandler initialized with {} endpoints",
          s3gEndpoints != null ? s3gEndpoints.size() : 0);
      if (s3gEndpoints != null) {
        LOG.info("Endpoints: {}", s3gEndpoints);
      }
      LOG.info("Load balance strategy: {}", loadBalanceStrategy.getClass().getSimpleName());
    }

    @Override
    protected String rewriteTarget(HttpServletRequest request) {

      String baseUrl = loadBalanceStrategy.selectEndpoint(s3gEndpoints);

      String requestUri = request.getRequestURI();
      String queryString = request.getQueryString();

      StringBuilder targetUrl = new StringBuilder(baseUrl);
      if (requestUri != null) {
        targetUrl.append(requestUri);
      }
      if (queryString != null) {
        targetUrl.append('?').append(queryString);
      }

      String finalUrl = targetUrl.toString();
      LOG.info("Rewriting target URL: [{}] {}", request.getMethod(), finalUrl);
      return finalUrl;
    }

    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {

      // Remove the Expect header to avoid Jetty's 100-continue handling issue:
      // In some scenarios (e.g. testPutObjectEmpty), when content-length != 0, Jetty triggers the 100-continue process,
      // causing the server to wait for a 100 response code. However, Jetty only sends the 100 response
      // when the InputStream is read, which can lead to request failures.
      if (request.getHeader(HttpHeaders.EXPECT) != null) {
        LOG.info("Removing Expect header: {}", request.getHeader(HttpHeaders.EXPECT));

        request = new HttpServletRequestWrapper(request) {
          @Override
          public String getHeader(String name) {
            if ("Expect".equalsIgnoreCase(name)) {
              return null;
            }
            return super.getHeader(name);
          }

          @Override
          public Enumeration<String> getHeaders(String name) {
            if ("Expect".equalsIgnoreCase(name)) {
              return Collections.emptyEnumeration();
            }
            return super.getHeaders(name);
          }

          @Override
          public Enumeration<String> getHeaderNames() {
            List<String> headerNames = new ArrayList<>();
            Enumeration<String> originalHeaders = super.getHeaderNames();
            while (originalHeaders.hasMoreElements()) {
              String headerName = originalHeaders.nextElement();
              if (!"Expect".equalsIgnoreCase(headerName)) {
                headerNames.add(headerName);
              }
            }
            return Collections.enumeration(headerNames);
          }
        };
      }

      super.service(request, response);
    }

    @Override
    protected void onProxyResponseFailure(HttpServletRequest clientRequest,
                                          HttpServletResponse proxyResponse,
                                          org.eclipse.jetty.client.api.Response serverResponse,
                                          Throwable failure) {
      LOG.error("===  Proxy Response Failure===");
      LOG.error("Client request: {} {}", clientRequest.getMethod(), clientRequest.getRequestURL());
      if (serverResponse != null) {
        LOG.error("Server response status: {}", serverResponse.getStatus());
      } else {
        LOG.error("Server response is null - connection failed");
      }

      LOG.error("Failure details:", failure);
      super.onProxyResponseFailure(clientRequest, proxyResponse, serverResponse, failure);
    }

    @Override
    protected void onProxyRewriteFailed(HttpServletRequest clientRequest,
                                        HttpServletResponse proxyResponse) {
      LOG.error("=== Proxy Rewrite Failed ===");
      LOG.error("Client request: {} {}", clientRequest.getMethod(), clientRequest.getRequestURL());
      LOG.error("Rewrite target returned null or invalid URL");

      try {
        proxyResponse.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
        proxyResponse.setContentType("text/plain");
        proxyResponse.getWriter().write("Proxy configuration error: Unable to rewrite target URL");
      } catch (IOException e) {
        LOG.error("Failed to send rewrite error response", e);
      }

      super.onProxyRewriteFailed(clientRequest, proxyResponse);
    }
  }
}
