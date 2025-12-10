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

package org.apache.hadoop.ozone.recon.spi.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.inject.Singleton;
import org.apache.hadoop.ozone.recon.metrics.Metric;
import org.apache.hadoop.ozone.recon.spi.MetricsServiceProvider;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the Prometheus Metrics Service provider.
 */
@Singleton
public class JmxServiceProviderImpl implements MetricsServiceProvider {

  private static final Logger LOG = LoggerFactory.getLogger(JmxServiceProviderImpl.class);
  private final CloseableHttpAsyncClient asyncHttpClient;
  private final ObjectMapper objectMapper;

  public static final String JMX_INSTANT_QUERY_API = "qry";
  private static final int DEFAULT_TIMEOUT_MS = 60000;
  private static final int MAX_TOTAL_CONNECTIONS = 100;
  private static final int MAX_CONNECTIONS_PER_ROUTE = 10;

  public JmxServiceProviderImpl() {
    this.asyncHttpClient = createAsyncHttpClient();
    this.asyncHttpClient.start();
    this.objectMapper = new ObjectMapper();
  }

  private CloseableHttpAsyncClient createAsyncHttpClient() {
    try {
      // Configure IO reactor for non-blocking I/O
      IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
              .setConnectTimeout(DEFAULT_TIMEOUT_MS)
              .setSoTimeout(DEFAULT_TIMEOUT_MS)
              .setIoThreadCount(Runtime.getRuntime().availableProcessors())
              .build();

      ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
      PoolingNHttpClientConnectionManager connManager =
              new PoolingNHttpClientConnectionManager(ioReactor);
      connManager.setMaxTotal(MAX_TOTAL_CONNECTIONS);
      connManager.setDefaultMaxPerRoute(MAX_CONNECTIONS_PER_ROUTE);

      RequestConfig requestConfig = RequestConfig.custom()
              .setConnectTimeout(DEFAULT_TIMEOUT_MS)
              .setSocketTimeout(DEFAULT_TIMEOUT_MS)
              .setConnectionRequestTimeout(DEFAULT_TIMEOUT_MS)
              .build();

      return HttpAsyncClients.custom()
              .setConnectionManager(connManager)
              .setDefaultRequestConfig(requestConfig)
              .build();

    } catch (Exception e) {
      throw new RuntimeException("Failed to create async HTTP client", e);
    }
  }

  /**
   * Returns {@link HttpURLConnection} after querying Metrics endpoint for the
   * given metric.
   *
   * @param api         api.
   * @param queryString query string with metric name and other filters.
   * @return HttpURLConnection
   */
  @Override
  public HttpURLConnection getMetricsResponse(String api, String queryString) {
    return null;
  }

  @Override
  public List<Metric> getMetricsInstant(String queryString) {
    return Collections.emptyList();
  }

  /**
   * Returns a list of {@link Metric} for the given instant query.
   *
   * @param queryString query string with metric name and other filters.
   * @return List of Json map of metrics response.
   */
  @Override
  public CompletableFuture<List<Map<String, Object>>> getMetricsAsync(String jmxEndpoint, String queryString) {
    // Remove the trailing slash from endpoint url.
    if (jmxEndpoint != null && jmxEndpoint.endsWith("/")) {
      jmxEndpoint = jmxEndpoint.substring(0, jmxEndpoint.length() - 1);
    }
    return getMetrics(jmxEndpoint, queryString);
  }

  @Override
  public void shutdown() {
    try {
      LOG.info("Shutting down async HTTP client...");
      asyncHttpClient.close();
    } catch (IOException e) {
      LOG.error("Error shutting down async HTTP client", e);
    }
  }

  /**
   * Returns a list of {@link Metric} for the given api and query string.
   *
   * @param jmxEndpoint endpoint for the jmx server
   * @param queryString query string with metric name and other filters.
   * @return List of Json map of metrics response.
   */
  private CompletableFuture<List<Map<String, Object>>> getMetrics(String jmxEndpoint, String queryString) {
    CompletableFuture<List<Map<String, Object>>> future = new CompletableFuture<>();

    try {
      String url = String.format(
          "%s?%s=%s",
          jmxEndpoint,
          JMX_INSTANT_QUERY_API,
          URLEncoder.encode(queryString, "UTF-8"));
      HttpGet request = new HttpGet(url);

      asyncHttpClient.execute(request, new FutureCallback<HttpResponse>() {
        @Override
        public void completed(HttpResponse response) {
          try {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode >= 200 && statusCode < 300) {
              String responseBody = EntityUtils.toString(response.getEntity());
              Map<String, Object> jsonResponse = objectMapper.readValue(responseBody, Map.class);
              List<Map<String, Object>> beans = (List<Map<String, Object>>) jsonResponse.get("beans");
              future.complete(beans);
            } else {
              future.completeExceptionally(new IOException("HTTP error code: " + statusCode));
            }
          } catch (Exception e) {
            future.completeExceptionally(e);
          }
        }

        @Override
        public void failed(Exception ex) {
          LOG.debug("HTTP request failed for {}: {}", url, ex.getMessage());
          future.completeExceptionally(ex);
        }

        @Override
        public void cancelled() {
          future.completeExceptionally(
                  new IOException("Request cancelled"));
        }
      });

    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }
}
