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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_HTTP_AUTH_TYPE;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.inject.Singleton;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.metrics.Metric;
import org.apache.hadoop.ozone.recon.spi.MetricsServiceProvider;
import org.apache.hadoop.security.SecurityUtil;

/**
 * Implementation of the Jmx Metrics Service provider.
 */
@Singleton
public class JmxServiceProviderImpl implements MetricsServiceProvider {

  public static final String JMX_INSTANT_QUERY_API = "qry";
  private URLConnectionFactory connectionFactory;
  private final String jmxEndpoint;
  private final OzoneConfiguration configuration;
  private ReconUtils reconUtils;

  public JmxServiceProviderImpl(
      ReconUtils reconUtils,
      String jmxEndpoint,
      URLConnectionFactory connectionFactory,
      OzoneConfiguration configuration) {
    // Remove the trailing slash from endpoint url.
    if (jmxEndpoint != null && jmxEndpoint.endsWith("/")) {
      jmxEndpoint = jmxEndpoint.substring(0, jmxEndpoint.length() - 1);
    }
    this.jmxEndpoint = jmxEndpoint;
    this.reconUtils = reconUtils;
    this.connectionFactory = connectionFactory;
    this.configuration = configuration;
  }

  /**
   * Returns {@link HttpURLConnection} after querying Metrics endpoint for the
   * given metric.
   *
   * @param api         api.
   * @param queryString query string with metric name and other filters.
   * @return HttpURLConnection
   * @throws Exception exception
   */
  @Override
  public HttpURLConnection getMetricsResponse(String api, String queryString)
      throws Exception {
    String url = String.format("%s?%s=%s", jmxEndpoint, api,
        queryString);
    return reconUtils.makeHttpCall(connectionFactory,
        url, isKerberosEnabled());
  }

  @Override
  public List<Metric> getMetricsInstant(String queryString) throws Exception {
    return Collections.emptyList();
  }

  /**
   * Returns a list of {@link Metric} for the given instant query.
   *
   * @param queryString query string with metric name and other filters.
   * @return List of Json map of metrics response.
   * @throws Exception exception
   */
  @Override
  public List<Map<String, Object>> getMetrics(String queryString)
      throws Exception {
    return getMetrics(JMX_INSTANT_QUERY_API, queryString);
  }

  /**
   * Returns a list of {@link Metric} for the given api and query string.
   *
   * @param api api
   * @param queryString query string with metric name and other filters.
   * @return List of Json map of metrics response.
   * @throws Exception
   */
  private List<Map<String, Object>> getMetrics(String api, String queryString)
      throws Exception {
    return SecurityUtil.doAsLoginUser(() -> {
      HttpURLConnection urlConnection =
          getMetricsResponse(api, queryString);
      if (Response.Status.fromStatusCode(urlConnection.getResponseCode())
          .getFamily() == Response.Status.Family.SUCCESSFUL) {
        try (InputStream inputStream = urlConnection.getInputStream()) {
          Map<String, Object> jsonMap = JsonUtils.getDefaultMapper().readValue(inputStream, Map.class);
          Object beansObj = jsonMap.get("beans");
          if (beansObj instanceof List) {
            return (List<Map<String, Object>>) beansObj;
          }
        }
      }
      return Collections.emptyList();
    });
  }

  private boolean isKerberosEnabled() {
    return configuration.get(HDDS_DATANODE_HTTP_AUTH_TYPE, "simple")
        .equals("kerberos");
  }
}
