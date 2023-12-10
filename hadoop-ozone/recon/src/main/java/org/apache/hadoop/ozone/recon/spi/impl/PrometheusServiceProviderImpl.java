/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.spi.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfigKeys;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.metrics.Metric;
import org.apache.hadoop.ozone.recon.spi.MetricsServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_METRICS_HTTP_CONNECTION_REQUEST_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_METRICS_HTTP_CONNECTION_REQUEST_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_METRICS_HTTP_CONNECTION_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_METRICS_HTTP_CONNECTION_TIMEOUT_DEFAULT;

/**
 * Implementation of the Prometheus Metrics Service provider.
 */
@Singleton
public class PrometheusServiceProviderImpl
    implements MetricsServiceProvider {

  public static final String PROMETHEUS_INSTANT_QUERY_API = "query";
  public static final String PROMETHEUS_RANGED_QUERY_API = "query_range";

  private static final Logger LOG =
      LoggerFactory.getLogger(PrometheusServiceProviderImpl.class);

  private URLConnectionFactory connectionFactory;
  private final String prometheusEndpoint;
  private ReconUtils reconUtils;

  public PrometheusServiceProviderImpl(OzoneConfiguration configuration,
                                       ReconUtils reconUtils) {

    int connectionTimeout = (int) configuration.getTimeDuration(
        OZONE_RECON_METRICS_HTTP_CONNECTION_TIMEOUT,
        OZONE_RECON_METRICS_HTTP_CONNECTION_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);
    int connectionRequestTimeout = (int) configuration.getTimeDuration(
        OZONE_RECON_METRICS_HTTP_CONNECTION_REQUEST_TIMEOUT,
        OZONE_RECON_METRICS_HTTP_CONNECTION_REQUEST_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);

    connectionFactory =
        URLConnectionFactory.newDefaultURLConnectionFactory(connectionTimeout,
            connectionRequestTimeout, configuration);

    String endpoint = configuration.getTrimmed(getEndpointConfigKey());
    // Remove the trailing slash from endpoint url.
    if (endpoint != null && endpoint.endsWith("/")) {
      endpoint = endpoint.substring(0, endpoint.length() - 1);
    }
    this.prometheusEndpoint = endpoint;
    this.reconUtils = reconUtils;
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
    String url = String.format("%s/api/v1/%s?%s", prometheusEndpoint, api,
        queryString);
    return reconUtils.makeHttpCall(connectionFactory,
        url, false);
  }

  /**
   * Returns the endpoint configuration key for the Metrics service provider.
   *
   * @return endpoint configuration key.
   */
  public static String getEndpointConfigKey() {
    return ReconConfigKeys.OZONE_RECON_PROMETHEUS_HTTP_ENDPOINT;
  }

  /**
   * Returns a list of {@link Metric} for the given instant query.
   *
   * @param queryString query string with metric name and other filters.
   * @return List of Json map of metrics response.
   * @throws Exception exception
   */
  @Override
  public List<Metric> getMetricsInstant(String queryString)
      throws Exception {
    return getMetrics(PROMETHEUS_INSTANT_QUERY_API, queryString);
  }

  /**
   * Returns a list of {@link Metric} for the given ranged query.
   *
   * @param queryString query string with metric name, start time, end time,
   *                    step and other filters.
   * @return List of Json map of metrics response.
   * @throws Exception exception
   */
  @Override
  public List<Metric> getMetricsRanged(String queryString) throws Exception {
    return getMetrics(PROMETHEUS_RANGED_QUERY_API, queryString);
  }

  /**
   * Returns a list of {@link Metric} for the given api and query string.
   *
   * @param api api
   * @param queryString query string with metric name and other filters.
   * @return List of Json map of metrics response.
   * @throws Exception
   */
  private List<Metric> getMetrics(String api, String queryString)
      throws Exception {
    HttpURLConnection urlConnection =
        getMetricsResponse(api, queryString);
    List<Metric> metrics = null;
    if (Response.Status.fromStatusCode(urlConnection.getResponseCode())
        .getFamily() == Response.Status.Family.SUCCESSFUL) {
      InputStream inputStream = urlConnection.getInputStream();
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> jsonMap = mapper.readValue(inputStream, Map.class);
      inputStream.close();
      String status = (String) jsonMap.get("status");
      if (status != null && status.equals("success")) {
        // For range queries, the resultType will be "matrix"
        // and for instant queries, the resultType will be "vector"
        Map<String, Object> data = (Map<String, Object>) jsonMap.get(
            "data");
        if (data != null) {
          String resultType = (String) data.get("resultType");
          if (resultType != null) {
            String valueKey = resultType.equals("matrix") ? "values" : "value";
            List<Map<String, Object>> result = (List<Map<String, Object>>)
                data.get("result");
            if (!result.isEmpty()) {
              metrics = new ArrayList<>();
              // value is an array of timestamp and value.
              // ex. "value": [1599032677.002,"1"] for "vector"
              // values is an array of array of timestamp and value.
              // ex. "values": [[1599032677.002,"1"]] for "matrix"
              for (Map<String, Object> metricJson : result) {
                Map<String, String> metadata =
                    (Map<String, String>) metricJson.get("metric");
                TreeMap<Double, Double> values = new TreeMap<>();
                List<List<Object>> valuesJson = new ArrayList<>();
                if (resultType.equals("matrix")) {
                  valuesJson = (List<List<Object>>) metricJson.get(valueKey);
                } else if (resultType.equals("vector")) {
                  valuesJson.add((List<Object>) metricJson.get(valueKey));
                }
                for (List<Object> value : valuesJson) {
                  if (value.size() == 2) {
                    values.put((Double) value.get(0),
                        Double.parseDouble((String) value.get(1)));
                  }
                }
                metrics.add(new Metric(metadata, values));
              }
            }
          }
        }
      } else {
        if (LOG.isErrorEnabled()) {
          LOG.error(String.format("Error while getting metrics: %s",
              jsonMap.get("error")));
        }
      }
    } else {
      LOG.error("Error while connecting to metrics endpoint. Got a " +
          "status code " + urlConnection.getResponseCode() + ": " +
          urlConnection.getResponseMessage());
    }
    return metrics;
  }
}

