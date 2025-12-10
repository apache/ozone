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

package org.apache.hadoop.ozone.recon.spi;

import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.ozone.recon.metrics.Metric;

/**
 * Interface to access Ozone metrics.
 */
public interface MetricsServiceProvider {

  /**
   * Returns {@link HttpURLConnection} after querying Metrics endpoint for the
   * given metric.
   *
   * @param api api.
   * @param queryString query string with metric name and other filters.
   * @return HttpURLConnection
   * @throws Exception exception
   */
  HttpURLConnection getMetricsResponse(String api, String queryString)
      throws Exception;

  /**
   * Returns a list of {@link Metric} for the given instant query.
   *
   * @param queryString query string with metric name and other filters.
   * @return List of Json map of metrics response.
   * @throws Exception exception
   */
  List<Metric> getMetricsInstant(String queryString) throws Exception;

  /**
   * Returns a list of {@link Map<String, Object>} for the given query.
   *
   * @param queryString query string with metric name, start time, end time,
   *                    step and other filters.
   * @return List of Json map of metrics response.
   * @throws Exception exception
   */
  List<Map<String, Object>> getMetrics(String queryString) throws Exception;
}
