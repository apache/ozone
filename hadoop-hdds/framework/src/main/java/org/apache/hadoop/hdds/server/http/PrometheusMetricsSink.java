/**
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
package org.apache.hadoop.hdds.server.http;

import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.hdds.utils.PrometheusMetricsSinkUtil;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;

/**
 * Metrics sink for prometheus exporter.
 * <p>
 * Stores the metric data in-memory and return with it on request.
 */
public class PrometheusMetricsSink implements MetricsSink {

  /**
   * Cached output lines for each metrics.
   */
  private final Map<String, Map<String, String>> metricLines =
      Collections.synchronizedSortedMap(new TreeMap<>());
  private final String servername;

  public PrometheusMetricsSink(String servername) {
    this.servername = servername;
  }

  @Override
  public void putMetrics(MetricsRecord metricsRecord) {
    for (AbstractMetric metrics : metricsRecord.metrics()) {
      if (metrics.type() == MetricType.COUNTER
          || metrics.type() == MetricType.GAUGE) {

        String metricName =
            PrometheusMetricsSinkUtil.getMetricName(metricsRecord.name(),
                metrics.name());

        // If there is no username this should be null
        String username =
            PrometheusMetricsSinkUtil.getUsername(metricsRecord.name(),
                metrics.name());

        String key = PrometheusMetricsSinkUtil.prometheusName(
            metricsRecord.name(), metricName);

        String prometheusMetricKeyAsString =
            getPrometheusMetricKeyAsString(metricsRecord, key, username);

        String metricKey = "# TYPE "
            + key
            + " "
            + metrics.type().toString().toLowerCase();

        metricLines.computeIfAbsent(metricKey,
                any -> Collections.synchronizedSortedMap(new TreeMap<>()))
            .put(prometheusMetricKeyAsString, String.valueOf(metrics.value()));
      }
    }
  }

  private String getPrometheusMetricKeyAsString(MetricsRecord metricsRecord,
      String key, String username) {
    StringBuilder prometheusMetricKey = new StringBuilder();
    prometheusMetricKey.append(key)
        .append("{");
    String sep = "";

    List<MetricsTag> metricsTags =
        PrometheusMetricsSinkUtil.addTags(key, username, servername,
            metricsRecord.tags());

    //add tags
    for (MetricsTag tag : metricsTags) {
      String tagName = tag.name().toLowerCase();

      //ignore specific tag which includes sub-hierarchy
      if (tagName.equals("numopenconnectionsperuser")) {
        continue;
      }

      prometheusMetricKey.append(sep)
          .append(tagName)
          .append("=\"")
          .append(tag.value())
          .append("\"");
      sep = ",";
    }
    prometheusMetricKey.append("}");

    return prometheusMetricKey.toString();
  }

  @Override
  public void flush() {

  }

  @Override
  public void init(SubsetConfiguration subsetConfiguration) {

  }

  public void writeMetrics(Writer writer) throws IOException {
    for (Map.Entry<String, Map<String, String>> metricEntry
        : metricLines.entrySet()) {
      writer.write(metricEntry.getKey() + "\n");

      for (Map.Entry<String, String> metric
          : metricEntry.getValue().entrySet()) {
        writer.write(metric.getKey() + " " + metric.getValue() + "\n");
      }
    }
  }
}
