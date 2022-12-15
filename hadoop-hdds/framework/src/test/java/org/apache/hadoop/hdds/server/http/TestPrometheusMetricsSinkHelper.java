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

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Class for unit tests for {@link PrometheusMetricsSinkHelper}.
 */
class TestPrometheusMetricsSinkHelper {

  @Test
  void addServerNameTagWithNonUgiMetrics() {
    // GIVEN
    String key = "key";
    List<MetricsTag> metricsTagList = new ArrayList<>();

    // WHEN
    PrometheusMetricsSinkHelper.addServerNameTag(key, metricsTagList);

    // THEN
    Assertions.assertEquals(0, metricsTagList.size());
  }

  @Test
  void addServerNameTagWithUgiMetrics() {
    // GIVEN
    String key = "ugi_metrics";
    List<MetricsTag> metricsTagList = new ArrayList<>();

    // WHEN
    PrometheusMetricsSinkHelper.addServerNameTag(key, metricsTagList);

    // THEN
    Assertions.assertEquals(1, metricsTagList.size());
  }

  @Test
  void replaceServerNameTagValueWithNonUgiMetrics() {
    // GIVEN
    String key = "non_ugi";
    String metricsKey = "{\"servername\"=\"unknown\"}";
    int serverPort = 0;

    // WHEN
    String newMetricsKey =
        PrometheusMetricsSinkHelper.replaceServerNameTagValue(key, metricsKey,
        serverPort);

    // THEN
    Assertions.assertEquals(metricsKey, newMetricsKey);
  }

  @Test
  void replaceServerNameTagValueWithUgiMetricsAndNonOMOrSCMPort() {
    // GIVEN
    String key = "ugi_metrics";
    String metricsKey = "{\"servername\"=\"unknown\"}";
    int serverPort = 0;

    // WHEN
    String newMetricsKey =
        PrometheusMetricsSinkHelper.replaceServerNameTagValue(key, metricsKey,
            serverPort);

    // THEN
    Assertions.assertEquals(metricsKey, newMetricsKey);
  }

  @Test
  void replaceServerNameTagValueWithUgiMetricsAndOMPort() {
    // GIVEN
    String key = "ugi_metrics";
    String metricsKey = "{\"servername\"=\"unknown\"}";
    int serverPort = OMConfigKeys.OZONE_OM_HTTP_BIND_PORT_DEFAULT;

    // WHEN
    String newMetricsKey =
        PrometheusMetricsSinkHelper.replaceServerNameTagValue(key, metricsKey,
            serverPort);

    // THEN
    Assertions.assertEquals("{\"servername\"=\"OM\"}", newMetricsKey);
  }

  @Test
  void replaceServerNameTagValueWithUgiMetricsAndSCMPort() {
    // GIVEN
    String key = "ugi_metrics";
    String metricsKey = "{\"servername\"=\"unknown\"}";
    int serverPort = ScmConfigKeys.OZONE_SCM_HTTP_BIND_PORT_DEFAULT;

    // WHEN
    String newMetricsKey =
        PrometheusMetricsSinkHelper.replaceServerNameTagValue(key, metricsKey,
            serverPort);

    // THEN
    Assertions.assertEquals("{\"servername\"=\"SCM\"}", newMetricsKey);
  }
}