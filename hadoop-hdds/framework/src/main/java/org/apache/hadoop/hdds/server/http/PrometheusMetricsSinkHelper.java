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

import java.util.List;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.ozone.om.OMConfigKeys;

/**
 * Helper class for Prometheus metrics sink.
 */
public final class PrometheusMetricsSinkHelper {

  private static final String UGI_METRICS = "ugi_metrics";
  private static final String SERVER_NAME = "unknown";

  /**
   * Never constructed.
   */
  private PrometheusMetricsSinkHelper() {
  }

  /**
   * Adds <tt>servername</tt> metrics tag.
   *
   * @param key           metrics entry key
   * @param metricTagList list of metric tags
   */
  public static void addServerNameTag(String key,
      List<MetricsTag> metricTagList) {
    if (!key.contains(UGI_METRICS)) {
      return;
    }

    final String name = "servername";
    final String description = "name of the server";
    MetricsInfo metricsInfo = createMetricsInfo(name, description);
    metricTagList.add(new MetricsTag(metricsInfo, SERVER_NAME));
  }

  private static MetricsInfo createMetricsInfo(String name,
      String description) {
    return new MetricsInfo() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public String description() {
        return description;
      }
    };
  }

  /**
   * Replaces default servername tag value with actual server name.
   *
   * @param metricEntryKey metric entry key
   * @param metricKey      metric key
   * @param serverPort     server port
   * @return <tt>metricKey</tt> with replaced servername tag value.
   */
  public static String replaceServerNameTagValue(String metricEntryKey,
      String metricKey, int serverPort) {
    if (!metricEntryKey.contains(UGI_METRICS)) {
      return metricKey;
    }

    String serverName = null;
    switch (serverPort) {
    case OMConfigKeys.OZONE_OM_HTTP_BIND_PORT_DEFAULT:
      serverName = "OM";
      break;
    case ScmConfigKeys.OZONE_SCM_HTTP_BIND_PORT_DEFAULT:
      serverName = "SCM";
      break;
    default:
      break;
    }

    if (serverName != null) {
      metricKey = metricKey.replace(SERVER_NAME, serverName);
    }

    return metricKey;
  }
}
