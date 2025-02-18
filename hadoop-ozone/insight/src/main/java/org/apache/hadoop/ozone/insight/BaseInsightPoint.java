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

package org.apache.hadoop.ozone.insight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.ozone.insight.LoggerSource.Level;

/**
 * Default implementation of Insight point logic.
 */
public abstract class BaseInsightPoint implements InsightPoint {

  /**
   * List the related metrics.
   */
  @Override
  public List<MetricGroupDisplay> getMetrics(Map<String, String> filters) {
    return new ArrayList<>();
  }

  /**
   * List the related configuration.
   */
  @Override
  public List<Class> getConfigurationClasses() {
    return new ArrayList<>();
  }

  /**
   * List the related loggers.
   *
   * @param verbose true if verbose logging is requested.
   * @param filters additional key value pair to further filter the output.
   *                (eg. datanode=123-2323-datanode-id)
   */
  @Override
  public List<LoggerSource> getRelatedLoggers(boolean verbose,
      Map<String, String> filters) {
    List<LoggerSource> loggers = new ArrayList<>();
    return loggers;
  }

  /**
   * Create scm client.
   */
  public ScmClient createScmClient(OzoneConfiguration ozoneConf)
      throws IOException {
    if (!HddsUtils.getHostNameFromConfigKeys(ozoneConf,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY).isPresent()) {

      throw new IllegalArgumentException(
          ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY
              + " should be set in ozone-site.xml");
    }

    return new ContainerOperationClient(ozoneConf);
  }

  /**
   * Convenient method to define default log levels.
   */
  public Level defaultLevel(boolean verbose) {
    return verbose ? Level.TRACE : Level.DEBUG;
  }

  public void addProtocolMessageMetrics(
      List<MetricGroupDisplay> metrics,
      String prefix,
      Component.Type type,
      Object[] types
  ) {
    addProtocolMessageMetrics(metrics, prefix, new Component(type), types);
  }

  /**
   * Default metrics for any message type based RPC ServerSide translators.
   */
  public void addProtocolMessageMetrics(
      List<MetricGroupDisplay> metrics,
      String prefix,
      Component component,
      Object[] types
  ) {

    MetricGroupDisplay messageTypeCounters =
        new MetricGroupDisplay(component, "Message type counters");
    for (Object type : types) {
      String typeName = type.toString();

      Map<String, String> typeFilter = new HashMap<>();
      typeFilter.put("type", typeName);
      MetricDisplay metricDisplay =
          new MetricDisplay("Number of " + typeName + " calls",
              prefix + "_counter", typeFilter);
      messageTypeCounters.addMetrics(metricDisplay);
    }
    metrics.add(messageTypeCounters);
  }

  /**
   * Rpc metrics for any hadoop rpc endpoint.
   */
  public void addRpcMetrics(List<MetricGroupDisplay> metrics,
      Component.Type component,
      Map<String, String> filter) {
    MetricGroupDisplay connection =
        new MetricGroupDisplay(component, "RPC connections");
    connection.addMetrics(new MetricDisplay("Open connections",
        "rpc_num_open_connections", filter));
    connection.addMetrics(
        new MetricDisplay("Dropped connections", "rpc_num_dropped_connections",
            filter));
    connection.addMetrics(
        new MetricDisplay("Received bytes", "rpc_received_bytes",
            filter));
    connection.addMetrics(
        new MetricDisplay("Sent bytes", "rpc_sent_bytes",
            filter));
    metrics.add(connection);

    MetricGroupDisplay queue = new MetricGroupDisplay(component, "RPC queue");
    queue.addMetrics(new MetricDisplay("RPC average queue time",
        "rpc_rpc_queue_time_avg_time", filter));
    queue.addMetrics(
        new MetricDisplay("RPC call queue length", "rpc_call_queue_length",
            filter));
    metrics.add(queue);

    MetricGroupDisplay performance =
        new MetricGroupDisplay(component, "RPC performance");
    performance.addMetrics(new MetricDisplay("RPC processing time average",
        "rpc_rpc_processing_time_avg_time", filter));
    performance.addMetrics(
        new MetricDisplay("Number of slow calls", "rpc_rpc_slow_calls",
            filter));
    metrics.add(performance);
  }

  @Override
  public boolean filterLog(Map<String, String> filters, String logLine) {
    if (filters == null) {
      return true;
    }
    boolean result = true;
    for (Entry<String, String> entry : filters.entrySet()) {
      if (!logLine.matches(
          String.format(".*\\[%s=%s\\].*", entry.getKey(), entry.getValue()))) {
        return false;
      }
    }
    return true;
  }
}
