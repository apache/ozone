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

package org.apache.hadoop.hdds.scm.node;

import java.util.Map;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.util.StringUtils;

/**
 * This class maintains Node related metrics.
 */
@InterfaceAudience.Private
@Metrics(about = "SCM NodeManager Metrics", context = OzoneConsts.OZONE)
public final class SCMNodeMetrics implements MetricsSource {

  public static final String SOURCE_NAME =
      SCMNodeMetrics.class.getSimpleName();

  private @Metric MutableCounterLong numHBProcessed;
  private @Metric MutableCounterLong numHBProcessingFailed;
  private @Metric MutableCounterLong numNodeReportProcessed;
  private @Metric MutableCounterLong numNodeReportProcessingFailed;
  private @Metric MutableCounterLong numNodeCommandQueueReportProcessed;
  private @Metric MutableCounterLong numNodeCommandQueueReportProcessingFailed;
  private @Metric String textMetric;

  private final MetricsRegistry registry;
  private final NodeManagerMXBean managerMXBean;
  private final MetricsInfo recordInfo = Interns.info("SCMNodeManager",
      "SCM NodeManager metrics");

  /** Private constructor. */
  private SCMNodeMetrics(NodeManagerMXBean managerMXBean) {
    this.managerMXBean = managerMXBean;
    this.registry = new MetricsRegistry(recordInfo);
    this.textMetric = "my_test_metric";
  }

  /**
   * Create and returns SCMNodeMetrics instance.
   *
   * @return SCMNodeMetrics
   */
  public static SCMNodeMetrics create(NodeManagerMXBean managerMXBean) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, "SCM NodeManager Metrics",
        new SCMNodeMetrics(managerMXBean));
  }

  /**
   * Unregister the metrics instance.
   */
  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  /**
   * Increments number of heartbeat processed count.
   */
  void incNumHBProcessed() {
    numHBProcessed.incr();
  }

  /**
   * Increments number of heartbeat processing failed count.
   */
  void incNumHBProcessingFailed() {
    numHBProcessingFailed.incr();
  }

  /**
   * Increments number of node report processed count.
   */
  void incNumNodeReportProcessed() {
    numNodeReportProcessed.incr();
  }

  /**
   * Increments number of node report processing failed count.
   */
  void incNumNodeReportProcessingFailed() {
    numNodeReportProcessingFailed.incr();
  }

  /**
   * Increments number of Command Queue reports processed.
   */
  void incNumNodeCommandQueueReportProcessed() {
    numNodeCommandQueueReportProcessed.incr();
  }

  /**
   * Increments number of Command Queue reports where processing failed.
   */
  void incNumNodeCommandQueueReportProcessingFailed() {
    numNodeCommandQueueReportProcessingFailed.incr();
  }

  /**
   * Get aggregated counter and gauge metrics.
   */
  @Override
  @SuppressWarnings("SuspiciousMethodCalls")
  public void getMetrics(MetricsCollector collector, boolean all) {
    Map<String, Map<String, Integer>> nodeCount = managerMXBean.getNodeCount();
    Map<String, Long> nodeInfo = managerMXBean.getNodeInfo();
    Map<String, String> nodeStatistics = managerMXBean.getNodeStatistics();
    int totalNodeCount = 0;
    /**
     * Loop over the Node map and create a metric for the cross product of all
     * Operational and health states, ie:
     *     InServiceHealthy
     *     InServiceStale
     *     ...
     *     EnteringMaintenanceHealthy
     *     ...
     */
    MetricsRecordBuilder metrics = collector.addRecord(registry.info());
    for (Map.Entry<String, Map<String, Integer>> e : nodeCount.entrySet()) {
      for (Map.Entry<String, Integer> h : e.getValue().entrySet()) {
        metrics.addGauge(
            Interns.info(
                StringUtils.camelize(e.getKey() + "_" + h.getKey() + "_nodes"),
                "Number of " + e.getKey() + " " + h.getKey() + " datanodes"),
            h.getValue());
        totalNodeCount += h.getValue();
      }
    }
    metrics.addGauge(
        Interns.info("AllNodes", "Number of datanodes"), totalNodeCount);

    String nodesOutOfSpace = nodeStatistics.get("NodesOutOfSpace");
    if (nodesOutOfSpace != null) {
      metrics.addGauge(
          Interns.info("NodesOutOfSpace", "Number of datanodes that are out of space because " +
              "they cannot allocate new containers or write to existing ones."),
          Integer.parseInt(nodesOutOfSpace));
    }

    for (Map.Entry<String, Long> e : nodeInfo.entrySet()) {
      metrics.addGauge(
          Interns.info(e.getKey(), diskMetricDescription(e.getKey())),
          e.getValue());
    }
    registry.snapshot(metrics, all);
  }

  private String diskMetricDescription(String metric) {
    StringBuilder sb = new StringBuilder();
    sb.append("Total");
    if (metric.indexOf("Maintenance") >= 0) {
      sb.append(" maintenance");
    } else if (metric.indexOf("Decommissioned") >= 0) {
      sb.append(" decommissioned");
    }
    if (metric.indexOf("DiskCapacity") >= 0) {
      sb.append(" disk capacity");
    } else if (metric.indexOf("DiskUsed") >= 0) {
      sb.append(" disk capacity used");
    } else if (metric.indexOf("DiskRemaining") >= 0) {
      sb.append(" disk capacity remaining");
    } else if (metric.indexOf("SSDCapacity") >= 0) {
      sb.append(" SSD capacity");
    } else if (metric.indexOf("SSDUsed") >= 0) {
      sb.append(" SSD capacity used");
    } else if (metric.indexOf("SSDRemaining") >= 0) {
      sb.append(" SSD capacity remaining");
    }
    return sb.toString();
  }
}

