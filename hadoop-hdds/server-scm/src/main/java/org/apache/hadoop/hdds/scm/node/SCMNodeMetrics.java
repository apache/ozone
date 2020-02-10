/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.node;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
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

  private static final String SOURCE_NAME =
      SCMNodeMetrics.class.getSimpleName();

  private @Metric MutableCounterLong numHBProcessed;
  private @Metric MutableCounterLong numHBProcessingFailed;
  private @Metric MutableCounterLong numNodeReportProcessed;
  private @Metric MutableCounterLong numNodeReportProcessingFailed;
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
   * Get aggregated counter and gauage metrics.
   */
  @Override
  @SuppressWarnings("SuspiciousMethodCalls")
  public void getMetrics(MetricsCollector collector, boolean all) {
    Map<String, Map<String, Integer>> nodeCount = managerMXBean.getNodeCount();
    Map<String, Long> nodeInfo = managerMXBean.getNodeInfo();

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
    for(Map.Entry<String, Map<String, Integer>> e : nodeCount.entrySet()) {
      for(Map.Entry<String, Integer> h : e.getValue().entrySet()) {
        metrics.addGauge(
            Interns.info(
                StringUtils.camelize(e.getKey()+"_"+h.getKey()+"_nodes"),
                "Number of "+e.getKey()+" "+h.getKey()+" datanodes"),
            h.getValue());
      }
    }

    registry.snapshot(metrics
        .addGauge(Interns.info("DiskCapacity",
            "Total disk capacity"),
            nodeInfo.get("DISKCapacity"))
        .addGauge(Interns.info("DiskUsed",
            "Total disk capacity used"),
            nodeInfo.get("DISKUsed"))
        .addGauge(Interns.info("DiskRemaining",
            "Total disk capacity remaining"),
            nodeInfo.get("DISKRemaining"))
        .addGauge(Interns.info("SSDCapacity",
            "Total ssd capacity"),
            nodeInfo.get("SSDCapacity"))
        .addGauge(Interns.info("SSDUsed",
            "Total ssd capacity used"),
            nodeInfo.get("SSDUsed"))
        .addGauge(Interns.info("SSDRemaining",
            "Total disk capacity remaining"),
            nodeInfo.get("SSDRemaining")),
        all);
  }
}
