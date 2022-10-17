/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.node;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.OzoneConsts;

import java.util.HashMap;
import java.util.Map;

/**
 * Class contains metrics related to the NodeDecommissionManager.
 */
@Metrics(about = "Node Decommission Metrics", context = OzoneConsts.OZONE)
public final class NodeDecommissionMetrics implements MetricsSource {
  public static final String METRICS_SOURCE_NAME =
        org.apache.hadoop.hdds.scm.node.NodeDecommissionMetrics
                .class.getSimpleName();

  @Metric("Number of nodes tracked for decommissioning and maintenance.")
  private MutableGaugeLong trackedDecommissioningMaintenanceNodesTotal;

  @Metric("Number of nodes tracked for recommissioning.")
  private MutableGaugeLong trackedRecommissionNodesTotal;

  @Metric("Number of nodes tracked with pipelines waiting to close.")
  private MutableGaugeLong trackedPipelinesWaitingToCloseTotal;

  @Metric("Number of containers under replicated in tracked nodes.")
  private MutableGaugeLong trackedContainersUnderReplicatedTotal;

  @Metric("Number of containers unhealthy in tracked nodes.")
  private MutableGaugeLong trackedContainersUnhealthyTotal;

  @Metric("Number of containers sufficiently replicated in tracked nodes.")
  private MutableGaugeLong trackedContainersSufficientlyReplicatedTotal;

  private MetricsRegistry registry;

  private Map<String, MutableGaugeLong> trackedSufficientlyReplicatedByHost;

  private Map<String, MutableGaugeLong> trackedUnderReplicatedByHost;

  private Map<String, MutableGaugeLong> trackedUnhealthyContainersByHost;

  private Map<String, MutableGaugeLong> trackedPipelinesWaitingToCloseByHost;

  /** Private constructor. */
  private NodeDecommissionMetrics() {
    this.registry = new MetricsRegistry(METRICS_SOURCE_NAME);
    trackedSufficientlyReplicatedByHost = new HashMap<>();
    trackedUnderReplicatedByHost = new HashMap<>();
    trackedUnhealthyContainersByHost = new HashMap();
    trackedPipelinesWaitingToCloseByHost = new HashMap();
  }

  /**
   * Create and returns NodeDecommissionMetrics instance.
   *
   * @return NodeDecommissionMetrics
   */
  public static NodeDecommissionMetrics create() {
    return DefaultMetricsSystem.instance().register(METRICS_SOURCE_NAME,
            "Metrics tracking the progress of nodes in the "
                    + "Decommissioning and Maintenance workflows.  "
                    + "Tracks num nodes in mode and container "
                    + "replications state and pipelines waiting to close",
            new NodeDecommissionMetrics());
  }

  /**
   * Get aggregated gauge metrics.
   */
  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector
        .addRecord(METRICS_SOURCE_NAME);
    trackedDecommissioningMaintenanceNodesTotal.snapshot(builder, all);
    trackedRecommissionNodesTotal.snapshot(builder, all);
    trackedPipelinesWaitingToCloseTotal.snapshot(builder, all);
    trackedContainersUnderReplicatedTotal.snapshot(builder, all);
    trackedContainersUnhealthyTotal.snapshot(builder, all);
    trackedContainersSufficientlyReplicatedTotal.snapshot(builder, all);

    registry.snapshot(builder, all);
  }

  /**
   * Unregister the metrics instance.
   */
  public void unRegister() {
    DefaultMetricsSystem.instance().unregisterSource(METRICS_SOURCE_NAME);
  }

  public void setTrackedDecommissioningMaintenanceNodesTotal(
            long numNodesTracked) {
    trackedDecommissioningMaintenanceNodesTotal
            .set(numNodesTracked);
  }

  public void setTrackedRecommissionNodesTotal(
          long numNodesTrackedRecommissioned) {
    trackedRecommissionNodesTotal.set(numNodesTrackedRecommissioned);
  }

  public void setTrackedPipelinesWaitingToCloseTotal(
          long numTrackedPipelinesWaitToClose) {
    trackedPipelinesWaitingToCloseTotal
            .set(numTrackedPipelinesWaitToClose);
  }

  public void setTrackedContainersUnderReplicatedTotal(
          long numTrackedUnderReplicated) {
    trackedContainersUnderReplicatedTotal
            .set(numTrackedUnderReplicated);
  }

  public void setTrackedContainersUnhealthyTotal(
          long numTrackedUnhealthy) {
    trackedContainersUnhealthyTotal
            .set(numTrackedUnhealthy);
  }

  public void setTrackedContainersSufficientlyReplicatedTotal(
          long numTrackedSufficientlyReplicated) {
    trackedContainersSufficientlyReplicatedTotal
            .set(numTrackedSufficientlyReplicated);
  }

  public long getTrackedDecommissioningMaintenanceNodesTotal() {
    return trackedDecommissioningMaintenanceNodesTotal.value();
  }

  public long getTrackedRecommissionNodesTotal() {
    return trackedRecommissionNodesTotal.value();
  }

  public long getTrackedPipelinesWaitingToCloseTotal() {
    return trackedPipelinesWaitingToCloseTotal.value();
  }

  public long getTrackedContainersUnderReplicatedTotal() {
    return trackedContainersUnderReplicatedTotal.value();
  }

  public long getTrackedContainersUnhealthyTotal() {
    return trackedContainersUnhealthyTotal.value();
  }

  public long getTrackedContainersSufficientlyReplicatedTotal() {
    return trackedContainersSufficientlyReplicatedTotal.value();
  }

  public void metricRecordPipelineWaitingToCloseByHost(String host,
                                                       long num) {
    trackedPipelinesWaitingToCloseByHost.computeIfAbsent(host,
            hostID -> registry.newGauge(
                    Interns.info("trackedPipelinesWaitingToClose-" + hostID,
                            "Number of pipelines waiting to close for "
                                    + "host in decommissioning and "
                                    + "maintenance mode"),
                    0L)
    ).set(num);
  }

  public void metricRecordOfReplicationByHost(String host,
                               long sufficientlyReplicated,
                               long underReplicated,
                               long unhealthy) {
    trackedSufficientlyReplicatedByHost.computeIfAbsent(host,
            hostID -> registry.newGauge(
                    Interns.info("trackedSufficientlyReplicated-" + hostID,
                            "Number of sufficiently replicated containers "
                                    + "for host in decommissioning and "
                                    + "maintenance mode"),
                    0L)
    ).set(sufficientlyReplicated);

    trackedUnderReplicatedByHost.computeIfAbsent(host,
            hostID -> registry.newGauge(
                    Interns.info("trackedUnderReplicated-" + hostID,
                            "Number of under-replicated containers "
                                    + "for host in decommissioning and "
                                    + "maintenance mode"),
                    0L)
    ).set(underReplicated);

    trackedUnhealthyContainersByHost.computeIfAbsent(host,
            hostID -> registry.newGauge(
                    Interns.info("trackedUnhealthyContainers-" + hostID,
                            "Number of unhealthy containers "
                                    + "for host in decommissioning and "
                                    + "maintenance mode"),
                    0L)
    ).set(unhealthy);
  }

  @VisibleForTesting
  public Long getTrackedPipelinesWaitingToCloseByHost(String host) {
    if (!trackedPipelinesWaitingToCloseByHost.containsKey(host)) {
      return null;
    }
    return trackedPipelinesWaitingToCloseByHost.get(host).value();
  }

  @VisibleForTesting
  public Long getTrackedSufficientlyReplicatedByHost(String host) {
    if (!trackedSufficientlyReplicatedByHost.containsKey(host)) {
      return null;
    }
    return trackedSufficientlyReplicatedByHost.get(host).value();
  }

  @VisibleForTesting
  public Long getTrackedUnderReplicatedByHost(String host) {
    if (!trackedUnderReplicatedByHost.containsKey(host)) {
      return null;
    }
    return trackedUnderReplicatedByHost.get(host).value();
  }

  @VisibleForTesting
  public Long getTrackedUnhealthyContainersByHost(String host) {
    if (!trackedUnhealthyContainersByHost.containsKey(host)) {
      return null;
    }
    return trackedUnhealthyContainersByHost.get(host).value();
  }
}
