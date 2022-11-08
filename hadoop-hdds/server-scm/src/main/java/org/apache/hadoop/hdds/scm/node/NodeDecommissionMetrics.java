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
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsTag;
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

  /**
   * Inner class for snapshot of Datanode ContainerState in
   * Decommissioning and Maintenance mode workflow.
   */
  public static final class ContainerStateInWorkflow {
    private long sufficientlyReplicated = 0;
    private long unhealthyContainers = 0;
    private long underReplicatedContainers = 0;
    private String host = "";
    private long pipelinesWaitingToClose = 0;

    private static final MetricsInfo HOST_UNDER_REPLICATED = Interns.info(
        "TrackedUnderReplicatedDN",
        "Number of under-replicated containers "
            + "for host in decommissioning and "
            + "maintenance mode");

    private static final MetricsInfo HOST_PIPELINES_TO_CLOSE = Interns.info(
        "TrackedPipelinesWaitingToCloseDN",
        "Number of pipelines waiting to close for "
            + "host in decommissioning and "
            + "maintenance mode");

    private static final MetricsInfo HOST_SUFFICIENTLY_REPLICATED = Interns
        .info(
            "TrackedSufficientlyReplicatedDN",
        "Number of sufficiently replicated containers "
            + "for host in decommissioning and "
            + "maintenance mode");

    private static final MetricsInfo HOST_UNHEALTHY_CONTAINERS = Interns.info(
        "TrackedUnhealthyContainersDN",
        "Number of unhealthy containers "
            + "for host in decommissioning and "
            + "maintenance mode");


    public ContainerStateInWorkflow(String host,
                                    long sufficiently,
                                    long under,
                                    long unhealthy,
                                    long pipelinesToClose) {
      this.host = host;
      sufficientlyReplicated = sufficiently;
      underReplicatedContainers = under;
      unhealthyContainers = unhealthy;
      pipelinesWaitingToClose = pipelinesToClose;
    }

    public String getHost() {
      return host;
    }

    public long getSufficientlyReplicated() {
      return sufficientlyReplicated;
    }

    public long getPipelinesWaitingToClose() {
      return pipelinesWaitingToClose;
    }

    public long getUnderReplicatedContainers() {
      return underReplicatedContainers;
    }

    public long getUnhealthyContainers() {
      return unhealthyContainers;
    }
  }

  private MetricsRegistry registry;

  private Map<String, ContainerStateInWorkflow> metricsByHost;

  /** Private constructor. */
  private NodeDecommissionMetrics() {
    this.registry = new MetricsRegistry(METRICS_SOURCE_NAME);
    metricsByHost = new HashMap<>();
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
  public synchronized void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector
        .addRecord(METRICS_SOURCE_NAME);
    trackedDecommissioningMaintenanceNodesTotal.snapshot(builder, all);
    trackedRecommissionNodesTotal.snapshot(builder, all);
    trackedPipelinesWaitingToCloseTotal.snapshot(builder, all);
    trackedContainersUnderReplicatedTotal.snapshot(builder, all);
    trackedContainersUnhealthyTotal.snapshot(builder, all);
    trackedContainersSufficientlyReplicatedTotal.snapshot(builder, all);

    MetricsRecordBuilder recordBuilder = builder;
    for (Map.Entry<String, ContainerStateInWorkflow> e :
        metricsByHost.entrySet()) {
      recordBuilder = recordBuilder.endRecord().addRecord(METRICS_SOURCE_NAME)
          .add(new MetricsTag(Interns.info("datanode",
              "datanode host in decommission maintenance workflow"),
              e.getValue().getHost()))
          .addGauge(ContainerStateInWorkflow.HOST_PIPELINES_TO_CLOSE,
              e.getValue().getPipelinesWaitingToClose())
          .addGauge(ContainerStateInWorkflow.HOST_UNDER_REPLICATED,
              e.getValue().getUnderReplicatedContainers())
          .addGauge(ContainerStateInWorkflow.HOST_SUFFICIENTLY_REPLICATED,
              e.getValue().getSufficientlyReplicated())
          .addGauge(ContainerStateInWorkflow.HOST_UNHEALTHY_CONTAINERS,
              e.getValue().getUnhealthyContainers());
    }
    recordBuilder.endRecord();
  }

  /**
   * Unregister the metrics instance.
   */
  public void unRegister() {
    DefaultMetricsSystem.instance().unregisterSource(METRICS_SOURCE_NAME);
  }

  public synchronized void setTrackedDecommissioningMaintenanceNodesTotal(
            long numNodesTracked) {
    trackedDecommissioningMaintenanceNodesTotal
        .set(numNodesTracked);
  }

  public synchronized void setTrackedRecommissionNodesTotal(
          long numNodesTrackedRecommissioned) {
    trackedRecommissionNodesTotal.set(numNodesTrackedRecommissioned);
  }

  public synchronized void setTrackedPipelinesWaitingToCloseTotal(
          long numTrackedPipelinesWaitToClose) {
    trackedPipelinesWaitingToCloseTotal
        .set(numTrackedPipelinesWaitToClose);
  }

  public synchronized void setTrackedContainersUnderReplicatedTotal(
          long numTrackedUnderReplicated) {
    trackedContainersUnderReplicatedTotal
        .set(numTrackedUnderReplicated);
  }

  public synchronized void setTrackedContainersUnhealthyTotal(
          long numTrackedUnhealthy) {
    trackedContainersUnhealthyTotal
        .set(numTrackedUnhealthy);
  }

  public synchronized void setTrackedContainersSufficientlyReplicatedTotal(
          long numTrackedSufficientlyReplicated) {
    trackedContainersSufficientlyReplicatedTotal
        .set(numTrackedSufficientlyReplicated);
  }

  public synchronized long getTrackedDecommissioningMaintenanceNodesTotal() {
    return trackedDecommissioningMaintenanceNodesTotal.value();
  }

  public synchronized long getTrackedRecommissionNodesTotal() {
    return trackedRecommissionNodesTotal.value();
  }

  public synchronized long getTrackedPipelinesWaitingToCloseTotal() {
    return trackedPipelinesWaitingToCloseTotal.value();
  }

  public synchronized long getTrackedContainersUnderReplicatedTotal() {
    return trackedContainersUnderReplicatedTotal.value();
  }

  public synchronized long getTrackedContainersUnhealthyTotal() {
    return trackedContainersUnhealthyTotal.value();
  }

  public synchronized long getTrackedContainersSufficientlyReplicatedTotal() {
    return trackedContainersSufficientlyReplicatedTotal.value();
  }

  public synchronized void metricRecordOfContainerStateByHost(
      Map<String, ContainerStateInWorkflow> containerStatesByHost) {
    metricsByHost.clear();
    metricsByHost.putAll(containerStatesByHost);
  }

  @VisibleForTesting
  public Long getTrackedPipelinesWaitingToCloseByHost(String host) {
    ContainerStateInWorkflow workflowMetrics = metricsByHost.get(host);
    return workflowMetrics == null ? null :
        workflowMetrics.getPipelinesWaitingToClose();
  }

  @VisibleForTesting
  public Long getTrackedSufficientlyReplicatedByHost(String host) {
    ContainerStateInWorkflow workflowMetrics = metricsByHost.get(host);
    return workflowMetrics == null ? null :
        workflowMetrics.getSufficientlyReplicated();
  }

  @VisibleForTesting
  public Long getTrackedUnderReplicatedByHost(String host) {
    ContainerStateInWorkflow workflowMetrics = metricsByHost.get(host);
    return workflowMetrics == null ? null :
        workflowMetrics.getUnderReplicatedContainers();
  }

  @VisibleForTesting
  public Long getTrackedUnhealthyContainersByHost(String host) {
    ContainerStateInWorkflow workflowMetrics = metricsByHost.get(host);
    return workflowMetrics == null ? null :
        workflowMetrics.getUnhealthyContainers();
  }
}
