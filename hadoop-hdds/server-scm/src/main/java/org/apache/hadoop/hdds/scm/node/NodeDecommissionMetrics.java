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

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
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

/**
 * Class contains metrics related to the NodeDecommissionManager.
 */
@Metrics(about = "Node Decommission Metrics", context = OzoneConsts.OZONE)
public final class NodeDecommissionMetrics implements MetricsSource {
  public static final String METRICS_SOURCE_NAME =
      org.apache.hadoop.hdds.scm.node.NodeDecommissionMetrics
          .class.getSimpleName();

  @Metric("Number of nodes tracked for decommissioning and maintenance.")
  private MutableGaugeLong decommissioningMaintenanceNodesTotal;

  @Metric("Number of nodes tracked for recommissioning.")
  private MutableGaugeLong recommissionNodesTotal;

  @Metric("Number of nodes tracked with pipelines waiting to close.")
  private MutableGaugeLong pipelinesWaitingToCloseTotal;

  @Metric("Number of containers under replicated in tracked nodes.")
  private MutableGaugeLong containersUnderReplicatedTotal;

  @Metric("Number of containers not fully closed in tracked nodes.")
  private MutableGaugeLong containersUnClosedTotal;

  @Metric("Number of containers sufficiently replicated in tracked nodes.")
  private MutableGaugeLong containersSufficientlyReplicatedTotal;

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
    decommissioningMaintenanceNodesTotal.snapshot(builder, all);
    recommissionNodesTotal.snapshot(builder, all);
    pipelinesWaitingToCloseTotal.snapshot(builder, all);
    containersUnderReplicatedTotal.snapshot(builder, all);
    containersUnClosedTotal.snapshot(builder, all);
    containersSufficientlyReplicatedTotal.snapshot(builder, all);

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
          .addGauge(ContainerStateInWorkflow.HOST_UNCLOSED_CONTAINERS,
              e.getValue().getUnclosedContainers())
          .addGauge(ContainerStateInWorkflow.HOST_START_TIME,
              e.getValue().getStartTime());
    }
    recordBuilder.endRecord();
  }

  /**
   * Unregister the metrics instance.
   */
  public void unRegister() {
    DefaultMetricsSystem.instance().unregisterSource(METRICS_SOURCE_NAME);
  }

  public synchronized void setDecommissioningMaintenanceNodesTotal(
            long numNodesTracked) {
    decommissioningMaintenanceNodesTotal
        .set(numNodesTracked);
  }

  public synchronized void setRecommissionNodesTotal(
          long numNodesTrackedRecommissioned) {
    recommissionNodesTotal.set(numNodesTrackedRecommissioned);
  }

  public synchronized void setPipelinesWaitingToCloseTotal(
          long numTrackedPipelinesWaitToClose) {
    pipelinesWaitingToCloseTotal
        .set(numTrackedPipelinesWaitToClose);
  }

  public synchronized void setContainersUnderReplicatedTotal(
          long numTrackedUnderReplicated) {
    containersUnderReplicatedTotal
        .set(numTrackedUnderReplicated);
  }

  public synchronized void setContainersUnClosedTotal(
          long numTrackedUnclosed) {
    containersUnClosedTotal.set(numTrackedUnclosed);
  }

  public synchronized void setContainersSufficientlyReplicatedTotal(
          long numTrackedSufficientlyReplicated) {
    containersSufficientlyReplicatedTotal
        .set(numTrackedSufficientlyReplicated);
  }

  public synchronized long getDecommissioningMaintenanceNodesTotal() {
    return decommissioningMaintenanceNodesTotal.value();
  }

  public synchronized long getRecommissionNodesTotal() {
    return recommissionNodesTotal.value();
  }

  public synchronized long getPipelinesWaitingToCloseTotal() {
    return pipelinesWaitingToCloseTotal.value();
  }

  public synchronized long getContainersUnderReplicatedTotal() {
    return containersUnderReplicatedTotal.value();
  }

  public synchronized long getContainersUnClosedTotal() {
    return containersUnClosedTotal.value();
  }

  public synchronized long getContainersSufficientlyReplicatedTotal() {
    return containersSufficientlyReplicatedTotal.value();
  }

  public synchronized void metricRecordOfContainerStateByHost(
      Map<String, ContainerStateInWorkflow> containerStatesByHost) {
    metricsByHost.clear();
    metricsByHost.putAll(containerStatesByHost);
  }

  @VisibleForTesting
  public Long getPipelinesWaitingToCloseByHost(String host) {
    ContainerStateInWorkflow workflowMetrics = metricsByHost.get(host);
    return workflowMetrics == null ? null :
        workflowMetrics.getPipelinesWaitingToClose();
  }

  @VisibleForTesting
  public Long getSufficientlyReplicatedByHost(String host) {
    ContainerStateInWorkflow workflowMetrics = metricsByHost.get(host);
    return workflowMetrics == null ? null :
        workflowMetrics.getSufficientlyReplicated();
  }

  @VisibleForTesting
  public Long getUnderReplicatedByHost(String host) {
    ContainerStateInWorkflow workflowMetrics = metricsByHost.get(host);
    return workflowMetrics == null ? null :
        workflowMetrics.getUnderReplicatedContainers();
  }

  @VisibleForTesting
  public Long getUnClosedContainersByHost(String host) {
    ContainerStateInWorkflow workflowMetrics = metricsByHost.get(host);
    return workflowMetrics == null ? null :
        workflowMetrics.getUnclosedContainers();
  }

  /**
   * Inner class for snapshot of Datanode ContainerState in
   * Decommissioning and Maintenance mode workflow.
   */
  public static final class ContainerStateInWorkflow {
    private long sufficientlyReplicated = 0;
    private long unclosedContainers = 0;
    private long underReplicatedContainers = 0;
    private String host = "";
    private long pipelinesWaitingToClose = 0;
    private long startTime = 0;

    private static final MetricsInfo HOST_UNDER_REPLICATED = Interns.info(
        "UnderReplicatedDN",
        "Number of under-replicated containers "
            + "for host in decommissioning and "
            + "maintenance mode");

    private static final MetricsInfo HOST_PIPELINES_TO_CLOSE = Interns.info(
        "PipelinesWaitingToCloseDN",
        "Number of pipelines waiting to close for "
            + "host in decommissioning and "
            + "maintenance mode");

    private static final MetricsInfo HOST_SUFFICIENTLY_REPLICATED = Interns
        .info(
            "SufficientlyReplicatedDN",
            "Number of sufficiently replicated containers "
                + "for host in decommissioning and "
                + "maintenance mode");

    private static final MetricsInfo HOST_UNCLOSED_CONTAINERS = Interns.info("UnclosedContainersDN",
        "Number of containers not fully closed for host in decommissioning and maintenance mode");

    private static final MetricsInfo HOST_START_TIME = Interns.info("StartTimeDN",
        "Time at which decommissioning was started");

    public ContainerStateInWorkflow(String host,
                                    long sufficiently,
                                    long under,
                                    long unclosed,
                                    long pipelinesToClose,
                                    long startTime) {
      this.host = host;
      sufficientlyReplicated = sufficiently;
      underReplicatedContainers = under;
      unclosedContainers = unclosed;
      pipelinesWaitingToClose = pipelinesToClose;
      this.startTime = startTime;
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

    public long getUnclosedContainers() {
      return unclosedContainers;
    }

    public long getStartTime() {
      return startTime;
    }
  }
}
