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

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Class contains metrics related to the NodeDecommissionManager.
 */
@Metrics(about = "Node Decommission Metrics", context = OzoneConsts.OZONE)
public final class NodeDecommissionMetrics implements MetricsSource {

    public static final String METRICS_SOURCE_NAME =
            org.apache.hadoop.hdds.scm.node.NodeDecommissionMetrics.class.getSimpleName();

    @Metric("Number of nodes tracked for decommissioning and maintenance.")
    private MutableGaugeLong totalTrackedDecommissioningMaintenanceNodes;

    @Metric("Number of nodes tracked for recommissioning.")
    private MutableGaugeLong totalTrackedRecommissionNodes;

    @Metric("Number of nodes tracked with pipelines waiting to close.")
    private MutableGaugeLong totalTrackedPipelinesWaitingToClose;

    @Metric("Number of containers under replicated in tracked nodes.")
    private MutableGaugeLong totalTrackedContainersUnderReplicated;

    @Metric("Number of containers over replicated in tracked nodes.")
    private MutableGaugeLong totalTrackedContainersOverReplicated;

    @Metric("Number of containers sufficiently replicated in tracked nodes.")
    private MutableGaugeLong totalTrackedContainersSufficientlyReplicated;

    private MetricsRegistry registry;

    private NodeDecommissionManager nodeDecommissionManager;

    private NodeDecommissionMetrics(NodeDecommissionManager manager) {
        this.registry = new MetricsRegistry(METRICS_SOURCE_NAME);
        this.nodeDecommissionManager = manager;
    }

    public static NodeDecommissionMetrics create(NodeDecommissionManager manager) {
        return DefaultMetricsSystem.instance().register(METRICS_SOURCE_NAME,
                "Metrics tracking the progress of nodes in the "
                        + "Decommissioning and Maintenance workflows.  "
                        + "Tracks num nodes in mode and container "
                        + "replications state and pipeline state",
                new NodeDecommissionMetrics(manager));
    }

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
        MetricsRecordBuilder builder = collector.addRecord(METRICS_SOURCE_NAME);
        totalTrackedDecommissioningMaintenanceNodes.snapshot(builder, all);
        totalTrackedRecommissionNodes.snapshot(builder, all);
        totalTrackedPipelinesWaitingToClose.snapshot(builder, all);
        totalTrackedContainersUnderReplicated.snapshot(builder, all);
        totalTrackedContainersOverReplicated.snapshot(builder, all);
        totalTrackedContainersSufficientlyReplicated.snapshot(builder, all);

    }

    public void unRegister() {
        DefaultMetricsSystem.instance().unregisterSource(METRICS_SOURCE_NAME);
    }

    public void setTotalTrackedDecommissioningMaintenanceNodes(
            long numNodesTracked) {
        totalTrackedDecommissioningMaintenanceNodes
                .set(numNodesTracked);
    }

    public void setTotalTrackedRecommissionNodes(
            long numNodesTrackedRecommissioned) {
        totalTrackedRecommissionNodes
                .set(numNodesTrackedRecommissioned);
    }

    public void setTotalTrackedPipelinesWaitingToClose(
            long numTrackedPipelinesWaitToClose) {
        totalTrackedPipelinesWaitingToClose
                .set(numTrackedPipelinesWaitToClose);
    }

    public void setTotalTrackedContainersUnderReplicated(
            long numTrackedUnderReplicated) {
        totalTrackedContainersUnderReplicated
                .set(numTrackedUnderReplicated);
    }

    public void setTotalTrackedContainersOverReplicated(
            long numTrackedOverReplicated) {
        totalTrackedContainersUnderReplicated
                .set(numTrackedOverReplicated);
    }

    public void setTotalTrackedContainersSufficientlyReplicated(
            long numTrackedSufficientlyReplicated) {
        totalTrackedContainersSufficientlyReplicated
                    .set(numTrackedSufficientlyReplicated);
    }

    public long getTotalTrackedDecommissioningMaintenanceNodes() {
        return totalTrackedDecommissioningMaintenanceNodes.value();
    }

    public long getTotalTrackedPipelinesWaitingToClose() {
        return totalTrackedPipelinesWaitingToClose.value();
    }

    public long getTotalTrackedContainersUnderReplicated() {
        return totalTrackedContainersUnderReplicated.value();
    }

    public long getTotalTrackedContainersOverReplicated() {
        return totalTrackedContainersUnderReplicated.value();
    }

    public long getTotalTrackedContainersSufficientlyReplicated() {
        return totalTrackedContainersSufficientlyReplicated.value();
    }
}
