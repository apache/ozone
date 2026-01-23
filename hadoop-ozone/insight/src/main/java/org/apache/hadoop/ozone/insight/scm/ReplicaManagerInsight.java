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

package org.apache.hadoop.ozone.insight.scm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.ozone.insight.BaseInsightPoint;
import org.apache.hadoop.ozone.insight.Component.Type;
import org.apache.hadoop.ozone.insight.LoggerSource;
import org.apache.hadoop.ozone.insight.MetricDisplay;
import org.apache.hadoop.ozone.insight.MetricGroupDisplay;

/**
 * Insight definition to check the replication manager internal state.
 */
public class ReplicaManagerInsight extends BaseInsightPoint {

  @Override
  public List<LoggerSource> getRelatedLoggers(boolean verbose,
      Map<String, String> filters) {
    List<LoggerSource> loggers = new ArrayList<>();
    loggers.add(new LoggerSource(Type.SCM, ReplicationManager.class,
        defaultLevel(verbose)));
    return loggers;
  }

  @Override
  public List<MetricGroupDisplay> getMetrics(Map<String, String> filters) {
    List<MetricGroupDisplay> display = new ArrayList<>();

    MetricGroupDisplay containerMetrics = new MetricGroupDisplay(Type.SCM,
        "ReplicationManager Container Metrics");
    containerMetrics.addMetrics(new MetricDisplay("Open Containers",
        "replication_manager_metrics_open_containers"));
    containerMetrics.addMetrics(new MetricDisplay("Closing Containers",
        "replication_manager_metrics_closing_containers"));
    containerMetrics.addMetrics(new MetricDisplay("Quasi-closed Containers",
        "replication_manager_metrics_quasi_closed_containers"));
    containerMetrics.addMetrics(new MetricDisplay("Closed Containers",
        "replication_manager_metrics_closed_containers"));
    containerMetrics.addMetrics(new MetricDisplay("Deleting Containers",
        "replication_manager_metrics_deleting_containers"));
    containerMetrics.addMetrics(new MetricDisplay("Deleted Containers",
        "replication_manager_metrics_deleted_containers"));
    containerMetrics.addMetrics(new MetricDisplay("Recovering Containers",
        "replication_manager_metrics_recovering_containers"));
    containerMetrics.addMetrics(new MetricDisplay("UnderReplicated Containers",
        "replication_manager_metrics_under_replicated_containers"));
    containerMetrics.addMetrics(new MetricDisplay("MisReplicated Containers",
        "replication_manager_metrics_mis_replicated_containers"));
    containerMetrics.addMetrics(new MetricDisplay("OverReplicated Containers",
        "replication_manager_metrics_over_replicated_containers"));
    containerMetrics.addMetrics(new MetricDisplay("Missing Containers",
        "replication_manager_metrics_missing_containers"));
    containerMetrics.addMetrics(new MetricDisplay("Unhealthy Containers",
        "replication_manager_metrics_unhealthy_containers"));
    containerMetrics.addMetrics(new MetricDisplay("Empty Containers",
        "replication_manager_metrics_empty_containers"));
    containerMetrics.addMetrics(new MetricDisplay("Open Unhealthy Containers",
        "replication_manager_metrics_open_unhealthy_containers"));
    containerMetrics.addMetrics(new MetricDisplay(
        "Stuck QuasiClosed Containers",
        "replication_manager_metrics_stuck_quasi_closed_containers"));
    display.add(containerMetrics);

    MetricGroupDisplay ecMetrics = new MetricGroupDisplay(Type.SCM,
        "ReplicationManager EC Metrics");
    ecMetrics.addMetrics(new MetricDisplay("EcReplicationCmdsSentTotal",
        "replication_manager_metrics_ec_replication_cmds_sent_total"));
    ecMetrics.addMetrics(new MetricDisplay("EcDeletionCmdsSentTotal",
        "replication_manager_metrics_ec_deletion_cmds_sent_total"));
    ecMetrics.addMetrics(new MetricDisplay("EcReplicasCreatedTotal",
        "replication_manager_metrics_ec_replicas_created_total"));
    ecMetrics.addMetrics(new MetricDisplay("EcReplicasDeletedTotal",
        "replication_manager_metrics_ec_replicas_deleted_total"));
    ecMetrics.addMetrics(new MetricDisplay("EcReconstructionCmdsSentTotal",
        "replication_manager_metrics_ec_reconstruction_cmds_sent_total"));
    ecMetrics.addMetrics(new MetricDisplay("EcReplicaCreateTimeoutTotal",
        "replication_manager_metrics_ec_replica_create_timeout_total"));
    ecMetrics.addMetrics(new MetricDisplay("EcReplicasDeletedTotal",
        "replication_manager_metrics_ec_replicas_deleted_total"));
    ecMetrics.addMetrics(new MetricDisplay("EcReplicaDeleteTimeoutTotal",
        "replication_manager_metrics_ec_replica_delete_timeout_total"));
    ecMetrics.addMetrics(new MetricDisplay(
        "EcReconstructionCmdsDeferredTotal",
        "replication_manager_metrics_ec_reconstruction_cmds_deferred_total"));
    ecMetrics.addMetrics(new MetricDisplay(
        "EcPartialReconstructionSkippedTotal",
        "replication_manager_metrics_ec_partial_reconstruction_skipped_total"));
    ecMetrics.addMetrics(new MetricDisplay(
        "EcPartialReconstructionCriticalTotal",
        "replication_manager_metrics_ec_partial_reconstruction_" +
            "critical_total"));
    ecMetrics.addMetrics(new MetricDisplay(
        "EcPartialReconstructionNoneOverloadedTotal",
        "replication_manager_metrics_ec_partial_reconstruction_none_" +
            "overloaded_total"));
    ecMetrics.addMetrics(new MetricDisplay(
        "EcPartialReplicationForOutOfServiceReplicasTotal",
        "replication_manager_metrics_ec_partial_replication_for_" +
            "out_of_service_replicas_total"));
    ecMetrics.addMetrics(new MetricDisplay(
        "EcPartialReplicationForMisReplicationTotal",
        "replication_manager_metrics_ec_partial_replication_for_" +
            "mis_replication_total"));
    display.add(ecMetrics);

    MetricGroupDisplay replicaMetrics = new MetricGroupDisplay(Type.SCM,
        "ReplicationManager Metrics");
    replicaMetrics.addMetrics(new MetricDisplay("InflightReplication",
        "replication_manager_metrics_inflight_replication"));
    replicaMetrics.addMetrics(new MetricDisplay("InflightDeletion",
        "replication_manager_metrics_inflight_deletion"));
    replicaMetrics.addMetrics(new MetricDisplay("InflightEcReplication",
        "replication_manager_metrics_inflight_ec_replication"));
    replicaMetrics.addMetrics(new MetricDisplay("InflightEcDeletion",
        "replication_manager_metrics_inflight_ec_deletion"));
    replicaMetrics.addMetrics(new MetricDisplay("UnderReplicatedQueueSize",
        "replication_manager_metrics_under_replicated_queue_size"));
    replicaMetrics.addMetrics(new MetricDisplay("OverReplicatedQueueSize",
        "replication_manager_metrics_over_replicated_queue_size"));
    replicaMetrics.addMetrics(new MetricDisplay("ReplicationCmdsSentTotal",
        "replication_manager_metrics_replication_cmds_sent_total"));
    replicaMetrics.addMetrics(new MetricDisplay("ReplicasCreatedTotal",
        "replication_manager_metrics_replicas_created_total"));
    replicaMetrics.addMetrics(new MetricDisplay(
        "ReplicaCreateTimeoutTotal",
        "replication_manager_metrics_replica_create_timeout_total"));
    replicaMetrics.addMetrics(new MetricDisplay(
        "DeletionCmdsSentTotal",
        "replication_manager_metrics_deletion_cmds_sent_total"));
    replicaMetrics.addMetrics(new MetricDisplay(
        "ReplicasDeletedTotal",
        "replication_manager_metrics_replicas_deleted_total"));
    replicaMetrics.addMetrics(new MetricDisplay(
        "ReplicaDeleteTimeoutTotal",
        "replication_manager_metrics_replica_delete_timeout_total"));
    replicaMetrics.addMetrics(new MetricDisplay(
        "DeleteContainerCmdsDeferredTotal",
        "replication_manager_metrics_delete_container_cmds_deferred_total"));
    replicaMetrics.addMetrics(new MetricDisplay(
        "ReplicateContainerCmdsDeferredTotal",
        "replication_manager_metrics_replicate_container_cmds_deferred_total"));
    replicaMetrics.addMetrics(new MetricDisplay(
        "PendingReplicationLimitReachedTotal",
        "replication_manager_metrics_pending_replication_limit_reached_total"));
    replicaMetrics.addMetrics(new MetricDisplay(
        "PartialReplicationTotal",
        "replication_manager_metrics_partial_replication_total"));
    replicaMetrics.addMetrics(new MetricDisplay(
        "PartialReplicationForMisReplicationTotal",
        "replication_manager_metrics_partial_replication_for_" +
            "mis_replication_total"));
    display.add(replicaMetrics);
    return display;
  }

  @Override
  public List<Class> getConfigurationClasses() {
    List<Class> result = new ArrayList<>();
    result.add(ReplicationManager.ReplicationManagerConfiguration.class);
    return result;
  }

  @Override
  public String getDescription() {
    return "SCM closed container replication manager";
  }

}
