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

package org.apache.hadoop.hdds.scm.container.replication;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;

import com.google.common.base.CaseFormat;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Class contains metrics related to ReplicationManager.
 */
@Metrics(about = "Replication Manager Metrics", context = OzoneConsts.OZONE)
public final class ReplicationManagerMetrics implements MetricsSource {

  public static final String METRICS_SOURCE_NAME =
      ReplicationManagerMetrics.class.getSimpleName();

  private static final MetricsInfo INFLIGHT_REPLICATION = Interns.info(
      "InflightReplication",
      "Tracked inflight container replication requests.");

  private static final MetricsInfo INFLIGHT_REPLICATION_SKIPPED = Interns.info(
      "InflightReplicationSkipped",
      "Tracked inflight container replication requests skipped" +
          " due to the configured limit.");

  private static final MetricsInfo INFLIGHT_DELETION = Interns.info(
      "InflightDeletion",
      "Tracked inflight container deletion requests.");

  private static final MetricsInfo INFLIGHT_DELETION_SKIPPED = Interns.info(
      "InflightDeletionSkipped",
      "Tracked inflight container deletion requests skipped" +
          " due to the configured limit.");

  private static final MetricsInfo INFLIGHT_MOVE = Interns.info(
      "InflightMove",
      "Tracked inflight container move requests.");

  private static final MetricsInfo INFLIGHT_EC_REPLICATION = Interns.info(
      "InflightEcReplication",
      "Tracked inflight EC container replication requests.");

  private static final MetricsInfo INFLIGHT_EC_DELETION = Interns.info(
      "InflightEcDeletion",
      "Tracked inflight EC container deletion requests.");

  private static final MetricsInfo UNDER_REPLICATED_QUEUE = Interns.info(
      "UnderReplicatedQueueSize",
      "Number of containers currently in the under replicated queue");

  private static final MetricsInfo OVER_REPLICATED_QUEUE = Interns.info(
      "OverReplicatedQueueSize",
      "Number of containers currently in the over replicated queue");

  // Setup metric names and descriptions for Container Lifecycle states
  private static final Map<LifeCycleState, MetricsInfo> LIFECYCLE_STATE_METRICS
      = Collections.unmodifiableMap(
          new LinkedHashMap<LifeCycleState, MetricsInfo>() {{
            for (LifeCycleState s : LifeCycleState.values()) {
              String name = CaseFormat.UPPER_UNDERSCORE
                  .to(CaseFormat.UPPER_CAMEL, s.toString());
              String metric = name + "Containers";
              String description = "Current count of Containers in " + name +
                  " state";
              put(s, Interns.info(metric, description));
            }
          }});

  // Setup metric names and descriptions for
  private static final Map<ContainerHealthState, MetricsInfo>
      CONTAINER_HEALTH_STATE_METRICS = Collections.unmodifiableMap(
          new LinkedHashMap<ContainerHealthState, MetricsInfo>() {{
            for (ContainerHealthState s :  ContainerHealthState.values()) {
              put(s, Interns.info(s.getMetricName(), s.getDescription()));
            }
          }});

  @Metric("Number of replication commands sent.")
  private MutableCounterLong replicationCmdsSentTotal;

  @Metric("Number of container replicas created successfully.")
  private MutableCounterLong replicasCreatedTotal;

  @Metric("Number of container replicas which timed out before being created.")
  private MutableCounterLong replicaCreateTimeoutTotal;

  @Metric("Number of deletion commands sent.")
  private MutableCounterLong deletionCmdsSentTotal;

  @Metric("Number of container replicas deleted successfully.")
  private MutableCounterLong replicasDeletedTotal;

  @Metric("Number of container replicas which timed out before being deleted.")
  private MutableCounterLong replicaDeleteTimeoutTotal;

  @Metric("Number of replication bytes total.")
  private MutableCounterLong replicationBytesTotal;

  @Metric("Number of replication bytes completed.")
  private MutableCounterLong replicationBytesCompletedTotal;

  @Metric("Number of deletion bytes total.")
  private MutableCounterLong deletionBytesTotal;

  @Metric("Number of deletion bytes completed.")
  private MutableCounterLong deletionBytesCompletedTotal;

  @Metric("Time elapsed for replication")
  private MutableRate replicationTime;

  @Metric("Time elapsed for deletion")
  private MutableRate deletionTime;

  @Metric("Number of inflight replication skipped" +
      " due to the configured limit.")
  private MutableCounterLong inflightReplicationSkippedTotal;

  @Metric("Number of inflight replication skipped" +
      " due to the configured limit.")
  private MutableCounterLong inflightDeletionSkippedTotal;

  @Metric("Number of times under replication processing has paused due to" +
      " reaching the cluster inflight replication limit.")
  private MutableCounterLong pendingReplicationLimitReachedTotal;

  private MetricsRegistry registry;

  private final ReplicationManager replicationManager;

  //EC Metrics
  @Metric("Number of EC Replication commands sent.")
  private MutableCounterLong ecReplicationCmdsSentTotal;

  @Metric("Number of EC Replica Deletion commands sent.")
  private MutableCounterLong ecDeletionCmdsSentTotal;

  @Metric("Number of EC Reconstruction commands sent.")
  private MutableCounterLong ecReconstructionCmdsSentTotal;

  @Metric("Number of EC replicas successfully created by Replication Manager.")
  private MutableCounterLong ecReplicasCreatedTotal;

  @Metric("Number of EC replicas successfully deleted by Replication Manager.")
  private MutableCounterLong ecReplicasDeletedTotal;

  @Metric("Number of EC replicas scheduled to be created which timed out.")
  private MutableCounterLong ecReplicaCreateTimeoutTotal;

  @Metric("Number of EC replicas scheduled for delete which timed out.")
  private MutableCounterLong ecReplicaDeleteTimeoutTotal;

  @Metric("Number of times partial EC reconstruction was needed due to " +
      "overloaded nodes, but skipped as there was still sufficient redundancy.")
  private MutableCounterLong ecPartialReconstructionSkippedTotal;

  @Metric("Number of times partial EC reconstruction was used due to " +
      "insufficient nodes available and reconstruction was critical.")
  private MutableCounterLong ecPartialReconstructionCriticalTotal;

  @Metric("Number of times partial EC reconstruction was used due to " +
      "insufficient nodes available and with no overloaded nodes.")
  private MutableCounterLong ecPartialReconstructionNoneOverloadedTotal;

  @Metric("Number of times EC decommissioning or entering maintenance mode " +
      "replicas were not all replicated due to insufficient nodes available.")
  private MutableCounterLong ecPartialReplicationForOutOfServiceReplicasTotal;

  @Metric("Number of times partial Ratis replication occurred due to " +
      "insufficient nodes available.")
  private MutableCounterLong partialReplicationTotal;

  @Metric("Number of times partial replication occurred to fix a " +
      "mis-replicated ratis container due to insufficient nodes available.")
  private MutableCounterLong partialReplicationForMisReplicationTotal;

  @Metric("Number of times partial replication occurred to fix a " +
      "mis-replicated EC container due to insufficient nodes available.")
  private MutableCounterLong ecPartialReplicationForMisReplicationTotal;

  @Metric("NUmber of Reconstruct EC Container commands that could not be sent "
      + "due to the pending commands on the target datanode")
  private MutableCounterLong ecReconstructionCmdsDeferredTotal;

  @Metric("Number of delete container commands that could not be sent due "
      + "to the pending commands on the target datanode")
  private MutableCounterLong deleteContainerCmdsDeferredTotal;

  @Metric("Number of replicate container commands that could not be sent due "
      + "to the pending commands on all source datanodes")
  private MutableCounterLong replicateContainerCmdsDeferredTotal;

  public ReplicationManagerMetrics(ReplicationManager manager) {
    this.registry = new MetricsRegistry(METRICS_SOURCE_NAME);
    this.replicationManager = manager;
  }

  public static ReplicationManagerMetrics create(ReplicationManager manager) {
    ReplicationManagerMetrics replicationManagerMetrics = (ReplicationManagerMetrics)
        DefaultMetricsSystem.instance().getSource(METRICS_SOURCE_NAME);
    if (replicationManagerMetrics == null) {
      return DefaultMetricsSystem.instance().register(METRICS_SOURCE_NAME,
          "SCM Replication manager (closed container replication) related "
              + "metrics",
          new ReplicationManagerMetrics(manager));
    }
    return replicationManagerMetrics;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(METRICS_SOURCE_NAME)
        .addGauge(INFLIGHT_REPLICATION, getInflightReplication())
        .addGauge(INFLIGHT_DELETION, getInflightDeletion())
        .addGauge(INFLIGHT_EC_REPLICATION, getEcReplication())
        .addGauge(INFLIGHT_EC_DELETION, getEcDeletion());

    builder.addGauge(UNDER_REPLICATED_QUEUE,
                    replicationManager.getQueue().underReplicatedQueueSize())
          .addGauge(OVER_REPLICATED_QUEUE,
              replicationManager.getQueue().overReplicatedQueueSize());

    ReplicationManagerReport report = replicationManager.getContainerReport();
    for (Map.Entry<HddsProtos.LifeCycleState, MetricsInfo> e :
        LIFECYCLE_STATE_METRICS.entrySet()) {
      builder.addGauge(e.getValue(), report.getStat(e.getKey()));
    }
    for (Map.Entry<ContainerHealthState, MetricsInfo> e :
        CONTAINER_HEALTH_STATE_METRICS.entrySet()) {
      builder.addGauge(e.getValue(), report.getStat(e.getKey()));
    }

    replicationCmdsSentTotal.snapshot(builder, all);
    replicasCreatedTotal.snapshot(builder, all);
    replicaCreateTimeoutTotal.snapshot(builder, all);
    deletionCmdsSentTotal.snapshot(builder, all);
    replicasDeletedTotal.snapshot(builder, all);
    replicaDeleteTimeoutTotal.snapshot(builder, all);
    ecReplicationCmdsSentTotal.snapshot(builder, all);
    ecDeletionCmdsSentTotal.snapshot(builder, all);
    ecReplicasCreatedTotal.snapshot(builder, all);
    ecReplicasDeletedTotal.snapshot(builder, all);
    ecReconstructionCmdsSentTotal.snapshot(builder, all);
    ecReplicaCreateTimeoutTotal.snapshot(builder, all);
    ecReplicasDeletedTotal.snapshot(builder, all);
    ecReplicaDeleteTimeoutTotal.snapshot(builder, all);
    ecReconstructionCmdsDeferredTotal.snapshot(builder, all);
    deleteContainerCmdsDeferredTotal.snapshot(builder, all);
    replicateContainerCmdsDeferredTotal.snapshot(builder, all);
    pendingReplicationLimitReachedTotal.snapshot(builder, all);
    ecPartialReconstructionSkippedTotal.snapshot(builder, all);
    ecPartialReconstructionCriticalTotal.snapshot(builder, all);
    ecPartialReconstructionNoneOverloadedTotal.snapshot(builder, all);
    ecPartialReplicationForOutOfServiceReplicasTotal.snapshot(builder, all);
    partialReplicationTotal.snapshot(builder, all);
    ecPartialReplicationForMisReplicationTotal.snapshot(builder, all);
    partialReplicationForMisReplicationTotal.snapshot(builder, all);
  }

  public void unRegister() {
    DefaultMetricsSystem.instance().unregisterSource(METRICS_SOURCE_NAME);
  }

  public void incrReplicationCmdsSentTotal() {
    this.replicationCmdsSentTotal.incr();
  }

  public void incrReplicasCreatedTotal() {
    this.replicasCreatedTotal.incr();
  }

  public void incrReplicaCreateTimeoutTotal() {
    this.replicaCreateTimeoutTotal.incr();
  }

  public void incrDeletionCmdsSentTotal() {
    this.deletionCmdsSentTotal.incr();
  }

  public void incrReplicasDeletedTotal() {
    this.replicasDeletedTotal.incr();
  }

  public void incrReplicaDeleteTimeoutTotal() {
    this.replicaDeleteTimeoutTotal.incr();
  }

  public void incrReplicationBytesTotal(long bytes) {
    this.replicationBytesTotal.incr(bytes);
  }

  public void incrReplicationBytesCompletedTotal(long bytes) {
    this.replicationBytesCompletedTotal.incr(bytes);
  }

  public void incrDeletionBytesTotal(long bytes) {
    this.deletionBytesTotal.incr(bytes);
  }

  public void incrDeletionBytesCompletedTotal(long bytes) {
    this.deletionBytesCompletedTotal.incr(bytes);
  }

  public void addReplicationTime(long millis) {
    this.replicationTime.add(millis);
  }

  public void addDeletionTime(long millis) {
    this.deletionTime.add(millis);
  }

  public void incrInflightSkipped(InflightType type) {
    switch (type) {
    case REPLICATION:
      this.inflightReplicationSkippedTotal.incr();
      return;
    case DELETION:
      this.inflightDeletionSkippedTotal.incr();
      return;
    default:
      throw new IllegalArgumentException("Unexpected type " + type);
    }
  }

  public long getInflightReplication() {
    return replicationManager.getContainerReplicaPendingOps()
          .getPendingOpCount(ContainerReplicaOp.PendingOpType.ADD,
              ReplicationType.RATIS);
  }

  public long getInflightReplicationSkipped() {
    return this.inflightReplicationSkippedTotal.value();
  }

  public long getInflightDeletion() {
    return replicationManager.getContainerReplicaPendingOps()
            .getPendingOpCount(ContainerReplicaOp.PendingOpType.DELETE,
              ReplicationType.RATIS);
  }

  public long getInflightDeletionSkipped() {
    return this.inflightDeletionSkippedTotal.value();
  }

  public long getReplicationCmdsSentTotal() {
    return this.replicationCmdsSentTotal.value();
  }

  public long getReplicasCreatedTotal() {
    return this.replicasCreatedTotal.value();
  }

  public long getReplicaCreateTimeoutTotal() {
    return this.replicaCreateTimeoutTotal.value();
  }

  public long getDeletionCmdsSentTotal() {
    return this.deletionCmdsSentTotal.value();
  }

  public long getReplicasDeletedTotal() {
    return this.replicasDeletedTotal.value();
  }

  public long getReplicaDeleteTimeoutTotal() {
    return this.replicaDeleteTimeoutTotal.value();
  }

  public long getDeletionBytesTotal() {
    return this.deletionBytesTotal.value();
  }

  public long getDeletionBytesCompletedTotal() {
    return this.deletionBytesCompletedTotal.value();
  }

  public long getReplicationBytesTotal() {
    return this.replicationBytesTotal.value();
  }

  public long getReplicationBytesCompletedTotal() {
    return this.replicationBytesCompletedTotal.value();
  }

  public void incrEcReplicationCmdsSentTotal() {
    this.ecReplicationCmdsSentTotal.incr();
  }

  public void incrEcDeletionCmdsSentTotal() {
    this.ecDeletionCmdsSentTotal.incr();
  }

  public void incrEcReplicasCreatedTotal() {
    this.ecReplicasCreatedTotal.incr();
  }

  public void incrEcReplicasDeletedTotal() {
    this.ecReplicasDeletedTotal.incr();
  }

  public void incrEcReconstructionCmdsSentTotal() {
    this.ecReconstructionCmdsSentTotal.incr();
  }

  public void incrECReconstructionCmdsDeferredTotal() {
    this.ecReconstructionCmdsDeferredTotal.incr();
  }

  public void incrDeleteContainerCmdsDeferredTotal() {
    this.deleteContainerCmdsDeferredTotal.incr();
  }

  public void incrReplicateContainerCmdsDeferredTotal() {
    this.replicateContainerCmdsDeferredTotal.incr();
  }

  public long getEcReplication() {
    return replicationManager.getContainerReplicaPendingOps()
        .getPendingOpCount(ContainerReplicaOp.PendingOpType.ADD,
            ReplicationType.EC);
  }

  public long getEcDeletion() {
    return replicationManager.getContainerReplicaPendingOps()
        .getPendingOpCount(ContainerReplicaOp.PendingOpType.DELETE,
            ReplicationType.EC);
  }

  public void incrEcReplicaCreateTimeoutTotal() {
    this.ecReplicaCreateTimeoutTotal.incr();
  }

  public long getEcDeletionCmdsSentTotal() {
    return ecDeletionCmdsSentTotal.value();
  }

  public long getEcReconstructionCmdsSentTotal() {
    return ecReconstructionCmdsSentTotal.value();
  }

  public long getEcReplicationCmdsSentTotal() {
    return ecReplicationCmdsSentTotal.value();
  }

  public void incrEcReplicaDeleteTimeoutTotal() {
    this.ecReplicaDeleteTimeoutTotal.incr();
  }

  public long getEcReplicaCreateTimeoutTotal() {
    return ecReplicaCreateTimeoutTotal.value();
  }

  public long getEcReplicaDeleteTimeoutTotal() {
    return ecReplicaDeleteTimeoutTotal.value();
  }

  public long getEcReplicasCreatedTotal() {
    return ecReplicasCreatedTotal.value();
  }

  public long getEcReplicasDeletedTotal() {
    return ecReplicasDeletedTotal.value();
  }

  public long getEcReconstructionCmdsDeferredTotal() {
    return ecReconstructionCmdsDeferredTotal.value();
  }

  public long getDeleteContainerCmdsDeferredTotal() {
    return deleteContainerCmdsDeferredTotal.value();
  }

  public long getReplicateContainerCmdsDeferredTotal() {
    return replicateContainerCmdsDeferredTotal.value();
  }

  public void incrPendingReplicationLimitReachedTotal() {
    this.pendingReplicationLimitReachedTotal.incr();
  }

  public long getPendingReplicationLimitReachedTotal() {
    return pendingReplicationLimitReachedTotal.value();
  }

  public long getECPartialReconstructionSkippedTotal() {
    return ecPartialReconstructionSkippedTotal.value();
  }

  public void incrECPartialReconstructionSkippedTotal() {
    this.ecPartialReconstructionSkippedTotal.incr();
  }

  public long getECPartialReconstructionCriticalTotal() {
    return ecPartialReconstructionCriticalTotal.value();
  }

  public void incrECPartialReconstructionCriticalTotal() {
    this.ecPartialReconstructionCriticalTotal.incr();
  }

  public long getEcPartialReconstructionNoneOverloadedTotal() {
    return ecPartialReconstructionNoneOverloadedTotal.value();
  }

  public void incrEcPartialReconstructionNoneOverloadedTotal() {
    this.ecPartialReconstructionNoneOverloadedTotal.incr();
  }

  public long getEcPartialReplicationForOutOfServiceReplicasTotal() {
    return ecPartialReplicationForOutOfServiceReplicasTotal.value();
  }

  public void incrEcPartialReplicationForOutOfServiceReplicasTotal() {
    this.ecPartialReplicationForOutOfServiceReplicasTotal.incr();
  }

  public long getPartialReplicationTotal() {
    return partialReplicationTotal.value();
  }

  public void incrPartialReplicationTotal() {
    this.partialReplicationTotal.incr();
  }

  public void incrEcPartialReplicationForMisReplicationTotal() {
    this.ecPartialReplicationForMisReplicationTotal.incr();
  }

  public long getEcPartialReplicationForMisReplicationTotal() {
    return this.ecPartialReplicationForMisReplicationTotal.value();
  }

  public void incrPartialReplicationForMisReplicationTotal() {
    this.partialReplicationForMisReplicationTotal.incr();
  }

  public long getPartialReplicationForMisReplicationTotal() {
    return this.partialReplicationForMisReplicationTotal.value();
  }

}
