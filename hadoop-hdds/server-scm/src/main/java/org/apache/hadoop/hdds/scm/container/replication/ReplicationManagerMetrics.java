/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import com.google.common.base.CaseFormat;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport.HealthState;

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

  // Setup metric names and descriptions for Container Lifecycle states
  private static final Map<LifeCycleState, MetricsInfo> LIFECYCLE_STATE_METRICS
      = Collections.unmodifiableMap(
          new LinkedHashMap<LifeCycleState, MetricsInfo>() {{
            for (LifeCycleState s : LifeCycleState.values()) {
              String name = CaseFormat.UPPER_UNDERSCORE
                  .to(CaseFormat.UPPER_CAMEL, s.toString());
              String metric = "Num" + name + "Containers";
              String description = "Containers in " + name + " state";
              put(s, Interns.info(metric, description));
            }
          }});

  // Setup metric names and descriptions for
  private static final Map<HealthState, MetricsInfo>
      CONTAINER_HEALTH_STATE_METRICS = Collections.unmodifiableMap(
          new LinkedHashMap<HealthState, MetricsInfo>() {{
            for (HealthState s :  HealthState.values()) {
              put(s, Interns.info(s.getMetricName(), s.getDescription()));
            }
          }});

  @Metric("Number of replication commands sent.")
  private MutableCounterLong numReplicationCmdsSent;

  @Metric("Number of replication commands completed.")
  private MutableCounterLong numReplicationCmdsCompleted;

  @Metric("Number of replication commands timeout.")
  private MutableCounterLong numReplicationCmdsTimeout;

  @Metric("Number of deletion commands sent.")
  private MutableCounterLong numDeletionCmdsSent;

  @Metric("Number of deletion commands completed.")
  private MutableCounterLong numDeletionCmdsCompleted;

  @Metric("Number of deletion commands timeout.")
  private MutableCounterLong numDeletionCmdsTimeout;

  @Metric("Number of replication bytes total.")
  private MutableCounterLong numReplicationBytesTotal;

  @Metric("Number of replication bytes completed.")
  private MutableCounterLong numReplicationBytesCompleted;

  @Metric("Number of deletion bytes total.")
  private MutableCounterLong numDeletionBytesTotal;

  @Metric("Number of deletion bytes completed.")
  private MutableCounterLong numDeletionBytesCompleted;

  @Metric("Time elapsed for replication")
  private MutableRate replicationTime;

  @Metric("Time elapsed for deletion")
  private MutableRate deletionTime;

  @Metric("Number of inflight replication skipped" +
      " due to the configured limit.")
  private MutableCounterLong numInflightReplicationSkipped;

  @Metric("Number of inflight replication skipped" +
      " due to the configured limit.")
  private MutableCounterLong numInflightDeletionSkipped;

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

  public ReplicationManagerMetrics(ReplicationManager manager) {
    this.registry = new MetricsRegistry(METRICS_SOURCE_NAME);
    this.replicationManager = manager;
  }

  public static ReplicationManagerMetrics create(ReplicationManager manager) {
    return DefaultMetricsSystem.instance().register(METRICS_SOURCE_NAME,
        "SCM Replication manager (closed container replication) related "
            + "metrics",
        new ReplicationManagerMetrics(manager));
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(METRICS_SOURCE_NAME)
        .addGauge(INFLIGHT_REPLICATION, getInflightReplication())
        .addGauge(INFLIGHT_REPLICATION_SKIPPED, getInflightReplicationSkipped())
        .addGauge(INFLIGHT_DELETION, getInflightDeletion())
        .addGauge(INFLIGHT_DELETION_SKIPPED, getInflightDeletionSkipped())
        .addGauge(INFLIGHT_MOVE, getInflightMove())
        .addGauge(INFLIGHT_EC_REPLICATION, getEcReplication())
        .addGauge(INFLIGHT_EC_DELETION, getEcDeletion());

    ReplicationManagerReport report = replicationManager.getContainerReport();
    for (Map.Entry<HddsProtos.LifeCycleState, MetricsInfo> e :
        LIFECYCLE_STATE_METRICS.entrySet()) {
      builder.addGauge(e.getValue(), report.getStat(e.getKey()));
    }
    for (Map.Entry<ReplicationManagerReport.HealthState, MetricsInfo> e :
        CONTAINER_HEALTH_STATE_METRICS.entrySet()) {
      builder.addGauge(e.getValue(), report.getStat(e.getKey()));
    }

    numReplicationCmdsSent.snapshot(builder, all);
    numReplicationCmdsCompleted.snapshot(builder, all);
    numReplicationCmdsTimeout.snapshot(builder, all);
    numDeletionCmdsSent.snapshot(builder, all);
    numDeletionCmdsCompleted.snapshot(builder, all);
    numDeletionCmdsTimeout.snapshot(builder, all);
    numReplicationBytesTotal.snapshot(builder, all);
    numReplicationBytesCompleted.snapshot(builder, all);
    numDeletionBytesTotal.snapshot(builder, all);
    numDeletionBytesCompleted.snapshot(builder, all);
    replicationTime.snapshot(builder, all);
    deletionTime.snapshot(builder, all);
    ecReplicationCmdsSentTotal.snapshot(builder, all);
    ecDeletionCmdsSentTotal.snapshot(builder, all);
    ecReplicasCreatedTotal.snapshot(builder, all);
    ecReplicasDeletedTotal.snapshot(builder, all);
    ecReconstructionCmdsSentTotal.snapshot(builder, all);
    ecReplicaCreateTimeoutTotal.snapshot(builder, all);
    ecReplicasDeletedTotal.snapshot(builder, all);
  }

  public void unRegister() {
    DefaultMetricsSystem.instance().unregisterSource(METRICS_SOURCE_NAME);
  }

  public void incrNumReplicationCmdsSent() {
    this.numReplicationCmdsSent.incr();
  }

  public void incrNumReplicationCmdsCompleted() {
    this.numReplicationCmdsCompleted.incr();
  }

  public void incrNumReplicationCmdsTimeout() {
    this.numReplicationCmdsTimeout.incr();
  }

  public void incrNumDeletionCmdsSent() {
    this.numDeletionCmdsSent.incr();
  }

  public void incrNumDeletionCmdsCompleted() {
    this.numDeletionCmdsCompleted.incr();
  }

  public void incrNumDeletionCmdsTimeout() {
    this.numDeletionCmdsTimeout.incr();
  }

  public void incrNumReplicationBytesTotal(long bytes) {
    this.numReplicationBytesTotal.incr(bytes);
  }

  public void incrNumReplicationBytesCompleted(long bytes) {
    this.numReplicationBytesCompleted.incr(bytes);
  }

  public void incrNumDeletionBytesTotal(long bytes) {
    this.numDeletionBytesTotal.incr(bytes);
  }

  public void incrNumDeletionBytesCompleted(long bytes) {
    this.numDeletionBytesCompleted.incr(bytes);
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
      this.numInflightReplicationSkipped.incr();
      return;
    case DELETION:
      this.numInflightDeletionSkipped.incr();
      return;
    default:
      throw new IllegalArgumentException("Unexpected type " + type);
    }
  }

  public long getInflightReplication() {
    return replicationManager.getLegacyReplicationManager()
        .getInflightCount(InflightType.REPLICATION);
  }

  public long getInflightReplicationSkipped() {
    return this.numInflightReplicationSkipped.value();
  }

  public long getInflightDeletion() {
    return replicationManager.getLegacyReplicationManager()
        .getInflightCount(InflightType.DELETION);
  }

  public long getInflightDeletionSkipped() {
    return this.numInflightDeletionSkipped.value();
  }

  public long getInflightMove() {
    return replicationManager.getInflightMove().size();
  }

  public long getNumReplicationCmdsSent() {
    return this.numReplicationCmdsSent.value();
  }

  public long getNumReplicationCmdsCompleted() {
    return this.numReplicationCmdsCompleted.value();
  }

  public long getNumReplicationCmdsTimeout() {
    return this.numReplicationCmdsTimeout.value();
  }

  public long getNumDeletionCmdsSent() {
    return this.numDeletionCmdsSent.value();
  }

  public long getNumDeletionCmdsCompleted() {
    return this.numDeletionCmdsCompleted.value();
  }

  public long getNumDeletionCmdsTimeout() {
    return this.numDeletionCmdsTimeout.value();
  }

  public long getNumDeletionBytesTotal() {
    return this.numDeletionBytesTotal.value();
  }

  public long getNumDeletionBytesCompleted() {
    return this.numDeletionBytesCompleted.value();
  }

  public long getNumReplicationBytesTotal() {
    return this.numReplicationBytesTotal.value();
  }

  public long getNumReplicationBytesCompleted() {
    return this.numReplicationBytesCompleted.value();
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

  public long getEcReplication() {
    return replicationManager.getContainerReplicaPendingOps()
        .getPendingOpCount(ContainerReplicaOp.PendingOpType.ADD);
  }

  public long getEcDeletion() {
    return replicationManager.getContainerReplicaPendingOps()
        .getPendingOpCount(ContainerReplicaOp.PendingOpType.DELETE);
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
}
