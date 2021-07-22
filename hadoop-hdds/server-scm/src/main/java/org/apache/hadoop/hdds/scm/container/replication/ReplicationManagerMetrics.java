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

import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Class contains metrics related to ReplicationManager.
 */
@Metrics(about = "Replication Manager Metrics", context = OzoneConsts.OZONE)
public final class ReplicationManagerMetrics {

  public static final String METRICS_SOURCE_NAME =
      ReplicationManagerMetrics.class.getSimpleName();

  @Metric("Tracked inflight container replication requests.")
  private MutableGaugeLong inflightReplication;

  @Metric("Tracked inflight container deletion requests.")
  private MutableGaugeLong inflightDeletion;

  @Metric("Tracked inflight container move requests.")
  private MutableGaugeLong inflightMove;

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

  private ReplicationManager replicationManager;

  public ReplicationManagerMetrics(ReplicationManager manager) {
    this.replicationManager = manager;
  }

  public static ReplicationManagerMetrics create(ReplicationManager manager) {
    return DefaultMetricsSystem.instance().register(METRICS_SOURCE_NAME,
        "SCM Replication manager (closed container replication) related "
            + "metrics",
        new ReplicationManagerMetrics(manager));
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

  public long getInflightReplication() {
    return replicationManager.getInflightReplication().size();
  }

  public long getInflightDeletion() {
    return replicationManager.getInflightDeletion().size();
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

  public long getNumReplicationBytesTotal() {
    return this.numReplicationBytesTotal.value();
  }

  public long getNumReplicationBytesCompleted() {
    return this.numReplicationBytesCompleted.value();
  }
}
