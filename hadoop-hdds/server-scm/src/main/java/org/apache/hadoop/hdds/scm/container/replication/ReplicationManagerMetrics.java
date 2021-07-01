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

  @Metric("Number of replicate commands sent.")
  private MutableCounterLong numReplicateCmdsSent;

  @Metric("Number of replicate commands completed.")
  private MutableCounterLong numReplicateCmdsCompleted;

  @Metric("Number of replicate commands timeout.")
  private MutableCounterLong numReplicateCmdsTimeout;

  @Metric("Number of delete commands sent.")
  private MutableCounterLong numDeleteCmdsSent;

  @Metric("Number of delete commands completed.")
  private MutableCounterLong numDeleteCmdsCompleted;

  @Metric("Number of delete commands timeout.")
  private MutableCounterLong numDeleteCmdsTimeout;

  @Metric("Number of replicate bytes total.")
  private MutableCounterLong numReplicateBytesTotal;

  @Metric("Number of replicate bytes completed.")
  private MutableCounterLong numReplicateBytesCompleted;

  public ReplicationManagerMetrics() {
  }

  public static ReplicationManagerMetrics create() {
    return DefaultMetricsSystem.instance().register(METRICS_SOURCE_NAME,
        "SCM Replication manager (closed container replication) related "
            + "metrics",
        new ReplicationManagerMetrics());
  }

  public void unRegister() {
    DefaultMetricsSystem.instance().unregisterSource(METRICS_SOURCE_NAME);
  }

  public void setInflightReplication(long replications) {
    this.inflightReplication.set(replications);
  }

  public void setInflightDeletion(long deletions) {
    this.inflightDeletion.set(deletions);
  }

  public void incrNumReplicateCmdsSent() {
    this.numReplicateCmdsSent.incr();
  }

  public void incrNumReplicateCmdsCompleted() {
    this.numReplicateCmdsCompleted.incr();
  }

  public void incrNumReplicateCmdsTimeout() {
    this.numReplicateCmdsTimeout.incr();
  }

  public void incrNumDeleteCmdsSent() {
    this.numDeleteCmdsSent.incr();
  }

  public void incrNumDeleteCmdsCompleted() {
    this.numDeleteCmdsCompleted.incr();
  }

  public void incrNumDeleteCmdsTimeout() {
    this.numDeleteCmdsTimeout.incr();
  }

  public void incrNumReplicateBytesTotal(long bytes) {
    this.numReplicateBytesTotal.incr(bytes);
  }

  public void incrNumReplicateBytesCompleted(long bytes) {
    this.numReplicateBytesCompleted.incr(bytes);
  }

  public long getInflightReplication() {
    return this.inflightReplication.value();
  }

  public long getInflightDeletion() {
    return this.inflightDeletion.value();
  }

  public long getNumReplicateCmdsSent() {
    return this.numReplicateCmdsSent.value();
  }

  public long getNumReplicateCmdsCompleted() {
    return this.numReplicateCmdsCompleted.value();
  }

  public long getNumReplicateCmdsTimeout() {
    return this.numReplicateCmdsTimeout.value();
  }

  public long getNumDeleteCmdsSent() {
    return this.numDeleteCmdsSent.value();
  }

  public long getNumDeleteCmdsCompleted() {
    return this.numDeleteCmdsCompleted.value();
  }

  public long getNumDeleteCmdsTimeout() {
    return this.numDeleteCmdsTimeout.value();
  }

  public long getNumReplicateBytesTotal() {
    return this.numReplicateBytesTotal.value();
  }

  public long getNumReplicateBytesCompleted() {
    return this.numReplicateBytesCompleted.value();
  }
}
