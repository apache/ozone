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

package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import java.util.EnumMap;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.ratis.protocol.RaftGroupId;

/**
 * This class is for maintaining Container State Machine statistics.
 */
@InterfaceAudience.Private
@Metrics(about = "Container State Machine Metrics", context = "dfs")
public class CSMMetrics {
  public static final String SOURCE_NAME =
      CSMMetrics.class.getSimpleName();
  private RaftGroupId gid;

  // ratis op metrics metrics
  private @Metric MutableCounterLong numWriteStateMachineOps;
  private @Metric MutableCounterLong numQueryStateMachineOps;
  private @Metric MutableCounterLong numApplyTransactionOps;
  private @Metric MutableCounterLong numReadStateMachineOps;
  private @Metric MutableCounterLong numBytesWrittenCount;
  private @Metric MutableCounterLong numBytesCommittedCount;

  private @Metric MutableRate transactionLatencyMs;
  private final EnumMap<Type, MutableRate> opsLatencyMs;
  private final EnumMap<Type, MutableRate> opsQueueingDelay;

  // TODO: https://issues.apache.org/jira/browse/HDDS-13555
  @SuppressWarnings("PMD.SingularField")
  private MetricsRegistry registry;

  // Failure Metrics
  private @Metric MutableCounterLong numWriteStateMachineFails;
  private @Metric MutableCounterLong numWriteDataFails;
  private @Metric MutableCounterLong numQueryStateMachineFails;
  private @Metric MutableCounterLong numApplyTransactionFails;
  private @Metric MutableCounterLong numReadStateMachineFails;
  private @Metric MutableCounterLong numReadStateMachineMissCount;
  private @Metric MutableCounterLong numStartTransactionVerifyFailures;
  private @Metric MutableCounterLong numContainerNotOpenVerifyFailures;
  private @Metric MutableCounterLong numDataCacheMiss;
  private @Metric MutableCounterLong numDataCacheHit;
  private @Metric MutableCounterLong numEvictedCacheCount;
  private @Metric MutableCounterLong pendingApplyTransactions;

  private @Metric MutableRate applyTransactionNs;
  private @Metric MutableRate writeStateMachineDataNs;
  private @Metric MutableRate writeStateMachineQueueingLatencyNs;
  private @Metric MutableRate untilApplyTransactionNs;
  private @Metric MutableRate startTransactionCompleteNs;

  public CSMMetrics(RaftGroupId gid) {
    this.gid = gid;
    this.opsLatencyMs = new EnumMap<>(ContainerProtos.Type.class);
    this.opsQueueingDelay = new EnumMap<>(ContainerProtos.Type.class);
    this.registry = new MetricsRegistry(CSMMetrics.class.getSimpleName());
    for (ContainerProtos.Type type : ContainerProtos.Type.values()) {
      opsLatencyMs.put(type, registry.newRate(type.toString() + "Ms", type + " op"));
      opsQueueingDelay.put(type, registry.newRate("queueingDelay" + type.toString() + "Ns", type + " op"));
    }
  }

  public static CSMMetrics create(RaftGroupId gid) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME + gid.toString(),
        "Container State Machine",
        new CSMMetrics(gid));
  }

  @Metric
  public String getRaftGroupId() {
    return gid.toString();
  }

  public void incNumWriteStateMachineOps() {
    numWriteStateMachineOps.incr();
  }

  public void incNumQueryStateMachineOps() {
    numQueryStateMachineOps.incr();
  }

  public void incNumReadStateMachineOps() {
    numReadStateMachineOps.incr();
  }

  public void incNumApplyTransactionsOps() {
    numApplyTransactionOps.incr();
  }

  public void incNumWriteStateMachineFails() {
    numWriteStateMachineFails.incr();
  }

  public void incNumWriteDataFails() {
    numWriteDataFails.incr();
  }

  public void incNumQueryStateMachineFails() {
    numQueryStateMachineFails.incr();
  }

  public void incNumBytesWrittenCount(long value) {
    numBytesWrittenCount.incr(value);
  }

  public void incNumBytesCommittedCount(long value) {
    numBytesCommittedCount.incr(value);
  }

  public void incNumReadStateMachineFails() {
    numReadStateMachineFails.incr();
  }

  public void incNumReadStateMachineMissCount() {
    numReadStateMachineMissCount.incr();
  }

  public void incNumApplyTransactionsFails() {
    numApplyTransactionFails.incr();
  }

  public long getNumWriteStateMachineOps() {
    return numWriteStateMachineOps.value();
  }

  public long getNumQueryStateMachineOps() {
    return numQueryStateMachineOps.value();
  }

  public long getNumApplyTransactionsOps() {
    return numApplyTransactionOps.value();
  }

  public long getNumWriteStateMachineFails() {
    return numWriteStateMachineFails.value();
  }

  public long getNumWriteDataFails() {
    return numWriteDataFails.value();
  }

  public long getNumQueryStateMachineFails() {
    return numQueryStateMachineFails.value();
  }

  public long getNumApplyTransactionsFails() {
    return numApplyTransactionFails.value();
  }

  public long getNumReadStateMachineFails() {
    return numReadStateMachineFails.value();
  }

  public long getNumReadStateMachineMissCount() {
    return numReadStateMachineMissCount.value();
  }

  public long getNumReadStateMachineOps() {
    return numReadStateMachineOps.value();
  }

  public long getNumBytesWrittenCount() {
    return numBytesWrittenCount.value();
  }

  public long getNumBytesCommittedCount() {
    return numBytesCommittedCount.value();
  }

  MutableRate getApplyTransactionLatencyNs() {
    return applyTransactionNs;
  }

  public void incPipelineLatencyMs(ContainerProtos.Type type,
      long latencyMillis) {
    opsLatencyMs.get(type).add(latencyMillis);
    transactionLatencyMs.add(latencyMillis);
  }

  public void recordQueueingDelay(ContainerProtos.Type type,
                                  long latencyNanos) {
    opsQueueingDelay.get(type).add(latencyNanos);
  }

  public void incNumStartTransactionVerifyFailures() {
    numStartTransactionVerifyFailures.incr();
  }

  public void incNumContainerNotOpenVerifyFailures() {
    numContainerNotOpenVerifyFailures.incr();
  }

  public void recordApplyTransactionCompletionNs(long latencyNanos) {
    applyTransactionNs.add(latencyNanos);
  }

  public void recordWriteStateMachineCompletionNs(long latencyNanos) {
    writeStateMachineDataNs.add(latencyNanos);
  }

  public void recordWriteStateMachineQueueingLatencyNs(long latencyNanos) {
    writeStateMachineQueueingLatencyNs.add(latencyNanos);
  }

  public void recordUntilApplyTransactionNs(long latencyNanos) {
    untilApplyTransactionNs.add(latencyNanos);
  }

  public void recordStartTransactionCompleteNs(long latencyNanos) {
    startTransactionCompleteNs.add(latencyNanos);
  }

  public void incNumDataCacheMiss() {
    numDataCacheMiss.incr();
  }

  public void incNumDataCacheHit() {
    numDataCacheHit.incr();
  }

  public void incNumEvictedCacheCount() {
    numEvictedCacheCount.incr();
  }

  public void incPendingApplyTransactions() {
    pendingApplyTransactions.incr();
  }

  public void decPendingApplyTransactions() {
    pendingApplyTransactions.incr(-1);
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME + gid.toString());
  }
}
