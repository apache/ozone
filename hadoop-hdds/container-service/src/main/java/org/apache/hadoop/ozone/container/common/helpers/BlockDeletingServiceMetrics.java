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

package org.apache.hadoop.ozone.container.common.helpers;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.container.common.impl.BlockDeletingService;

/**
 * Metrics related to Block Deleting Service running on Datanode.
 */
@Metrics(name = "BlockDeletingService Metrics", about = "Metrics related to "
    + "background block deleting service on Datanode", context = "dfs")
public final class BlockDeletingServiceMetrics {

  private static BlockDeletingServiceMetrics instance;
  public static final String SOURCE_NAME =
      BlockDeletingService.class.getSimpleName();

  @Metric(about = "The number of successful delete blocks")
  private MutableCounterLong successCount;

  @Metric(about = "The total bytes for blocks successfully deleted.")
  private MutableCounterLong successBytes;

  @Metric(about = "The number of failed delete blocks.")
  private MutableCounterLong failureCount;

  @Metric(about = "The number of out of order delete block transaction.")
  private MutableCounterLong outOfOrderDeleteBlockTransactionCount;

  @Metric(about = "The total number of blocks pending for processing.")
  private MutableGaugeLong totalPendingBlockCount;

  @Metric(about = "The total bytes used by blocks pending for deletion.")
  private MutableGaugeLong totalPendingBlockBytes;

  @Metric(about = "The total number of DeleteBlockTransaction received")
  private MutableCounterLong receivedTransactionCount;

  @Metric(about = "The total number of DeleteBlockTransaction that is a retry Transaction")
  private MutableCounterLong receivedRetryTransactionCount;

  @Metric(about = "The total number of Container received to be processed")
  private MutableCounterLong receivedContainerCount;

  @Metric(about = "The total number of blocks received to be processed.")
  private MutableGaugeLong receivedBlockCount;

  @Metric(about = "The total number of blocks marked count.")
  private MutableGaugeLong markedBlockCount;

  @Metric(about = "The total number of blocks chosen to be deleted.")
  private MutableGaugeLong totalBlockChosenCount;

  @Metric(about = "The total number of Container chosen to be deleted.")
  private MutableGaugeLong totalContainerChosenCount;

  @Metric(about = "The total number of transactions which failed due to container lock wait timeout.")
  private MutableGaugeLong totalLockTimeoutTransactionCount;

  @Metric(about = "The number of delete block transactions successful.")
  private MutableCounterLong processedTransactionSuccessCount;

  @Metric(about = "The number of delete block transactions failed.")
  private MutableGaugeLong processedTransactionFailCount;

  private BlockDeletingServiceMetrics() {
  }

  public static BlockDeletingServiceMetrics create() {
    if (instance == null) {
      MetricsSystem ms = DefaultMetricsSystem.instance();
      instance = ms.register(SOURCE_NAME, "BlockDeletingService",
          new BlockDeletingServiceMetrics());
    }

    return instance;
  }

  /**
   * Unregister the metrics instance.
   */
  public static void unRegister() {
    instance = null;
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  public void incrSuccessCount(long count) {
    this.successCount.incr(count);
  }

  public void incrSuccessBytes(long bytes) {
    this.successBytes.incr(bytes);
  }

  public void incrFailureCount() {
    this.failureCount.incr();
  }

  public void incrProcessedTransactionSuccessCount(long count) {
    processedTransactionSuccessCount.incr(count);
  }

  public void incrProcessedTransactionFailCount(long count) {
    processedTransactionFailCount.incr(count);
  }

  public void incrReceivedTransactionCount(long count) {
    receivedTransactionCount.incr(count);
  }

  public void incrReceivedRetryTransactionCount(long count) {
    receivedRetryTransactionCount.incr(count);
  }

  public void incrReceivedContainerCount(long count) {
    receivedContainerCount.incr(count);
  }

  public void incrTotalBlockChosenCount(long count) {
    totalBlockChosenCount.incr(count);
  }

  public void incrTotalContainerChosenCount(long count) {
    totalContainerChosenCount.incr(count);
  }

  public void incrReceivedBlockCount(long count) {
    receivedBlockCount.incr(count);
  }

  public void incrMarkedBlockCount(long count) {
    markedBlockCount.incr(count);
  }

  public void setTotalPendingBlockCount(long count) {
    this.totalPendingBlockCount.set(count);
  }

  public void setTotalPendingBlockBytes(long bytes) {
    this.totalPendingBlockBytes.set(bytes);
  }

  public void incrTotalLockTimeoutTransactionCount() {
    totalLockTimeoutTransactionCount.incr();
  }

  public long getSuccessCount() {
    return successCount.value();
  }

  public long getSuccessBytes() {
    return successBytes.value();
  }

  public long getFailureCount() {
    return failureCount.value();
  }

  public void incOutOfOrderDeleteBlockTransactionCount() {
    this.outOfOrderDeleteBlockTransactionCount.incr();
  }

  public long getOutOfOrderDeleteBlockTransactionCount() {
    return outOfOrderDeleteBlockTransactionCount.value();
  }

  public long getTotalPendingBlockCount() {
    return totalPendingBlockCount.value();
  }

  public long getTotalPendingBlockBytes() {
    return totalPendingBlockBytes.value();
  }

  public long getTotalBlockChosenCount() {
    return totalBlockChosenCount.value();
  }

  public long getTotalContainerChosenCount() {
    return totalContainerChosenCount.value();
  }

  public long getTotalLockTimeoutTransactionCount() {
    return totalLockTimeoutTransactionCount.value();
  }

  public long getProcessedTransactionSuccessCount() {
    return processedTransactionSuccessCount.value();
  }

  public long getProcessedTransactionFailCount() {
    return processedTransactionFailCount.value();
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder()
        .append("successCount = ").append(successCount.value()).append('\t')
        .append("successBytes = ").append(successBytes.value()).append('\t')
        .append("failureCount = ").append(failureCount.value()).append('\t')
        .append("outOfOrderDeleteBlockTransactionCount = ")
        .append(outOfOrderDeleteBlockTransactionCount.value()).append('\t')
        .append("totalPendingBlockCount = ").append(totalPendingBlockCount.value()).append('\t')
        .append("totalPendingBlockBytes = ").append(totalPendingBlockBytes.value()).append('\t')
        .append("totalBlockChosenCount = ").append(totalBlockChosenCount.value()).append('\t')
        .append("totalContainerChosenCount = ").append(totalContainerChosenCount.value()).append('\t')
        .append("receivedTransactionCount = ").append(receivedTransactionCount.value()).append('\t')
        .append("receivedRetryTransactionCount = ").append(receivedRetryTransactionCount.value()).append('\t')
        .append("processedTransactionSuccessCount = ").append(processedTransactionSuccessCount.value()).append('\t')
        .append("processedTransactionFailCount = ").append(processedTransactionFailCount.value()).append('\t')
        .append("receivedContainerCount = ").append(receivedContainerCount.value()).append('\t')
        .append("receivedBlockCount = ").append(receivedBlockCount.value()).append('\t')
        .append("markedBlockCount = ").append(markedBlockCount.value()).append('\t')
        .append("totalLockTimeoutTransactionCount = ").append(totalLockTimeoutTransactionCount.value()).append('\t');
    return buffer.toString();
  }
}
