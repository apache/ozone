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

package org.apache.hadoop.ozone.recon.metrics;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Metrics for Recon Task Controller operations.
 * This class tracks queue management and system-wide reprocess operations.
 */
@InterfaceAudience.Private
@Metrics(about = "Recon Task Controller Metrics", context = OzoneConsts.OZONE)
public final class ReconTaskControllerMetrics {

  private static final String SOURCE_NAME =
      ReconTaskControllerMetrics.class.getSimpleName();

  // Queue Management Metrics
  @Metric(about = "Current number of Recon events including OM DB Update batch events and Recon reinit events" +
      " in the queue")
  private MutableGaugeLong eventCurrentQueueSize;

  @Metric(about = "Total count of OM DB Update events plus Recon reinit events buffered since startup")
  private MutableCounterLong eventBufferedCount;

  @Metric(about = "Count of events dropped due to buffer issues")
  private MutableCounterLong eventDropCount;

  @Metric(about = "Total count of all Recon events processed")
  private MutableCounterLong totalEventCount;

  // System-Wide Reprocess Failure Categories
  @Metric(about = "Count of checkpoint creation failures")
  private MutableCounterLong reprocessCheckpointFailures;

  @Metric(about = "Count of reprocess execution failures")
  private MutableCounterLong reprocessExecutionFailures;

  @Metric(about = "Count of stage database replacement failures")
  private MutableCounterLong reprocessStageDatabaseFailures;

  // System-Wide Reprocess Success and Submission Tracking
  @Metric(about = "Count of all successful reprocess executions")
  private MutableCounterLong reprocessSuccessCount;

  @Metric(about = "Total count of reinitialization events submitted to queue")
  private MutableCounterLong totalReprocessSubmittedToQueue;

  private ReconTaskControllerMetrics() {
  }

  public static ReconTaskControllerMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
        "Recon Task Controller Metrics",
        new ReconTaskControllerMetrics());
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  // Queue Management Operations
  public void setEventCurrentQueueSize(long size) {
    this.eventCurrentQueueSize.set(size);
  }

  public void incrEventBufferedCount(long count) {
    this.eventBufferedCount.incr(count);
  }

  public void incrEventDropCount(long count) {
    this.eventDropCount.incr(count);
  }

  public void incrTotalEventCount(long count) {
    this.totalEventCount.incr(count);
  }

  // Reprocess Failure Operations
  public void incrReprocessCheckpointFailures() {
    this.reprocessCheckpointFailures.incr();
  }

  public void incrReprocessExecutionFailures() {
    this.reprocessExecutionFailures.incr();
  }

  public void incrReprocessStageDatabaseFailures() {
    this.reprocessStageDatabaseFailures.incr();
  }

  // Reprocess Success Operations
  public void incrReprocessSuccessCount() {
    this.reprocessSuccessCount.incr();
  }

  public void incrTotalReprocessSubmittedToQueue() {
    this.totalReprocessSubmittedToQueue.incr();
  }

  // Getters for testing
  public long getEventCurrentQueueSize() {
    return eventCurrentQueueSize.value();
  }

  public long getEventBufferedCount() {
    return eventBufferedCount.value();
  }

  public long getEventDropCount() {
    return eventDropCount.value();
  }

  public long getTotalEventCount() {
    return totalEventCount.value();
  }

  public long getReprocessCheckpointFailures() {
    return reprocessCheckpointFailures.value();
  }

  public long getReprocessExecutionFailures() {
    return reprocessExecutionFailures.value();
  }

  public long getReprocessStageDatabaseFailures() {
    return reprocessStageDatabaseFailures.value();
  }

  public long getReprocessSuccessCount() {
    return reprocessSuccessCount.value();
  }

  public long getTotalReprocessSubmittedToQueue() {
    return totalReprocessSubmittedToQueue.value();
  }
}
