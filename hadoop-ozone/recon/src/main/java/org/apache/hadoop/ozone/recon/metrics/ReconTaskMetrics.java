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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Per-task metrics for Recon task delta processing and reprocess operations.
 * Provides granular visibility into individual task performance.
 */
@InterfaceAudience.Private
@Metrics(about = "Recon Task Metrics", context = OzoneConsts.OZONE)
public final class ReconTaskMetrics implements MetricsSource {

  private static final String SOURCE_NAME =
      ReconTaskMetrics.class.getSimpleName();

  private final MetricsRegistry registry = new MetricsRegistry(SOURCE_NAME);

  // Static metric required for Hadoop Metrics framework registration
  @Metric(about = "Total number of unique tasks tracked")
  private MutableCounterLong numTasksTracked;

  // Per-task delta processing metrics stored in ConcurrentMaps
  private final ConcurrentMap<String, MutableCounterLong> taskDeltaProcessingSuccess =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, MutableCounterLong> taskDeltaProcessingFailures =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, MutableRate> taskDeltaProcessingDuration =
      new ConcurrentHashMap<>();

  // Per-task reprocess metrics
  private final ConcurrentMap<String, MutableCounterLong> taskReprocessFailures =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, MutableRate> taskReprocessDuration =
      new ConcurrentHashMap<>();

  private ReconTaskMetrics() {
  }

  public static ReconTaskMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
        "Recon Task Metrics",
        new ReconTaskMetrics());
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  // Task Delta Processing Operations
  public void incrTaskDeltaProcessingSuccess(String taskName) {
    taskDeltaProcessingSuccess
        .computeIfAbsent(taskName, k ->
            registry.newCounter(
                "TaskDeltaProcessingSuccess_" + sanitizeTaskName(taskName),
                "Success count for task " + taskName,
                0L))
        .incr();
  }

  public void incrTaskDeltaProcessingFailures(String taskName) {
    taskDeltaProcessingFailures
        .computeIfAbsent(taskName, k ->
            registry.newCounter(
                "TaskDeltaProcessingFailures_" + sanitizeTaskName(taskName),
                "Failure count for task " + taskName,
                0L))
        .incr();
  }

  public void updateTaskDeltaProcessingDuration(String taskName, long duration) {
    taskDeltaProcessingDuration
        .computeIfAbsent(taskName, k ->
            registry.newRate(
                "TaskDeltaProcessingDuration_" + sanitizeTaskName(taskName),
                "Processing duration for task " + taskName))
        .add(duration);
  }

  // Task Reprocess Operations
  public void incrTaskReprocessFailures(String taskName) {
    taskReprocessFailures
        .computeIfAbsent(taskName, k ->
            registry.newCounter(
                "TaskReprocessFailures_" + sanitizeTaskName(taskName),
                "Reprocess failure count for task " + taskName,
                0L))
        .incr();
  }

  public void updateTaskReprocessDuration(String taskName, long duration) {
    taskReprocessDuration
        .computeIfAbsent(taskName, k ->
            registry.newRate(
                "TaskReprocessDuration_" + sanitizeTaskName(taskName),
                "Reprocess duration for task " + taskName))
        .add(duration);
  }

  /**
   * Sanitize task name for use in metric names.
   * Replaces non-alphanumeric characters with underscores.
   */
  private String sanitizeTaskName(String taskName) {
    return taskName.replaceAll("[^a-zA-Z0-9]", "_");
  }

  // Getters for testing
  public long getTaskDeltaProcessingSuccess(String taskName) {
    MutableCounterLong counter = taskDeltaProcessingSuccess.get(taskName);
    return counter != null ? counter.value() : 0L;
  }

  public long getTaskDeltaProcessingFailures(String taskName) {
    MutableCounterLong counter = taskDeltaProcessingFailures.get(taskName);
    return counter != null ? counter.value() : 0L;
  }

  public MutableRate getTaskDeltaProcessingDuration(String taskName) {
    return taskDeltaProcessingDuration.get(taskName);
  }

  public long getTaskReprocessFailures(String taskName) {
    MutableCounterLong counter = taskReprocessFailures.get(taskName);
    return counter != null ? counter.value() : 0L;
  }

  public MutableRate getTaskReprocessDuration(String taskName) {
    return taskReprocessDuration.get(taskName);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder recordBuilder = collector.addRecord(SOURCE_NAME);

    // Snapshot static metric
    numTasksTracked.snapshot(recordBuilder, all);

    // Snapshot all dynamic per-task metrics
    taskDeltaProcessingSuccess.values().forEach(
        metric -> metric.snapshot(recordBuilder, all));
    taskDeltaProcessingFailures.values().forEach(
        metric -> metric.snapshot(recordBuilder, all));
    taskDeltaProcessingDuration.values().forEach(
        metric -> metric.snapshot(recordBuilder, all));
    taskReprocessFailures.values().forEach(
        metric -> metric.snapshot(recordBuilder, all));
    taskReprocessDuration.values().forEach(
        metric -> metric.snapshot(recordBuilder, all));
  }
}
