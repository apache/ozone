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

package org.apache.hadoop.hdds.server.events;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

/**
 * Metrics source for EventExecutor implementations.
 */
@Metrics(about = "Executor Metrics", context = "EventQueue")
public class EventExecutorMetrics implements MetricsSource {
  private final String name;
  private final String description;
  private final MetricsRegistry registry;

  @Metric("Number of tasks queued")
  private MutableCounterLong queued;

  @Metric("Number of tasks scheduled")
  private MutableCounterLong scheduled;

  @Metric("Number of tasks completed")
  private MutableCounterLong done;

  @Metric("Number of tasks failed")
  private MutableCounterLong failed;

  @Metric("Number of tasks dropped")
  private MutableCounterLong dropped;

  @Metric("Number of tasks with long execution time")
  private MutableCounterLong longExecution;

  @Metric("Number of tasks with long wait time in queue")
  private MutableCounterLong longWaitInQueue;

  public EventExecutorMetrics(String name, String description) {
    this.name = name;
    this.description = description;

    registry = new MetricsRegistry(name);
    init();
  }

  public void init() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.register(name, description, this);
  }

  public void unregister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(name);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(name);
    registry.snapshot(builder, all);
  }

  public void incrementQueued() {
    queued.incr();
  }

  public void incrementScheduled() {
    scheduled.incr();
  }

  public void incrementDone() {
    done.incr();
  }

  public void incrementFailed() {
    failed.incr();
  }

  public void incrementDropped() {
    dropped.incr();
  }

  public void incrementDropped(int count) {
    dropped.incr(count);
  }

  public void incrementLongExecution() {
    longExecution.incr();
  }

  public void incrementLongWaitInQueue() {
    longWaitInQueue.incr();
  }

  public long getQueued() {
    return queued.value();
  }

  public long getScheduled() {
    return scheduled.value();
  }

  public long getDone() {
    return done.value();
  }

  public long getFailed() {
    return failed.value();
  }

  public long getDropped() {
    return dropped.value();
  }

  public long getLongExecution() {
    return longExecution.value();
  }

  public long getLongWaitInQueue() {
    return longWaitInQueue.value();
  }
}
