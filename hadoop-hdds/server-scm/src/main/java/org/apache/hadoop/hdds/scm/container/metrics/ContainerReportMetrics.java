/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.metrics;

import org.apache.hadoop.hdds.server.events.EventExecutorMetrics;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

/**
 * Metrics for Container Report Event Executors.
 */
@Metrics(context = "EventQueue")
public class ContainerReportMetrics implements EventExecutorMetrics {
  private static final String EVENT_QUEUE = "EventQueue";
  private String name;
  
  @Metric
  private MutableCounterLong queued;

  @Metric
  private MutableCounterLong done;

  @Metric
  private MutableCounterLong failed;

  @Metric
  private MutableCounterLong scheduled;

  @Metric
  private MutableCounterLong dropped;

  @Metric
  private MutableCounterLong longWaitInQueue;

  @Metric
  private MutableCounterLong longTimeExecution;

  public ContainerReportMetrics(String name) {
    this.name = name;
    DefaultMetricsSystem.instance().register(EVENT_QUEUE + this.name,
        "Event Executor metrics ", this);
  }

  @Override
  public void stop() {
    DefaultMetricsSystem.instance().unregisterSource(EVENT_QUEUE + name);
  }
  
  @Override
  public String getName() {
    return name;
  }

  @Override
  public void incrFailedEvents(long delta) {
    failed.incr(delta);
  }

  @Override
  public void incrSuccessfulEvents(long delta) {
    done.incr(delta);
  }

  @Override
  public void incrQueuedEvents(long delta) {
    queued.incr(delta);
  }

  @Override
  public void incrScheduledEvents(long delta) {
    scheduled.incr(delta);
  }

  @Override
  public void incrDroppedEvents(long delta) {
    dropped.incr(delta);
  }

  @Override
  public void incrLongWaitInQueueEvents(long delta) {
    longWaitInQueue.incr(delta);
  }

  @Override
  public void incrLongTimeExecutionEvents(long delta) {
    longTimeExecution.incr(delta);
  }
  
  @Override
  public long failedEvents() {
    return failed.value();
  }

  @Override
  public long successfulEvents() {
    return done.value();
  }

  @Override
  public long queuedEvents() {
    return queued.value();
  }

  @Override
  public long scheduledEvents() {
    return scheduled.value();
  }

  @Override
  public long droppedEvents() {
    return dropped.value();
  }

  @Override
  public long longWaitInQueueEvents() {
    return longWaitInQueue.value();
  }

  @Override
  public long longTimeExecutionEvents() {
    return longTimeExecution.value();
  }
}
