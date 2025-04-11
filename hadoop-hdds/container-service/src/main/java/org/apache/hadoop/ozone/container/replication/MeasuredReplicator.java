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

package org.apache.hadoop.ozone.container.replication;

import java.time.Duration;
import java.time.Instant;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.container.replication.AbstractReplicationTask.Status;
import org.apache.hadoop.util.Time;

/**
 * ContainerReplicator wrapper with additional metrics.
 */
@Metrics(about = "Closed container replication metrics", context = "dfs")
public class MeasuredReplicator implements ContainerReplicator, AutoCloseable {

  private static final String NAME = ContainerReplicator.class.getSimpleName();

  private final ContainerReplicator delegate;
  private final String name;

  @Metric(about = "Number of successful replication tasks")
  private MutableCounterLong success;

  @Metric(about = "Time spent on successful replication tasks")
  private MutableGaugeLong successTime;

  @Metric(about = "Number of failed replication attempts")
  private MutableCounterLong failure;

  @Metric(about = "Time spent waiting in the queue before starting the task")
  private MutableGaugeLong queueTime;

  @Metric(about = "Time spent on failed replication attempts")
  private MutableGaugeLong failureTime;

  @Metric(about = "Bytes transferred for failed replication attempts")
  private MutableGaugeLong failureBytes;

  @Metric(about = "Bytes transferred for successful replication tasks")
  private MutableGaugeLong transferredBytes;

  public MeasuredReplicator(ContainerReplicator delegate, String name) {
    this.delegate = delegate;
    this.name = name;
    DefaultMetricsSystem.instance().register(metricsName(),
        "Closed container " + name + " replication metrics", this);
  }

  private String metricsName() {
    return NAME + "/" + name;
  }

  @Override
  public void replicate(ReplicationTask task) {
    long start = Time.monotonicNow();

    long msInQueue =
        Duration.between(task.getQueued(), Instant.now()).toMillis();
    queueTime.incr(msInQueue);
    delegate.replicate(task);
    long elapsed = Time.monotonicNow() - start;
    if (task.getStatus() == Status.FAILED) {
      failure.incr();
      failureBytes.incr(task.getTransferredBytes());
      failureTime.incr(elapsed);
    } else if (task.getStatus() == Status.DONE) {
      transferredBytes.incr(task.getTransferredBytes());
      success.incr();
      successTime.incr(elapsed);
    }
  }

  @Override
  public void close() throws Exception {
    DefaultMetricsSystem.instance().unregisterSource(metricsName());
  }

  MutableCounterLong getSuccess() {
    return success;
  }

  MutableGaugeLong getSuccessTime() {
    return successTime;
  }

  MutableGaugeLong getFailureTime() {
    return failureTime;
  }

  MutableCounterLong getFailure() {
    return failure;
  }

  MutableGaugeLong getQueueTime() {
    return queueTime;
  }

  MutableGaugeLong getTransferredBytes() {
    return transferredBytes;
  }

  MutableGaugeLong getFailureBytes() {
    return failureBytes;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + name + "}@"
        + Integer.toHexString(hashCode());
  }
}
