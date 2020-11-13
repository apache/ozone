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
package org.apache.hadoop.ozone.container.replication;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.container.replication.ReplicationTask.Status;

/**
 * ContainerReplicator wrapper with additional metrics.
 */
@Metrics(about = "Closed container replication metrics", context = "dfs")
public class MeasuredReplicator implements ContainerReplicator, AutoCloseable {

  private static final String NAME = ContainerReplicator.class.toString();
  private @Metric
  MutableCounterLong success;

  private @Metric
  MutableCounterLong failure;

  private @Metric
  MutableGaugeLong successTime;

  private ContainerReplicator delegate;

  public MeasuredReplicator(ContainerReplicator delegate) {
    this.delegate = delegate;
    DefaultMetricsSystem.instance()
        .register(NAME, "Closed container replication", this);
  }

  @Override
  public void replicate(ReplicationTask task) {
    long start = System.currentTimeMillis();
    try {
      delegate.replicate(task);
    } finally {
      successTime.incr(System.currentTimeMillis() - start);
    }
    if (task.getStatus() == Status.FAILED) {
      failure.incr();
    } else if (task.getStatus() == Status.DONE) {
      success.incr();
    }
  }

  @Override
  public void close() throws Exception {
    DefaultMetricsSystem.instance().unregisterSource(NAME);
  }

}
