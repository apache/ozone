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

package org.apache.hadoop.ozone.recon.metrics.impl;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.recon.metrics.ReconOmTaskMetrics;

/**
 * This class contains extra metrics for Container-Key mapper task.
 */
@Metrics(about = "Metrics for ContainerKeyMapper task", context = "recon")
public class ContainerKeyMapperTaskMetrics extends ReconOmTaskMetrics {
  private static final String SOURCE_NAME = ContainerKeyMapperTaskMetrics.class.getSimpleName();
  private static ContainerKeyMapperTaskMetrics instance;

  ContainerKeyMapperTaskMetrics() {
    super("ContainerKeyMapperTask", SOURCE_NAME);
  }

  public static synchronized ContainerKeyMapperTaskMetrics register() {
    if (null == instance) {
      instance = DefaultMetricsSystem.instance().register(
          SOURCE_NAME,
          "ContainerKeyMapperTask metrics",
          new ContainerKeyMapperTaskMetrics()
      );
    }
    return instance;
  }

  public static synchronized void unregister() {
    if (null != instance) {
      DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
      instance = null;
    }
  }

  private @Metric MutableCounterLong putKeyEventCount;
  private @Metric MutableCounterLong deleteKeyEventCount;
  private @Metric MutableCounterLong updateKeyEventCount;

  private @Metric MutableCounterLong taskWriteToDBCount;
  private @Metric MutableCounterLong taskWriteToDBFailureCount;
  private @Metric MutableRate writeToDBLatency;

  public void incrPutKeyEventCount() {
    this.putKeyEventCount.incr();
  }

  public void incrDeleteKeyEventCount() {
    this.deleteKeyEventCount.incr();
  }

  public void incrUpdateKeyEventCount() {
    this.updateKeyEventCount.incr();
  }

  public void incrTaskWriteToDBCount() {
    this.taskWriteToDBCount.incr();
  }

  public void incrTaskWriteToDBFailureCount() {
    this.taskWriteToDBFailureCount.incr();
  }

  public void updateWriteToDBLatency(long time) {
    this.writeToDBLatency.add(time);
  }
}
