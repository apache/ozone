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

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.recon.metrics.ReconOmTaskMetrics;

/**
 * This class contains extra metrics for Ozone Manager table insight task.
 */
@Metrics(about = "Metrics for OmTableInsight task", context = "recon")
public class OmTableInsightTaskMetrics extends ReconOmTaskMetrics {
  private static final String SOURCE_NAME = OmTableInsightTaskMetrics.class.getSimpleName();
  private static final MetricsInfo TASK_INFO = Interns.info(
      "ReconTaskMetrics", "Task metrics for OmTableInsight"
  );
  private static OmTableInsightTaskMetrics instance;

  OmTableInsightTaskMetrics() {
    super("OmTableInsightTask", SOURCE_NAME);
  }

  public static synchronized OmTableInsightTaskMetrics register() {
    if (null == instance) {
      instance = DefaultMetricsSystem.instance().register(
          SOURCE_NAME,
          "OmTableInsightTask metrics",
          new OmTableInsightTaskMetrics()
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

  private @Metric MutableCounterLong putEventCount;
  private @Metric MutableCounterLong deleteEventCount;
  private @Metric MutableCounterLong updateEventCount;

  private @Metric MutableCounterLong taskWriteToDBCount;
  private @Metric MutableRate writeToDBLatency;

  public void incrPutEventCount() {
    this.putEventCount.incr();
  }

  public void incrDeleteEventCount() {
    this.deleteEventCount.incr();
  }

  public void incrUpdateEventCount() {
    this.updateEventCount.incr();
  }

  public void incrTaskWriteToDBCount() {
    this.taskWriteToDBCount.incr();
  }

  public void updateWriteToDBLatency(long time) {
    this.writeToDBLatency.add(time);
  }
}
