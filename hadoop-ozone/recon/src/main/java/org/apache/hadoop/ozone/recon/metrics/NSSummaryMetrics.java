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
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Class for tracking metrics related to NSSummary task in Recon.
 */
@InterfaceAudience.Private
@Metrics(about = "Recon NSSummaryTask Metrics", context = OzoneConsts.OZONE)
public final class NSSummaryMetrics {

  private static final String SOURCE_NAME =
      NSSummaryMetrics.class.getSimpleName();

  @Metric(about = "NSSummary reprocess duration in milliseconds")
  private MutableRate reprocessDurationMs;

  @Metric(about = "Total number of NSSummary reprocess failures")
  private MutableCounterLong reprocessFailuresTotal;

  @Metric(about = "Number of active threads in NSSummary executor")
  private MutableGaugeLong executorActiveThreads;

  @Metric(about = "Size of DB flush batches")
  private MutableStat dbFlushSize;

  @Metric(about = "Number of DB flush failures")
  private MutableCounterLong dbFlushFailure;

  private NSSummaryMetrics() {
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  public static NSSummaryMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
        "Recon NSSummary Task Metrics",
        new NSSummaryMetrics());
  }

  public void updateReprocessDurationMs(long durationMs) {
    this.reprocessDurationMs.add(durationMs);
  }

  public void incrReprocessFailuresTotal() {
    this.reprocessFailuresTotal.incr();
  }

  public void setExecutorActiveThreads(long activeThreads) {
    this.executorActiveThreads.set(activeThreads);
  }

  public void updateDbFlushSize(long size) {
    this.dbFlushSize.add(size);
  }

  public void incrDbFlushFailure() {
    this.dbFlushFailure.incr();
  }

  public long getReprocessFailuresTotal() {
    return reprocessFailuresTotal.value();
  }

  public long getExecutorActiveThreads() {
    return executorActiveThreads.value();
  }

  public long getDbFlushFailure() {
    return dbFlushFailure.value();
  }

  public MutableRate getReprocessDurationMs() {
    return reprocessDurationMs;
  }

  public MutableStat getDbFlushSize() {
    return dbFlushSize;
  }
}
