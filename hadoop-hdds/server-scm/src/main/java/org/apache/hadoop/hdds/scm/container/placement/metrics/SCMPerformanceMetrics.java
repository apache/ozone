/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.placement.metrics;

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
import org.apache.hadoop.util.Time;

/**
 * Including SCM performance related metrics.
 */
@InterfaceAudience.Private
@Metrics(about = "SCM Performance Metrics", context = OzoneConsts.OZONE)
public final class SCMPerformanceMetrics implements MetricsSource {
  private static final String SOURCE_NAME =
      SCMPerformanceMetrics.class.getSimpleName();

  private MetricsRegistry registry;
  private static SCMPerformanceMetrics instance;

  @Metric(about = "Number of failed deleteKey operations")
  private MutableCounterLong deleteKeyFailure;
  @Metric(about = "Number of successful deleteKey operations")
  private MutableCounterLong deleteKeySuccess;
  @Metric(about = "Latency for deleteKey failure in nanoseconds")
  private MutableRate deleteKeyFailureLatencyNs;
  @Metric(about = "Latency for deleteKey success in nanoseconds")
  private MutableRate deleteKeySuccessLatencyNs;

  public SCMPerformanceMetrics() {
    this.registry = new MetricsRegistry(SOURCE_NAME);
  }

  public static SCMPerformanceMetrics create() {
    if (instance != null) {
      return instance;
    }
    MetricsSystem ms = DefaultMetricsSystem.instance();
    instance = ms.register(SOURCE_NAME, "SCM Performance Metrics",
        new SCMPerformanceMetrics());
    return instance;
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder recordBuilder = collector.addRecord(SOURCE_NAME);
    deleteKeySuccess.snapshot(recordBuilder, true);
    deleteKeySuccessLatencyNs.snapshot(recordBuilder, true);
    deleteKeyFailure.snapshot(recordBuilder, true);
    deleteKeyFailureLatencyNs.snapshot(recordBuilder, true);
  }

  public void updateDeleteKeySuccessStats(long startNanos) {
    deleteKeySuccess.incr();
    deleteKeySuccessLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateDeleteKeyFailureStats(long startNanos) {
    deleteKeyFailure.incr();
    deleteKeyFailureLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }
}

