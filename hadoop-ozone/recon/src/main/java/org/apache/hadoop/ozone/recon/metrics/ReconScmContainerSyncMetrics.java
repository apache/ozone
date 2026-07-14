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

import com.google.common.base.CaseFormat;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Metrics for Recon SCM container sync execution.
 */
@InterfaceAudience.Private
@Metrics(about = "Recon SCM Container Sync Metrics", context = OzoneConsts.OZONE)
public final class ReconScmContainerSyncMetrics implements MetricsSource {

  private static final String SOURCE_NAME =
      ReconScmContainerSyncMetrics.class.getSimpleName();

  private static final HddsProtos.LifeCycleState[] SYNC_STATES = {
      HddsProtos.LifeCycleState.OPEN,
      HddsProtos.LifeCycleState.QUASI_CLOSED,
      HddsProtos.LifeCycleState.CLOSED,
      HddsProtos.LifeCycleState.DELETED
  };

  private static final MetricsInfo SCM_CONTAINER_SYNC_STATUS = Interns.info(
      "scmContainerSyncStatus",
      "SCM container sync status: 0=idle, 1=in progress, 2=success, 3=failure");

  private static final MetricsInfo SCM_CONTAINER_SYNC_DURATION_MS = Interns.info(
      "scmContainerSyncDurationMs",
      "Time taken by the SCM container sync in milliseconds");

  /**
   * SCM container sync is currently running.
   */
  public static final int SCM_CONTAINER_SYNC_STATUS_IN_PROGRESS = 1;
  /**
   * SCM container sync completed successfully.
   */
  public static final int SCM_CONTAINER_SYNC_STATUS_SUCCESS = 2;
  /**
   * SCM container sync completed with one or more failed passes.
   */
  public static final int SCM_CONTAINER_SYNC_STATUS_FAILURE = 3;

  private final AtomicInteger scmContainerSyncStatus = new AtomicInteger();
  private final AtomicLong scmContainerSyncDurationMs = new AtomicLong();
  private final Map<HddsProtos.LifeCycleState, AtomicLong>
      containerSyncDurationMs;
  private final Map<HddsProtos.LifeCycleState, AtomicLong>
      containerCountDrift;
  private final Map<HddsProtos.LifeCycleState, MetricsInfo>
      containerSyncDurationMetricInfo;
  private final Map<HddsProtos.LifeCycleState, MetricsInfo>
      containerCountDriftMetricInfo;

  private ReconScmContainerSyncMetrics() {
    containerSyncDurationMs = initStateGaugeValues();
    containerCountDrift = initStateGaugeValues();
    containerSyncDurationMetricInfo = initSyncDurationMetricInfo();
    containerCountDriftMetricInfo = initCountDriftMetricInfo();
  }

  public static ReconScmContainerSyncMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
        "Recon SCM Container Sync Metrics",
        new ReconScmContainerSyncMetrics());
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  public void setScmContainerSyncStatus(int status) {
    scmContainerSyncStatus.set(status);
  }

  public void setScmContainerSyncDurationMs(long durationMs) {
    scmContainerSyncDurationMs.set(durationMs);
  }

  public void setContainerSyncDurationMs(
      HddsProtos.LifeCycleState state, long durationMs) {
    setStateGauge(containerSyncDurationMs, state, durationMs);
  }

  public void setContainerCountDrift(
      HddsProtos.LifeCycleState state, long drift) {
    setStateGauge(containerCountDrift, state, drift);
  }

  public int getScmContainerSyncStatus() {
    return scmContainerSyncStatus.get();
  }

  public long getScmContainerSyncDurationMs() {
    return scmContainerSyncDurationMs.get();
  }

  public long getContainerSyncDurationMs(
      HddsProtos.LifeCycleState state) {
    return getStateGauge(containerSyncDurationMs, state);
  }

  public long getContainerCountDrift(
      HddsProtos.LifeCycleState state) {
    return getStateGauge(containerCountDrift, state);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(SOURCE_NAME);
    builder.addGauge(SCM_CONTAINER_SYNC_STATUS, getScmContainerSyncStatus());
    builder.addGauge(SCM_CONTAINER_SYNC_DURATION_MS,
        getScmContainerSyncDurationMs());
    for (HddsProtos.LifeCycleState state : SYNC_STATES) {
      builder.addGauge(containerSyncDurationMetricInfo.get(state),
          getContainerSyncDurationMs(state));
      builder.addGauge(containerCountDriftMetricInfo.get(state),
          getContainerCountDrift(state));
    }
  }

  private static Map<HddsProtos.LifeCycleState, AtomicLong>
      initStateGaugeValues() {
    Map<HddsProtos.LifeCycleState, AtomicLong> gauges =
        new EnumMap<>(HddsProtos.LifeCycleState.class);
    for (HddsProtos.LifeCycleState state : SYNC_STATES) {
      gauges.put(state, new AtomicLong());
    }
    return Collections.unmodifiableMap(gauges);
  }

  private static Map<HddsProtos.LifeCycleState, MetricsInfo>
      initSyncDurationMetricInfo() {
    Map<HddsProtos.LifeCycleState, MetricsInfo> metrics =
        new EnumMap<>(HddsProtos.LifeCycleState.class);
    for (HddsProtos.LifeCycleState state : SYNC_STATES) {
      String stateName = metricStateName(state);
      metrics.put(state, Interns.info(
          CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, stateName)
              + "ContainerSyncDurationMs",
          "Time taken by the " + stateName
              + " container sync pass in milliseconds"));
    }
    return Collections.unmodifiableMap(metrics);
  }

  private static Map<HddsProtos.LifeCycleState, MetricsInfo>
      initCountDriftMetricInfo() {
    Map<HddsProtos.LifeCycleState, MetricsInfo> metrics =
        new EnumMap<>(HddsProtos.LifeCycleState.class);
    for (HddsProtos.LifeCycleState state : SYNC_STATES) {
      String stateName = metricStateName(state);
      metrics.put(state, Interns.info(
          CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, stateName)
              + "ContainerCountDrift",
          "Last successfully observed container count drift at start of sync pass "
              + "(SCM count minus Recon count for " + stateName + " state)."));
    }
    return Collections.unmodifiableMap(metrics);
  }

  private static String metricStateName(HddsProtos.LifeCycleState state) {
    return CaseFormat.UPPER_UNDERSCORE.to(
        CaseFormat.UPPER_CAMEL, state.name());
  }

  private static void setStateGauge(
      Map<HddsProtos.LifeCycleState, AtomicLong> gauges,
      HddsProtos.LifeCycleState state,
      long value) {
    AtomicLong gauge = gauges.get(state);
    if (gauge != null) {
      gauge.set(value);
    }
  }

  private static long getStateGauge(
      Map<HddsProtos.LifeCycleState, AtomicLong> gauges,
      HddsProtos.LifeCycleState state) {
    AtomicLong gauge = gauges.get(state);
    return gauge != null ? gauge.get() : 0L;
  }
}
