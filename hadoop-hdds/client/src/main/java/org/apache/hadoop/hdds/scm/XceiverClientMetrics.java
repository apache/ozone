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
package org.apache.hadoop.hdds.scm;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.util.PerformanceMetrics;
import org.apache.hadoop.util.TopNMetrics;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import java.util.EnumMap;


/**
 * The client metrics for the Storage Container protocol.
 */
@InterfaceAudience.Private
@Metrics(about = "Storage Container Client Metrics", context = "dfs")
public class XceiverClientMetrics implements MetricsSource {
  public static final String SOURCE_NAME = XceiverClientMetrics.class
      .getSimpleName();
  private @Metric MutableCounterLong pendingOps;
  private @Metric MutableCounterLong totalOps;
  private @Metric MutableCounterLong ecReconstructionTotal;
  private @Metric MutableCounterLong ecReconstructionFailsTotal;
  private EnumMap<ContainerProtos.Type, MutableCounterLong> pendingOpsArray;
  private EnumMap<ContainerProtos.Type, MutableCounterLong> opsArray;
  private EnumMap<ContainerProtos.Type, PerformanceMetrics> containerOpsLatency;
  private MetricsRegistry registry;
  private ConfigurationSource conf;
  private int[] intervals;
  private int[] latencyMsThresholdsOrder;
  private final Map<Integer, Map<ContainerProtos.Type, TopNMetrics>> latencyMsThresholdsMap = new HashMap<>();
  private final Set<ContainerProtos.Type> topLatencyMetricsSet = new HashSet<>(Arrays.asList(
      ContainerProtos.Type.GetBlock,
      ContainerProtos.Type.PutBlock,
      ContainerProtos.Type.WriteChunk,
      ContainerProtos.Type.ReadChunk
  ));

  public XceiverClientMetrics(ConfigurationSource conf) {
    init(conf);
  }

  public void init(ConfigurationSource configuration) {
    this.conf = configuration;
    intervals = conf.getInts(OzoneConfigKeys
        .OZONE_XCEIVER_CLIENT_METRICS_PERCENTILES_INTERVALS_SECONDS_KEY);
    latencyMsThresholdsOrder = conf.getInts(OzoneConfigKeys
        .OZONE_XCEIVER_CLIENT_TOP_METRICS_LATENCY_RECORD_THRESHOLD_MS_KEY);
    int latencyMsRecordCount = conf.getInt(OzoneConfigKeys
        .OZONE_XCEIVER_CLIENT_TOP_METRICS_LATENCY_RECORD_COUNT_KEY, 0);
    int numEnumEntries = ContainerProtos.Type.values().length;
    this.registry = new MetricsRegistry(SOURCE_NAME);
    Arrays.sort(latencyMsThresholdsOrder);
    this.pendingOpsArray = new EnumMap<>(ContainerProtos.Type.class);
    this.opsArray = new EnumMap<>(ContainerProtos.Type.class);
    this.containerOpsLatency = new EnumMap<>(ContainerProtos.Type.class);
    for (ContainerProtos.Type type : ContainerProtos.Type.values()) {
      pendingOpsArray.put(type, registry.newCounter("numPending" + type,
          "number of pending" + type + " ops", (long) 0));
      opsArray.put(type, registry.newCounter("opCount" + type,
          "number of" + type + " ops", (long) 0));
      containerOpsLatency.put(type, new PerformanceMetrics(registry,
          type + "Latency", "latency of " + type, "Ops", "Time", intervals));
    }

    for (int thresholds : latencyMsThresholdsOrder) {
      Map<ContainerProtos.Type, TopNMetrics> metrics = new HashMap<>();
      for (ContainerProtos.Type type : topLatencyMetricsSet) {
        String metricName = type.name() + "Exceed" + thresholds + "MsCount";
        String metricDescription = "Top count of " + type.name() +
            " operations exceeding the threshold of " + thresholds + " milliseconds";
        metrics.put(type, TopNMetrics.create(registry, metricName, metricDescription,
            latencyMsRecordCount));
      }
      latencyMsThresholdsMap.put(thresholds, metrics);
    }

  }

  public static XceiverClientMetrics create(ConfigurationSource configuration) {
    DefaultMetricsSystem.initialize(SOURCE_NAME);
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, "Storage Container Client Metrics",
        new XceiverClientMetrics(configuration));
  }

  public void incrPendingContainerOpsMetrics(ContainerProtos.Type type) {
    pendingOps.incr();
    totalOps.incr();
    opsArray.get(type).incr();
    pendingOpsArray.get(type).incr();
  }

  public void decrPendingContainerOpsMetrics(ContainerProtos.Type type) {
    pendingOps.incr(-1);
    pendingOpsArray.get(type).incr(-1);
  }

  public void addContainerOpsLatency(ContainerProtos.Type type,
      long latencyMillis, String datanode) {
    recordLatencyMetricsIfNeeded(type, latencyMillis, datanode);
    containerOpsLatency.get(type).add(latencyMillis);
  }

  private void recordLatencyMetricsIfNeeded(ContainerProtos.Type type, long latencyMillis,
      String datanode) {
    if (latencyMsThresholdsOrder.length > 0 && topLatencyMetricsSet.contains(type)) {
      for (int thresholdMs : latencyMsThresholdsOrder) {
        if (latencyMillis > thresholdMs) {
          latencyMsThresholdsMap.get(thresholdMs).get(type).add(datanode, 1L);
        } else {
          // Break the loop if latencyMillis is not greater than the current thresholdMs,
          // because subsequent thresholds are larger due to the array's monotonically increasing.
          break;
        }
      }
    }
  }

  public long getPendingContainerOpCountMetrics(ContainerProtos.Type type) {
    return pendingOpsArray.get(type).value();
  }

  public void incECReconstructionTotal() {
    ecReconstructionTotal.incr();
  }

  public void incECReconstructionFailsTotal() {
    ecReconstructionFailsTotal.incr();
  }

  @VisibleForTesting
  public long getTotalOpCount() {
    return totalOps.value();
  }

  @VisibleForTesting
  public long getContainerOpCountMetrics(ContainerProtos.Type type) {
    return opsArray.get(type).value();
  }

  @VisibleForTesting
  public void reset() {
    init(conf);
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean b) {
    MetricsRecordBuilder recordBuilder = collector.addRecord(SOURCE_NAME);

    pendingOps.snapshot(recordBuilder, b);
    totalOps.snapshot(recordBuilder, b);
    ecReconstructionTotal.snapshot(recordBuilder, b);
    ecReconstructionFailsTotal.snapshot(recordBuilder, b);

    for (ContainerProtos.Type type : ContainerProtos.Type.values()) {
      pendingOpsArray.get(type).snapshot(recordBuilder, b);
      opsArray.get(type).snapshot(recordBuilder, b);
      containerOpsLatency.get(type).snapshot(recordBuilder, b);
    }

    for (Map.Entry<Integer, Map<ContainerProtos.Type, TopNMetrics>> entry :
        latencyMsThresholdsMap.entrySet()) {
      entry.getValue().values().forEach(value -> value.snapshot(recordBuilder, b));
    }
  }
}
