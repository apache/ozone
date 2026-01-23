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

package org.apache.hadoop.hdds.scm;

import com.google.common.annotations.VisibleForTesting;
import java.util.EnumMap;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.utils.IOUtils;
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
import org.apache.hadoop.ozone.util.PerformanceMetrics;

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

  // TODO: https://issues.apache.org/jira/browse/HDDS-13555
  @SuppressWarnings("PMD.SingularField")
  private MetricsRegistry registry;

  public XceiverClientMetrics() {
    init();
  }

  public void init() {
    OzoneConfiguration conf = new OzoneConfiguration();
    int[] intervals = conf.getInts(OzoneConfigKeys.OZONE_XCEIVER_CLIENT_METRICS_PERCENTILES_INTERVALS_SECONDS_KEY);

    this.registry = new MetricsRegistry(SOURCE_NAME);

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
  }

  public static XceiverClientMetrics create() {
    DefaultMetricsSystem.initialize(SOURCE_NAME);
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, "Storage Container Client Metrics",
        new XceiverClientMetrics());
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
      long latencyMillis) {
    containerOpsLatency.get(type).add(latencyMillis);
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
    init();
  }

  public void unRegister() {
    IOUtils.closeQuietly(containerOpsLatency.values());
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean b) {
    MetricsRecordBuilder recordBuilder = collector.addRecord(SOURCE_NAME);

    pendingOps.snapshot(recordBuilder, true);
    totalOps.snapshot(recordBuilder, true);
    ecReconstructionTotal.snapshot(recordBuilder, true);
    ecReconstructionFailsTotal.snapshot(recordBuilder, true);

    for (ContainerProtos.Type type : ContainerProtos.Type.values()) {
      pendingOpsArray.get(type).snapshot(recordBuilder, b);
      opsArray.get(type).snapshot(recordBuilder, b);
      containerOpsLatency.get(type).snapshot(recordBuilder, b);
    }
  }
}
