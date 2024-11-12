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

package org.apache.hadoop.ozone.container.common.helpers;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.util.MetricUtil;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

/**
 *
 * This class is for maintaining  the various Storage Container
 * DataNode statistics and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values;
 *  for example:
 *  <p> {@link #numOps}.inc()
 *
 */
@InterfaceAudience.Private
@Metrics(about = "Storage Container DataNode Metrics", context = "dfs")
public class ContainerMetrics implements Closeable {
  public static final String STORAGE_CONTAINER_METRICS =
      "StorageContainerMetrics";
  @Metric private MutableCounterLong numOps;
  @Metric private MutableCounterLong containerDeleteFailedNonEmpty;
  @Metric private MutableCounterLong containerDeleteFailedBlockCountNotZero;
  @Metric private MutableCounterLong containerForceDelete;
  @Metric private MutableCounterLong numReadStateMachine;
  @Metric private MutableCounterLong bytesReadStateMachine;

  /** for remote requests. */
  private final EnumMap<ContainerProtos.Type, MutableCounterLong> numOpsArray;
  private final EnumMap<ContainerProtos.Type, MutableCounterLong> opsBytesArray;
  private final EnumMap<ContainerProtos.Type, MutableRate> opsLatency;
  private final EnumMap<ContainerProtos.Type, MutableQuantiles[]> opsLatQuantiles;
  /** for local short-circuit requests. */
  private final EnumMap<ContainerProtos.Type, MutableCounterLong> numLocalOpsArray;
  private final EnumMap<ContainerProtos.Type, MutableCounterLong> opsLocalBytesArray;
  private final EnumMap<ContainerProtos.Type, MutableRate> opsLocalLatencyNs;
  private final EnumMap<ContainerProtos.Type, MutableRate> opsLocalInQueueLatencyNs;
  private MetricsRegistry registry = null;

  public ContainerMetrics(int[] intervals) {
    final int len = intervals.length;
    this.numOpsArray = new EnumMap<>(ContainerProtos.Type.class);
    this.opsBytesArray = new EnumMap<>(ContainerProtos.Type.class);
    this.opsLatency = new EnumMap<>(ContainerProtos.Type.class);
    this.opsLatQuantiles = new EnumMap<>(ContainerProtos.Type.class);
    this.registry = new MetricsRegistry("StorageContainerMetrics");

    for (ContainerProtos.Type type : ContainerProtos.Type.values()) {
      numOpsArray.put(type, registry.newCounter(
          "num" + type, "number of " + type + " ops", (long) 0));
      opsBytesArray.put(type, registry.newCounter(
          "bytes" + type, "bytes used by " + type + "op", (long) 0));
      opsLatency.put(type, registry.newRate("latencyNs" + type, type + " op"));

      MutableQuantiles[] latQuantiles = new MutableQuantiles[len];
      for (int j = 0; j < len; j++) {
        int interval = intervals[j];
        String quantileName = type + "Nanos" + interval + "s";
        latQuantiles[j] = registry.newQuantiles(quantileName,
            "latency of Container ops", "ops", "latencyNs", interval);
      }
      opsLatQuantiles.put(type, latQuantiles);
    }

    this.numLocalOpsArray = new EnumMap<>(ContainerProtos.Type.class);
    this.opsLocalBytesArray = new EnumMap<>(ContainerProtos.Type.class);
    this.opsLocalLatencyNs = new EnumMap<>(ContainerProtos.Type.class);
    this.opsLocalInQueueLatencyNs = new EnumMap<>(ContainerProtos.Type.class);

    List<ContainerProtos.Type> localTypeList = new ArrayList<>();
    localTypeList.add(ContainerProtos.Type.GetBlock);
    localTypeList.add(ContainerProtos.Type.Echo);
    for (ContainerProtos.Type type : localTypeList) {
      numLocalOpsArray.put(type, registry.newCounter(
          "numLocal" + type, "number of " + type + " ops", (long) 0));
      opsLocalBytesArray.put(type, registry.newCounter(
          "localBytes" + type, "bytes used by " + type + "op", (long) 0));
      opsLocalLatencyNs.put(type, registry.newRate("localLatencyNs" + type, type + " op"));
      opsLocalInQueueLatencyNs.put(type, registry.newRate("localInQueueLatencyNs" + type, type + " op"));
    }
  }

  public static ContainerMetrics create(ConfigurationSource conf) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    // Percentile measurement is off by default, by watching no intervals
    int[] intervals =
        conf.getInts(DFSConfigKeysLegacy.DFS_METRICS_PERCENTILES_INTERVALS_KEY);
    return ms.register(STORAGE_CONTAINER_METRICS,
                       "Storage Container Node Metrics",
                       new ContainerMetrics(intervals));
  }

  public static void remove() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(STORAGE_CONTAINER_METRICS);
  }

  @Override
  public void close() {
    opsLatQuantiles.values().forEach(MetricUtil::stop);
  }

  public void incContainerOpsMetrics(ContainerProtos.Type type) {
    numOps.incr();
    numOpsArray.get(type).incr();
  }

  public void incContainerOpsLatencies(ContainerProtos.Type type, long latencyNs) {
    opsLatency.get(type).add(latencyNs);
    for (MutableQuantiles q: opsLatQuantiles.get(type)) {
      q.add(latencyNs);
    }
  }

  public void incContainerBytesStats(ContainerProtos.Type type, long bytes) {
    opsBytesArray.get(type).incr(bytes);
  }

  public void incContainerLocalOpsMetrics(ContainerProtos.Type type) {
    numOps.incr();
    numLocalOpsArray.get(type).incr();
  }

  public long getContainerLocalOpsMetrics(ContainerProtos.Type type) {
    return numLocalOpsArray.get(type).value();
  }

  public void incContainerLocalOpsLatencies(ContainerProtos.Type type, long nanoSeconds) {
    opsLocalLatencyNs.get(type).add(nanoSeconds);
  }

  public void incContainerLocalOpsInQueueLatencies(ContainerProtos.Type type, long nanoSeconds) {
    opsLocalInQueueLatencyNs.get(type).add(nanoSeconds);
  }

  public void incContainerLocalBytesStats(ContainerProtos.Type type, long bytes) {
    opsLocalBytesArray.get(type).incr(bytes);
  }

  public void incContainerDeleteFailedBlockCountNotZero() {
    containerDeleteFailedBlockCountNotZero.incr();
  }
  public void incContainerDeleteFailedNonEmpty() {
    containerDeleteFailedNonEmpty.incr();
  }

  public void incContainersForceDelete() {
    containerForceDelete.incr();
  }

  public long getContainerDeleteFailedNonEmpty() {
    return containerDeleteFailedNonEmpty.value();
  }

  public long getContainerDeleteFailedBlockCountNotZero() {
    return containerDeleteFailedBlockCountNotZero.value();
  }

  public long getContainerForceDelete() {
    return containerForceDelete.value();
  }

  public void incNumReadStateMachine() {
    numReadStateMachine.incr();
  }

  public long getNumReadStateMachine() {
    return numReadStateMachine.value();
  }

  public void incBytesReadStateMachine(long bytes) {
    bytesReadStateMachine.incr(bytes);
  }

  public long getBytesReadStateMachine() {
    return bytesReadStateMachine.value();
  }
}
