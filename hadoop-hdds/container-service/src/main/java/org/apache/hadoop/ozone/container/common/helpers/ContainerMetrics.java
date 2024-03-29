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
public class ContainerMetrics {
  public static final String STORAGE_CONTAINER_METRICS =
      "StorageContainerMetrics";
  @Metric private MutableCounterLong numOps;
  @Metric private MutableCounterLong containerDeleteFailedNonEmpty;
  @Metric private MutableCounterLong containerDeleteFailedBlockCountNotZero;
  @Metric private MutableCounterLong containerForceDelete;
  @Metric private MutableCounterLong numReadStateMachine;
  @Metric private MutableCounterLong bytesReadStateMachine;


  private MutableCounterLong[] numOpsArray;
  private MutableCounterLong[] opsBytesArray;
  private MutableRate[] opsLatency;
  private MutableQuantiles[][] opsLatQuantiles;
  private MetricsRegistry registry = null;

  public ContainerMetrics(int[] intervals) {
    int numEnumEntries = ContainerProtos.Type.values().length;
    final int len = intervals.length;
    this.numOpsArray = new MutableCounterLong[numEnumEntries];
    this.opsBytesArray = new MutableCounterLong[numEnumEntries];
    this.opsLatency = new MutableRate[numEnumEntries];
    this.opsLatQuantiles = new MutableQuantiles[numEnumEntries][len];
    this.registry = new MetricsRegistry("StorageContainerMetrics");
    for (int i = 0; i < numEnumEntries; i++) {
      numOpsArray[i] = registry.newCounter(
          "num" + ContainerProtos.Type.forNumber(i + 1),
          "number of " + ContainerProtos.Type.forNumber(i + 1) + " ops",
          (long) 0);
      opsBytesArray[i] = registry.newCounter(
          "bytes" + ContainerProtos.Type.forNumber(i + 1),
          "bytes used by " + ContainerProtos.Type.forNumber(i + 1) + "op",
          (long) 0);
      opsLatency[i] = registry.newRate(
          "latency" + ContainerProtos.Type.forNumber(i + 1),
          ContainerProtos.Type.forNumber(i + 1) + " op");

      for (int j = 0; j < len; j++) {
        int interval = intervals[j];
        String quantileName = ContainerProtos.Type.forNumber(i + 1) + "Nanos"
            + interval + "s";
        opsLatQuantiles[i][j] = registry.newQuantiles(quantileName,
            "latency of Container ops", "ops", "latency", interval);
      }
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

  public void incContainerOpsMetrics(ContainerProtos.Type type) {
    numOps.incr();
    numOpsArray[type.ordinal()].incr();
  }

  public long getContainerOpsMetrics(ContainerProtos.Type type) {
    return numOpsArray[type.ordinal()].value();
  }

  public void incContainerOpsLatencies(ContainerProtos.Type type,
                                       long latencyMillis) {
    opsLatency[type.ordinal()].add(latencyMillis);
    for (MutableQuantiles q: opsLatQuantiles[type.ordinal()]) {
      q.add(latencyMillis);
    }
  }

  public void incContainerBytesStats(ContainerProtos.Type type, long bytes) {
    opsBytesArray[type.ordinal()].incr(bytes);
  }

  public long getContainerBytesMetrics(ContainerProtos.Type type) {
    return opsBytesArray[type.ordinal()].value();
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
