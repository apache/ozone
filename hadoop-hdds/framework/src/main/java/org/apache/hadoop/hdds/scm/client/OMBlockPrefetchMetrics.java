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

package org.apache.hadoop.hdds.scm.client;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * Metrics related to OM Prefetch Block operations.
 */
public class OMBlockPrefetchMetrics {
  private static final String SOURCE_NAME = OMBlockPrefetchMetrics.class.getSimpleName();

  @Metric(about = "Latency of reading from the prefetch queue in nanoseconds")
  private MutableRate readFromQueueLatencyNs;

  @Metric(about = "Latency for performing sorting in nanoseconds")
  private MutableRate sortingLogicLatencyNs;

  @Metric(about = "Latency for prefetching blocks in nanoseconds")
  private MutableRate prefetchLatencyNs;

  @Metric(about = "Number of cache hits")
  private MutableCounterLong cacheHits;

  @Metric(about = "Number of cache misses")
  private MutableCounterLong cacheMisses;

  public static OMBlockPrefetchMetrics register() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
        "Ozone Manager Block Prefetch Client Metrics",
        new OMBlockPrefetchMetrics());
  }

  public static void unregister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  public void addReadFromQueueLatency(long latencyInNs) {
    readFromQueueLatencyNs.add(latencyInNs);
  }

  public void addSortingLogicLatency(long latencyInNs) {
    sortingLogicLatencyNs.add(latencyInNs);
  }

  public MutableRate getPrefetchLatencyNs() {
    return prefetchLatencyNs;
  }

  public void incrementCacheHits() {
    cacheHits.incr();
  }

  public void incrementCacheMisses() {
    cacheMisses.incr();
  }
}
