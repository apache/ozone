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

package org.apache.hadoop.ozone.container.common.utils;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * Metrics for the usage of ContainerDB.
 */
public final class ContainerCacheMetrics {

  @Metric("Rate to measure the db open latency")
  private MutableRate dbOpenLatency;

  @Metric("Rate to measure the db close latency")
  private MutableRate dbCloseLatency;

  @Metric("Number of Container Cache Hits")
  private MutableCounterLong numCacheHits;

  @Metric("Number of Container Cache Misses")
  private MutableCounterLong numCacheMisses;

  @Metric("Number of DB.get Ops")
  private MutableCounterLong numDbGetOps;

  @Metric("Number of DB.remove Ops")
  private MutableCounterLong numDbRemoveOps;

  @Metric("Number of Container Cache Evictions")
  private MutableCounterLong numCacheEvictions;

  private ContainerCacheMetrics() {
  }

  public static ContainerCacheMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    String name = "ContainerCacheMetrics";

    return ms.register(name, "null", new ContainerCacheMetrics());
  }

  public void incNumDbGetOps() {
    numDbGetOps.incr();
  }

  public void incNumDbRemoveOps() {
    numDbRemoveOps.incr();
  }

  public void incNumCacheMisses() {
    numCacheMisses.incr();
  }

  public void incNumCacheHits() {
    numCacheHits.incr();
  }

  public void incNumCacheEvictions() {
    numCacheEvictions.incr();
  }

  public void incDbCloseLatency(long millis) {
    dbCloseLatency.add(millis);
  }

  public void incDbOpenLatency(long millis) {
    dbOpenLatency.add(millis);
  }

  public long getNumDbGetOps() {
    return numDbGetOps.value();
  }

  public long getNumDbRemoveOps() {
    return numDbRemoveOps.value();
  }

  public long getNumCacheMisses() {
    return numCacheMisses.value();
  }

  public long getNumCacheHits() {
    return numCacheHits.value();
  }

  public long getNumCacheEvictions() {
    return numCacheEvictions.value();
  }
}
