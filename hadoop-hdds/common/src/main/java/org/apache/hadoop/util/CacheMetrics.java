/**
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
package org.apache.hadoop.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

/**
 * Reusable component that emits cache metrics for a particular cache.
 */
public final class CacheMetrics implements MetricsSource {
  enum CacheMetricsInfo implements MetricsInfo {
    CacheName("Cache Metrics."),
    Size("Size of the cache."),
    HitCount("Number of time the lookup methods return a cached value."),
    HitRate("Ratio of cache requests which were hit."),
    MissCount("Number of times the requested value is not in the cache."),
    MissRate("Ratio of cache requests which were missed."),
    LoadSuccessCount("Number of times the cache successfully " +
        "load new values"),
    LoadExceptionCount("Number of times the cache encounters exception " +
        "loading new values."),
    EvictionCount("Number of values which were evicted.");

    private final String desc;

    CacheMetricsInfo(String desc) {
      this.desc = desc;
    }

    @Override
    public String description() {
      return desc;
    }
  }

  public static final String SOURCE_NAME =
      CacheMetrics.class.getSimpleName();

  public static final String NAME = CacheMetrics.class.getSimpleName();

  private final Cache<?, ?> cache;
  private final String name;

  private CacheMetrics(Cache<?, ?> cache, String name) {
    this.cache = cache;
    this.name = name;
  }

  public static CacheMetrics create(Cache<?, ?> cache, String name) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(NAME, "Cache Metrics",
        new CacheMetrics(cache, name));
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder recordBuilder = collector.addRecord(SOURCE_NAME)
        .setContext("Cache metrics")
        .tag(CacheMetricsInfo.CacheName, name);
    CacheStats stats = cache.stats();

    recordBuilder
        .addGauge(CacheMetricsInfo.Size, cache.size())
        .addGauge(CacheMetricsInfo.HitCount, stats.hitCount())
        .addGauge(CacheMetricsInfo.HitRate, stats.hitRate())
        .addGauge(CacheMetricsInfo.MissCount, stats.missCount())
        .addGauge(CacheMetricsInfo.MissRate, stats.missRate())
        .addGauge(CacheMetricsInfo.LoadExceptionCount,
            stats.loadExceptionCount())
        .addGauge(CacheMetricsInfo.LoadSuccessCount,
            stats.loadSuccessCount())
        .addGauge(CacheMetricsInfo.EvictionCount,
            stats.evictionCount());

  }

  public void unregister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(NAME);
  }
}
