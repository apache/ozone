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

package org.apache.hadoop.hdds.utils;

import org.apache.hadoop.hdds.utils.db.cache.CacheStats;
import org.apache.hadoop.hdds.utils.db.cache.TableCache;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

/**
 * This class emits table level cache metrics.
 */
public final class TableCacheMetrics implements MetricsSource {

  public static final String SOURCE_NAME = TableCacheMetrics.class.getSimpleName();

  private final TableCache<?, ?> cache;
  private final String tableName;

  private TableCacheMetrics(TableCache<?, ?> cache, String name) {
    this.cache = cache;
    this.tableName = name;
  }

  public static TableCacheMetrics create(TableCache<?, ?> cache,
                                         String tableName) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    TableCacheMetrics tableMetrics = new TableCacheMetrics(cache, tableName);
    return ms.register(tableMetrics.getSourceName(), "Table cache metrics",
        tableMetrics);
  }

  private String getSourceName() {
    return tableName + "Cache";
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder recordBuilder = collector.addRecord(SOURCE_NAME)
        .setContext("Table cache metrics")
        .tag(MetricsInfos.TableName, tableName);
    CacheStats stats = cache.getStats();
    recordBuilder
        .addGauge(MetricsInfos.Size, cache.size())
        .addGauge(MetricsInfos.HitCount, stats.getCacheHits())
        .addGauge(MetricsInfos.MissCount, stats.getCacheMisses())
        .addGauge(MetricsInfos.IterationCount, stats.getIterationTimes());
  }

  public void unregister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(getSourceName());
  }

  private enum MetricsInfos implements MetricsInfo {
    TableName("Table Name."),
    Size("Size of the cache."),
    HitCount("Number of time the lookup methods return a cached value."),
    MissCount("Number of times the requested value is not in the cache."),
    IterationCount("Number of times the table cache is iterated through.");

    private final String desc;

    MetricsInfos(String desc) {
      this.desc = desc;
    }

    @Override
    public String description() {
      return desc;
    }
  }
}
