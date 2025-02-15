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

package org.apache.hadoop.ozone.util;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableStat;

/**
 * The {@code PerformanceMetrics} class encapsulates a collection of related
 * metrics including a MutableStat, MutableQuantiles, and a MutableMinMax.
 * This class provides methods to update these metrics and to
 * snapshot their values for reporting.
 */
public class PerformanceMetrics implements Closeable {
  private final MutableStat stat;
  private final List<MutableQuantiles> quantiles;
  private final MutableMinMax minMax;

  /**
   * Initializes aggregated metrics for the specified metrics source.
   *
   * @param source the metrics source
   * @param registry the metrics registry
   * @param intervals the intervals for quantiles computation. Note, each
   *        interval in 'intervals' increases memory usage, as it corresponds
   *        to a separate quantile calculator.
   * @return {@link PerformanceMetrics} instances created, mapped by field name
   */
  public static synchronized <T> Map<String, PerformanceMetrics> initializeMetrics(T source,
      MetricsRegistry registry, String sampleName, String valueName,
      int[] intervals) {
    try {
      return PerformanceMetricsInitializer.initialize(
          source, registry, sampleName, valueName, intervals);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Failed to initialize PerformanceMetrics", e);
    }
  }

  /**
   * Helper method to create PerformanceMetrics.
   *
   * @param registry the metrics registry
   * @param name metric name
   * @param description metric description
   * @param sampleName sample name
   * @param valueName value name
   * @param intervals intervals for quantiles
   */
  public PerformanceMetrics(
      MetricsRegistry registry, String name, String description,
      String sampleName, String valueName, int[] intervals) {
    stat = registry.newStat(name, description, sampleName, valueName, false);
    quantiles = MetricUtil.createQuantiles(registry, name, description, sampleName, valueName, intervals);
    minMax = new MutableMinMax(registry, name, description, valueName);
  }

  @Override
  public void close() {
    MetricUtil.stop(quantiles);
  }

  /**
   * Adds a value to all the aggregated metrics.
   *
   * @param value the value to add
   */
  public void add(long value) {
    this.stat.add(value);
    this.quantiles.forEach(quantile -> quantile.add(value));
    this.minMax.add(value);
  }

  /**
   * Snapshots the values of all the aggregated metrics for reporting.
   *
   * @param recordBuilder the metrics record builder
   * @param all flag to indicate whether to snapshot all metrics or only changed
   */
  public void snapshot(MetricsRecordBuilder recordBuilder, boolean all) {
    this.stat.snapshot(recordBuilder, all);
    this.quantiles.forEach(quantile -> quantile.snapshot(recordBuilder, all));
    this.minMax.snapshot(recordBuilder, all);
  }
}

