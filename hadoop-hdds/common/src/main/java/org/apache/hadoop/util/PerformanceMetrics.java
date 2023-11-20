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

import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableStat;

import java.util.List;

/**
 * The {@code PerformanceMetrics} class encapsulates a collection of related
 * metrics including a MutableStat, MutableQuantiles, and a MutableMinMax.
 * This class provides methods to update these metrics and to
 * snapshot their values for reporting.
 */
public class PerformanceMetrics {
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
   */
  public static synchronized <T> void initializeMetrics(T source,
      MetricsRegistry registry, String sampleName, String valueName,
      int[] intervals) {
    try {
      PerformanceMetricsInitializer.initialize(
          source, registry, sampleName, valueName, intervals);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Failed to initialize PerformanceMetrics", e);
    }
  }

  /**
   * Construct an instance of PerformanceMetrics with the specified MutableStat,
   * MutableQuantiles, and MutableMinMax.
   *
   * @param stat the stat metric
   * @param quantiles the quantiles metrics
   * @param minMax the min/max tracker
   */
  public PerformanceMetrics(MutableStat stat,
      List<MutableQuantiles> quantiles, MutableMinMax minMax) {
    this.stat = stat;
    this.quantiles = quantiles;
    this.minMax = minMax;
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

