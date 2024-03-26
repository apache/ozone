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

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;

import java.lang.reflect.Field;

/**
 * Utility class for initializing PerformanceMetrics in a MetricsSource.
 */
public final class PerformanceMetricsInitializer {
  private PerformanceMetricsInitializer() { }

  /**
   * Initializes aggregated metrics in the given metrics source.
   *
   * @param source the metrics source
   * @param registry the metrics registry
   * @param sampleName sample name
   * @param valueName value name
   * @param intervals intervals for quantiles
   * @throws IllegalAccessException if unable to access the field
   */
  public static <T> void initialize(T source, MetricsRegistry registry,
      String sampleName, String valueName, int[] intervals)
      throws IllegalAccessException {
    Field[] fields = source.getClass().getDeclaredFields();

    for (Field field : fields) {
      if (field.getType() == PerformanceMetrics.class) {
        Metric annotation = field.getAnnotation(Metric.class);
        if (annotation != null) {
          String description = annotation.about();
          String name = field.getName();
          PerformanceMetrics performanceMetrics =
              getMetrics(registry, name, description,
                  sampleName, valueName, intervals);
          field.setAccessible(true);
          field.set(source, performanceMetrics);
        }
      }
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
   * @return an instance of PerformanceMetrics
   */
  public static PerformanceMetrics getMetrics(
      MetricsRegistry registry, String name, String description,
      String sampleName, String valueName, int[] intervals) {
    return new PerformanceMetrics(
        registry.newStat(
            name, description, sampleName, valueName, false),
        MetricUtil.createQuantiles(
            registry, name, description, sampleName, valueName, intervals),
        new MutableMinMax(registry, name, description, valueName));
  }
}
