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

package org.apache.hadoop.ozone.metrics;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableMetric;


/**
 * The metrics Ozone system.
 */
public final class MetricsSystem {

  static {
    CustomMetricsFactory.registerAsDefaultMutableMetricsFactory();
  }

  private MetricsSystem() {
  }

  /**
   * Initialize this metric system as default.
   * @param prefix for the metrics system configuration
   * @return the metrics system instance
   */
  public static org.apache.hadoop.metrics2.MetricsSystem initialize(String prefix) {
    return DefaultMetricsSystem.initialize(prefix);
  }

  /**
   * Register a metrics source.
   * @param <T>   the actual type of the source object
   * @param source object to register
   * @param name  of the source. Must be unique or null (then extracted from
   *              the annotations of the source object.)
   * @param desc  the description of the source (or null. See above.)
   * @return the source object
   * @exception MetricsException Metrics Exception.
   */
  public static <T> T register(String name, String desc, T source) {
    return instance().register(name, desc, source);
  }

  /**
   * Unregister a metrics source.
   * @param name of the source. This is the name you use to call register()
   */
  public static void unregisterSource(String name) {
    DefaultMetricsSystem.instance().unregisterSource(name);
  }

  /**
   * @return the metrics system object
   */
  public static org.apache.hadoop.metrics2.MetricsSystem instance() {
    return DefaultMetricsSystem.instance();
  }

  /**
   * Shutdown the metrics system.
   */
  public static void shutdown() {
    DefaultMetricsSystem.shutdown();
  }

  @VisibleForTesting
  public static void setMiniClusterMode(boolean choice) {
    DefaultMetricsSystem.setMiniClusterMode(choice);
  }

  /**
   * Register new mutable quantiles metric.
   * @param registry      of the metric type
   * @param name          of the metric
   * @param description   of the metric
   * @param sampleName    of the metric (e.g. "Ops")
   * @param valueName     of the metric (e.g. "Time", "Latency")
   * @param interval      rollover interval (in seconds) of the estimator.
   * @return a new mutable quantiles metric object
   */
  public static MutableQuantiles registerNewMutableQuantiles(
      MetricsRegistry registry, String name, String description, String sampleName, String valueName, int interval) {
    if (interval <= 0) {
      throw new MetricsException("Interval should be positive.  Value passed" +
                                 " is: " + interval);
    }
    MutableQuantiles metric = new MutableQuantiles(name, description, sampleName, valueName, interval);
    addMetric(registry, name, metric);
    return metric;
  }

  /**
   * Register new mutable rate metric with default false 'extended' and true 'returnExisting'.
   *
   * @param registry          of the metric type
   * @param name              of the metric
   * @param description       of the metric
   * @return a new mutable rate metric object
   */
  public static MutableRate registerNewMutableRate(
      MetricsRegistry registry, String name, String description) {
    return registerNewMutableRate(registry, name, description, false, true);
  }

  /**
   * Register new mutable rate metric.
   * @param registry          of the metric type
   * @param name              of the metric
   * @param description       of the metric
   * @param extended          create extended stats (stdev, min/max etc.) by default.
   * @param returnExisting    return existing metric if it exists
   * @return a new mutable rate metric object
   */
  public static MutableRate registerNewMutableRate(
      MetricsRegistry registry, String name, String description, boolean extended, boolean returnExisting) {

    if (returnExisting) {
      MutableMetric rate = registry.get(name);
      if (rate != null) {
        if (rate instanceof MutableRate) {
          return (MutableRate) rate;
        }
        throw new MetricsException("Unexpected metrics type " + rate.getClass()
                                   + " for " + name);
      }
    }
    MutableRate metric = new MutableRate(name, description, extended);
    addMetric(registry, name, metric);
    return metric;
  }

  /**
   * Register new mutable stat metric.
   *
   * @param registry    of the metric type
   * @param name        of the metric
   * @param description        of the metric
   * @param sampleName  of the metric (e.g. "Ops")
   * @param valueName   of the metric (e.g. "Time", "Latency")
   * @param extended    create extended stats (stdev, min/max etc.) by default.
   * @return a new mutable stat metric object
   */
  public static MutableStat registerNewMutableStat(
      MetricsRegistry registry, String name, String description,
      String sampleName, String valueName, boolean extended) {

    MutableStat metric = new MutableStat(name, description, sampleName, valueName, extended);
    addMetric(registry, name, metric);
    return metric;
  }

  private static void addMetric(MetricsRegistry registry, String name, MutableMetric metric) {
    try {
      Method add = registry.getClass().getDeclaredMethod("add", String.class, MutableMetric.class);
      add.setAccessible(true);
      add.invoke(registry, name, metric);
      add.setAccessible(false);
    } catch (InvocationTargetException | NoSuchMethodException
             | IllegalAccessException ex) {
      throw new MetricsException("Problem to use 'add' registry method.Incorrect metrics source registry class "
                                 + registry.getClass() + " for " + name);
    }
  }
}
