/*
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
package org.apache.hadoop.ozone.metrics;

import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableMetric;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;


/**
 * The metrics Ozone system.
 */
public final class OzoneMetricsSystem {

  static {
    OzoneMetricsFactory.registerAsDefaultMutableMetricsFactory();
  }

  private OzoneMetricsSystem() {
  }

  public static MetricsSystem initialize(String prefix) {
    return DefaultMetricsSystem.initialize(prefix);
  }

  public static <T> T register(String name, String desc, T sink) {
    return instance().register(name, desc, sink);
  }

  public static void unregisterSource(String name) {
    DefaultMetricsSystem.instance().unregisterSource(name);
  }

  public static MetricsSystem instance() {
    return DefaultMetricsSystem.instance();
  }

  public static void shutdown() {
    DefaultMetricsSystem.shutdown();
  }

  public static void setMiniClusterMode(boolean choice) {
    DefaultMetricsSystem.setMiniClusterMode(choice);
  }

  public static void registerNewMutableMetrics(MetricsRegistry registry, Map<String, MutableMetric> metrics) {
    try {
      Method add = registry.getClass().getDeclaredMethod("add", String.class, MutableMetric.class);
      add.setAccessible(true);
      for (Map.Entry<String, MutableMetric> entry : metrics.entrySet()) {
        add.invoke(registry, entry.getKey(), entry.getValue());
      }
      add.setAccessible(false);
    } catch (InvocationTargetException | NoSuchMethodException
             | IllegalAccessException ex) {
      System.out.println("OOooooooops");
    }
  }

  public static MutableQuantiles registerNewMutableQuantiles(
      MetricsRegistry registry, String name, String desc, String sampleName, String valueName, int interval) {
    if (interval <= 0) {
      throw new MetricsException("Interval should be positive.  Value passed" +
                                 " is: " + interval);
    }
    MutableQuantiles metric = new MutableQuantiles(name, desc, sampleName, valueName, interval);
    addMetric(registry, name, metric);
    return metric;
  }

  public static OzoneMutableRate registerNewMutableRate(
      MetricsRegistry registry, String name, String desc) {
    return registerNewMutableRate(registry, name, desc, false, true);
  }
  public static OzoneMutableRate registerNewMutableRate(
      MetricsRegistry registry, String name, String desc, boolean extended, boolean returnExisting) {

    if (returnExisting) {
      MutableMetric rate = registry.get(name);
      if (rate != null) {
        if (rate instanceof OzoneMutableRate) {
          return (OzoneMutableRate) rate;
        }
        throw new MetricsException("Unexpected metrics type " + rate.getClass()
                                   + " for " + name);
      }
    }
    OzoneMutableRate metric = new OzoneMutableRate(name, desc, extended);
    addMetric(registry, name, metric);
    return metric;
  }

  public static OzoneMutableStat registerNewMutableStat(
      MetricsRegistry registry, String name, String desc,
      String sampleName, String valueName, boolean extended) {

    OzoneMutableStat metric = new OzoneMutableStat(name, desc, sampleName, valueName, extended);
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
      System.out.println("OOooooooops");
    }
  }

  public static <T extends MutableMetric> T registerNewRate(MetricsRegistry registry, String metricName, T metric) {
    addMetric(registry, metricName, metric);
    return metric;
  }
}
