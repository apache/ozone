/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.ratis.util.UncheckedAutoCloseable;

/**
 * Metrics to count all the subtypes of a specific message.
 */
public final class ProtocolMessageMetrics<KEY> implements MetricsSource {

  private final String name;

  private final String description;

  private final boolean quantileEnable;

  private final Map<KEY, AtomicLong> counters =
      new ConcurrentHashMap<>();

  private final Map<KEY, AtomicLong> elapsedTimes =
      new ConcurrentHashMap<>();

  private final Map<KEY, MutableQuantiles[]> quantiles =
      new ConcurrentHashMap<>();

  private final AtomicInteger concurrency = new AtomicInteger(0);

  public static <KEY> ProtocolMessageMetrics<KEY> create(String name,
      String description, KEY[] types, ConfigurationSource conf) {
    return new ProtocolMessageMetrics<KEY>(name, description, types, conf);
  }

  private ProtocolMessageMetrics(String name, String description,
      KEY[] values, ConfigurationSource conf) {
    this.name = name;
    this.description = description;
    int[] intervals = conf.getInts(
        OzoneConfigKeys.OZONE_PROTOCOL_MESSAGE_METRICS_PERCENTILES_INTERVALS);
    quantileEnable = (intervals.length > 0);
    for (KEY value : values) {
      counters.put(value, new AtomicLong(0));
      elapsedTimes.put(value, new AtomicLong(0));
      if (quantileEnable) {
        MetricsRegistry registry =
            new MetricsRegistry(value.toString() + "MessageMetrics");
        MutableQuantiles[] mutableQuantiles =
            new MutableQuantiles[intervals.length];
        quantiles.put(value, mutableQuantiles);
        for (int i = 0; i < intervals.length; i++) {
          mutableQuantiles[i] = registry.newQuantiles(
              intervals[i] + "s",
              value.toString() + "rpc time in milli second",
              "ops", "latencyMs", intervals[i]);
        }
      }
    }
  }

  public void increment(KEY key, long duration) {
    counters.get(key).incrementAndGet();
    elapsedTimes.get(key).addAndGet(duration);
    if (quantileEnable) {
      for (MutableQuantiles q : quantiles.get(key)) {
        q.add(duration);
      }
    }
  }

  public UncheckedAutoCloseable measure(KEY key) {
    final long startTime = System.currentTimeMillis();
    concurrency.incrementAndGet();
    return () -> {
      concurrency.decrementAndGet();
      counters.get(key).incrementAndGet();
      long delta = System.currentTimeMillis() - startTime;
      elapsedTimes.get(key).addAndGet(delta);
      if (quantileEnable) {
        for (MutableQuantiles q : quantiles.get(key)) {
          q.add(delta);
        }
      }
    };
  }

  public void register() {
    DefaultMetricsSystem.instance()
        .register(name, description, this);
  }

  public void unregister() {
    DefaultMetricsSystem.instance().unregisterSource(name);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    counters.forEach((key, value) -> {
      MetricsRecordBuilder builder = collector.addRecord(name);
      builder.add(
          new MetricsTag(Interns.info("type", "Message type"), key.toString()));
      builder.addCounter(new MetricName("counter", "Number of distinct calls"),
          value.longValue());
      builder.addCounter(
          new MetricName("time", "Sum of the duration of the calls"),
          elapsedTimes.get(key).longValue());
      if (quantileEnable) {
        for (MutableQuantiles mutableQuantiles : quantiles.get(key)) {
          mutableQuantiles.snapshot(builder, all);
        }
      }
      builder.endRecord();
    });
    MetricsRecordBuilder builder = collector.addRecord(name);
    builder.addCounter(new MetricName("concurrency",
            "Number of requests processed concurrently"), concurrency.get());
  }

  /**
   * Simple metrics info implementation.
   */
  public static class MetricName implements MetricsInfo {
    private final String name;
    private final String description;

    public MetricName(String name, String description) {
      this.name = name;
      this.description = description;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String description() {
      return description;
    }
  }

  public boolean isQuantileEnable() {
    return quantileEnable;
  }
}
