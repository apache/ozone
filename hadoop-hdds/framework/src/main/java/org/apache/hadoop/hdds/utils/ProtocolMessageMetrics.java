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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.UncheckedAutoCloseable;

/**
 * Metrics to count all the subtypes of a specific message.
 */
public class ProtocolMessageMetrics<KEY> implements MetricsSource {

  private final String name;

  private final String description;

  private final Map<KEY, AtomicLong> counters =
      new ConcurrentHashMap<>();

  private final Map<KEY, AtomicLong> elapsedTimes =
      new ConcurrentHashMap<>();

  private final AtomicInteger concurrency = new AtomicInteger(0);

  public static <KEY> ProtocolMessageMetrics<KEY> create(String name,
      String description, KEY[] types) {
    return new ProtocolMessageMetrics<KEY>(name, description, types);
  }

  public ProtocolMessageMetrics(String name, String description,
      KEY[] values) {
    this.name = name;
    this.description = description;
    for (KEY value : values) {
      counters.put(value, new AtomicLong(0));
      elapsedTimes.put(value, new AtomicLong(0));
    }
  }

  public void increment(KEY key, long duration) {
    counters.get(key).incrementAndGet();
    elapsedTimes.get(key).addAndGet(duration);
  }

  public UncheckedAutoCloseable measure(KEY key) {
    final long startTime = Time.monotonicNow();
    concurrency.incrementAndGet();
    return () -> {
      concurrency.decrementAndGet();
      counters.get(key).incrementAndGet();
      elapsedTimes.get(key).addAndGet(Time.monotonicNow() - startTime);
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
      MetricsRecordBuilder builder =
          collector.addRecord(name);
      builder.add(
          new MetricsTag(Interns.info("type", "Message type"), key.toString()));
      builder.addCounter(new MetricName("counter", "Number of distinct calls"),
          value.longValue());
      builder.addCounter(
          new MetricName("time", "Sum of the duration of the calls"),
          elapsedTimes.get(key).longValue());
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
}
