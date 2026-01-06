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

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
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
public class ProtocolMessageMetrics<KEY extends Enum<KEY>> implements MetricsSource {

  private final String name;

  private final String description;

  private final Map<KEY, Stats> stats;

  private final AtomicInteger concurrency = new AtomicInteger(0);

  private static final MetricsInfo TYPE_TAG_INFO =
      Interns.info("type", "Message type");

  private static final MetricsInfo COUNTER_INFO =
      Interns.info("counter", "Number of distinct calls");

  private static final MetricsInfo TIME_INFO =
      Interns.info("time", "Sum of the duration of the calls");

  private static final MetricsInfo CONCURRENCY_INFO =
      Interns.info("concurrency",
          "Number of requests processed concurrently");

  public static <KEY extends Enum<KEY>> ProtocolMessageMetrics<KEY> create(String name,
      String description, KEY[] types) {
    return new ProtocolMessageMetrics<>(name, description, types);
  }

  public ProtocolMessageMetrics(String name, String description,
      KEY[] values) {
    this.name = name;
    this.description = description;
    final Class<KEY> enumClass = values[0].getDeclaringClass();
    final EnumMap<KEY, Stats> map = new EnumMap<>(enumClass);
    for (KEY value : values) {
      map.put(value, new Stats());
    }
    this.stats = Collections.unmodifiableMap(map);
  }

  public void increment(KEY key, long duration) {
    stats.get(key).add(duration);
  }

  public UncheckedAutoCloseable measure(KEY key) {
    final long startTime = Time.monotonicNow();
    concurrency.incrementAndGet();
    return () -> {
      concurrency.decrementAndGet();
      stats.get(key).add(Time.monotonicNow() - startTime);
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
    stats.forEach((key, stat) -> {
      MetricsRecordBuilder builder =
          collector.addRecord(name);
      builder.add(
          new MetricsTag(TYPE_TAG_INFO, key.toString()));
      builder.addCounter(COUNTER_INFO,
          stat.counter());
      builder.addCounter(
          TIME_INFO,
          stat.time());
      builder.endRecord();

    });
    MetricsRecordBuilder builder = collector.addRecord(name);
    builder.addCounter(CONCURRENCY_INFO, concurrency.get());
  }

  /**
   * Holds counters for a single message type.
   */
  private static final class Stats {
    private final AtomicLong counter = new AtomicLong(0);
    private final AtomicLong time = new AtomicLong(0);

    void add(long duration) {
      counter.incrementAndGet();
      time.addAndGet(duration);
    }

    long counter() {
      return counter.get();
    }

    long time() {
      return time.get();
    }
  }
}
