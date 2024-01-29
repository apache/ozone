/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.util;

import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.ratis.util.function.CheckedRunnable;
import org.apache.ratis.util.function.CheckedSupplier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Encloses helpers to deal with metrics.
 */
public final class MetricUtil {
  private MetricUtil() {
  }

  public static <T, E extends Exception> T captureLatencyNs(
      MutableRate metric,
      CheckedSupplier<T, E> block) throws E {
    long start = Time.monotonicNowNanos();
    try {
      return block.get();
    } finally {
      metric.add(Time.monotonicNowNanos() - start);
    }
  }

  public static <E extends IOException> void captureLatencyNs(
      MutableRate metric,
      CheckedRunnable<E> block) throws IOException {
    long start = Time.monotonicNowNanos();
    try {
      block.run();
    } finally {
      metric.add(Time.monotonicNowNanos() - start);
    }
  }

  public static <T, E extends IOException> T captureLatencyNs(
      Consumer<Long> latencySetter,
      CheckedSupplier<T, E> block) throws E {
    long start = Time.monotonicNowNanos();
    try {
      return block.get();
    } finally {
      latencySetter.accept(Time.monotonicNowNanos() - start);
    }
  }

  /**
   * Creates MutableQuantiles metrics with one or multiple intervals.
   *
   * @param registry    The MetricsRegistry to register the new MutableQuantiles
   *                   instances.
   * @param name        The base name of the metric.
   * @param description The description of the metric.
   * @param sampleName of the metric (e.g., "Ops")
   * @param valueName of the metric (e.g., "Time" or "Latency")
   * @param intervals   An array of intervals for the quantiles.
   * @return A list of created MutableQuantiles instances.
   */
  public static List<MutableQuantiles> createQuantiles(MetricsRegistry registry,
      String name, String description, String sampleName, String valueName,
      int... intervals) {
    if (intervals == null) {
      throw new IllegalArgumentException(
          "At least one interval should be provided.");
    }
    if (intervals.length == 0) {
      return new ArrayList<>();
    }

    return Arrays.stream(intervals).mapToObj(interval -> {
      String quantileName = name + interval + "s";
      return registry.newQuantiles(quantileName, description,
          sampleName, valueName, interval);
    }).collect(Collectors.toList());
  }
}
