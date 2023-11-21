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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.ratis.util.function.CheckedRunnable;
import org.apache.ratis.util.function.CheckedSupplier;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Encloses helpers to deal with metrics.
 */
public final class MetricUtil {

  private static final ExecutorService EXECUTOR =
      new ThreadPoolExecutor(1, 10, 100, TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<>(500_000),
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("metric-mutable-rate-handler-%d")
              .build(),
          new ThreadPoolExecutor.CallerRunsPolicy());

  private MetricUtil() {
  }

  public static <T, E extends Exception> T captureLatencyNs(
      MutableRate metric,
      CheckedSupplier<T, E> block) throws E {
    long start = Time.monotonicNowNanos();
    try {
      return block.get();
    } finally {
      executeStatAddAction(metric::add, Time.monotonicNowNanos() - start);
    }
  }

  public static <E extends IOException> void captureLatencyNs(
      MutableRate metric,
      CheckedRunnable<E> block) throws IOException {
    long start = Time.monotonicNowNanos();
    try {
      block.run();
    } finally {
      executeStatAddAction(metric::add, Time.monotonicNowNanos() - start);
    }
  }

  public static <T, E extends IOException> T captureLatencyNs(
      Consumer<Long> latencySetter,
      CheckedSupplier<T, E> block) throws E {
    long start = Time.monotonicNowNanos();
    try {
      return block.get();
    } finally {
      executeStatAddAction(latencySetter, Time.monotonicNowNanos() - start);
    }
  }

  public static void executeStatAddAction(Consumer<Long> metric,
                                          long statValue) {
    EXECUTOR.submit(() -> metric.accept(statValue));
  }

}
