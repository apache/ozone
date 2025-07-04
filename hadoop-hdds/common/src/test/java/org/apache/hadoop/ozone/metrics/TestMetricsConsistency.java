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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.filter.GlobFilter;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.ozone.metrics.util.MetricsRecordBuilderImpl;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.Test;

/**
 * Test for checking the correctness of metrics while concurrent collecting.
 */
public class TestMetricsConsistency {

  public static MetricsRecordBuilderImpl getMetricsRecordBuilder() {
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    MetricsInfo metricsInfo = new MetricsInfo() {
      @Override
      public String name() {
        return "name";
      }

      @Override
      public String description() {
        return "desc";
      }
    };
    MetricsFilter metricsFilter = new GlobFilter();
    return new MetricsRecordBuilderImpl(collector, metricsInfo, metricsFilter, metricsFilter, true);
  }

  @Test
  void testOzoneMutableRateExtendedConsistencyTest() throws InterruptedException {
    MutableRate rate = new MutableRate("Test name", "Test description", true);
    final MetricsRecordBuilderImpl builder = getMetricsRecordBuilder();
    ExecutorService executor = Executors.newFixedThreadPool(10);
    try {
      for (int i = 0; i < 1000; i++) {
        int finalI = i;

        executor.submit(() -> {
          rate.add(finalI);
          if (finalI % 100 == 0) {
            rate.snapshot(getMetricsRecordBuilder());
          }
        });
      }
    } finally {
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    rate.snapshot(builder);
    AbstractMetric numOpsMetric = builder.metrics().get(0);
    assertEquals(1000, numOpsMetric.value().intValue());
  }

  @Test
  void testOzoneMutableRateNotExtendedConsistencyTest() throws InterruptedException {
    MutableRate metric = new MutableRate("Test name", "Test description", false);
    final MetricsRecordBuilderImpl builder = getMetricsRecordBuilder();
    ExecutorService executor = Executors.newFixedThreadPool(10);
    try {
      for (int i = 0; i < 10000; i++) {
        int finalI = i;

        executor.submit(() -> {
          metric.add(finalI);
          if (finalI % 100 == 0) {
            metric.snapshot(getMetricsRecordBuilder());
          }
        });
      }
    } finally {
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
    metric.snapshot(builder);
    AbstractMetric numOpsMetric = builder.metrics().get(0);
    assertEquals(10000, numOpsMetric.value().intValue());
  }

  @Test
  void testOzoneMutableStatConsistencyTest() throws InterruptedException {
    MutableStat metric = new MutableStat(
        "Test_name",
        "Test_description",
        "Test_sample_name",
        "Test_value_name");
    final MetricsRecordBuilderImpl builder = getMetricsRecordBuilder();
    ExecutorService executor = Executors.newFixedThreadPool(10);
    try {
      for (int i = 0; i < 10000; i++) {
        int finalI = i;

        executor.submit(() -> {
          metric.add(finalI);
          if (finalI % 100 == 0) {
            metric.snapshot(getMetricsRecordBuilder());
          }
        });
      }
    } finally {
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
    metric.snapshot(builder);
    AbstractMetric numOpsMetric = builder.metrics().get(0);
    assertEquals(10000, numOpsMetric.value().intValue());
  }


  @Test
  void testMutableQuantilesConsistencyTest() throws Exception {
    MutableQuantiles metric = new MutableQuantiles(
        "Test_name",
        "Test_description",
        "Test_sample_name",
        "Test_value_name",
        1);

    final MetricsRecordBuilderImpl builder = getMetricsRecordBuilder();
    ExecutorService executor = Executors.newFixedThreadPool(10);
    try {
      for (int i = 0; i < 10000; i++) {
        int finalI = i;

        executor.submit(() -> {
          metric.add(finalI);
          if (finalI % 101 == 0) {
            metric.snapshot(getMetricsRecordBuilder());
          }
        });
      }

    } finally {
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
    LambdaTestUtils.await(5000, 500,
        () -> {
          metric.snapshot(builder);
          return !builder.metrics().isEmpty();
        });
    AbstractMetric numOpsMetric = builder.metrics().get(0);
    assertEquals(10000, numOpsMetric.value().intValue());
  }
}
