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

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.ozone.metrics.util.MetricsRecordBuilderImpl;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.hadoop.ozone.metrics.TestMetricsConsistency.getMetricsRecordBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * Tests for MutableQuantiles.
 */
class TestMutableQuantiles {

  @Test
  void testIsChangedAfterTaskStart() throws InterruptedException {
    MutableQuantiles quantiles = getMutableQuantiles();
    insertTenElements(quantiles);
    Thread.sleep(1100L);
    quantiles.stop();
    assertEquals(true, quantiles.changed());
  }

  @Test
  void testSnapshotHasCorrectQuantilesSize() throws InterruptedException {
    MutableQuantiles quantiles = getMutableQuantiles();
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();
    insertTenElements(quantiles);

    Thread.sleep(1100L);

    quantiles.snapshot(metricsRecordBuilder);

    quantiles.stop();

    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();

    assertEquals(6L, metrics.size());
  }

  @Test
  void testMetrics() throws InterruptedException {
    MutableQuantiles quantiles = getMutableQuantiles();
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();
    insertTenElements(quantiles);

    Thread.sleep(1100L);

    quantiles.snapshot(metricsRecordBuilder);

    quantiles.stop();

    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();

    assertEquals(10L, metrics.get(0).value());
    assertEquals(4L, metrics.get(1).value());
    assertEquals(6L, metrics.get(2).value());
    assertEquals(8L, metrics.get(3).value());
    assertEquals(8L, metrics.get(4).value());
    assertEquals(8L, metrics.get(5).value());
  }

  private static MutableQuantiles getMutableQuantiles() {
    return new MutableQuantiles("Test_name",
        "Test_description",
        "Test_sample_name",
        "Test_value_name",
        1);
  }

  private static void insertTenElements(MutableQuantiles metric) {
    for (int i = 0; i < 10; i++) {
      metric.add(i);
    }
  }
}
