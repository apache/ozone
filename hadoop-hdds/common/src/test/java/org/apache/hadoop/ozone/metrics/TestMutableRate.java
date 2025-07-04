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

import static org.apache.hadoop.ozone.metrics.TestMetricsConsistency.getMetricsRecordBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.ozone.metrics.util.MetricsRecordBuilderImpl;
import org.junit.jupiter.api.Test;

/**
 * Tests for OzoneMutableRate.
 */
class TestMutableRate {

  @Test
  void testOzoneMutableStatHasEmptyMetricsAfterCreationExtended() {
    MutableRate metric = createMutableStat();
    metric.setExtended(true);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();
    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    assertEquals(0, metrics.size());
  }

  @Test
  void testOzoneMutableStatMetricsSizeAfterInsertElementsExtended() {
    MutableRate metric = createMutableStat();
    metric.setExtended(true);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();
    insertTenElements(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    assertEquals(8, metrics.size());
  }

  @Test
  void testOzoneMutableStatMetricsValuesAfterInsertElementsExtended() {
    MutableRate metric = createMutableStat();
    metric.setExtended(true);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    insertTenElements(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    assertEquals(10L, metrics.get(0).value());
    assertEquals(5.5, metrics.get(1).value());
    assertEquals(1.0, metrics.get(3).value());
    assertEquals(10.0, metrics.get(4).value());
    assertEquals(1.0, metrics.get(5).value());
    assertEquals(10.0, metrics.get(6).value());
    assertEquals(10L, metrics.get(7).value());
  }

  @Test
  void testOzoneMutableStatMetricsSizeAfterInsertWithSumElementsExtended() {
    MutableRate metric = createMutableStat();
    metric.setExtended(true);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();
    metric.add(5, 100);
    metric.add(5, 100);
    metric.add(5, 100);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    assertEquals(8, metrics.size());
  }

  @Test
  void testOzoneMutableStatMetricsValuesAfterInsertWithSumElementsExtended() {
    MutableRate metric = createMutableStat();
    metric.setExtended(true);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    metric.add(5, 100);
    metric.add(5, 100);
    metric.add(5, 100);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    assertEquals(15L, metrics.get(0).value());
    assertEquals(20.0, metrics.get(1).value());
    assertEquals(Float.MAX_VALUE, Float.parseFloat(metrics.get(3).value().toString()));
    assertEquals(Float.MIN_VALUE, Float.parseFloat(metrics.get(4).value().toString()));
    assertEquals(Float.MAX_VALUE, Float.parseFloat(metrics.get(5).value().toString()));
    assertEquals(Float.MIN_VALUE, Float.parseFloat(metrics.get(6).value().toString()));
    assertEquals(15L, metrics.get(7).value());
  }

  @Test
  void testOzoneMutableStatHasEmptyMetricsAfterCreationNotExtended() {
    MutableRate metric = createMutableStat();
    metric.setExtended(false);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();
    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    assertEquals(0, metrics.size());
  }

  @Test
  void testOzoneMutableStatMetricsSizeAfterInsertElementsNotExtended() {
    MutableRate metric = createMutableStat();
    metric.setExtended(false);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();
    insertTenElements(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    assertEquals(2, metrics.size());
  }

  @Test
  void testOzoneMutableStatMetricsValuesAfterInsertElementsNotExtended() {
    MutableRate metric = createMutableStat();
    metric.setExtended(false);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    insertTenElements(metric);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    assertEquals(10L, metrics.get(0).value());
    assertEquals(5.5, metrics.get(1).value());
  }

  @Test
  void testOzoneMutableStatMetricsSizeAfterInsertWithSumElementsNotExtended() {
    MutableRate metric = createMutableStat();
    metric.setExtended(false);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();
    metric.add(5, 100);
    metric.add(5, 100);
    metric.add(5, 100);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    assertEquals(2, metrics.size());
  }

  @Test
  void testOzoneMutableStatMetricsValuesAfterInsertWithSumElementsNotExtended() {
    MutableRate metric = createMutableStat();
    metric.setExtended(false);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    metric.add(5, 100);
    metric.add(5, 100);
    metric.add(5, 100);

    metric.snapshot(metricsRecordBuilder);
    List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
    assertEquals(15L, metrics.get(0).value());
    assertEquals(20.0, metrics.get(1).value());
  }

  @Test
  void testOzoneMutableStatChangedWhenElementsAdded() {
    MutableRate metric = createMutableStat();

    insertTenElements(metric);
    assertTrue(metric.changed());
  }

  @Test
  void testOzoneMutableStatNotChangedWhenNoElementsAdded() {
    MutableRate metric = createMutableStat();

    assertFalse(metric.changed());
  }

  @Test
  void testGetLastStatWithAddMethod() {
    MutableRate metric = createMutableStat();

    insertTenElements(metric);

    SampleStat sampleStat = metric.lastStat();

    assertEquals(1, sampleStat.min());
    assertEquals(10, sampleStat.max());
    assertEquals(5.5, sampleStat.mean());
    assertEquals(10, sampleStat.numSamples());
  }

  @Test
  void testGetLastStatWithAddSumMethod() {
    MutableRate metric = createMutableStat();
    metric.add(5, 100);
    metric.add(5, 100);
    metric.add(5, 100);

    SampleStat sampleStat = metric.lastStat();

    assertEquals(Float.MAX_VALUE, sampleStat.min());
    assertEquals(Float.MIN_VALUE, sampleStat.max());
    assertEquals(20.0, sampleStat.mean());
    assertEquals(15, sampleStat.numSamples());
  }

  @Test
  void testOzoneMutableStatMetricsSizeAfterInsertElements1() {
    MutableRate metric = createMutableStat();
    metric.setUpdateTimeStamp(true);
    assertEquals(0, metric.getSnapshotTimeStamp());
  }

  @Test
  void testOzoneMutableStatMetricsSizeAfterInsertElements2() {
    MutableRate metric = createMutableStat();
    metric.setUpdateTimeStamp(true);
    MetricsRecordBuilderImpl metricsRecordBuilder = getMetricsRecordBuilder();

    insertTenElements(metric);

    metric.snapshot(metricsRecordBuilder);

    assertTrue(metric.getSnapshotTimeStamp() > 0);
  }

  private static void insertTenElements(MutableStat metric) {
    for (int i = 1; i <= 10; i++) {
      metric.add(i);
    }
  }

  private static MutableRate createMutableStat() {
    return new MutableRate(
        "Test_name",
        "Test_description",
        false);
  }
}
