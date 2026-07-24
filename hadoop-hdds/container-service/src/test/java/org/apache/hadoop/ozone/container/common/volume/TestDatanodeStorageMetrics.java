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

package org.apache.hadoop.ozone.container.common.volume;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsRecordImpl;
import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DatanodeStorageMetrics}.
 *
 * <p>Tests verify:
 * <ul>
 *   <li>Correct aggregation of Capacity and Used across multiple volumes.</li>
 *   <li>Attribute names match exactly (guards the CSD context regex contract).</li>
 *   <li>UsedPercentage arithmetic (100 * Used / Capacity).</li>
 *   <li>Zero-capacity guard: UsedPercentage returns 0 instead of NaN/divide-by-zero.</li>
 * </ul>
 */
class TestDatanodeStorageMetrics {

  @Test
  void testAggregationAcrossTwoVolumes() {
    // vol1: capacity=100, scmUsed=40  vol2: capacity=300, scmUsed=60
    // expected: Capacity=400, Used=100, UsedPercentage=25.0
    StorageLocationReport vol1 = StorageLocationReport.newBuilder()
        .setId("vol1").setCapacity(100L).setScmUsed(40L).setRemaining(60L)
        .build();
    StorageLocationReport vol2 = StorageLocationReport.newBuilder()
        .setId("vol2").setCapacity(300L).setScmUsed(60L).setRemaining(240L)
        .build();

    MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);
    when(volumeSet.getStorageReport())
        .thenReturn(new StorageLocationReport[]{vol1, vol2});

    DatanodeStorageMetrics metrics = DatanodeStorageMetrics.create(volumeSet);
    try {
      MetricsCollectorImpl collector = new MetricsCollectorImpl();
      metrics.getMetrics(collector, true);

      assertThat(collector.getRecords()).hasSize(1);
      MetricsRecordImpl rec = collector.getRecords().get(0);

      // Record name determines the JMX name= segment — must match verbatim.
      assertThat(rec.name()).isEqualTo(DatanodeStorageMetrics.SOURCE_NAME);

      Iterable<AbstractMetric> all = rec.metrics();
      assertThat(findLong(all, "Capacity")).isEqualTo(400L);
      assertThat(findLong(all, "Used")).isEqualTo(100L);
      assertThat(findDouble(all, "UsedPercentage")).isEqualTo(25.0);
    } finally {
      metrics.unregister();
    }
  }

  @Test
  void testZeroCapacityReturnsZeroPercentage() {
    // No volumes → capacity=0, used=0; UsedPercentage must be 0.0, not NaN.
    MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);
    when(volumeSet.getStorageReport()).thenReturn(new StorageLocationReport[0]);

    DatanodeStorageMetrics metrics = DatanodeStorageMetrics.create(volumeSet);
    try {
      MetricsCollectorImpl collector = new MetricsCollectorImpl();
      metrics.getMetrics(collector, true);

      Iterable<AbstractMetric> all = collector.getRecords().get(0).metrics();
      assertThat(findLong(all, "Capacity")).isEqualTo(0L);
      assertThat(findLong(all, "Used")).isEqualTo(0L);
      assertThat(findDouble(all, "UsedPercentage")).isEqualTo(0.0);
    } finally {
      metrics.unregister();
    }
  }

  private static long findLong(Iterable<AbstractMetric> metrics, String name) {
    for (AbstractMetric m : metrics) {
      if (name.equals(m.name())) {
        return m.value().longValue();
      }
    }
    throw new AssertionError("Missing metric: " + name);
  }

  private static double findDouble(Iterable<AbstractMetric> metrics, String name) {
    for (AbstractMetric m : metrics) {
      if (name.equals(m.name())) {
        return m.value().doubleValue();
      }
    }
    throw new AssertionError("Missing metric: " + name);
  }
}
