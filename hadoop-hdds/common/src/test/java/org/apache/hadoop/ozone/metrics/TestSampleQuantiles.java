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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.metrics2.util.Quantile;
import org.junit.jupiter.api.Test;

/**
 * Tests for SampleQuantiles.
 */
class TestSampleQuantiles {

  private static final Quantile[] QUANTILES = {new Quantile(0.50, 0.050),
      new Quantile(0.75, 0.025), new Quantile(0.90, 0.010),
      new Quantile(0.95, 0.005), new Quantile(0.99, 0.001)};

  @Test
  void testSnapshotEmptySnaphotReturnEmptyMap() {
    SampleQuantiles quantiles = new SampleQuantiles(QUANTILES);

    Map<Quantile, Long> snapshot = quantiles.snapshot();
    assertEquals(0, snapshot.size());
  }

  @Test
  void testSnapshotHasCorrectQuantilesSize() {
    SampleQuantiles quantiles = new SampleQuantiles(QUANTILES);

    insertTenElements(quantiles);

    Map<Quantile, Long> snapshot = quantiles.snapshot();
    assertEquals(5, snapshot.size());
  }

  @Test
  void testClearDontRemoveQuantilesFromSnapshot() {
    SampleQuantiles quantiles = new SampleQuantiles(QUANTILES);

    insertTenElements(quantiles);

    quantiles.clear();

    Map<Quantile, Long> snapshot = quantiles.snapshot();
    assertEquals(5, snapshot.size());
  }

  @Test
  void testClearResetQuantilesInSnapshot() {
    SampleQuantiles quantiles = new SampleQuantiles(QUANTILES);

    insertTenElements(quantiles);

    quantiles.clear();

    Map<Quantile, Long> snapshot = quantiles.snapshot();
    snapshot.forEach((key, value) -> assertEquals(0, value));
  }

  @Test
  void testGetCount() {
    SampleQuantiles quantiles = new SampleQuantiles(QUANTILES);

    insertTenElements(quantiles);

    long count = quantiles.getCount();
    assertEquals(10, count);
  }

  private static void insertTenElements(SampleQuantiles metric) {
    for (int i = 0; i < 10; i++) {
      metric.insert(i);
    }
  }
}
