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

import org.junit.jupiter.api.Test;


/**
 * Tests for SampleStat.
 */
class TestSampleStat {

  @Test
  void testAddTenElements() {
    SampleStat metric = new SampleStat();

    insertTenElements(metric);

    assertEquals(10, metric.numSamples());
    assertEquals(1, metric.min());
    assertEquals(10, metric.max());
    assertEquals(55, metric.total());
    assertTrue(metric.variance() > 9 && metric.variance() < 10);
  }

  @Test
  void testAddWithSum() {
    SampleStat metric = new SampleStat();

    metric.add(5, 100);
    metric.add(5, 100);
    metric.add(5, 100);

    assertEquals(15, metric.numSamples());
    assertEquals(Float.MAX_VALUE, metric.min());
    assertEquals(Float.MIN_VALUE, metric.max());
    assertEquals(300, metric.total());
    assertTrue(metric.variance() > 1327 && metric.variance() < 1329);
  }

  @Test
  void testReset() {
    SampleStat metric = new SampleStat();

    insertTenElements(metric);

    metric.reset();
    assertEquals(0, metric.numSamples());
    assertEquals(Float.MAX_VALUE, metric.min());
    assertEquals(Float.MIN_VALUE, metric.max());
    assertEquals(0, metric.stddev());
    assertEquals(0, metric.total());
    assertEquals(0, metric.variance());
  }

  @Test
  void testCopy() {
    SampleStat metric = new SampleStat();

    insertTenElements(metric);

    SampleStat anotherMetric = new SampleStat();
    metric.copyTo(anotherMetric);

    assertEquals(anotherMetric.numSamples(), metric.numSamples());
    assertEquals(anotherMetric.min(), metric.min());
    assertEquals(anotherMetric.max(), metric.max());
    assertEquals(anotherMetric.stddev(), metric.stddev());
    assertEquals(anotherMetric.mean(), metric.mean());
    assertEquals(anotherMetric.total(), metric.total());
    assertEquals(anotherMetric.variance(), metric.variance());
  }

  private static void insertTenElements(SampleStat metric) {
    for (int i = 1; i <= 10; i++) {
      metric.add(i);
    }
  }
}
