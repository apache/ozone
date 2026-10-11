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

package org.apache.hadoop.ozone.om.ratis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.lib.MetricsAnnotations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests double buffer metrics without a flush thread or periodic metrics collection.
 */
class TestOzoneManagerDoubleBufferMetrics {

  private OzoneManagerDoubleBufferMetrics metrics;

  @BeforeEach
  void setup() {
    metrics = new OzoneManagerDoubleBufferMetrics();
    MetricsAnnotations.makeSource(metrics);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testFlushMetrics(boolean snapshot) {
    metrics.updateFlush(1);
    if (snapshot) {
      metrics.getQueueSize().snapshot(new MetricsCollectorImpl().addRecord("doubleBuffer"), true);
    }
    for (int size : new int[] {1, 98, 1}) {
      metrics.updateFlush(size);
    }

    assertEquals(4, metrics.getTotalNumOfFlushOperations());
    assertEquals(101, metrics.getTotalNumOfFlushedTransactions());
    assertEquals(98, metrics.getMaxNumberOfTransactionsFlushedInOneIteration());
    assertEquals(25.25f, metrics.getAvgFlushTransactionsInOneIteration());
    assertEquals(snapshot ? 3 : 4, metrics.getQueueSize().lastStat().numSamples());
    // Queue size totals cover the sampling interval and are calculated using a floating-point mean.
    assertEquals(snapshot ? 100 : 101, metrics.getQueueSize().lastStat().total(), 1e-9);
  }

  @Test
  void testFlushTime() {
    metrics.updateFlushTime(10);
    metrics.updateFlushTime(20);

    assertEquals(2, metrics.getFlushTime().lastStat().numSamples());
    assertEquals(15, metrics.getFlushTime().lastStat().mean(), 1e-9);
  }
}
