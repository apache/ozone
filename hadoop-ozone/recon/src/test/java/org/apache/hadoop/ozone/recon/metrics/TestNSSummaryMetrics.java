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

package org.apache.hadoop.ozone.recon.metrics;

import static org.apache.ozone.test.MetricsAsserts.getLongCounter;
import static org.apache.ozone.test.MetricsAsserts.getLongGauge;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link NSSummaryMetrics}.
 */
class TestNSSummaryMetrics {

  @Test
  void testRecordInvalidTreeDetection() {
    NSSummaryMetrics metrics = new NSSummaryMetrics();
    long before = Time.now();

    metrics.recordInvalidTreeDetection();
    metrics.recordInvalidTreeDetection();

    MetricsRecordBuilder builder = getMetrics(metrics);
    assertThat(getLongCounter("invalidTreeDetectionCount", builder))
        .isEqualTo(2L);
    assertThat(getLongGauge("lastInvalidTreeDetectionMillis", builder))
        .isBetween(before, Time.now());
  }

  @Test
  void testLastDetectedMillisDoesNotRegress() {
    NSSummaryMetrics metrics = new NSSummaryMetrics();

    metrics.recordInvalidTreeDetection(200L);
    metrics.recordInvalidTreeDetection(100L);

    assertThat(metrics.getInvalidTreeDetectionCount()).isEqualTo(2L);
    assertThat(metrics.getLastInvalidTreeDetectionMillis())
        .isEqualTo(200L);
  }
}
