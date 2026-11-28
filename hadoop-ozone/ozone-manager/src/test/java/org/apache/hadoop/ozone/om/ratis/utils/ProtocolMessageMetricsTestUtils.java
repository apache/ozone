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

package org.apache.hadoop.ozone.om.ratis.utils;

import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;

/**
 * Test helpers for reading counters back out of a live {@link ProtocolMessageMetrics}.
 */
public final class ProtocolMessageMetricsTestUtils {

  private ProtocolMessageMetricsTestUtils() {
  }

  /**
   * Reads the {@code counter} value (number of calls) recorded for the given request type from the
   * live {@link ProtocolMessageMetrics} source. Returns {@code 0} if the type has no recorded calls.
   */
  public static long getRequestCount(ProtocolMessageMetrics<?> metrics, Enum<?> type) {
    return readMetric(metrics, type, "counter");
  }

  /**
   * Reads the {@code time} value (summed call latency, in milliseconds) recorded for the given
   * request type from the live {@link ProtocolMessageMetrics} source. Returns {@code 0} if the type
   * has no recorded calls.
   */
  public static long getRequestTime(ProtocolMessageMetrics<?> metrics, Enum<?> type) {
    return readMetric(metrics, type, "time");
  }

  private static long readMetric(ProtocolMessageMetrics<?> metrics, Enum<?> type, String metricName) {
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    metrics.getMetrics(collector, true);
    for (MetricsRecord record : collector.getRecords()) {
      boolean matchesType = false;
      for (MetricsTag tag : record.tags()) {
        if ("type".equals(tag.name()) && type.toString().equals(tag.value())) {
          matchesType = true;
          break;
        }
      }
      if (!matchesType) {
        continue;
      }
      for (AbstractMetric metric : record.metrics()) {
        if (metricName.equals(metric.name())) {
          return metric.value().longValue();
        }
      }
    }
    return 0;
  }
}
