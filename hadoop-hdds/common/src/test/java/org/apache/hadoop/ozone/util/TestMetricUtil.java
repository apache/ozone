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

package org.apache.hadoop.ozone.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TestMetricUtil {

  @AfterEach
  void resetMiniClusterMode() {
    DefaultMetricsSystem.setMiniClusterMode(false);
  }

  @Test
  void testGetMetricsSourceName() {
    assertEquals("source", MetricUtil.getMetricsSourceName("source", "id"));

    DefaultMetricsSystem.setMiniClusterMode(true);
    assertEquals("source-id",
        MetricUtil.getMetricsSourceName("source", "id"));
    assertEquals("source",
        MetricUtil.getMetricsSourceName("source", null));
    assertEquals("source-service-id",
        MetricUtil.getMetricsSourceName("source", "service", "id"));
    assertEquals("source-id",
        MetricUtil.getMetricsSourceName("source", null, "id"));
  }
}
