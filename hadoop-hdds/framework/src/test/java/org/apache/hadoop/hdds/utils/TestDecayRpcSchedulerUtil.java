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

package org.apache.hadoop.hdds.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.apache.hadoop.metrics2.MetricsTag;
import org.junit.jupiter.api.Test;

/**
 * Test class for DecayRpcSchedulerUtil.
 */
public class TestDecayRpcSchedulerUtil {

  private static final String USERNAME = "testUser";
  private static final String METRIC_NAME_VOLUME = "Volume";

  private static final String RECORD_NAME =
      "org.apache.hadoop.ipc_.DecayRpcScheduler";
  private static final String METRIC_NAME =
      "Caller(" + USERNAME + ")." + METRIC_NAME_VOLUME;

  private static final String RANDOM_RECORD_NAME = "JvmMetrics";
  private static final String RANDOM_METRIC_NAME = "ThreadsNew";

  @Test
  void testSplitMetricNameIfNeeded() {
    // Split the metric name and return only the
    // name of the metric type.
    String splitName = DecayRpcSchedulerUtil
        .splitMetricNameIfNeeded(RECORD_NAME, METRIC_NAME);

    assertEquals(METRIC_NAME_VOLUME, splitName);

    // This metric name should remain the same.
    String unchangedName = DecayRpcSchedulerUtil
        .splitMetricNameIfNeeded(RANDOM_RECORD_NAME, RANDOM_METRIC_NAME);

    assertEquals(RANDOM_METRIC_NAME, unchangedName);
  }

  @Test
  void testCheckMetricNameForUsername() {
    // Get the username from the metric name.
    String decayRpcSchedulerUsername = DecayRpcSchedulerUtil
        .checkMetricNameForUsername(RECORD_NAME, METRIC_NAME);

    assertEquals(USERNAME, decayRpcSchedulerUsername);

    // This metric doesn't contain a username in the metric name.
    // DecayRpcSchedulerUtil.checkMetricNameForUsername()
    // should return null.
    String nullUsername = DecayRpcSchedulerUtil
        .checkMetricNameForUsername(RANDOM_RECORD_NAME, RANDOM_METRIC_NAME);

    assertNull(nullUsername);
  }

  @Test
  void testCreateUsernameTagWithNullUsername() {
    // GIVEN
    final String username = null;

    // WHEN
    Optional<MetricsTag> optionalMetricsTag =
        DecayRpcSchedulerUtil.createUsernameTag(username);

    // THEN
    assertFalse(optionalMetricsTag.isPresent());
  }

  @Test
  void testCreateUsernameTagWithNotNullUsername() {
    // GIVEN
    final String username = "username";

    // WHEN
    Optional<MetricsTag> optionalMetricsTag =
        DecayRpcSchedulerUtil.createUsernameTag(username);

    // THEN
    assertTrue(optionalMetricsTag.isPresent());
    assertEquals(username, optionalMetricsTag.get().value());
    assertEquals(username, optionalMetricsTag.get().name());
    assertEquals("caller username",
        optionalMetricsTag.get().description());
  }
}
