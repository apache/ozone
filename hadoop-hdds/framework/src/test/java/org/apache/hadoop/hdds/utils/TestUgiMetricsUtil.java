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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.apache.hadoop.metrics2.MetricsTag;
import org.junit.jupiter.api.Test;

/**
 * Class for unit tests for {@link UgiMetricsUtil}.
 */
class TestUgiMetricsUtil {

  @Test
  void testCreateServernameTagWithNonCompatibleKey() {
    // GIVEN
    final String key = "non_ugi";
    final String servername = "servername";

    // WHEN
    Optional<MetricsTag> optionalMetricsTag =
        UgiMetricsUtil.createServernameTag(key, servername);

    // THEN
    assertFalse(optionalMetricsTag.isPresent());
  }

  @Test
  void testCreateServernameTagWithCompatibleKey() {
    // GIVEN
    final String key = "ugi_metrics";
    final String servername = "servername";

    // WHEN
    Optional<MetricsTag> optionalMetricsTag =
        UgiMetricsUtil.createServernameTag(key, servername);

    // THEN
    assertTrue(optionalMetricsTag.isPresent());
    assertEquals(servername, optionalMetricsTag.get().value());
    assertEquals(servername, optionalMetricsTag.get().name());
    assertEquals("name of the server",
        optionalMetricsTag.get().description());
  }

}
