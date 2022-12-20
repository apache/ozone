/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.utils;

import java.util.Optional;
import org.apache.hadoop.metrics2.MetricsTag;
import org.junit.jupiter.api.Assertions;
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
    Assertions.assertFalse(optionalMetricsTag.isPresent());
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
    Assertions.assertTrue(optionalMetricsTag.isPresent());
    Assertions.assertEquals(servername, optionalMetricsTag.get().value());
    Assertions.assertEquals(servername, optionalMetricsTag.get().name());
    Assertions.assertEquals("name of the server",
        optionalMetricsTag.get().description());
  }

}