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

package org.apache.hadoop.hdds.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestDurationUtil {

  private static Stream<Arguments> paramsForPositiveCases() {
    return Stream.of(
        arguments(
            "0s",
            Duration.ZERO
        ),
        arguments(
            "2562047788015215h 30m 7s",
            Duration.ofSeconds(Long.MAX_VALUE)
        ),
        arguments(
            "1s",
            Duration.ofSeconds(1)
        ),
        arguments(
            "30s",
            Duration.ofSeconds(30)
        ),
        arguments(
            "1m 0s",
            Duration.ofMinutes(1)
        ),
        arguments(
            "2m 30s",
            Duration.ofMinutes(2).plusSeconds(30)
        ),
        arguments(
            "1h 30m 45s",
            Duration.ofHours(1).plusMinutes(30).plusSeconds(45)
        ),
        arguments(
            "24h 0m 0s",
            Duration.ofDays(1)
        ),
        arguments(
            "48h 0m 0s",
            Duration.ofDays(2)
        )
    );
  }

  private static Collection<Duration> paramsForNegativeCases() {
    return Arrays.asList(Duration.ofSeconds(-1L), Duration.ofSeconds(Long.MIN_VALUE));
  }

  @ParameterizedTest
  @MethodSource("paramsForPositiveCases")
  void testDuration(String expected, Duration actual) {
    assertEquals(expected, DurationUtil.getPrettyDuration(actual));
  }

  @ParameterizedTest
  @MethodSource("paramsForNegativeCases")
  void testDuration(Duration param) {
    assertThrows(IllegalStateException.class, () -> DurationUtil.getPrettyDuration(param));
  }
}

