/*
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
package org.apache.hadoop.hdds.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class DurationUtilTest {

  private static Stream<Arguments> paramsForPositiveCases() {
    return Stream.of(
        arguments(
            "0s",
            DurationUtil.getPrettyDuration(0)
        ),
        arguments(
            "2562047788015215h 30m 7s",
            DurationUtil.getPrettyDuration(Long.MAX_VALUE)
        ),
        arguments(
            "1s",
            DurationUtil.getPrettyDuration(Duration.ofSeconds(1).getSeconds())
        ),
        arguments(
            "30s",
            DurationUtil.getPrettyDuration(Duration.ofSeconds(30).getSeconds())
        ),
        arguments(
            "1m 0s",
            DurationUtil.getPrettyDuration(Duration.ofMinutes(1).getSeconds())
        ),
        arguments(
            "2m 30s",
            DurationUtil.getPrettyDuration(Duration.ofMinutes(2).getSeconds() + Duration.ofSeconds(30).getSeconds())
        ),
        arguments(
            "1h 30m 45s",
            DurationUtil.getPrettyDuration(
                Duration.ofHours(1).getSeconds() +
                Duration.ofMinutes(30).getSeconds() +
                Duration.ofSeconds(45).getSeconds())
        ),
        arguments(
            "24h 0m 0s",
            DurationUtil.getPrettyDuration(Duration.ofDays(1).getSeconds())
        ),
        arguments(
            "48h 0m 0s",
            DurationUtil.getPrettyDuration(Duration.ofDays(2).getSeconds())
        )
    );
  }

  private static Collection<Long> paramsForNegativeCases() {
    return Arrays.asList(-1L, Long.MIN_VALUE);
  }

  @ParameterizedTest
  @MethodSource("paramsForPositiveCases")
  void testDuration(String expected, String actual) {
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("paramsForNegativeCases")
  void testDuration(Long param) {
    assertThrows(IllegalStateException.class, () -> DurationUtil.getPrettyDuration(param));
  }
}

