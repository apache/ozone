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
package org.apache.hadoop.hdds.conf;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Test the configuration reflection utility class.
 */
class TestConfigurationReflectionUtil {

  static Stream<Arguments> data() {
    return Stream.of(
        arguments(ConfigurationExample.class, "waitTime",
            Optional.of(ConfigType.TIME),
            Optional.of("ozone.scm.client.wait"),
            Optional.of("30m")),
        arguments(ConfigurationExampleGrandParent.class, "number",
            Optional.of(ConfigType.AUTO),
            Optional.of("number"),
            Optional.of("2")),
        arguments(ConfigurationExample.class, "secure",
            Optional.of(ConfigType.AUTO),
            Optional.of("ozone.scm.client.secure"),
            Optional.of("true")),
        arguments(ConfigurationExample.class, "no-such-field",
            Optional.empty(),
            Optional.empty(),
            Optional.empty()),
        arguments(ConfigFileAppender.class, "document",
            Optional.empty(),
            Optional.empty(),
            Optional.empty()),
        arguments(ConfigurationExample.class, "threshold",
            Optional.of(ConfigType.DOUBLE),
            Optional.of("ozone.scm.client.threshold"),
            Optional.of("10"))
    );
  }

  @ParameterizedTest
  @MethodSource("data")
  void testForGivenClasses(Class<?> testClass, String fieldName,
      Optional<ConfigType> expectedType,
      Optional<String> expectedKey,
      Optional<String> expectedDefault) {
    Optional<ConfigType> type = ConfigurationReflectionUtil.getType(
        testClass, fieldName);
    assertEquals(expectedType, type);

    Optional<String> key = ConfigurationReflectionUtil.getKey(
        testClass, fieldName);
    assertEquals(expectedKey, key);

    Optional<String> defaultValue = ConfigurationReflectionUtil.getDefaultValue(
        testClass, fieldName);
    assertEquals(expectedDefault, defaultValue);
  }

  @Test
  void listReconfigurableProperties() {
    Set<String> props =
        ConfigurationReflectionUtil.mapReconfigurableProperties(
            ConfigurationExample.class).keySet();

    String prefix = "ozone.scm.client";
    assertEquals(ImmutableSet.of(
        prefix + ".dynamic",
        prefix + ".grandpa.dyna"
    ), props);
  }
}
