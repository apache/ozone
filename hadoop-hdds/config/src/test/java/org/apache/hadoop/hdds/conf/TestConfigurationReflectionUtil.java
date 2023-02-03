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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
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
            ConfigType.TIME, true,
            "ozone.scm.client.wait", true,
            "30m", true),
        arguments(ConfigurationExampleGrandParent.class, "number",
            ConfigType.AUTO, true,
            "number", true,
            "2", true),
        arguments(ConfigurationExample.class, "secure",
            ConfigType.AUTO, true,
            "ozone.scm.client.secure", true,
            "true", true),
        arguments(ConfigurationExample.class, "no-such-field",
            null, false,
            "", false,
            "", false),
        arguments(ConfigFileAppender.class, "document",
            null, false,
            "", false,
            "", false),
        arguments(ConfigurationExample.class, "threshold",
            ConfigType.DOUBLE, true,
            "ozone.scm.client.threshold", true,
            "10", true)
    );
  }

  @ParameterizedTest
  @MethodSource("data")
  void testForGivenClasses(Class<?> testClass, String fieldName,
      ConfigType expectedType, boolean typePresent,
      String expectedKey, boolean keyPresent,
      String expectedDefault, boolean defaultValuePresent) {
    Optional<ConfigType> type = ConfigurationReflectionUtil.getType(
        testClass, fieldName);
    assertEquals(typePresent, type.isPresent());
    type.ifPresent(actual -> assertEquals(expectedType, actual));

    Optional<String> key = ConfigurationReflectionUtil.getKey(
        testClass, fieldName);
    assertEquals(keyPresent, key.isPresent());
    key.ifPresent(actual -> assertEquals(expectedKey, actual));

    Optional<String> defaultValue = ConfigurationReflectionUtil.getDefaultValue(
        testClass, fieldName);
    assertEquals(defaultValuePresent, defaultValue.isPresent());
    defaultValue.ifPresent(actual -> assertEquals(expectedDefault, actual));
  }
}
