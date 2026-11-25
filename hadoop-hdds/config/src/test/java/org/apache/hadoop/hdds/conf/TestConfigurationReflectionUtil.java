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

package org.apache.hadoop.hdds.conf;

import static org.apache.hadoop.hdds.conf.ConfigurationReflectionUtil.assertNoDuplicatedKeys;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test the configuration reflection utility class.
 */
class TestConfigurationReflectionUtil {

  static Stream<Arguments> data() {
    return Stream.of(
        arguments(ConfigurationExample.class, "waitTime",
            Optional.of(ConfigType.TIME),
            Optional.of("ozone.test.config.wait"),
            Optional.of("30m")),
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
            Optional.of("ozone.test.config.threshold"),
            Optional.of("10"))
    );
  }

  @ParameterizedTest
  @MethodSource("data")
  void testForGivenClasses(Class<?> testClass, String fieldName,
      Optional<ConfigType> expectedType,
      Optional<String> expectedKey,
      Optional<String> expectedDefault) {
    final Optional<Config> config = getConfigAnnotationByName(testClass, fieldName);
    final Optional<ConfigType> type = config.map(Config::type);
    assertEquals(expectedType, type);

    final Optional<String> key = config.map(Config::key);
    assertEquals(expectedKey, key);

    final Optional<String> defaultValue = config.map(Config::defaultValue);
    assertEquals(expectedDefault, defaultValue);
  }

  static Optional<Config> getConfigAnnotationByName(Class<?> configClass, String fieldName) {
    for (Class<?> clazz = configClass; clazz != null;) {
      Optional<Config> config = Stream.of(clazz.getDeclaredFields())
          .filter(f -> f.getName().equals(fieldName))
          .findFirst()
          .map(f -> f.getAnnotation(Config.class));

      if (config.isPresent()) {
        return config;
      }

      clazz = clazz.getSuperclass();
      if (Object.class.equals(clazz)) {
        clazz = null;
      }
    }
    return Optional.empty();
  }

  @Test
  void listReconfigurableProperties() {
    Set<String> props =
        ConfigurationReflectionUtil.mapReconfigurableProperties(
            ConfigurationExample.class).keySet();

    String prefix = "ozone.test.config";
    assertEquals(ImmutableSet.of(
        prefix + ".dynamic"
    ), props);
  }

  @Test
  void testDuplicatedConfig() {
    assertThrows(IllegalStateException.class, () -> assertNoDuplicatedKeys(DuplicatedConfig.class));
  }

  static class DuplicatedConfig {
    @Config(key = "key",
        defaultValue = "1",
        tags = ConfigTag.HDDS,
        description = "key1"
    )
    private String key1;

    @Config(key = "key",
        defaultValue = "2",
        tags = ConfigTag.HDDS,
        description = "key2"
    )
    private String key2;
  }
}
