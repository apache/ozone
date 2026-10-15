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

package org.apache.hadoop.ozone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

/**
 * Tests detection of leaked Hadoop metrics sources.
 */
class TestMetricsLeakAssertion {

  private MetricsSystem originalMetricsSystem;
  private MetricsSystem metricsSystem;
  private final List<String> registeredSources = new ArrayList<>();

  @BeforeEach
  void setUp() {
    originalMetricsSystem = DefaultMetricsSystem.instance();
    // A subclass also verifies that the assertion finds inherited fields.
    metricsSystem = new MetricsSystemImpl() { };
    DefaultMetricsSystem.setInstance(metricsSystem);
  }

  @AfterEach
  void tearDown() {
    try {
      registeredSources.forEach(metricsSystem::unregisterSource);
      metricsSystem.shutdown();
    } finally {
      DefaultMetricsSystem.setInstance(originalMetricsSystem);
    }
  }

  @Test
  void testUnexpectedSourceFails() {
    registerSource("LeakedMetricsSource");

    AssertionError error = assertThrows(AssertionError.class, MetricsLeakAssertion::assertNoLeaks);
    assertThat(error).hasMessageContaining("LeakedMetricsSource");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ContainerCacheMetrics", "ContainerCacheMetrics-1", "ContainerCacheMetrics-123",
      "JvmMetricsOM", "RpcActivityForPort15000", "VolumeIOStats-/data/volume", "keyTableCache", "keyTableCache-1"})
  void testAllowedSourcePasses(String sourceName) {
    registerSource(sourceName);

    assertDoesNotThrow(MetricsLeakAssertion::assertNoLeaks);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ContainerCacheMetrics-unexpected", "ContainerCacheMetrics-", "keyTableCache-unexpected"})
  void testUnexpectedSuffixFails(String sourceName) {
    registerSource(sourceName);

    AssertionError error = assertThrows(AssertionError.class, MetricsLeakAssertion::assertNoLeaks);
    assertThat(error).hasMessageContaining(sourceName);
  }

  @Test
  void testMissingSourcesFieldFails() {
    DefaultMetricsSystem.setInstance(mock(MetricsSystem.class));

    AssertionError error = assertThrows(AssertionError.class, MetricsLeakAssertion::assertNoLeaks);
    assertThat(error).hasMessageContaining("field not found");
  }

  @Test
  void testInvalidSourcesFieldFails() {
    DefaultMetricsSystem.setInstance(new MetricsSystemImpl() {
      @SuppressWarnings("PMD.UnusedPrivateField") // Accessed reflectively by MetricsLeakAssertion.
      private final String allSources = "not a map";
    });

    AssertionError error = assertThrows(AssertionError.class, MetricsLeakAssertion::assertNoLeaks);
    assertThat(error).hasMessageContaining("is not a Map");
  }

  @Test
  void testMetricsSystemAccessFailureFails() {
    SecurityException failure = new SecurityException("metrics access denied");
    try (MockedStatic<DefaultMetricsSystem> defaultMetricsSystem = mockStatic(DefaultMetricsSystem.class)) {
      defaultMetricsSystem.when(DefaultMetricsSystem::instance).thenThrow(failure);

      AssertionError error = assertThrows(AssertionError.class, MetricsLeakAssertion::assertNoLeaks);
      assertSame(failure, error.getCause());
    }
  }

  private void registerSource(String name) {
    metricsSystem.register(name, name, (MetricsSource) (collector, all) -> { });
    registeredSources.add(name);
  }
}
