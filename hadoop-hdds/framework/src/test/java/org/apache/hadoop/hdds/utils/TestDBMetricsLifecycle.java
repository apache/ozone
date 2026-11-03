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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.cache.CacheStats;
import org.apache.hadoop.hdds.utils.db.cache.TableCache;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests that closing one database preserves another database's metrics. */
class TestDBMetricsLifecycle {

  private MetricsSystem originalMetricsSystem;
  private MetricsSystemImpl metricsSystem;
  private boolean originalMiniClusterMode;

  @BeforeEach
  void setUp() throws Exception {
    // Initialize the JVM-wide RocksDB metrics before installing the test metrics system.
    Class.forName(RocksDatabase.class.getName());
    originalMetricsSystem = DefaultMetricsSystem.instance();
    originalMiniClusterMode = DefaultMetricsSystem.inMiniClusterMode();
    DefaultMetricsSystem.setMiniClusterMode(true);
    metricsSystem = new MetricsSystemImpl("test");
    DefaultMetricsSystem.setInstance(metricsSystem);
    metricsSystem.start();
  }

  @AfterEach
  void tearDown() throws Exception {
    try {
      sources().forEach(metricsSystem::unregisterSource);
      metricsSystem.shutdown();
    } finally {
      DefaultMetricsSystem.setInstance(originalMetricsSystem);
      DefaultMetricsSystem.setMiniClusterMode(originalMiniClusterMode);
    }
  }

  static Stream<Arguments> metrics() {
    return Stream.of(
        Arguments.of("DBCheckpointMetrics", (Supplier<Runnable>) () -> {
          DBCheckpointMetrics metrics = DBCheckpointMetrics.create("OM Metrics");
          return metrics::unRegister;
        }),
        Arguments.of("TableCacheMetrics", (Supplier<Runnable>) () -> {
          TableCache<?, ?> cache = mock(TableCache.class);
          when(cache.getStats()).thenReturn(new CacheStats(0, 0, 0));
          TableCacheMetrics metrics = TableCacheMetrics.create(cache, "keyTable");
          return metrics::unregister;
        }),
        Arguments.of("RocksDBStoreMetrics", (Supplier<Runnable>) () -> {
          RocksDBStoreMetrics metrics = RocksDBStoreMetrics.create(null, mock(RocksDatabase.class), "om.db");
          return metrics::unregister;
        }));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("metrics")
  void testIndependentShutdown(String name, Supplier<Runnable> factory) throws Exception {
    Set<String> before = sources();
    Runnable closeFirst = null;
    Runnable closeSecond = null;
    try {
      closeFirst = factory.get();
      Set<String> first = sources();
      first.removeAll(before);
      closeSecond = factory.get();
      Set<String> second = sources();
      second.removeAll(before);
      second.removeAll(first);
      assertFalse(first.isEmpty(), name);
      assertEquals(first.size(), second.size(), name);
      Runnable closing = closeFirst;
      closeFirst = null;
      closing.run();
      Set<String> remaining = sources();
      assertTrue(remaining.containsAll(second),
          "Closing one database must preserve another database's metrics: " + name);
      remaining.retainAll(first);
      assertTrue(remaining.isEmpty(), "Closed database metrics remain registered: " + remaining);
    } finally {
      try {
        if (closeFirst != null) {
          closeFirst.run();
        }
      } finally {
        if (closeSecond != null) {
          closeSecond.run();
        }
      }
    }
    assertEquals(before, sources());
  }

  @Test
  void testCheckpointMetricsRegistrationAfterShutdown() throws Exception {
    Set<String> before = sources();
    DBCheckpointMetrics metrics = DBCheckpointMetrics.create("OM Metrics");
    Runnable cleanup = metrics::unRegister;
    try {
      Set<String> registered = sources();
      assertEquals(before.size() + 1, registered.size());
      metrics.incNumCheckpoints();
      Runnable closing = cleanup;
      cleanup = null;
      closing.run();
      assertEquals(before, sources());
      metrics.register("OM Metrics");
      cleanup = metrics::unRegister;
      assertEquals(registered, sources());
      metrics.incNumCheckpoints();
      assertEquals(2, metrics.getNumCheckpoints());
    } finally {
      if (cleanup != null) {
        cleanup.run();
      }
    }
    assertEquals(before, sources());
  }

  private static Set<String> sources() throws Exception {
    Field field = DefaultMetricsSystem.instance().getClass().getDeclaredField("allSources");
    field.setAccessible(true);
    return new HashSet<>(((Map<String, ?>) field.get(DefaultMetricsSystem.instance())).keySet());
  }
}
