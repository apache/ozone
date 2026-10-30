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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.ozone.test.MetricsAsserts.assertCounter;
import static org.apache.ozone.test.MetricsAsserts.assertGauge;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.om.ha.OMHAMetrics;
import org.apache.hadoop.ozone.om.lock.OMLockMetrics;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerDoubleBufferMetrics;
import org.apache.hadoop.ozone.om.service.KeyLifecycleServiceMetrics;
import org.apache.hadoop.ozone.om.snapshot.OMSnapshotDirectoryMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests that stopping an OM releases only its own metrics in a mini cluster. */
class TestOMMetricsLifecycle {

  private MetricsSystem originalMetricsSystem;
  private MetricsSystemImpl metricsSystem;
  private boolean originalMiniClusterMode;

  @BeforeEach
  void setUp() {
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
        Arguments.of("OMMetrics", (Supplier<Runnable>) () -> {
          OMMetrics metrics = OMMetrics.create(new OzoneConfiguration());
          return metrics::unRegister;
        }),
        Arguments.of("OmSnapshotMetrics", (Supplier<Runnable>) () -> {
          OmSnapshotMetrics metrics = OmSnapshotMetrics.create();
          return metrics::unregister;
        }),
        Arguments.of("OMPerformanceMetrics", (Supplier<Runnable>) () -> {
          OMPerformanceMetrics metrics = OMPerformanceMetrics.register();
          return metrics::unregister;
        }),
        Arguments.of("OMLockMetrics", (Supplier<Runnable>) () -> {
          OMLockMetrics metrics = OMLockMetrics.create();
          return metrics::unRegister;
        }),
        Arguments.of("OMHAMetrics", (Supplier<Runnable>) () -> {
          OMHAMetrics metrics = OMHAMetrics.create("om", "leader");
          return metrics::unRegister;
        }),
        Arguments.of("OzoneManagerDoubleBufferMetrics", (Supplier<Runnable>) () -> {
          OzoneManagerDoubleBufferMetrics metrics = OzoneManagerDoubleBufferMetrics.create();
          return metrics::unRegister;
        }),
        Arguments.of("OMSnapshotDirectoryMetrics", (Supplier<Runnable>) () -> {
          OMSnapshotDirectoryMetrics metrics = OMSnapshotDirectoryMetrics.create(new OzoneConfiguration(),
              "OM Metrics", mock(OMMetadataManager.class));
          return metrics::unRegister;
        }),
        Arguments.of("DeletingServiceMetrics", (Supplier<Runnable>) () -> {
          DeletingServiceMetrics metrics = DeletingServiceMetrics.create();
          return metrics::unregister;
        }),
        Arguments.of("KeyLifecycleServiceMetrics", (Supplier<Runnable>) () -> {
          KeyLifecycleServiceMetrics metrics = KeyLifecycleServiceMetrics.create();
          return metrics::unregister;
        }),
        Arguments.of("BucketUtilizationMetrics", (Supplier<Runnable>) () -> {
          BucketUtilizationMetrics metrics = BucketUtilizationMetrics.create(mock(OMMetadataManager.class));
          return metrics::unRegister;
        }));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("metrics")
  void testIndependentShutdown(String name, Supplier<Runnable> factory) throws Exception {
    boolean miniClusterMode = DefaultMetricsSystem.inMiniClusterMode();
    DefaultMetricsSystem.setMiniClusterMode(true);
    Set<String> before = sources();
    Runnable closeFirst = factory.get();
    Set<String> first = sources();
    first.removeAll(before);
    Runnable closeSecond = factory.get();
    Set<String> second = sources();
    second.removeAll(before);
    second.removeAll(first);
    try {
      assertFalse(first.isEmpty(), name);
      assertEquals(first.size(), second.size(), name);
      closeFirst.run();
      closeFirst.run();
      Set<String> remaining = sources();
      assertTrue(remaining.containsAll(second), "Stopping one OM must preserve another OM's metrics: " + name);
      remaining.retainAll(first);
      assertTrue(remaining.isEmpty(), "Stopped OM metrics remain registered: " + remaining);
      closeSecond.run();
      assertEquals(before, sources());
    } finally {
      closeFirst.run();
      closeSecond.run();
      Set<String> added = sources();
      added.removeAll(before);
      added.forEach(DefaultMetricsSystem.instance()::unregisterSource);
      DefaultMetricsSystem.setMiniClusterMode(miniClusterMode);
    }
  }

  @Test
  void testDirectConstructorCleanupPreservesReplacement() throws Exception {
    DefaultMetricsSystem.setMiniClusterMode(false);
    Set<String> before = sources();
    OMMetrics direct = new OMMetrics(0);
    OMMetrics replacement = null;
    try {
      assertEquals(before.size() + 2, sources().size());
      direct.unRegister();
      assertEquals(before, sources());
      replacement = OMMetrics.create(new OzoneConfiguration());
      Set<String> replacementSources = sources();
      direct.unRegister();
      assertEquals(replacementSources, sources(), "Direct constructor cleanup must preserve replacement metrics");
    } finally {
      direct.unRegister();
      if (replacement != null) {
        replacement.unRegister();
      }
    }
    assertEquals(before, sources());
  }

  @Test
  void testMetricsSurviveRestart() throws Exception {
    boolean miniClusterMode = DefaultMetricsSystem.inMiniClusterMode();
    DefaultMetricsSystem.setMiniClusterMode(true);
    Set<String> before = sources();
    OMMetrics metrics = OMMetrics.create(new OzoneConfiguration());
    Set<String> registered = sources();
    try {
      metrics.setNumKeys(7);
      metrics.getSnapshotMetrics().incNumKeyLookups();
      metrics.unRegister();
      assertEquals(before, sources());
      metrics.register();
      metrics.register();
      assertEquals(registered, sources());
      assertGauge("NumKeys", 7L,
          getMetrics(metrics));
      assertCounter("NumKeyLookup", 1L,
          getMetrics(metrics.getSnapshotMetrics()));
    } finally {
      metrics.unRegister();
      DefaultMetricsSystem.setMiniClusterMode(miniClusterMode);
    }
  }

  @ParameterizedTest(name = "{0} replacement")
  @MethodSource("metrics")
  void testRepeatedShutdownPreservesReplacement(String name, Supplier<Runnable> factory) throws Exception {
    boolean miniClusterMode = DefaultMetricsSystem.inMiniClusterMode();
    DefaultMetricsSystem.setMiniClusterMode(false);
    Set<String> before = sources();
    Runnable closeFirst = factory.get();
    closeFirst.run();
    Runnable closeReplacement = factory.get();
    Set<String> registered = sources();
    try {
      closeFirst.run();
      assertEquals(registered, sources(), "Repeated shutdown removed replacement metrics: " + name);
    } finally {
      closeReplacement.run();
      DefaultMetricsSystem.setMiniClusterMode(miniClusterMode);
    }
    assertEquals(before, sources());
  }

  @Test
  void testFailedMetadataManagerConstructionReleasesMetrics(@TempDir Path directory) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, directory.toString());
    Files.createFile(directory.resolve(OM_DB_NAME));
    assertThrows(IOException.class, () -> new OmMetadataManagerImpl(conf, null));
    assertTrue(sources().stream().noneMatch(name -> name.startsWith("OMLockMetrics")));
  }

  private static Set<String> sources() throws Exception {
    Field field = DefaultMetricsSystem.instance().getClass().getDeclaredField("allSources");
    field.setAccessible(true);
    return new HashSet<>(((Map<String, ?>) field.get(DefaultMetricsSystem.instance())).keySet());
  }
}
