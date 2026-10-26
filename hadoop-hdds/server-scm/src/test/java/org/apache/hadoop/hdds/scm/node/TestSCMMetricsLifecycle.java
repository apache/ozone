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

package org.apache.hadoop.hdds.scm.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerMetrics;
import org.apache.hadoop.hdds.scm.container.metrics.SCMContainerManagerMetrics;
import org.apache.hadoop.hdds.scm.safemode.SafeModeMetrics;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests independent registration and cleanup of SCM metrics in HA mini clusters. */
class TestSCMMetricsLifecycle {

  static Stream<Arguments> metrics() {
    return Stream.of(
        metric(() -> SCMNodeMetrics.create(mock(NodeManagerMXBean.class)), SCMNodeMetrics::unRegister),
        metric(SCMContainerManagerMetrics::create, SCMContainerManagerMetrics::unRegister),
        metric(SafeModeMetrics::create, SafeModeMetrics::unRegister),
        metric(NodeDecommissionMetrics::create, NodeDecommissionMetrics::unRegister),
        metric(ContainerBalancerMetrics::create, ContainerBalancerMetrics::unregister));
  }

  private static <T> Arguments metric(Supplier<T> create, Consumer<T> unregister) {
    return Arguments.of(create, unregister);
  }

  @ParameterizedTest
  @MethodSource("metrics")
  <T> void unregisterOnlyOwnedSource(Supplier<T> create, Consumer<T> unregister) throws Exception {
    boolean miniClusterMode = DefaultMetricsSystem.inMiniClusterMode();
    DefaultMetricsSystem.setMiniClusterMode(true);
    MetricsSystem ms = DefaultMetricsSystem.initialize(getClass().getSimpleName());
    Set<String> original = sources(ms);
    T first = create.get();
    Set<String> firstSources = sources(ms);
    T second = create.get();
    try {
      assertEquals(original.size() + 2, sources(ms).size());
      Set<String> expected = sources(ms);
      expected.removeAll(firstSources);
      expected.addAll(original);
      unregister.accept(first);
      Set<String> surviving = sources(ms);
      assertEquals(expected, surviving);
      unregister.accept(first);
      assertEquals(surviving, sources(ms), "Repeated cleanup must retain the other SCM's metrics");
      unregister.accept(second);
      assertEquals(original, sources(ms));
    } finally {
      Set<String> remaining = sources(ms);
      remaining.removeAll(original);
      remaining.forEach(ms::unregisterSource);
      DefaultMetricsSystem.shutdown();
      DefaultMetricsSystem.setMiniClusterMode(miniClusterMode);
    }
  }

  @ParameterizedTest
  @MethodSource("metrics")
  <T> void repeatedCleanupRetainsReplacementSource(Supplier<T> create, Consumer<T> unregister) throws Exception {
    boolean miniClusterMode = DefaultMetricsSystem.inMiniClusterMode();
    DefaultMetricsSystem.setMiniClusterMode(false);
    MetricsSystem ms = DefaultMetricsSystem.initialize(getClass().getSimpleName());
    Set<String> original = sources(ms);
    T first = create.get();
    try {
      unregister.accept(first);
      T second = create.get();
      try {
        Set<String> surviving = sources(ms);
        assertEquals(original.size() + 1, surviving.size());
        unregister.accept(first);
        assertEquals(surviving, sources(ms), "Repeated cleanup must retain the replacement SCM's metrics");
      } finally {
        unregister.accept(second);
      }
      assertEquals(original, sources(ms));
    } finally {
      Set<String> remaining = sources(ms);
      remaining.removeAll(original);
      remaining.forEach(ms::unregisterSource);
      DefaultMetricsSystem.shutdown();
      DefaultMetricsSystem.setMiniClusterMode(miniClusterMode);
    }
  }

  @Test
  void unregisterDoesNotAcquireMetricsSamplingMonitor() throws Exception {
    NodeDecommissionMetrics metrics = NodeDecommissionMetrics.create();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      // Sampling holds the metrics-system monitor before calling synchronized getMetrics().
      // Cleanup must not acquire this source's monitor before the metrics-system monitor.
      synchronized (metrics) {
        executor.submit(metrics::unRegister).get(5, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
      metrics.unRegister();
    }
  }

  @SuppressWarnings("unchecked")
  private static Set<String> sources(MetricsSystem ms) throws Exception {
    Field field = ms.getClass().getDeclaredField("allSources");
    field.setAccessible(true);
    return new HashSet<>(((Map<String, ?>) field.get(ms)).keySet());
  }
}
