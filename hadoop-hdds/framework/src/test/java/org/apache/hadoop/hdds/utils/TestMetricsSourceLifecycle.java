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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.cache.CacheBuilder;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ipc_.metrics.RpcDetailedMetrics;
import org.apache.hadoop.ipc_.metrics.RpcMetrics;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.grpc.metrics.GrpcMetrics;
import org.apache.hadoop.ozone.util.CacheMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests that closing a metrics owner leaves live peers registered. */
class TestMetricsSourceLifecycle {
  private MetricsSystem original;
  private MetricsSystemImpl metricsSystem;
  private boolean miniClusterMode;

  @BeforeEach
  void setUp() {
    original = DefaultMetricsSystem.instance();
    miniClusterMode = DefaultMetricsSystem.inMiniClusterMode();
    DefaultMetricsSystem.setMiniClusterMode(true);
    metricsSystem = new MetricsSystemImpl("test");
    DefaultMetricsSystem.setInstance(metricsSystem);
    metricsSystem.start();
  }

  @AfterEach
  void tearDown() throws Exception {
    try {
      for (String name : sourceNames()) {
        metricsSystem.unregisterSource(name);
      }
      metricsSystem.shutdown();
    } finally {
      DefaultMetricsSystem.setInstance(original);
      DefaultMetricsSystem.setMiniClusterMode(miniClusterMode);
    }
  }

  @Test
  void testRpcMetrics() throws Exception {
    Server server = mock(Server.class);
    when(server.getListenerAddress()).thenReturn(new InetSocketAddress(12345));
    when(server.getServerName()).thenReturn("test");
    assertIndependentSources(() -> RpcMetrics.create(server, new OzoneConfiguration()), RpcMetrics::shutdown);
  }

  @Test
  void testRpcDetailedMetrics() throws Exception {
    assertIndependentSources(() -> RpcDetailedMetrics.create(12345), RpcDetailedMetrics::shutdown);
  }

  @Test
  void testCacheMetrics() throws Exception {
    assertIndependentSources(() -> CacheMetrics.create(CacheBuilder.newBuilder().build(), "cache"),
        CacheMetrics::unregister);
  }

  @Test
  void testNettyMetrics() throws Exception {
    assertIndependentSources(NettyMetrics::create, NettyMetrics::unregister);
  }

  @Test
  void testGrpcMetrics() throws Exception {
    assertIndependentSources(() -> GrpcMetrics.create(new OzoneConfiguration()), GrpcMetrics::unRegister);
  }

  @Test
  void testProtocolMessageMetrics() throws Exception {
    assertIndependentSources(() -> {
      ProtocolMessageMetrics<Operation> metrics = ProtocolMessageMetrics.create("protocol", "test", Operation.class);
      metrics.register();
      return metrics;
    }, ProtocolMessageMetrics::unregister);
  }

  private <T> void assertIndependentSources(Supplier<T> create, Consumer<T> close) throws Exception {
    Set<String> before = sourceNames();
    T first = null;
    T second = null;
    try {
      first = create.get();
      Set<String> firstNames = sourceNames();
      firstNames.removeAll(before);
      second = create.get();
      Set<String> secondNames = sourceNames();
      secondNames.removeAll(before);
      secondNames.removeAll(firstNames);
      assertThat(firstNames).hasSize(1);
      assertThat(secondNames).hasSize(1);
      T closing = first;
      first = null;
      close.accept(closing);
      Set<String> remaining = sourceNames();
      remaining.removeAll(before);
      assertThat(remaining).containsExactlyInAnyOrderElementsOf(secondNames);
      for (String name : secondNames) {
        assertThat(metricsSystem.getSource(name)).isNotNull();
      }
    } finally {
      try {
        if (first != null) {
          close.accept(first);
        }
      } finally {
        if (second != null) {
          close.accept(second);
        }
      }
    }
    assertThat(sourceNames()).containsExactlyInAnyOrderElementsOf(before);
  }

  private Set<String> sourceNames() throws Exception {
    Field field = MetricsSystemImpl.class.getDeclaredField("allSources");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, MetricsSource> sources = (Map<String, MetricsSource>) field.get(metricsSystem);
    return new HashSet<>(sources.keySet());
  }

  private enum Operation {
    READ
  }
}
