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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OmSnapshotInternalMetrics}.
 */
class TestOmSnapshotInternalMetrics {

  @Test
  void testUnregisterWithMultipleOmNodeIds() throws Exception {
    MetricsSystem metricsSystem = DefaultMetricsSystem.initialize("TestOmSnapshotInternalMetrics");
    Field field = metricsSystem.getClass().getDeclaredField("allSources");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, MetricsSource> sources = (Map<String, MetricsSource>) field.get(metricsSystem);
    Set<String> initialSources = new HashSet<>(sources.keySet());
    boolean miniClusterMode = DefaultMetricsSystem.inMiniClusterMode();
    DefaultMetricsSystem.setMiniClusterMode(true);
    try {
      OmSnapshotInternalMetrics first = OmSnapshotInternalMetrics.create(
          "service1", "om1");
      Set<String> firstSources = new HashSet<>(sources.keySet());
      assertEquals(initialSources.size() + 1, firstSources.size());
      OmSnapshotInternalMetrics second = OmSnapshotInternalMetrics.create(
          "service1", "om2");
      assertNotSame(first, second);
      assertEquals(initialSources.size() + 2, sources.size());

      second.unregister();
      assertEquals(firstSources, sources.keySet());
      first.unregister();
      assertEquals(initialSources, sources.keySet());
    } finally {
      Set<String> remainingSources = new HashSet<>(sources.keySet());
      remainingSources.removeAll(initialSources);
      remainingSources.forEach(metricsSystem::unregisterSource);
      DefaultMetricsSystem.shutdown();
      DefaultMetricsSystem.setMiniClusterMode(miniClusterMode);
    }
  }
}
