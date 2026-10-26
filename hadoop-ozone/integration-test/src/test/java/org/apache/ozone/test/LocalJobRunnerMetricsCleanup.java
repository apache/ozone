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

package org.apache.ozone.test;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

/**
 * Cleans up metrics registered by blocking local DistCp jobs in a mini cluster test.
 * Hadoop also creates local runners for temporary staging clusters, so the
 * Job handle alone does not expose every source that the operation creates.
 */
public final class LocalJobRunnerMetricsCleanup implements AutoCloseable {
  private final MetricsSystem metricsSystem;
  private final Set<String> existingSources;
  private boolean closed;

  public LocalJobRunnerMetricsCleanup() throws ReflectiveOperationException {
    if (!DefaultMetricsSystem.inMiniClusterMode()) {
      throw new IllegalStateException("Local JobRunner metrics cleanup requires mini cluster mode");
    }
    metricsSystem = DefaultMetricsSystem.instance();
    existingSources = getSources();
  }

  @Override
  public void close() throws ReflectiveOperationException {
    if (closed) {
      return;
    }
    closed = true;
    Set<String> sources = getSources();
    sources.removeAll(existingSources);
    for (String name : sources) {
      metricsSystem.unregisterSource(name);
      // LocalJobRunnerMetrics.create() initializes the shared metrics system once per runner.
      metricsSystem.shutdown();
    }
  }

  private Set<String> getSources() throws ReflectiveOperationException {
    synchronized (metricsSystem) {
      Field field = MetricsSystemImpl.class.getDeclaredField("allSources");
      field.setAccessible(true);
      Map<?, ?> sources = (Map<?, ?>) field.get(metricsSystem);
      Set<String> names = new HashSet<>();
      for (Object name : sources.keySet()) {
        if (name instanceof String && ((String) name).startsWith("LocalJobRunnerMetrics-")) {
          names.add((String) name);
        }
      }
      return names;
    }
  }
}
