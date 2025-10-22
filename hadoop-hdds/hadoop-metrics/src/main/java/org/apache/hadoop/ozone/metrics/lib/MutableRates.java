/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.metrics.lib;

import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.ozone.metrics.MetricsRecordBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to manage a group of mutable rate metrics
 *
 * This class synchronizes all accesses to the metrics it
 * contains, so it should not be used in situations where
 * there is high contention on the metrics.
 * {@link MutableRatesWithAggregation} is preferable in that
 * situation.
 */
public class MutableRates extends MutableMetric {
  static final Logger LOG = LoggerFactory.getLogger(MutableRates.class);
  private final MetricsRegistry registry;
  private final Set<Class<?>> protocolCache = new HashSet<>();

  MutableRates(MetricsRegistry registry) {
    this.registry = checkNotNull(registry, "metrics registry");
  }

  /**
   * Initialize the registry with all the methods in a protocol
   * so they all show up in the first snapshot.
   * Convenient for JMX implementations.
   * @param protocol the protocol class
   */
  public void init(Class<?> protocol) {
    if (protocolCache.contains(protocol)) return;
    protocolCache.add(protocol);
    for (Method method : protocol.getDeclaredMethods()) {
      String name = method.getName();
      LOG.debug(name);
      try { registry.newRate(name, name, false, true); }
      catch (Exception e) {
        LOG.error("Error creating rate metrics for "+ method.getName(), e);
      }
    }
  }

  /**
   * Add a rate sample for a rate metric
   * @param name of the rate metric
   * @param elapsed time
   */
  public void add(String name, long elapsed) {
    registry.add(name, elapsed);
  }

  @Override
  public void snapshot(MetricsRecordBuilder rb, boolean all) {
    registry.snapshot(rb, all);
  }
}
