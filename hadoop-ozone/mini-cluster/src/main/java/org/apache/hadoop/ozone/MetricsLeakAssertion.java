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

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

/**
 * Asserts that no metrics sources remain registered in the
 * {@link DefaultMetricsSystem} after a mini cluster is shut down.
 *
 * <p>Hadoop's {@code MetricsSystemImpl} does not remove registered sources on
 * {@code stop()} or {@code shutdown()}; a metrics class that forgets to call
 * {@code unregisterSource(...)} leaks its registration silently.  This helper
 * inspects the private {@code allSources} map via reflection and fails the
 * test if anything is still registered.
 */
public final class MetricsLeakAssertion {

  private static final String ALL_SOURCES_FIELD = "allSources";

  private MetricsLeakAssertion() {
  }

  /**
   * Throws an {@link AssertionError} if any metrics sources are still
   * registered with the default metrics system, or if the expected
   * {@code allSources} field cannot be found or read (e.g. a Hadoop version
   * change restructured {@code MetricsSystemImpl}), so that a broken or
   * missing check fails loudly instead of going unnoticed.
   */
  public static void assertNoLeaks() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    Field field = findAllSourcesField(ms.getClass());
    if (field == null) {
      throw new AssertionError("Cannot check for metrics leaks: '" + ALL_SOURCES_FIELD +
          "' field not found on " + ms.getClass().getName() +
          ". The metrics system implementation may have changed.");
    }
    final Map<String, MetricsSource> allSources;
    try {
      field.setAccessible(true);
      Object value = field.get(ms);
      if (!(value instanceof Map)) {
        throw new AssertionError("Cannot check for metrics leaks: '" + ALL_SOURCES_FIELD +
            "' on " + ms.getClass().getName() + " is not a Map.");
      }
      @SuppressWarnings("unchecked")
      Map<String, MetricsSource> sources = (Map<String, MetricsSource>) value;
      allSources = sources;
    } catch (IllegalAccessException e) {
      throw new AssertionError("Cannot check for metrics leaks: unable to access '" +
          ALL_SOURCES_FIELD + "' on " + ms.getClass().getName() + ".", e);
    }
    if (!allSources.isEmpty()) {
      Set<String> leaked = new TreeSet<>(allSources.keySet());
      throw new AssertionError("Found " + leaked.size() +
          " metrics source(s) still registered after cluster shutdown: " + leaked);
    }
  }

  private static Field findAllSourcesField(Class<?> clazz) {
    Class<?> current = clazz;
    while (current != null) {
      try {
        return current.getDeclaredField(ALL_SOURCES_FIELD);
      } catch (NoSuchFieldException e) {
        current = current.getSuperclass();
      }
    }
    return null;
  }
}
