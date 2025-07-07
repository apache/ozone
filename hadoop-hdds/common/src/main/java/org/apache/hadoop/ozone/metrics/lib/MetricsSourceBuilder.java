/*
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

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.apache.hadoop.ozone.metrics.MetricsCollector;
import org.apache.hadoop.ozone.metrics.MetricsException;
import org.apache.hadoop.ozone.metrics.MetricsInfo;
import org.apache.hadoop.ozone.metrics.MetricsSource;
import org.apache.hadoop.ozone.metrics.annotation.Metric;
import org.apache.hadoop.ozone.metrics.annotation.Metrics;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class to build {@link MetricsSource} object from annotations.
 * <p>
 * For a given source object:
 * <ul>
 * <li>Sets the {@link Field}s annotated with {@link Metric} to
 * {@link MutableMetric} and adds it to the {@link MetricsRegistry}.</li>
 * <li>
 * For {@link Method}s annotated with {@link Metric} creates
 * {@link MutableMetric} and adds it to the {@link MetricsRegistry}.</li>
 * </ul>
 */
public class MetricsSourceBuilder {
  private static final Logger LOG =
      LoggerFactory.getLogger(MetricsSourceBuilder.class);

  private final Object source;
  private final MutableMetricsFactory factory;
  private final MetricsRegistry registry;
  private MetricsInfo info;
  private boolean hasAtMetric = false;
  private boolean hasRegistry = false;

  MetricsSourceBuilder(Object source, MutableMetricsFactory factory) {
    this.source = checkNotNull(source, "source");
    this.factory = checkNotNull(factory, "mutable metrics factory");
    Class<?> cls = source.getClass();
    registry = initRegistry(source);

    for (Field field : ReflectionUtils.getDeclaredFieldsIncludingInherited(cls)) {
      add(source, field);
    }
    for (Method method : ReflectionUtils.getDeclaredMethodsIncludingInherited(cls)) {
      add(source, method);
    }
  }

  public MetricsSource build() {
    if (source instanceof MetricsSource) {
      if (hasAtMetric && !hasRegistry) {
        throw new MetricsException("Hybrid metrics: registry required.");
      }
      return (MetricsSource) source;
    }
    else if (!hasAtMetric) {
      throw new MetricsException("No valid @Metric annotation found.");
    }
    return new MetricsSource() {
      @Override
      public void getMetrics(MetricsCollector builder, boolean all) {
        registry.snapshot(builder.addRecord(registry.info()), all);
      }
    };
  }

  public MetricsInfo info() {
    return info;
  }

  private MetricsRegistry initRegistry(Object source) {
    Class<?> cls = source.getClass();
    MetricsRegistry r = null;
    // Get the registry if it already exists.
    for (Field field : ReflectionUtils.getDeclaredFieldsIncludingInherited(cls)) {
      if (field.getType() != MetricsRegistry.class) continue;
      try {
        field.setAccessible(true);
        r = (MetricsRegistry) field.get(source);
        hasRegistry = r != null;
        break;
      } catch (Exception e) {
        LOG.warn("Error accessing field "+ field, e);
        continue;
      }
    }
    // Create a new registry according to annotation
    for (Annotation annotation : cls.getAnnotations()) {
      if (annotation instanceof Metrics) {
        Metrics ma = (Metrics) annotation;
        info = factory.getInfo(cls, ma);
        if (r == null) {
          r = new MetricsRegistry(info);
        }
        r.setContext(ma.context());
      }
    }
    if (r == null) return new MetricsRegistry(cls.getSimpleName());
    return r;
  }

  /**
   * Change the declared field {@code field} in {@code source} Object to
   * {@link MutableMetric}
   */
  private void add(Object source, Field field) {
    for (Annotation annotation : field.getAnnotations()) {
      if (!(annotation instanceof Metric)) {
        continue;
      }
      try {
        // skip fields already set
        field.setAccessible(true);
        if (field.get(source) != null) continue;
      } catch (Exception e) {
        LOG.warn("Error accessing field "+ field +" annotated with"+
                 annotation, e);
        continue;
      }
      MutableMetric mutable = factory.newForField(field, (Metric) annotation, registry);
      if (mutable != null) {
        try {
          field.set(source, mutable); // Set the source field to MutableMetric
          hasAtMetric = true;
        } catch (Exception e) {
          throw new MetricsException("Error setting field "+ field +
                                     " annotated with "+ annotation, e);
        }
      }
    }
  }

  /** Add {@link MutableMetric} for a method annotated with {@link Metric} */
  private void add(Object source, Method method) {
    for (Annotation annotation : method.getAnnotations()) {
      if (!(annotation instanceof Metric)) {
        continue;
      }
      factory.newForMethod(source, method, (Metric) annotation, registry);
      hasAtMetric = true;
    }
  }
}
