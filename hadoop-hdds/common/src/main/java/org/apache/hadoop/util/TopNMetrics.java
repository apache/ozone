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
package org.apache.hadoop.util;

import com.google.common.collect.Ordering;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableMetric;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A mutable metric that tracks the Top values of a dataset over time.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class TopNMetrics extends MutableMetric {
  private final Map<String, Long> currentValues = new ConcurrentHashMap<>();
  private Map<String, Long> previousValues = new ConcurrentHashMap<>();
  private final int topCount;
  private MetricsRegistry registry;
  private final MetricsInfo metricsInfo;
  private final MetricsInfo topTag;
  private final MetricsInfo nameTag;

  public static TopNMetrics create(MetricsRegistry registry, String name, String description,
      int topCount) {
    return new TopNMetrics(registry, name, description, topCount);
  }


  private TopNMetrics(MetricsRegistry registry, String name, String description,
      int topCount) {
    this.registry = registry;
    this.metricsInfo = Interns.info(name + "TopN", description);
    this.topTag = Interns.info("top", "Top " + topCount + " values");
    this.nameTag = Interns.info("name", "Name of the value");
    registry.newGauge(metricsInfo, 0L);
    this.topCount = topCount;
  }


  public synchronized void add(String name, long value) {
    currentValues.compute(name, (k, v) -> (v == null) ? value : v + value);
    setChanged();
  }

  private  Map<String, Long> last() {
    return changed() ? currentValues : previousValues;
  }

  @Override
  public void snapshot(MetricsRecordBuilder builder, boolean all) {
    MetricsCollector collector = builder.parent();
    if (all || changed()) {
      List<Map.Entry<String, Long>> greatest = Ordering.from(
              Comparator.comparingLong((Map.Entry<String, Long> o) -> o.getValue()))
          .greatestOf(last().entrySet(), topCount);

      int order = 1;
      for (Map.Entry<String, Long> entry : greatest) {
        MetricsRecordBuilder opBuilder = collector.addRecord(registry.info());
        opBuilder.tag(topTag, String.valueOf(order));
        opBuilder.tag(nameTag, entry.getKey());
        opBuilder.addGauge(metricsInfo, entry.getValue());
        opBuilder.endRecord();
        order++;
      }

      if (changed()) {
        previousValues = new HashMap<>(currentValues);
        currentValues.clear();
        clearChanged();
      }
    }
  }

}
