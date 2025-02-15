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

package org.apache.hadoop.ozone.util;

import static org.apache.hadoop.metrics2.lib.Interns.info;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableMetric;
import org.apache.hadoop.metrics2.util.SampleStat.MinMax;

/**
 * A mutable metric that tracks the minimum and maximum
 * values of a dataset over time.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableMinMax extends MutableMetric {
  private final MinMax intervalMinMax = new MinMax();
  private final MinMax prevMinMax = new MinMax();
  private final MetricsInfo iMinInfo;
  private final MetricsInfo iMaxInfo;

  /**
   * Construct a minMax metric.
   * @param registry    MetricsRegistry of the metric
   * @param name        of the metric
   * @param description of the metric
   * @param valueName   of the metric (e.g. "Time", "Latency")
   */
  public MutableMinMax(MetricsRegistry registry,
      String name, String description, String valueName) {
    String ucName = StringUtils.capitalize(name);
    String desc = StringUtils.uncapitalize(description);
    String uvName = StringUtils.capitalize(valueName);
    String lvName = StringUtils.uncapitalize(valueName);
    iMinInfo = info(ucName + "IMin" + uvName,
        "Min " + lvName + " for " + desc + "in the last reporting interval");
    iMaxInfo = info(ucName + "IMax" + uvName,
        "Max " + lvName + " for " + desc + "in the last reporting interval");
    // hadoop.metrics2 only supports standard types of Metrics registered
    // with annotations, but not custom types of metrics.
    // Registering here is for compatibility with metric classes
    // that are only registered with annotations and do not override getMetrics.
    registry.newGauge(iMinInfo, 0);
    registry.newGauge(iMaxInfo, 0);
  }

  /**
   * Add a snapshot to the metric.
   * @param value of the metric
   */
  public synchronized void add(long value) {
    intervalMinMax.add(value);
    setChanged();
  }

  private MinMax lastMinMax() {
    return changed() ? intervalMinMax : prevMinMax;
  }

  @Override
  public synchronized void snapshot(MetricsRecordBuilder builder, boolean all) {
    if (all || this.changed()) {
      builder.addGauge(iMinInfo, lastMinMax().min());
      builder.addGauge(iMaxInfo, lastMinMax().max());
      if (changed()) {
        prevMinMax.reset(intervalMinMax);
        intervalMinMax.reset();
        clearChanged();
      }
    }
  }
}
