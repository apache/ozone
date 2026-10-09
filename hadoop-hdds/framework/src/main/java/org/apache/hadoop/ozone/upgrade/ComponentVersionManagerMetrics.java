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

package org.apache.hadoop.ozone.upgrade;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.util.MetricUtil;

/**
 * Metrics for {@link ComponentVersionManager}.
 */
@Metrics(about = "Component Version Manager Metrics", context = OzoneConsts.OZONE)
public final class ComponentVersionManagerMetrics implements MetricsSource {

  public static final String METRICS_SOURCE_NAME =
      ComponentVersionManagerMetrics.class.getSimpleName();

  private static final MetricsInfo SOFTWARE_VERSION = Interns.info(
      "SoftwareVersion",
      "Software version in serialized int form.");
  private static final MetricsInfo APPARENT_VERSION = Interns.info(
      "ApparentVersion",
      "Current apparent version in serialized int form.");

  private final ComponentVersionManager versionManager;
  private final String sourceName;

  private ComponentVersionManagerMetrics(ComponentVersionManager versionManager, String sourceName) {
    this.versionManager = versionManager;
    this.sourceName = sourceName;
  }

  public static ComponentVersionManagerMetrics create(ComponentVersionManager versionManager) {
    return create(versionManager, null);
  }

  public static ComponentVersionManagerMetrics create(ComponentVersionManager versionManager, String component) {
    String sourceName = MetricUtil.qualifySourceName(METRICS_SOURCE_NAME, component);
    ComponentVersionManagerMetrics metrics = (ComponentVersionManagerMetrics) DefaultMetricsSystem.instance()
            .getSource(sourceName);
    if (metrics == null) {
      return DefaultMetricsSystem.instance().register(
          sourceName,
          "Metrics for component version management.",
          new ComponentVersionManagerMetrics(versionManager, sourceName));
    }
    return metrics;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(sourceName);
    builder
        .addGauge(SOFTWARE_VERSION,
            versionManager.getSoftwareVersion().serialize())
        .addGauge(APPARENT_VERSION,
            versionManager.getApparentVersion().serialize());
  }

  public void unRegister() {
    DefaultMetricsSystem.instance().unregisterSource(sourceName);
  }
}
