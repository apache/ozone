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

package org.apache.hadoop.hdds.server.http;

import org.apache.hadoop.hdds.utils.HddsVersionInfo;
import org.apache.hadoop.hdds.utils.VersionInfo;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Exposes build version and git revision as a Prometheus info metric.
 *
 * <p>Follows the OpenMetrics Info pattern: the value is always 1, and the
 * identifying strings are carried as labels. The metric name uses the
 * conventional {@code _build_info} suffix.</p>
 *
 * <pre>
 * ozone_build_info{component="OM",version="2.0.0",revision="abc1234"} 1
 * </pre>
 */
@Metrics(about = "Ozone build version info", context = OzoneConsts.OZONE)
public final class BuildInfoMetrics implements MetricsSource {

  public static final String METRICS_SOURCE_NAME = "OzoneBuildInfo";
  /**
   * Record name chosen so that prometheusName("Ozone", "BuildInfo") produces
   * the conventional "ozone_build_info" metric name.
   */
  public static final String RECORD_NAME = "Ozone";

  private final String component;
  private final String version;
  private final String revision;

  private BuildInfoMetrics(String component, VersionInfo versionInfo) {
    this.component = component;
    this.version = versionInfo.getVersion();
    this.revision = versionInfo.getRevision();
  }

  /**
   * Register a build-info source for the given component, if not already
   * registered. Safe to call from each {@code BaseHttpServer} instance.
   */
  public static synchronized BuildInfoMetrics create(String component) {
    BuildInfoMetrics source =
        new BuildInfoMetrics(component, HddsVersionInfo.HDDS_VERSION_INFO);
    return DefaultMetricsSystem.instance().register(METRICS_SOURCE_NAME,
        "Ozone build version info", source);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(RECORD_NAME)
        .add(new MetricsTag(
            Interns.info("component", "Ozone component name"), component)).add(new MetricsTag(Interns.info("revision", "Source control revision"), revision))
        .add(new MetricsTag(Interns.info("version", "Ozone build version"), version))
        .addGauge(Interns.info("BuildInfo", "Always 1; identifying info is in labels"), 1L);
    builder.endRecord();
  }
}
