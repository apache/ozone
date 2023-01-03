/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.ha;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Class to maintain metrics and info related to OM HA.
 */
@Metrics(about = "OM HA Metrics", context = OzoneConsts.OZONE)
public class OMHAMetrics {

  public static final String SOURCE_NAME =
      OMHAMetrics.class.getSimpleName();
  private final MetricsRegistry metricsRegistry;

  public OMHAMetrics(String ratisRoles) {
    this.metricsRegistry = new MetricsRegistry(SOURCE_NAME)
        .tag("OMRoles", "OM roles", ratisRoles);
  }

  /**
   * Create and return OMHAMetrics instance.
   * @return OMHAMetrics
   */
  public static synchronized OMHAMetrics create(String ratisRoles) {
    OMHAMetrics metrics = new OMHAMetrics(ratisRoles);
    return DefaultMetricsSystem.instance()
        .register(SOURCE_NAME, "Metrics for OM HA", metrics);
  }

  /**
   * Unregister the metrics instance.
   */
  public static void unRegister() {
    DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
  }

  @Metric("Number of OM nodes")
  private MutableGaugeInt numOfOMNodes;

  public void setNumOfOMNodes(int count) {
    numOfOMNodes.set(count);
  }

  public MutableGaugeInt getNumOfOMNodes() {
    return numOfOMNodes;
  }

  public MetricsRegistry getMetricsRegistry() {
    return metricsRegistry;
  }
}
