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

package org.apache.hadoop.hdds.scm.security;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * Metrics related to Root CA rotation in SCM.
 */
@Metrics(name = "Root CA Rotation Metrics", about = "Metrics related to " +
    "Root CA rotation in SCM", context = "SCM")
public final class RootCARotationMetrics {
  public static final String NAME =
      RootCARotationMetrics.class.getSimpleName();

  private final MetricsSystem ms;

  @Metric(about = "Number of total tries, both successes and failures.")
  private MutableCounterLong numTotalRotation;

  @Metric(about = "Number of successful rotations")
  private MutableCounterLong numSuccessRotation;

  @Metric(about = "Time(nano second) spent on last successful rotation")
  private MutableGaugeLong successTimeInNs;

  /**
   * Create and register metrics named {@link RootCARotationMetrics#NAME}
   * for {@link RootCARotationManager}.
   *
   * @return {@link RootCARotationMetrics}
   */
  public static RootCARotationMetrics create() {
    MetricsSystem metricsSystem = DefaultMetricsSystem.instance();
    return metricsSystem.register(NAME, "Root CA Rotation Metrics",
        new RootCARotationMetrics(metricsSystem));
  }

  public void unRegister() {
    MetricsSystem metricsSystem = DefaultMetricsSystem.instance();
    metricsSystem.unregisterSource(NAME);
  }

  private RootCARotationMetrics(MetricsSystem ms) {
    this.ms = ms;
  }

  MutableGaugeLong getSuccessTimeInNs() {
    return successTimeInNs;
  }

  public void setSuccessTimeInNs(long time) {
    this.successTimeInNs.set(time);
  }

  public void incrSuccessRotationNum() {
    this.numSuccessRotation.incr();
  }

  public void incrTotalRotationNum() {
    this.numTotalRotation.incr();
  }

  public long getSuccessRotationNum() {
    return this.numSuccessRotation.value();
  }

  public long getTotalRotationNum() {
    return this.numTotalRotation.value();
  }
}
