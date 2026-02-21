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

package org.apache.hadoop.ozone.recon.metrics;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Runtime metrics for ContainerHealthTaskV2 execution.
 */
@InterfaceAudience.Private
@Metrics(about = "ContainerHealthTaskV2 Metrics", context = OzoneConsts.OZONE)
public final class ContainerHealthTaskV2Metrics {

  private static final String SOURCE_NAME =
      ContainerHealthTaskV2Metrics.class.getSimpleName();

  @Metric(about = "ContainerHealthTaskV2 runtime in milliseconds")
  private MutableRate runTimeMs;

  @Metric(about = "ContainerHealthTaskV2 successful runs")
  private MutableCounterLong runSuccessCount;

  @Metric(about = "ContainerHealthTaskV2 failed runs")
  private MutableCounterLong runFailureCount;

  private ContainerHealthTaskV2Metrics() {
  }

  public static ContainerHealthTaskV2Metrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(
        SOURCE_NAME,
        "ContainerHealthTaskV2 Metrics",
        new ContainerHealthTaskV2Metrics());
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  public void addRunTime(long runtimeMs) {
    runTimeMs.add(runtimeMs);
  }

  public void incrSuccess() {
    runSuccessCount.incr();
  }

  public void incrFailure() {
    runFailureCount.incr();
  }
}
