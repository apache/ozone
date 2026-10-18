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
 * Runtime metrics for ContainerHealthTask execution.
 */
@InterfaceAudience.Private
@Metrics(about = "ContainerHealthTask Metrics", context = OzoneConsts.OZONE)
public final class ContainerHealthTaskMetrics {

  private static final String SOURCE_NAME =
      ContainerHealthTaskMetrics.class.getSimpleName();

  @Metric(about = "ContainerHealthTask runtime in milliseconds")
  private MutableRate runTimeMs;

  @Metric(about = "ContainerHealthTask successful runs")
  private MutableCounterLong runSuccessCount;

  @Metric(about = "ContainerHealthTask failed runs")
  private MutableCounterLong runFailureCount;

  private ContainerHealthTaskMetrics() {
  }

  public static ContainerHealthTaskMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(
        SOURCE_NAME,
        "ContainerHealthTask Metrics",
        new ContainerHealthTaskMetrics());
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
