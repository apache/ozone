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

package org.apache.hadoop.ozone.container.ec.reconstruction;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Metrics class for EC Reconstruction.
 */
@InterfaceAudience.Private
@Metrics(about = "EC Reconstruction Coordinator Metrics",
    context = OzoneConsts.OZONE)
public final class ECReconstructionMetrics {
  private static final String SOURCE =
      ECReconstructionMetrics.class.getSimpleName();

  private @Metric MutableCounterLong blockGroupReconstructionTotal;
  private @Metric MutableCounterLong blockGroupReconstructionFailsTotal;
  private @Metric MutableCounterLong reconstructionTotal;
  private @Metric MutableCounterLong reconstructionFailsTotal;

  private ECReconstructionMetrics() {
  }

  public static ECReconstructionMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE, "EC Reconstruction Coordinator Metrics",
        new ECReconstructionMetrics());
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE);
  }

  public void incBlockGroupReconstructionTotal(long count) {
    blockGroupReconstructionTotal.incr(count);
  }

  public void incBlockGroupReconstructionFailsTotal(long count) {
    blockGroupReconstructionFailsTotal.incr(count);
  }

  public void incReconstructionTotal() {
    reconstructionTotal.incr();
  }

  public void incReconstructionFailsTotal() {
    reconstructionFailsTotal.incr();
  }

  public long getReconstructionTotal() {
    return reconstructionTotal.value();
  }

  public long getBlockGroupReconstructionTotal() {
    return blockGroupReconstructionTotal.value();
  }
}
