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
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Metrics for Recon SCM targeted sync execution.
 */
@InterfaceAudience.Private
@Metrics(about = "Recon SCM Container Sync Metrics", context = OzoneConsts.OZONE)
public final class ReconScmContainerSyncMetrics {

  private static final String SOURCE_NAME =
      ReconScmContainerSyncMetrics.class.getSimpleName();

  /**
   * No targeted sync has run yet, or the latest scheduler cycle did not run one.
   */
  public static final int TARGETED_SYNC_STATUS_IDLE = 0;
  /**
   * Targeted sync is currently running.
   */
  public static final int TARGETED_SYNC_STATUS_IN_PROGRESS = 1;
  /**
   * The last targeted sync completed successfully.
   */
  public static final int TARGETED_SYNC_STATUS_SUCCESS = 2;
  /**
   * The last targeted sync completed with one or more failed passes.
   */
  public static final int TARGETED_SYNC_STATUS_FAILURE = 3;

  @Metric(about = "Targeted sync status: 0=idle, 1=in progress, "
      + "2=success, 3=failure")
  private MutableGaugeInt targetedSyncStatus;

  @Metric(about = "Time taken by the last targeted sync in milliseconds")
  private MutableGaugeLong lastTargetedSyncDurationMs;

  private ReconScmContainerSyncMetrics() {
  }

  public static ReconScmContainerSyncMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
        "Recon SCM Container Sync Metrics",
        new ReconScmContainerSyncMetrics());
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  public void setTargetedSyncStatus(int status) {
    targetedSyncStatus.set(status);
  }

  public void setLastTargetedSyncDurationMs(long durationMs) {
    lastTargetedSyncDurationMs.set(durationMs);
  }

  public int getTargetedSyncStatus() {
    return targetedSyncStatus.value();
  }

  public long getLastTargetedSyncDurationMs() {
    return lastTargetedSyncDurationMs.value();
  }
}
