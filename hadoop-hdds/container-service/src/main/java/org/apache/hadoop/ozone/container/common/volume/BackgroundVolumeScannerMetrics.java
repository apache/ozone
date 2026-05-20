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

package org.apache.hadoop.ozone.container.common.volume;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * This class captures the Background Storage Volume Scanner Metrics.
 **/
@InterfaceAudience.Private
@Metrics(about = "Background Volume Scanner Metrics", context = "dfs")
public class BackgroundVolumeScannerMetrics {
  public static final String SOURCE_NAME = BackgroundVolumeScannerMetrics.class.getSimpleName();

  @Metric("number of volumes scanned in the last iteration")
  private MutableGaugeLong numVolumesScannedInLastIteration;

  @Metric("number of iterations the volume scanner has completed since the last restart")
  private MutableCounterLong numScanIterations;

  @Metric("number of data volume scans since the last restart")
  private MutableCounterLong numDataVolumeScans;

  @Metric("number of metadata volume scans since the last restart")
  private MutableCounterLong numMetadataVolumeScans;

  @Metric("number of volume scanner iterations skipped because the minimum gap " +
      "since the last iteration had not elapsed")
  private MutableCounterLong numIterationsSkipped;

  public BackgroundVolumeScannerMetrics() {
  }

  public static BackgroundVolumeScannerMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, "Background Volume Scanner Metrics", new BackgroundVolumeScannerMetrics());
  }

  /**
   * Return the number of volumes scanned in the last iteration.
   */
  public long getNumVolumesScannedInLastIteration() {
    return numVolumesScannedInLastIteration.value();
  }

  public void setNumVolumesScannedInLastIteration(long value) {
    numVolumesScannedInLastIteration.set(value);
  }

  /**
   * Return the number of iterations the volume scanner has completed since the last restart.
   */
  public long getNumScanIterations() {
    return numScanIterations.value();
  }

  public void incNumScanIterations() {
    numScanIterations.incr();
  }

  /**
   * Return the number of data volumes scanned since the last restart.
   */
  public long getNumDataVolumeScans() {
    return numDataVolumeScans.value();
  }

  public void incNumDataVolumeScans(long count) {
    numDataVolumeScans.incr(count);
  }

  /**
   * Return the number of metadata volumes scanned since the last restart.
   */
  public long getNumMetadataVolumeScans() {
    return numMetadataVolumeScans.value();
  }

  public void incNumMetadataVolumeScans(long count) {
    numMetadataVolumeScans.incr(count);
  }

  /**
   * Return the number of volume scanner iterations skipped because the minimum gap
   * since the last iteration had not elapsed.
   */
  public long getNumIterationsSkipped() {
    return numIterationsSkipped.value();
  }

  public void incNumIterationsSkipped() {
    numIterationsSkipped.incr();
  }

  public void unregister() {
    DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
  }
}
