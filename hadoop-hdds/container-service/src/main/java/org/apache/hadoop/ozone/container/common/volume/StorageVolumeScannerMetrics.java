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

import java.util.Collection;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * This class captures the Storage Volume Scanner Metrics.
 **/
@InterfaceAudience.Private
@Metrics(about = "Storage Volume Scanner Metrics", context = "dfs")
public class StorageVolumeScannerMetrics {
  public static final String SOURCE_NAME = StorageVolumeScannerMetrics.class.getSimpleName();

  @Metric("number of volumes scanned in the last iteration")
  private MutableGaugeLong numVolumesScannedInLastIteration;

  @Metric("number of iterations of scanner completed since the restart")
  private MutableCounterLong numScanIterations;

  @Metric("number of data volume sets scanned")
  private MutableGaugeLong numDataVolumesScanned;

  @Metric("number of metadata volume sets scanned")
  private MutableGaugeLong numMetadataVolumesScanned;

  @Metric("number of checks skipped because the minimum gap since the last check had not elapsed")
  private MutableCounterLong numIterationsSkipped;

  public StorageVolumeScannerMetrics() {
  }

  public static StorageVolumeScannerMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, "Storage Volume Scanner Metrics", new StorageVolumeScannerMetrics());
  }

  /**
   * Return the number of volumes scanned during current {@link StorageVolumeChecker#checkAllVolumeSets()} iteration.
   */
  public long getNumVolumesScannedInLastIteration() {
    return numVolumesScannedInLastIteration.value();
  }

  public void setNumVolumesScannedInLastIteration(long value) {
    numVolumesScannedInLastIteration.set(value);
  }

  /**
   * Return the number of {@link StorageVolumeChecker#checkAllVolumes(Collection)} invocations.
   */
  public long getNumScanIterations() {
    return numScanIterations.value();
  }

  public void incNumScanIterations() {
    numScanIterations.incr();
  }

  /**
   * Return the number of {@link StorageVolumeChecker#checkAllVolumeSets()} invocations
   * on data volume sets.
   */
  public long getNumDataVolumesScanned() {
    return numDataVolumesScanned.value();
  }

  public void incNumDataVolumesScanned(long count) {
    numDataVolumesScanned.incr(count);
  }

  /**
   * Return the number of {@link StorageVolumeChecker#checkAllVolumeSets()} invocations
   * on metadata volume sets.
   */
  public long getNumMetadataVolumesScanned() {
    return numMetadataVolumesScanned.value();
  }

  public void incNumMetadataVolumesScanned(long count) {
    numMetadataVolumesScanned.incr(count);
  }

  /**
   * Return the number of checks skipped because the minimum gap since the
   * last check had not elapsed.
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
