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
 * Metrics for Recon OM synchronization operations.
 * This class tracks delta and full snapshot sync operations between Recon and OM.
 */
@InterfaceAudience.Private
@Metrics(about = "Recon OM Sync Metrics", context = OzoneConsts.OZONE)
public final class ReconSyncMetrics {

  private static final String SOURCE_NAME =
      ReconSyncMetrics.class.getSimpleName();

  // Delta Fetch Operations
  @Metric(about = "Time taken to fetch delta updates from OM")
  private MutableRate deltaFetchDuration;

  @Metric(about = "Count of successful delta fetch operations")
  private MutableCounterLong deltaFetchSuccess;

  @Metric(about = "Count of failed delta fetch operations")
  private MutableCounterLong deltaFetchFailures;

  @Metric(about = "Total size of delta data fetched in bytes")
  private MutableCounterLong deltaDataFetchSize;

  // Delta Apply Operations (Conversion + DB Apply Combined)
  @Metric(about = "Time taken to apply delta updates to Recon OM DB")
  private MutableRate deltaApplyDuration;

  @Metric(about = "Count of failed delta apply operations")
  private MutableCounterLong deltaApplyFailures;

  // Full DB Snapshot Metrics
  @Metric(about = "Time taken to fetch full DB snapshot")
  private MutableRate fullDBRequestLatency;

  @Metric(about = "Total count of full DB fetch requests made")
  private MutableCounterLong fullDBFetchRequests;

  @Metric(about = "Total size of downloaded snapshots in bytes")
  private MutableCounterLong snapshotSizeBytes;

  @Metric(about = "Count of successful snapshot downloads")
  private MutableCounterLong snapshotDownloadSuccess;

  @Metric(about = "Count of failed snapshot downloads")
  private MutableCounterLong snapshotDownloadFailures;

  private ReconSyncMetrics() {
  }

  public static ReconSyncMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
        "Recon OM Sync Metrics",
        new ReconSyncMetrics());
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  // Delta Fetch Operations
  public void updateDeltaFetchDuration(long duration) {
    this.deltaFetchDuration.add(duration);
  }

  public void incrDeltaFetchSuccess() {
    this.deltaFetchSuccess.incr();
  }

  public void incrDeltaFetchFailures() {
    this.deltaFetchFailures.incr();
  }

  public void incrDeltaDataFetchSize(long size) {
    this.deltaDataFetchSize.incr(size);
  }

  // Delta Apply Operations
  public void updateDeltaApplyDuration(long duration) {
    this.deltaApplyDuration.add(duration);
  }

  public void incrDeltaApplyFailures() {
    this.deltaApplyFailures.incr();
  }

  // Full DB Snapshot Operations
  public void updateFullDBRequestLatency(long duration) {
    this.fullDBRequestLatency.add(duration);
  }

  public void incrFullDBFetchRequests() {
    this.fullDBFetchRequests.incr();
  }

  public void incrSnapshotSizeBytes(long size) {
    this.snapshotSizeBytes.incr(size);
  }

  public void incrSnapshotDownloadSuccess() {
    this.snapshotDownloadSuccess.incr();
  }

  public void incrSnapshotDownloadFailures() {
    this.snapshotDownloadFailures.incr();
  }

  // Getters for testing
  public long getDeltaFetchSuccess() {
    return deltaFetchSuccess.value();
  }

  public long getDeltaFetchFailures() {
    return deltaFetchFailures.value();
  }

  public long getDeltaDataFetchSize() {
    return deltaDataFetchSize.value();
  }

  public long getDeltaApplyFailures() {
    return deltaApplyFailures.value();
  }

  public MutableRate getDeltaFetchDuration() {
    return deltaFetchDuration;
  }

  public MutableRate getDeltaApplyDuration() {
    return deltaApplyDuration;
  }

  public long getFullDBFetchRequests() {
    return fullDBFetchRequests.value();
  }

  public long getSnapshotSizeBytes() {
    return snapshotSizeBytes.value();
  }

  public long getSnapshotDownloadSuccess() {
    return snapshotDownloadSuccess.value();
  }

  public long getSnapshotDownloadFailures() {
    return snapshotDownloadFailures.value();
  }

  public MutableRate getFullDBRequestLatency() {
    return fullDBRequestLatency;
  }
}
