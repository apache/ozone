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
import org.apache.hadoop.metrics2.lib.MutableGaugeFloat;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Class for tracking metrics related to Ozone manager sync operations.
 */
@InterfaceAudience.Private
@Metrics(about = "Recon OzoneManagerSync Metrics", context = OzoneConsts.OZONE)
public final class OzoneManagerSyncMetrics {

  private static final String SOURCE_NAME =
      OzoneManagerSyncMetrics.class.getSimpleName();

  @Metric(about = "Number of OM snapshot requests made by Recon.")
  private MutableCounterLong numSnapshotRequests;

  @Metric(about = "Number of OM snapshot requests that failed.")
  private MutableCounterLong numSnapshotRequestsFailed;

  @Metric(about = "Number of OM delta requests made by Recon that had " +
      "at least 1 update in the response.")
  private MutableCounterLong numNonZeroDeltaRequests;

  @Metric(about = "Number of OM delta requests that failed.")
  private MutableCounterLong numDeltaRequestsFailed;

  @Metric(about = "Total number of updates got through OM delta request")
  private MutableCounterLong numUpdatesInDeltaTotal;

  @Metric(about = "Average number of updates got per OM delta request")
  private MutableGaugeFloat averageNumUpdatesInDeltaRequest;

  @Metric(about = "The lag of sequence number between Recon and OM")
  private MutableGaugeLong sequenceNumberLag;

  private OzoneManagerSyncMetrics() {
  }

  public static OzoneManagerSyncMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
        "Recon Ozone Manager Sync Metrics",
        new OzoneManagerSyncMetrics());
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  public void incrNumSnapshotRequests() {
    this.numSnapshotRequests.incr();
  }

  public void incrNumSnapshotRequestsFailed() {
    this.numSnapshotRequestsFailed.incr();
  }

  public void incrNumDeltaRequestsFailed() {
    this.numSnapshotRequestsFailed.incr();
  }

  public void incrNumUpdatesInDeltaTotal(long n) {
    this.numUpdatesInDeltaTotal.incr(n);
    this.numNonZeroDeltaRequests.incr();
    setAverageNumUpdatesInDeltaRequest(
        (float) this.numUpdatesInDeltaTotal.value() /
            (float) this.numNonZeroDeltaRequests.value());
  }

  public void setAverageNumUpdatesInDeltaRequest(float avg) {
    averageNumUpdatesInDeltaRequest.set(avg);
  }

  public long getNumSnapshotRequests() {
    return numSnapshotRequests.value();
  }

  public long getNumSnapshotRequestsFailed() {
    return numSnapshotRequestsFailed.value();
  }

  public long getNumDeltaRequestsFailed() {
    return numDeltaRequestsFailed.value();
  }

  public long getNumUpdatesInDeltaTotal() {
    return numUpdatesInDeltaTotal.value();
  }

  public float getAverageNumUpdatesInDeltaRequest() {
    return averageNumUpdatesInDeltaRequest.value();
  }

  public long getNumNonZeroDeltaRequests() {
    return numNonZeroDeltaRequests.value();
  }

  public void setSequenceNumberLag(long lag) {
    sequenceNumberLag.set(lag);
  }

  public long getSequenceNumberLag() {
    return sequenceNumberLag.value();
  }
}
