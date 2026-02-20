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

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * This class contains internal Snapshot Operation metrics.
 */
@Metrics(about = "Snapshot Internal Operation Metrics", context = OzoneConsts.OZONE)
public class OmSnapshotInternalMetrics {

  public static final String METRICS_SOURCE_NAME =
      OmSnapshotInternalMetrics.class.getSimpleName();
  private MetricsRegistry registry;

  /*
   * Total internal snapshot deletion operation metrics since last restart.
   */
  @Metric("Total no. of snapshots purged")
  private MutableCounterLong numSnapshotPurges;
  @Metric("Total no. of snapshot set properties")
  private MutableCounterLong numSnapshotSetProperties;
  @Metric("Total no. of move table keys requests")
  private MutableCounterLong numSnapshotMoveTableKeys;

  @Metric("Total no. of snapshot purge failures")
  private MutableCounterLong numSnapshotPurgeFails;
  @Metric("Total no. of snapshot set property failures")
  private MutableCounterLong numSnapshotSetPropertyFails;
  @Metric("Total no. of snapshot move table keys failures")
  private MutableCounterLong numSnapshotMoveTableKeysFails;

  /*
   * Snapshot defragmentation metrics since last restart.
   */
  @Metric("Total no. of snapshot defragmentation operations (full and incremental)")
  private MutableCounterLong numSnapshotDefrag;
  @Metric("Total no. of snapshot defragmentation failures (full and incremental)")
  private MutableCounterLong numSnapshotDefragFails;
  @Metric("Total no. of snapshots skipped for defragmentation (non-active or already defragmented)")
  private MutableCounterLong numSnapshotDefragSnapshotSkipped;
  @Metric("Total no. of full defragmentation operations")
  private MutableCounterLong numSnapshotFullDefrag;
  @Metric("Total no. of full defragmentation failures")
  private MutableCounterLong numSnapshotFullDefragFails;
  @Metric("Total no. of tables compacted during full defragmentation")
  private MutableCounterLong numSnapshotFullDefragTablesCompacted;
  @Metric("Total no. of incremental defragmentation operations")
  private MutableCounterLong numSnapshotIncDefrag;
  @Metric("Total no. of incremental defragmentation failures")
  private MutableCounterLong numSnapshotIncDefragFails;
  @Metric("Total no. of delta files processed during incremental defragmentation")
  private MutableCounterLong numSnapshotIncDefragDeltaFilesProcessed;

  public OmSnapshotInternalMetrics() {
    this.registry = new MetricsRegistry(METRICS_SOURCE_NAME);
  }

  public static OmSnapshotInternalMetrics create() {
    return DefaultMetricsSystem.instance().register(METRICS_SOURCE_NAME,
        "Metrics tracking the progress of snapshot internal operations",
        new OmSnapshotInternalMetrics());
  }

  /**
   * Unregister the metrics instance.
   */
  public static void unregister() {
    DefaultMetricsSystem.instance().unregisterSource(METRICS_SOURCE_NAME);
  }

  public void incNumSnapshotPurges() {
    numSnapshotPurges.incr();
  }

  public void incNumSnapshotSetProperties() {
    numSnapshotSetProperties.incr();
  }

  public void incNumSnapshotMoveTableKeys() {
    numSnapshotMoveTableKeys.incr();
  }

  public void incNumSnapshotPurgeFails() {
    numSnapshotPurgeFails.incr();
  }

  public void incNumSnapshotSetPropertyFails() {
    numSnapshotSetPropertyFails.incr();
  }

  public void incNumSnapshotMoveTableKeysFails() {
    numSnapshotMoveTableKeysFails.incr();
  }

  public long getNumSnapshotPurges() {
    return numSnapshotPurges.value();
  }

  public long getNumSnapshotSetProperties() {
    return numSnapshotSetProperties.value();
  }

  public long getNumSnapshotMoveTableKeys() {
    return numSnapshotMoveTableKeys.value();
  }

  public long getNumSnapshotPurgeFails() {
    return numSnapshotPurgeFails.value();
  }

  public long getNumSnapshotSetPropertyFails() {
    return numSnapshotSetPropertyFails.value();
  }

  public long getNumSnapshotMoveTableKeysFails() {
    return numSnapshotMoveTableKeysFails.value();
  }

  public void incNumSnapshotDefrag() {
    numSnapshotDefrag.incr();
  }

  public void incNumSnapshotDefragFails() {
    numSnapshotDefragFails.incr();
  }

  public void incNumSnapshotDefragSnapshotSkipped() {
    numSnapshotDefragSnapshotSkipped.incr();
  }

  public void incNumSnapshotFullDefrag() {
    numSnapshotFullDefrag.incr();
  }

  public void incNumSnapshotFullDefragFails() {
    numSnapshotFullDefragFails.incr();
  }

  public void incNumSnapshotFullDefragTablesCompacted(long count) {
    numSnapshotFullDefragTablesCompacted.incr(count);
  }

  public void incNumSnapshotIncDefrag() {
    numSnapshotIncDefrag.incr();
  }

  public void incNumSnapshotIncDefragFails() {
    numSnapshotIncDefragFails.incr();
  }

  public void incNumSnapshotIncDefragDeltaFilesProcessed(long count) {
    numSnapshotIncDefragDeltaFilesProcessed.incr(count);
  }

  public long getNumSnapshotDefrag() {
    return numSnapshotDefrag.value();
  }

  public long getNumSnapshotDefragFails() {
    return numSnapshotDefragFails.value();
  }

  public long getNumSnapshotDefragSnapshotSkipped() {
    return numSnapshotDefragSnapshotSkipped.value();
  }

  public long getNumSnapshotFullDefrag() {
    return numSnapshotFullDefrag.value();
  }

  public long getNumSnapshotFullDefragFails() {
    return numSnapshotFullDefragFails.value();
  }

  public long getNumSnapshotFullDefragTablesCompacted() {
    return numSnapshotFullDefragTablesCompacted.value();
  }

  public long getNumSnapshotIncDefrag() {
    return numSnapshotIncDefrag.value();
  }

  public long getNumSnapshotIncDefragFails() {
    return numSnapshotIncDefragFails.value();
  }

  public long getNumSnapshotIncDefragDeltaFilesProcessed() {
    return numSnapshotIncDefragDeltaFilesProcessed.value();
  }
}
