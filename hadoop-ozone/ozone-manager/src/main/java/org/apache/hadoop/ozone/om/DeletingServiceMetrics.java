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

import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Class contains metrics related to the OM Deletion services.
 */
@Metrics(about = "Deletion Service Metrics", context = OzoneConsts.OZONE)
public final class DeletingServiceMetrics {

  public static final String METRICS_SOURCE_NAME =
      DeletingServiceMetrics.class.getSimpleName();
  private MetricsRegistry registry;

  /*
   * Total directory deletion metrics across all iterations of DirectoryDeletingService since last restart.
   */
  @Metric("Total no. of deleted directories sent for purge")
  private MutableGaugeLong numDirsSentForPurge;
  @Metric("Total no. of sub-directories sent for purge")
  private MutableGaugeLong numSubDirsSentForPurge;
  @Metric("Total no. of sub-files sent for purge")
  private MutableGaugeLong numSubFilesSentForPurge;
  /*
   * Total key deletion metrics across all iterations of KeyDeletingService since last restart.
   */
  @Metric("Total no. of keys processed")
  private MutableGaugeLong numKeysProcessed;
  @Metric("Total no. of deleted keys sent for purge")
  private MutableGaugeLong numKeysSentForPurge;
  /*
   * Directory purge request metrics.
   */
  @Metric("Total no. of directories purged")
  private MutableGaugeLong numDirsPurged;
  @Metric("Total no. of subFiles moved to deletedTable")
  private MutableGaugeLong numSubFilesMovedToDeletedTable;
  @Metric("Total no. of subDirectories moved to deletedDirTable")
  private MutableGaugeLong numSubDirsMovedToDeletedDirTable;
  /*
   * Key purge request metrics.
   */
  @Metric("Total no. of keys purged")
  private MutableGaugeLong numKeysPurged;
  @Metric("Total no. of rename entries purged")
  private MutableGaugeLong numRenameEntriesPurged;

  /*
   * Key deletion metrics in the last 24 hours.
   */
  private static final long METRIC_RESET_INTERVAL = TimeUnit.DAYS.toSeconds(1);
  @Metric("Last time the metrics were reset")
  private MutableGaugeLong metricsResetTimeStamp;
  @Metric("No. of reclaimed keys in the last interval")
  private MutableGaugeLong keysReclaimedInInterval;
  @Metric("Replicated size of reclaimed keys in the last interval (bytes)")
  private MutableGaugeLong reclaimedSizeInInterval;

  /*
   * Deletion service state metrics.
   */
  @Metric("Key Deleting Service last run timestamp in ms")
  private MutableGaugeLong kdsLastRunTimestamp;
  @Metric("Key Deleting Service current run timestamp in ms")
  private MutableGaugeLong kdsCurRunTimestamp;

  /*
   * Deletion service last run metrics.
   */
  @Metric("AOS: No. of reclaimed keys in the last run")
  private MutableGaugeLong aosKeysReclaimedLast;
  @Metric("AOS: Replicated size of reclaimed keys in the last run (bytes)")
  private MutableGaugeLong aosReclaimedSizeLast;
  @Metric("AOS: No. of iterated keys in the last run")
  private MutableGaugeLong aosKeysIteratedLast;
  @Metric("AOS: No. of not reclaimable keys the last run")
  private MutableGaugeLong aosKeysNotReclaimableLast;
  @Metric("Snapshot: No. of reclaimed keys in the last run")
  private MutableGaugeLong snapKeysReclaimedLast;
  @Metric("Snapshot: Replicated size of reclaimed keys in the last run (bytes)")
  private MutableGaugeLong snapReclaimedSizeLast;
  @Metric("Snapshot: No. of iterated keys in the last run")
  private MutableGaugeLong snapKeysIteratedLast;
  @Metric("Snapshot: No. of not reclaimable keys the last run")
  private MutableGaugeLong snapKeysNotReclaimableLast;

  /**
   * Metric to track the term ID of the last key that was purged from the
   * Active Object Store (AOS). This term ID represents the state of the
   * most recent successful purge operation in the AOS. This value would be used ensure that a background
   * KeyDeletingService/DirectoryDeletingService doesn't start the next run until the previous run has been flushed.
   */
  @Metric("Last Purge Key termIndex on Active Object Store")
  private MutableGaugeLong lastAOSPurgeTermId;
  @Metric("Last Purge Key transactionId on Active Object Store")
  private MutableGaugeLong lastAOSPurgeTransactionId;

  private DeletingServiceMetrics() {
    this.registry = new MetricsRegistry(METRICS_SOURCE_NAME);
  }

  /**
   * Creates and returns DeletingServiceMetrics instance.
   *
   * @return DeletingServiceMetrics
   */
  public static DeletingServiceMetrics create() {
    return DefaultMetricsSystem.instance().register(METRICS_SOURCE_NAME,
        "Metrics tracking the progress of deletion of directories and keys in the OM",
        new DeletingServiceMetrics());
  }

  /**
   * Unregister the metrics instance.
   */
  public static void unregister() {
    DefaultMetricsSystem.instance().unregisterSource(METRICS_SOURCE_NAME);
  }

  public void incrNumDirsSentForPurge(long dirDel) {
    numDirsSentForPurge.incr(dirDel);
  }

  public void incrNumSubDirsSentForPurge(long delta) {
    numSubDirsSentForPurge.incr(delta);
  }

  public void incrNumSubFilesSentForPurge(long delta) {
    numSubFilesSentForPurge.incr(delta);
  }

  public void incrementDirectoryDeletionTotalMetrics(long dirDel, long dirMove, long filesMove) {
    incrNumDirsSentForPurge(dirDel);
    incrNumSubDirsSentForPurge(dirMove);
    incrNumSubFilesSentForPurge(filesMove);
  }

  public long getNumDirsSentForPurge() {
    return numDirsSentForPurge.value();
  }

  public long getNumSubDirsSentForPurge() {
    return numSubDirsSentForPurge.value();
  }

  public long getNumSubFilesSentForPurge() {
    return numSubFilesSentForPurge.value();
  }

  public void incrNumKeysProcessed(long keysProcessed) {
    this.numKeysProcessed.incr(keysProcessed);
  }

  public void incrNumKeysSentForPurge(long keysPurge) {
    this.numKeysSentForPurge.incr(keysPurge);
  }

  public void incrNumDirPurged(long dirPurged) {
    this.numDirsPurged.incr(dirPurged);
  }

  public void incrNumSubFilesMoved(long subKeys) {
    this.numSubFilesMovedToDeletedTable.incr(subKeys);
  }

  public void incrNumSubDirectoriesMoved(long subDirectories) {
    this.numSubDirsMovedToDeletedDirTable.incr(subDirectories);
  }

  public long getNumDirsPurged() {
    return numDirsPurged.value();
  }

  public long getNumSubFilesMovedToDeletedTable() {
    return numSubFilesMovedToDeletedTable.value();
  }

  public long getNumSubDirsMovedToDeletedDirTable() {
    return numSubDirsMovedToDeletedDirTable.value();
  }

  public void incrNumKeysPurged(long keysPurged) {
    this.numKeysPurged.incr(keysPurged);
  }

  public void incrNumRenameEntriesPurged(long renameEntriesPurged) {
    this.numRenameEntriesPurged.incr(renameEntriesPurged);
  }

  public void setKdsLastRunTimestamp(long timestamp) {
    this.kdsLastRunTimestamp.set(timestamp);
  }

  public void setKdsCurRunTimestamp(long timestamp) {
    this.kdsCurRunTimestamp.set(timestamp);
  }

  private void resetMetrics() {
    this.keysReclaimedInInterval.set(0);
    this.reclaimedSizeInInterval.set(0);
  }

  private void checkAndResetMetrics() {
    long currentTime = Instant.now().getEpochSecond();
    if (metricsResetTimeStamp.value() == 0) {
      this.metricsResetTimeStamp.set(currentTime);
    }
    if (currentTime - metricsResetTimeStamp.value() > METRIC_RESET_INTERVAL) {
      resetMetrics();
      this.metricsResetTimeStamp.set(currentTime);
    }
  }

  public void updateIntervalCumulativeMetrics(long keysReclaimed, long replicatedSizeBytes) {
    checkAndResetMetrics();
    this.keysReclaimedInInterval.incr(keysReclaimed);
    this.reclaimedSizeInInterval.incr(replicatedSizeBytes);
  }

  public long getKeysReclaimedInInterval() {
    return keysReclaimedInInterval.value();
  }

  public long getReclaimedSizeInInterval() {
    return reclaimedSizeInInterval.value();
  }

  public void updateAosLastRunMetrics(long keysReclaimed, long replicatedSizeBytes, long iteratedKeys,
      long notReclaimableKeys) {
    this.aosKeysReclaimedLast.set(keysReclaimed);
    this.aosReclaimedSizeLast.set(replicatedSizeBytes);
    this.aosKeysIteratedLast.set(iteratedKeys);
    this.aosKeysNotReclaimableLast.set(notReclaimableKeys);
  }

  public long getAosKeysReclaimedLast() {
    return aosKeysReclaimedLast.value();
  }

  public long getAosReclaimedSizeLast() {
    return aosReclaimedSizeLast.value();
  }

  public long getAosKeysIteratedLast() {
    return aosKeysIteratedLast.value();
  }

  public long getAosKeysNotReclaimableLast() {
    return aosKeysNotReclaimableLast.value();
  }

  public void updateSnapLastRunMetrics(long keysReclaimed, long replicatedSizeBytes, long iteratedKeys,
      long notReclaimableKeys) {
    this.snapKeysReclaimedLast.set(keysReclaimed);
    this.snapReclaimedSizeLast.set(replicatedSizeBytes);
    this.snapKeysIteratedLast.set(iteratedKeys);
    this.snapKeysNotReclaimableLast.set(notReclaimableKeys);
  }

  public long getSnapKeysReclaimedLast() {
    return snapKeysReclaimedLast.value();
  }

  public long getSnapReclaimedSizeLast() {
    return snapReclaimedSizeLast.value();
  }

  public long getSnapKeysIteratedLast() {
    return snapKeysIteratedLast.value();
  }

  public long getSnapKeysNotReclaimableLast() {
    return snapKeysNotReclaimableLast.value();
  }

  public synchronized TransactionInfo getLastAOSTransactionInfo() {
    return TransactionInfo.valueOf(lastAOSPurgeTermId.value(), lastAOSPurgeTransactionId.value());
  }

  public synchronized void setLastAOSTransactionInfo(TransactionInfo transactionInfo) {
    TransactionInfo previousTransactionInfo = getLastAOSTransactionInfo();
    if (transactionInfo.compareTo(previousTransactionInfo) > 0) {
      this.lastAOSPurgeTermId.set(transactionInfo.getTerm());
      this.lastAOSPurgeTransactionId.set(transactionInfo.getTransactionIndex());
    }
  }

  @VisibleForTesting
  public void resetDirectoryMetrics() {
    numDirsPurged.set(0);
    numSubFilesMovedToDeletedTable.set(0);
    numSubDirsMovedToDeletedDirTable.set(0);
    numDirsSentForPurge.set(0);
    numSubDirsSentForPurge.set(0);
    numSubFilesSentForPurge.set(0);
  }

}
