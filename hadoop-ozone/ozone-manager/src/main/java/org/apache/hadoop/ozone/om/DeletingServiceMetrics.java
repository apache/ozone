/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om;

import com.google.common.annotations.VisibleForTesting;
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


  /*
   * Total directory deletion metrics across all iterations of DirectoryDeletingService since last restart.
   */
  @Metric("Total no. of deleted directories sent for purge")
  private MutableGaugeLong numDirsSentForPurge;
  @Metric("Total no. of sub-directories sent for purge")
  private MutableGaugeLong numSubDirsSentForPurge;
  @Metric("Total no. of sub-files sent for purge")
  private MutableGaugeLong numSubFilesSentForPurge;

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

  /*
   * Total key deletion metrics across all iterations of KeyDeletingService since last restart.
   */
  @Metric("Total no. of keys processed")
  private MutableGaugeLong numKeysProcessed;
  @Metric("Total no. of deleted keys sent for purge")
  private MutableGaugeLong numKeysSentForPurge;

  public void incrNumKeysProcessed(long keysProcessed) {
    this.numKeysProcessed.incr(keysProcessed);
  }

  public void incrNumKeysSentForPurge(long keysPurge) {
    this.numKeysSentForPurge.incr(keysPurge);
  }

  /*
   * Directory purge request metrics.
   */
  @Metric("Total no. of directories purged")
  private MutableGaugeLong numDirsPurged;
  @Metric("Total no. of subFiles moved to deletedTable")
  private MutableGaugeLong numSubFilesMovedToDeletedTable;
  @Metric("Total no. of subDirectories moved to deletedDirTable")
  private MutableGaugeLong numSubDirsMovedToDeletedDirTable;

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

  /*
   * Key purge request metrics.
   */
  @Metric("Total no. of keys purged")
  private MutableGaugeLong numKeysPurged;

  public void incrNumKeysPurged(long keysPurged) {
    this.numKeysPurged.incr(keysPurged);
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
