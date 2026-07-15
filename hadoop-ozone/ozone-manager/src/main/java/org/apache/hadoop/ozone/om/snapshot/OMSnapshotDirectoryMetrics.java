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

package org.apache.hadoop.ozone.om.snapshot;

import static org.apache.hadoop.ozone.OzoneConsts.ROCKSDB_SST_SUFFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIRECTORY_METRICS_UPDATE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIRECTORY_METRICS_UPDATE_INTERVAL_DEFAULT;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.ha.OMPeriodicMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics for tracking db.snapshots directory space usage and SST file counts.
 * Provides aggregate metrics.
 * Metrics are updated asynchronously to avoid blocking operations.
 */
@InterfaceAudience.Private
@Metrics(about = "OM Snapshot Directory Metrics", context = OzoneConsts.OZONE)
public final class OMSnapshotDirectoryMetrics extends OMPeriodicMetrics implements MetricsSource {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMSnapshotDirectoryMetrics.class);
  private static final String SOURCE_NAME =
      OMSnapshotDirectoryMetrics.class.getSimpleName();

  // Aggregate metrics
  private @Metric MutableGaugeLong dbSnapshotsDirSize;
  private @Metric MutableGaugeLong totalSstFilesCount;
  private @Metric MutableGaugeLong numSnapshots;
  private @Metric MutableGaugeLong sstBackupDirSize;
  private @Metric MutableGaugeLong sstBackupSstFilesCount;

  private final OMMetadataManager metadataManager;
  private final MetricsRegistry registry = new MetricsRegistry(SOURCE_NAME);

  OMSnapshotDirectoryMetrics(ConfigurationSource conf,
      OMMetadataManager metadataManager) {
    super("OMSnapshotDirectoryMetrics",
        conf.getTimeDuration(OZONE_OM_SNAPSHOT_DIRECTORY_METRICS_UPDATE_INTERVAL,
        OZONE_OM_SNAPSHOT_DIRECTORY_METRICS_UPDATE_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS));
    this.metadataManager = metadataManager;
    this.dbSnapshotsDirSize = registry.newGauge(
        SnapshotMetricsInfo.DbSnapshotsDirSize.name(),
        SnapshotMetricsInfo.DbSnapshotsDirSize.description(), 0L);
    this.totalSstFilesCount = registry.newGauge(
        SnapshotMetricsInfo.TotalSstFilesCount.name(),
        SnapshotMetricsInfo.TotalSstFilesCount.description(), 0L);
    this.numSnapshots = registry.newGauge(
        SnapshotMetricsInfo.NumSnapshots.name(),
        SnapshotMetricsInfo.NumSnapshots.description(), 0L);
    this.sstBackupDirSize = registry.newGauge(
        SnapshotMetricsInfo.SstBackupDirSize.name(),
        SnapshotMetricsInfo.SstBackupDirSize.description(), 0L);
    this.sstBackupSstFilesCount = registry.newGauge(
        SnapshotMetricsInfo.SstBackupSstFilesCount.name(),
        SnapshotMetricsInfo.SstBackupSstFilesCount.description(), 0L);
  }

  public static OMSnapshotDirectoryMetrics create(ConfigurationSource conf,
      String parent, OMMetadataManager metadataManager) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, parent,
        new OMSnapshotDirectoryMetrics(conf, metadataManager));
  }

  /**
   * @return if the update was successful.
   * Updates aggregate metrics synchronously.
   */
  @Override
  protected boolean updateMetrics() {
    DBStore store = metadataManager.getStore();
    if (!(store instanceof RDBStore)) {
      LOG.debug("Store is not RDBStore, skipping snapshot directory metrics update");
      resetMetrics();
      return false;
    }

    String snapshotsParentDir = store.getSnapshotsParentDir();

    if (snapshotsParentDir == null) {
      resetMetrics();
      return false;
    }

    File snapshotsDir = new File(snapshotsParentDir);
    if (!snapshotsDir.exists() || !snapshotsDir.isDirectory()) {
      resetMetrics();
      return false;
    }

    try {
      // Calculate aggregate metrics
      String sstBackupDir = store.getRocksDBCheckpointDiffer() != null
          ? store.getRocksDBCheckpointDiffer().getSSTBackupDir() : null;
      calculateAndUpdateMetrics(snapshotsDir,
          sstBackupDir != null ? new File(sstBackupDir) : null);
    } catch (Exception e) {
      LOG.warn("Error calculating snapshot directory metrics", e);
      resetMetrics();
      return false;
    }
    return true;
  }

  /**
     * Calculates & updates directory size metrics accounting for snapshot and
   * backup SST Directory. (only counts each inode once).
   * Uses Files.getAttribute to get the inode number and tracks visited inodes.
   *
   * @param snapshotsDir the directory containing all checkpointDirs.
   * @param sstBackupDir the backup SST directory
   */
  private void calculateAndUpdateMetrics(File snapshotsDir,
      File sstBackupDir) throws IOException {
    Set<Object> visitedSnapshotsInodes = new HashSet<>();
    long snapshotsTotalSize = 0;
    long snapshotsSstFileCount = 0;
    int snapshotCount = 0;

    if (snapshotsDir != null && snapshotsDir.exists() && snapshotsDir.isDirectory()) {
      try (Stream<Path> checkpointDirs = Files.list(snapshotsDir.toPath())) {
        for (Path checkpointDir : checkpointDirs.collect(Collectors.toList())) {
          if (Files.isDirectory(checkpointDir)) {
            snapshotCount++;
            SizeAndCount sizeAndCount =
                calculateDirSize(checkpointDir, visitedSnapshotsInodes);
            snapshotsTotalSize += sizeAndCount.size;
            snapshotsSstFileCount += sizeAndCount.count;
          }
        }
      }
    }

    long backupDirSize = 0;
    long backupSstFileCount = 0;
    if (sstBackupDir != null && sstBackupDir.exists() &&
        sstBackupDir.isDirectory()) {
      SizeAndCount sizeAndCount =
          calculateDirSize(sstBackupDir.toPath(), new HashSet<>());
      backupDirSize = sizeAndCount.size;
      backupSstFileCount = sizeAndCount.count;
    }

    numSnapshots.set(snapshotCount);
    totalSstFilesCount.set(snapshotsSstFileCount);
    dbSnapshotsDirSize.set(snapshotsTotalSize);
    sstBackupDirSize.set(backupDirSize);
    sstBackupSstFilesCount.set(backupSstFileCount);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Updated snapshot directory metrics: size={}, sstFiles={}, " +
              "snapshots={}, backupSize={}, backupSstFiles={}",
          snapshotsTotalSize, snapshotsSstFileCount, snapshotCount,
          backupDirSize, backupSstFileCount);
    }
  }

  private SizeAndCount calculateDirSize(Path directory, Set<Object> visitedInodes) throws IOException {
    long size = 0;
    long count = 0;
    try (Stream<Path> files = Files.list(directory)) {
      for (Path path : files.collect(Collectors.toList())) {
        if (Files.isRegularFile(path)) {
          try {
            // Get inode number
            Object fileKey = IOUtils.getINode(path);
            if (fileKey == null) {
              // Fallback: use file path + size as unique identifier
              fileKey = path.toAbsolutePath() + ":" + Files.size(path);
            }
            // Only count this file if we haven't seen this inode before
            if (visitedInodes.add(fileKey)) {
              if (path.toFile().getName().endsWith(ROCKSDB_SST_SUFFIX)) {
                count++;
              }
              size += Files.size(path);
            }
          } catch (UnsupportedOperationException | IOException e) {
            // Fallback: if we can't get inode, just count the file size.
            LOG.warn("Could not get inode for {}, using file size directly: {}",
                path, e.getMessage());
            size += Files.size(path);
            if (path.toFile().getName().endsWith(ROCKSDB_SST_SUFFIX)) {
              count++;
            }
          }
        }
      }
    }
    return new SizeAndCount(size, count);
  }

  private static final class SizeAndCount {
    private final long size;
    private final long count;

    private SizeAndCount(long size, long count) {
      this.size = size;
      this.count = count;
    }
  }

  /**
   * Resets all metrics to zero.
   */
  private void resetMetrics() {
    dbSnapshotsDirSize.set(0);
    totalSstFilesCount.set(0);
    numSnapshots.set(0);
    sstBackupDirSize.set(0);
    sstBackupSstFilesCount.set(0);
  }

  /**
   * Implements MetricsSource to provide metrics.
   * Reads from cached values updated by updateMetrics().
   */
  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    // Add aggregate metrics
    collector.addRecord(SOURCE_NAME)
        .setContext("Snapshot Directory Metrics")
        .addGauge(SnapshotMetricsInfo.DbSnapshotsDirSize, dbSnapshotsDirSize.value())
        .addGauge(SnapshotMetricsInfo.TotalSstFilesCount, totalSstFilesCount.value())
        .addGauge(SnapshotMetricsInfo.NumSnapshots, numSnapshots.value())
        .addGauge(SnapshotMetricsInfo.SstBackupDirSize, sstBackupDirSize.value())
        .addGauge(SnapshotMetricsInfo.SstBackupSstFilesCount, sstBackupSstFilesCount.value())
        .addGauge(SnapshotMetricsInfo.LastUpdateTime, getLastUpdateTime());
  }

  @VisibleForTesting
  public long getDbSnapshotsDirSize() {
    return dbSnapshotsDirSize.value();
  }

  @VisibleForTesting
  public long getTotalSstFilesCount() {
    return totalSstFilesCount.value();
  }

  @VisibleForTesting
  public long getNumSnapshots() {
    return numSnapshots.value();
  }

  @VisibleForTesting
  public long getSstBackupDirSize() {
    return sstBackupDirSize.value();
  }

  @VisibleForTesting
  public long getSstBackupSstFilesCount() {
    return sstBackupSstFilesCount.value();
  }

  public void unRegister() {
    stop();
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  /**
   * Metrics info enum for snapshot directory metrics.
   */
  enum SnapshotMetricsInfo implements MetricsInfo {
    // Aggregate metrics
    DbSnapshotsDirSize("Total size of db.snapshots directory in bytes"),
    TotalSstFilesCount("Total number of SST files across all snapshots"),
    NumSnapshots("Total number of snapshot checkpoint directories"),
    SstBackupDirSize("Total size of backup SST directory in bytes"),
    SstBackupSstFilesCount("Total number of SST files in backup SST directory"),
    LastUpdateTime("Time stamp when the snapshot directory metrics were last updated");

    private final String desc;

    SnapshotMetricsInfo(String desc) {
      this.desc = desc;
    }

    @Override
    public String description() {
      return desc;
    }
  }
}
