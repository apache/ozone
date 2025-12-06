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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics for tracking db.snapshots directory space usage and SST file counts.
 * Provides both aggregate metrics and per-checkpoint-directory metrics.
 * Metrics are updated asynchronously to avoid blocking operations.
 */
@InterfaceAudience.Private
@Metrics(about = "OM Snapshot Directory Metrics", context = OzoneConsts.OZONE)
public final class OMSnapshotDirectoryMetrics implements MetricsSource {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMSnapshotDirectoryMetrics.class);
  private static final String SOURCE_NAME =
      OMSnapshotDirectoryMetrics.class.getSimpleName();

  // Aggregate metrics
  private @Metric MutableGaugeLong dbSnapshotsDirSize;
  private @Metric MutableGaugeLong totalSstFilesCount;
  private @Metric MutableGaugeLong numSnapshots;

  private final AtomicLong lastUpdateTime = new AtomicLong(0);
  private final AtomicReference<CompletableFuture<Void>> currentUpdateFutureRef =
      new AtomicReference<>();
  private final OMMetadataManager metadataManager;
  private final MetricsRegistry registry = new MetricsRegistry(SOURCE_NAME);

  // Per-checkpoint-directory metrics storage
  private volatile Map<String, CheckpointMetrics> checkpointMetricsMap = new HashMap<>();

  private Timer updateTimer;

  /**
   * Starts the periodic metrics update task.
   *
   * @param conf OzoneConfiguration for reading update interval
   */
  public void start(OzoneConfiguration conf) {
    long updateInterval = conf.getTimeDuration(OZONE_OM_SNAPSHOT_DIRECTORY_METRICS_UPDATE_INTERVAL,
        OZONE_OM_SNAPSHOT_DIRECTORY_METRICS_UPDATE_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);

    updateTimer = new Timer("OMSnapshotDirectoryMetricsUpdate", true);
    updateTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        updateMetricsAsync();
      }
    }, 0, updateInterval);
  }

  /**
   * Stops the periodic metrics update task.
   */
  public void stop() {
    if (updateTimer != null) {
      updateTimer.cancel();
      updateTimer = null;
    }
  }

  public void unRegister() {
    stop();
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  /**
   * Internal class to store per-checkpoint metrics.
   */
  private static class CheckpointMetrics {
    private final long size;
    private final int sstFileCount;

    CheckpointMetrics(long size, int sstFileCount) {
      this.size = size;
      this.sstFileCount = sstFileCount;
    }

    public long getSize() {
      return size;
    }

    public int getSstFileCount() {
      return sstFileCount;
    }
  }

  private OMSnapshotDirectoryMetrics(OMMetadataManager metadataManager) {
    this.metadataManager = metadataManager;
  }

  public static OMSnapshotDirectoryMetrics create(String parent,
      OMMetadataManager metadataManager) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
        parent,
        new OMSnapshotDirectoryMetrics(metadataManager));
  }

  /**
   * Updates all metrics (aggregate and per-checkpoint) asynchronously
   * in a background thread.
   */
  public void updateMetricsAsync() {
    CompletableFuture<Void> currentUpdateFuture = currentUpdateFutureRef.get();
    if (currentUpdateFuture != null && !currentUpdateFuture.isDone()) {
      return;
    }

    CompletableFuture<Void> newFuture = CompletableFuture.runAsync(() -> {
      try {
        updateMetrics();
        lastUpdateTime.set(System.currentTimeMillis());
      } catch (Exception e) {
        LOG.warn("Failed to update snapshot directory metrics", e);
      } finally {
        currentUpdateFutureRef.set(null);
      }
    });

    // Atomically set the future only if the current value matches what we checked
    // This prevents race conditions where multiple threads try to set a new future
    CompletableFuture<Void> expected = currentUpdateFutureRef.get();
    if (expected == null || expected.isDone()) {
      // Only set if still null or done (double-check after creating future)
      if (!currentUpdateFutureRef.compareAndSet(expected, newFuture)) {
        // Another thread set a future, cancel this one
        newFuture.cancel(false);
      }
    } else {
      // Another thread started an update, cancel this one
      newFuture.cancel(false);
    }
  }

  /**
   * Updates all metrics synchronously - both aggregate and per-checkpoint-directory.
   */
  @VisibleForTesting
  void updateMetrics() throws IOException {
    DBStore store = metadataManager.getStore();
    if (!(store instanceof RDBStore)) {
      LOG.debug("Store is not RDBStore, skipping snapshot directory metrics update");
      resetMetrics();
      return;
    }

    RDBStore rdbStore = (RDBStore) store;
    String snapshotsParentDir = rdbStore.getSnapshotsParentDir();

    if (snapshotsParentDir == null) {
      resetMetrics();
      return;
    }

    File snapshotsDir = new File(snapshotsParentDir);
    if (!snapshotsDir.exists() || !snapshotsDir.isDirectory()) {
      resetMetrics();
      return;
    }

    try {
      // Calculate aggregate metrics
      long totalSize = FileUtils.sizeOfDirectory(snapshotsDir);
      dbSnapshotsDirSize.set(totalSize);

      // Calculate per-checkpoint-directory metrics and aggregate totals
      File[] checkpointDirs = snapshotsDir.listFiles(File::isDirectory);
      int totalSstCount = 0;
      int snapshotCount = 0;
      Map<String, CheckpointMetrics> newCheckpointMetricsMap = new HashMap<>();

      if (checkpointDirs != null) {
        snapshotCount = checkpointDirs.length;

        for (File checkpointDir : checkpointDirs) {
          String checkpointDirName = checkpointDir.getName();
          long checkpointSize = 0;
          int sstFileCount = 0;

          try {
            checkpointSize = FileUtils.sizeOfDirectory(checkpointDir);
            File[] sstFiles = checkpointDir.listFiles((dir, name) ->
                name.toLowerCase().endsWith(ROCKSDB_SST_SUFFIX));
            if (sstFiles != null) {
              sstFileCount = sstFiles.length;
            }
          } catch (Exception e) {
            LOG.debug("Error calculating metrics for checkpoint directory {}",
                checkpointDirName, e);
            // Continue with other directories even if one fails
            continue;
          }

          totalSstCount += sstFileCount;
          newCheckpointMetricsMap.put(checkpointDirName,
              new CheckpointMetrics(checkpointSize, sstFileCount));
        }
      }

      // Update aggregate metrics
      totalSstFilesCount.set(totalSstCount);
      numSnapshots.set(snapshotCount);

      // Atomically update per-checkpoint metrics map
      checkpointMetricsMap = Collections.unmodifiableMap(newCheckpointMetricsMap);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Updated snapshot directory metrics: size={}, sstFiles={}, snapshots={}",
            totalSize, totalSstCount, snapshotCount);
      }

    } catch (Exception e) {
      LOG.warn("Error calculating snapshot directory metrics", e);
      resetMetrics();
    }
  }

  /**
   * Resets all metrics to zero.
   */
  private void resetMetrics() {
    dbSnapshotsDirSize.set(0);
    totalSstFilesCount.set(0);
    numSnapshots.set(0);
    checkpointMetricsMap = new HashMap<>();
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
        .addGauge(SnapshotMetricsInfo.NumSnapshots, numSnapshots.value());

    // Add per-checkpoint-directory metrics from cached map
    Map<String, CheckpointMetrics> currentMetrics = checkpointMetricsMap;
    for (Map.Entry<String, CheckpointMetrics> entry : currentMetrics.entrySet()) {
      String checkpointDirName = entry.getKey();
      CheckpointMetrics metrics = entry.getValue();

      collector.addRecord(SOURCE_NAME)
          .setContext("Per-Checkpoint Directory Metrics")
          .tag(SnapshotMetricsInfo.CheckpointDirName, checkpointDirName)
          .addGauge(SnapshotMetricsInfo.CheckpointDirSize, metrics.getSize())
          .addGauge(SnapshotMetricsInfo.CheckpointSstFilesCount, metrics.getSstFileCount());
    }
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
  public long getLastUpdateTime() {
    return lastUpdateTime.get();
  }

  @VisibleForTesting
  public Map<String, CheckpointMetrics> getCheckpointMetricsMap() {
    return Collections.unmodifiableMap(checkpointMetricsMap);
  }

  /**
   * Metrics info enum for snapshot directory metrics.
   */
  enum SnapshotMetricsInfo implements MetricsInfo {
    // Aggregate metrics
    DbSnapshotsDirSize("Total size of db.snapshots directory in bytes"),
    TotalSstFilesCount("Total number of SST files across all snapshots"),
    NumSnapshots("Total number of snapshot checkpoint directories"),

    // Per-checkpoint-directory metric tag
    CheckpointDirName("Checkpoint directory name"),

    // Per-checkpoint-directory metrics
    CheckpointDirSize("Size of checkpoint directory in bytes"),
    CheckpointSstFilesCount("Number of SST files in checkpoint directory");

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
