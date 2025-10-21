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

import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DEFRAG_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DEFRAG_LIMIT_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.om.lock.FlatResource.SNAPSHOT_GC_LOCK;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult.EmptyTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRawSSTFileReader;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.snapshot.MultiSnapshotLocks;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background service for defragmenting snapshots in the active snapshot chain.
 * When snapshots are taken, they capture the entire OM RocksDB state but may contain
 * fragmented data. This service defragments snapshots by creating new compacted
 * RocksDB instances with only the necessary data for tracked column families.
 * <p>
 * The service processes snapshots in the active chain sequentially, starting with
 * the first non-defragmented snapshot. For the first snapshot in the chain, it
 * performs a full defragmentation by copying all keys. For subsequent snapshots,
 * it uses incremental defragmentation based on diffs from the previous defragmented
 * snapshot.
 */
public class SnapshotDefragService extends BackgroundService
    implements BootstrapStateHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(SnapshotDefragService.class);

  // Use only a single thread for snapshot defragmentation to avoid conflicts
  private static final int DEFRAG_CORE_POOL_SIZE = 1;

  private final OzoneManager ozoneManager;
  private final AtomicLong runCount = new AtomicLong(0);

  // Number of snapshots to be processed in a single iteration
  private final long snapshotLimitPerTask;

  private final AtomicLong snapshotsDefraggedCount;
  private final AtomicBoolean running;

  private final MultiSnapshotLocks snapshotIdLocks;

  private final BootstrapStateHandler.Lock lock = new BootstrapStateHandler.Lock();

  public SnapshotDefragService(long interval, TimeUnit unit, long serviceTimeout,
      OzoneManager ozoneManager, OzoneConfiguration configuration) {
    super("SnapshotDefragService", interval, unit, DEFRAG_CORE_POOL_SIZE,
        serviceTimeout, ozoneManager.getThreadNamePrefix());
    this.ozoneManager = ozoneManager;
    this.snapshotLimitPerTask = configuration
        .getLong(SNAPSHOT_DEFRAG_LIMIT_PER_TASK,
            SNAPSHOT_DEFRAG_LIMIT_PER_TASK_DEFAULT);
    snapshotsDefraggedCount = new AtomicLong(0);
    running = new AtomicBoolean(false);
    IOzoneManagerLock omLock = ozoneManager.getMetadataManager().getLock();
    this.snapshotIdLocks = new MultiSnapshotLocks(omLock, SNAPSHOT_GC_LOCK, true);
  }

  @Override
  public void start() {
    running.set(true);
    super.start();
  }

  @VisibleForTesting
  public void pause() {
    running.set(false);
  }

  @VisibleForTesting
  public void resume() {
    running.set(true);
  }

  /**
   * Checks if rocks-tools native library is available.
   */
  private boolean isRocksToolsNativeLibAvailable() {
    try {
      return ManagedRawSSTFileReader.tryLoadLibrary();
    } catch (Exception e) {
      LOG.warn("Failed to check native code availability", e);
      return false;
    }
  }

  /**
   * Checks if a snapshot needs defragmentation by examining its YAML metadata.
   */
  private boolean needsDefragmentation(SnapshotInfo snapshotInfo) {
    String snapshotPath = OmSnapshotManager.getSnapshotPath(
        ozoneManager.getConfiguration(), snapshotInfo);

    try {
      // Read snapshot local metadata from YAML
      OmSnapshotLocalData snapshotLocalData = ozoneManager.getOmSnapshotManager()
          .getSnapshotLocalDataManager()
          .getOmSnapshotLocalData(snapshotInfo);

      // Check if snapshot needs compaction (defragmentation)
      boolean needsDefrag = snapshotLocalData.getNeedsDefrag();
      LOG.debug("Snapshot {} needsDefragmentation field value: {}",
          snapshotInfo.getName(), needsDefrag);

      return needsDefrag;
    } catch (IOException e) {
      LOG.warn("Failed to read YAML metadata for snapshot {}, assuming defrag needed",
          snapshotInfo.getName(), e);
      return true;
    }
  }

  /**
   * Performs full defragmentation for the first snapshot in the chain.
   * This is a simplified implementation that demonstrates the concept.
   */
  private void performFullDefragmentation(SnapshotInfo snapshotInfo,
      OmSnapshot omSnapshot) throws IOException {

    // TODO: Implement full defragmentation
  }

  /**
   * Performs incremental defragmentation using diff from previous defragmented snapshot.
   */
  private void performIncrementalDefragmentation(SnapshotInfo currentSnapshot,
      SnapshotInfo previousDefraggedSnapshot, OmSnapshot currentOmSnapshot)
      throws IOException {

    // TODO: Implement incremental defragmentation
  }

  private final class SnapshotDefragTask implements BackgroundTask {

    @Override
    public BackgroundTaskResult call() throws Exception {
      // Check OM leader and readiness
      if (shouldRun()) {
        final long count = runCount.incrementAndGet();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Initiating Snapshot Defragmentation Task: run # {}", count);
        }
        triggerSnapshotDefragOnce();
      }

      return EmptyTaskResult.newResult();
    }
  }

  public synchronized boolean triggerSnapshotDefragOnce() throws IOException {
    // Check if rocks-tools native lib is available
    if (!isRocksToolsNativeLibAvailable()) {
      LOG.warn("Rocks-tools native library is not available. " +
          "Stopping SnapshotDefragService.");
      return false;
    }

    Optional<OmSnapshotManager> snapshotManager = Optional.ofNullable(ozoneManager)
        .map(OzoneManager::getOmSnapshotManager);
    if (!snapshotManager.isPresent()) {
      LOG.debug("OmSnapshotManager not available, skipping defragmentation task");
      return false;
    }

    // Get the SnapshotChainManager to iterate through the global snapshot chain
    final SnapshotChainManager snapshotChainManager =
        ((OmMetadataManagerImpl) ozoneManager.getMetadataManager()).getSnapshotChainManager();

    final Table<String, SnapshotInfo> snapshotInfoTable =
        ozoneManager.getMetadataManager().getSnapshotInfoTable();

    // Use iterator(false) to iterate forward through the snapshot chain
    Iterator<UUID> snapshotIterator = snapshotChainManager.iterator(false);

    long snapshotLimit = snapshotLimitPerTask;

    while (snapshotLimit > 0 && running.get() && snapshotIterator.hasNext()) {
      // Get SnapshotInfo for the current snapshot in the chain
      UUID snapshotId = snapshotIterator.next();
      String snapshotTableKey = snapshotChainManager.getTableKey(snapshotId);
      SnapshotInfo snapshotToDefrag = snapshotInfoTable.get(snapshotTableKey);
      if (snapshotToDefrag == null) {
        LOG.warn("Snapshot with ID '{}' not found in snapshot info table", snapshotId);
        continue;
      }

      // Skip deleted snapshots
      if (snapshotToDefrag.getSnapshotStatus() == SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED) {
        LOG.debug("Skipping deleted snapshot: {} (ID: {})",
            snapshotToDefrag.getName(), snapshotToDefrag.getSnapshotId());
        continue;
      }

      // Check if this snapshot needs defragmentation
      if (!needsDefragmentation(snapshotToDefrag)) {
        LOG.debug("Skipping already defragged snapshot: {} (ID: {})",
            snapshotToDefrag.getName(), snapshotToDefrag.getSnapshotId());
        continue;
      }

      LOG.info("Will defrag snapshot: {} (ID: {})",
          snapshotToDefrag.getName(), snapshotToDefrag.getSnapshotId());

      // Acquire MultiSnapshotLocks
      if (!snapshotIdLocks.acquireLock(Collections.singletonList(snapshotToDefrag.getSnapshotId()))
          .isLockAcquired()) {
        LOG.error("Abort. Failed to acquire lock on snapshot: {} (ID: {})",
            snapshotToDefrag.getName(), snapshotToDefrag.getSnapshotId());
        break;
      }

      try {
        LOG.info("Processing snapshot defragmentation for: {} (ID: {})",
            snapshotToDefrag.getName(), snapshotToDefrag.getSnapshotId());

        // Get snapshot through SnapshotCache for proper locking
        try (UncheckedAutoCloseableSupplier<OmSnapshot> snapshotSupplier =
                 snapshotManager.get().getSnapshot(snapshotToDefrag.getSnapshotId())) {

          OmSnapshot omSnapshot = snapshotSupplier.get();

          UUID pathPreviousSnapshotId = snapshotToDefrag.getPathPreviousSnapshotId();
          boolean isFirstSnapshotInPath = pathPreviousSnapshotId == null;
          if (isFirstSnapshotInPath) {
            LOG.info("Performing full defragmentation for first snapshot (in path): {}",
                snapshotToDefrag.getName());
            performFullDefragmentation(snapshotToDefrag, omSnapshot);
          } else {
            final String psIdtableKey = snapshotChainManager.getTableKey(pathPreviousSnapshotId);
            SnapshotInfo previousDefraggedSnapshot = snapshotInfoTable.get(psIdtableKey);

            LOG.info("Performing incremental defragmentation for snapshot: {} " +
                    "based on previous defragmented snapshot: {}",
                snapshotToDefrag.getName(), previousDefraggedSnapshot.getName());

            // If previous path snapshot is not null, it must have been defragmented already
            // Sanity check to ensure previous snapshot exists and is defragmented
            if (needsDefragmentation(previousDefraggedSnapshot)) {
              LOG.error("Fatal error before defragging snapshot: {}. " +
                      "Previous snapshot in path {} was not defragged while it is expected to be.",
                  snapshotToDefrag.getName(), previousDefraggedSnapshot.getName());
              break;
            }

            performIncrementalDefragmentation(snapshotToDefrag,
                previousDefraggedSnapshot, omSnapshot);
          }

          // TODO: Update snapshot metadata here?

          // Close and evict the original snapshot DB from SnapshotCache
          // TODO: Implement proper eviction from SnapshotCache
          LOG.info("Defragmentation completed for snapshot: {}",
              snapshotToDefrag.getName());

          snapshotLimit--;
          snapshotsDefraggedCount.getAndIncrement();

        } catch (OMException ome) {
          if (ome.getResult() == OMException.ResultCodes.FILE_NOT_FOUND) {
            LOG.info("Snapshot {} was deleted during defragmentation",
                snapshotToDefrag.getName());
          } else {
            LOG.error("OMException during snapshot defragmentation for: {}",
                snapshotToDefrag.getName(), ome);
          }
        }

      } catch (Exception e) {
        LOG.error("Exception during snapshot defragmentation for: {}",
            snapshotToDefrag.getName(), e);
        return false;
      } finally {
        // Release lock MultiSnapshotLocks
        snapshotIdLocks.releaseLock();
        LOG.debug("Released MultiSnapshotLocks on snapshot: {} (ID: {})",
            snapshotToDefrag.getName(), snapshotToDefrag.getSnapshotId());

      }
    }

    return true;
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    // TODO: Can be parallelized for different buckets
    queue.add(new SnapshotDefragTask());
    return queue;
  }

  /**
   * Returns true if the service run conditions are satisfied, false otherwise.
   */
  private boolean shouldRun() {
    if (ozoneManager == null) {
      // OzoneManager can be null for testing
      return true;
    }
    if (ozoneManager.getOmRatisServer() == null) {
      LOG.warn("OzoneManagerRatisServer is not initialized yet");
      return false;
    }
    // The service only runs if current OM node is ready
    return running.get() && ozoneManager.isRunning();
  }

  public AtomicLong getSnapshotsDefraggedCount() {
    return snapshotsDefraggedCount;
  }

  @Override
  public BootstrapStateHandler.Lock getBootstrapStateLock() {
    return lock;
  }

  @Override
  public void shutdown() {
    running.set(false);
    super.shutdown();
  }
}

