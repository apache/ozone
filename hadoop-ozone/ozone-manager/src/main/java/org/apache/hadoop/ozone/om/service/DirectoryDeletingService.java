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

package org.apache.hadoop.ozone.om.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableDirFilter;
import org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableKeyFilter;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgePathRequest;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a background service to delete orphan directories and its
 * sub paths(sub-dirs and sub-files).
 *
 * <p>
 * This will scan the metadata of om periodically to get the orphan dirs from
 * DeletedDirectoryTable and find its sub paths. It will fetch all sub-files
 * from FileTable and move those to DeletedTable so that OM's
 * KeyDeletingService will cleanup those files later. It will fetch all
 * sub-directories from the DirectoryTable and move those to
 * DeletedDirectoryTable so that these will be visited in next iterations.
 *
 * <p>
 * After moving all sub-files and sub-dirs the parent orphan directory will be
 * deleted by this service. It will continue traversing until all the leaf path
 * components of an orphan directory is visited.
 */
public class DirectoryDeletingService extends AbstractKeyDeletingService {
  private static final Logger LOG =
      LoggerFactory.getLogger(DirectoryDeletingService.class);

  // Using multi thread for DirDeletion. Multiple threads would read
  // from parent directory info from deleted directory table concurrently
  // and send deletion requests.
  private int ratisByteLimit;
  private final AtomicBoolean suspended;
  private final AtomicBoolean isRunningOnAOS;
  private final SnapshotChainManager snapshotChainManager;
  private final boolean deepCleanSnapshots;
  private final ExecutorService deletionThreadPool;
  private final int numberOfParallelThreadsPerStore;

  public DirectoryDeletingService(long interval, TimeUnit unit,
      long serviceTimeout, OzoneManager ozoneManager,
      OzoneConfiguration configuration, int dirDeletingServiceCorePoolSize, boolean deepCleanSnapshots) {
    super(DirectoryDeletingService.class.getSimpleName(), interval, unit,
        dirDeletingServiceCorePoolSize, serviceTimeout, ozoneManager, null);
    int limit = (int) configuration.getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    this.numberOfParallelThreadsPerStore = dirDeletingServiceCorePoolSize;
    this.deletionThreadPool = new ThreadPoolExecutor(0, numberOfParallelThreadsPerStore, interval, unit,
        new LinkedBlockingDeque<>(Integer.MAX_VALUE));

    // always go to 90% of max limit for request as other header will be added
    this.ratisByteLimit = (int) (limit * 0.9);
    this.suspended = new AtomicBoolean(false);
    this.isRunningOnAOS = new AtomicBoolean(false);
    this.snapshotChainManager = ((OmMetadataManagerImpl)ozoneManager.getMetadataManager()).getSnapshotChainManager();
    this.deepCleanSnapshots = deepCleanSnapshots;
  }

  private boolean shouldRun() {
    if (getOzoneManager() == null) {
      // OzoneManager can be null for testing
      return true;
    }
    return getOzoneManager().isLeaderReady() && !suspended.get();
  }

  public boolean isRunningOnAOS() {
    return isRunningOnAOS.get();
  }

  /**
   * Suspend the service.
   */
  @VisibleForTesting
  public void suspend() {
    suspended.set(true);
  }

  /**
   * Resume the service if suspended.
   */
  @VisibleForTesting
  public void resume() {
    suspended.set(false);
  }

  public void setRatisByteLimit(int ratisByteLimit) {
    this.ratisByteLimit = ratisByteLimit;
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new DirDeletingTask(this, null));
    if (deepCleanSnapshots) {
      Iterator<UUID> iterator = null;
      try {
        iterator = snapshotChainManager.iterator(true);
      } catch (IOException e) {
        LOG.error("Error while initializing snapshot chain iterator.");
        return queue;
      }
      while (iterator.hasNext()) {
        UUID snapshotId = iterator.next();
        queue.add(new DirDeletingTask(this, snapshotId));
      }
    }
    return queue;
  }

  @Override
  public void shutdown() {
    super.shutdown();
  }

  private static final class DeletedDirSupplier implements Closeable {
    private TableIterator<String, ? extends KeyValue<String, OmKeyInfo>>
        deleteTableIterator;

    private DeletedDirSupplier(TableIterator<String, ? extends KeyValue<String, OmKeyInfo>> deleteTableIterator) {
      this.deleteTableIterator = deleteTableIterator;
    }

    private synchronized Table.KeyValue<String, OmKeyInfo> get() {
      if (deleteTableIterator.hasNext()) {
        return deleteTableIterator.next();
      }
      return null;
    }

    @Override
    public void close() {
      IOUtils.closeQuietly(deleteTableIterator);
    }
  }

  private final class DirDeletingTask implements BackgroundTask {
    private final DirectoryDeletingService directoryDeletingService;
    private final UUID snapshotId;

    private DirDeletingTask(DirectoryDeletingService service, UUID snapshotId) {
      this.directoryDeletingService = service;
      this.snapshotId = snapshotId;
    }

    @Override
    public int getPriority() {
      return 0;
    }

    private OzoneManagerProtocolProtos.SetSnapshotPropertyRequest getSetSnapshotRequestUpdatingExclusiveSize(
        long exclusiveSize, long exclusiveReplicatedSize, UUID snapshotID) {
      OzoneManagerProtocolProtos.SnapshotSize snapshotSize = OzoneManagerProtocolProtos.SnapshotSize.newBuilder()
          .setExclusiveSize(exclusiveSize)
          .setExclusiveReplicatedSize(exclusiveReplicatedSize)
          .build();
      return OzoneManagerProtocolProtos.SetSnapshotPropertyRequest.newBuilder()
          .setSnapshotKey(snapshotChainManager.getTableKey(snapshotID))
          .setSnapshotSizeDeltaFromDirDeepCleaning(snapshotSize)
          .build();
    }

    /**
     *
     * @param currentSnapshotInfo if null, deleted directories in AOS should be processed.
     * @param keyManager KeyManager of the underlying store.
     */
    private void processDeletedDirsForStore(SnapshotInfo currentSnapshotInfo, KeyManager keyManager,
        long remainingBufLimit, long rnCnt) throws IOException, ExecutionException, InterruptedException {
      String volume, bucket; String snapshotTableKey;
      if (currentSnapshotInfo != null) {
        volume = currentSnapshotInfo.getVolumeName();
        bucket = currentSnapshotInfo.getBucketName();
        snapshotTableKey = currentSnapshotInfo.getTableKey();
      } else {
        volume = null; bucket = null; snapshotTableKey = null;
      }

      try (DeletedDirSupplier dirSupplier = new DeletedDirSupplier(currentSnapshotInfo == null ?
          keyManager.getDeletedDirEntries() : keyManager.getDeletedDirEntries(volume, bucket))) {
        // This is to avoid race condition b/w purge request and snapshot chain update. For AOS taking the global
        // snapshotId since AOS could process multiple buckets in one iteration. While using path
        // previous snapshotId for a snapshot since it would process only one bucket.
        UUID expectedPreviousSnapshotId = currentSnapshotInfo == null ?
            snapshotChainManager.getLatestGlobalSnapshotId() :
            SnapshotUtils.getPreviousSnapshotId(currentSnapshotInfo, snapshotChainManager);
        Map<UUID, Pair<Long, Long>> exclusiveSizeMap = Maps.newConcurrentMap();

        CompletableFuture<Boolean> processedAllDeletedDirs = CompletableFuture.completedFuture(true);
        for (int i = 0; i < numberOfParallelThreadsPerStore; i++) {
          CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
            try {
              return processDeletedDirectories(currentSnapshotInfo, keyManager, dirSupplier, remainingBufLimit,
                  expectedPreviousSnapshotId, exclusiveSizeMap, rnCnt);
            } catch (Throwable e) {
              return false;
            }
          }, deletionThreadPool);
          processedAllDeletedDirs = future.thenCombine(future, (a, b) -> a && b);
        }
        // If AOS or all directories have been processed for snapshot, update snapshot size delta and deep clean flag
        // if it is a snapshot.
        if (processedAllDeletedDirs.get()) {
          List<OzoneManagerProtocolProtos.SetSnapshotPropertyRequest> setSnapshotPropertyRequests = new ArrayList<>();

          for (Map.Entry<UUID, Pair<Long, Long>> entry : exclusiveSizeMap.entrySet()) {
            UUID snapshotID = entry.getKey();
            long exclusiveSize = entry.getValue().getLeft();
            long exclusiveReplicatedSize = entry.getValue().getRight();
            setSnapshotPropertyRequests.add(getSetSnapshotRequestUpdatingExclusiveSize(
                exclusiveSize, exclusiveReplicatedSize, snapshotID));
          }

          // Updating directory deep clean flag of snapshot.
          if (currentSnapshotInfo != null) {
            setSnapshotPropertyRequests.add(OzoneManagerProtocolProtos.SetSnapshotPropertyRequest.newBuilder()
                .setSnapshotKey(snapshotTableKey)
                .setDeepCleanedDeletedDir(true)
                .build());
          }
          submitSetSnapshotRequests(setSnapshotPropertyRequests);
        }
      }
    }

    /**
     * Processes deleted directories for snapshot management, determining whether
     * directories and files can be purged, and calculates exclusive size mappings
     * for snapshots.
     *
     * @param currentSnapshotInfo Information about the current snapshot whose deleted directories are being processed.
     * @param keyManager Key manager of the underlying storage system to handle key operations.
     * @param dirSupplier Supplier for fetching pending deleted directories to be processed.
     * @param remainingBufLimit Remaining buffer limit for processing directories and files.
     * @param expectedPreviousSnapshotId The UUID of the previous snapshot expected in the chain.
     * @param totalExclusiveSizeMap A map for storing total exclusive size and exclusive replicated size
     *                              for each snapshot.
     * @param runCount The number of times the processing task has been executed.
     * @return A boolean indicating whether the processed directory list is empty.
     */
    private boolean processDeletedDirectories(SnapshotInfo currentSnapshotInfo, KeyManager keyManager,
        DeletedDirSupplier dirSupplier, long remainingBufLimit, UUID expectedPreviousSnapshotId,
        Map<UUID, Pair<Long, Long>> totalExclusiveSizeMap, long runCount) {
      OmSnapshotManager omSnapshotManager = getOzoneManager().getOmSnapshotManager();
      IOzoneManagerLock lock = getOzoneManager().getMetadataManager().getLock();
      String snapshotTableKey = currentSnapshotInfo == null ? null : currentSnapshotInfo.getTableKey();
      try (ReclaimableDirFilter reclaimableDirFilter = new ReclaimableDirFilter(getOzoneManager(),
          omSnapshotManager, snapshotChainManager, currentSnapshotInfo, keyManager, lock);
          ReclaimableKeyFilter reclaimableFileFilter = new ReclaimableKeyFilter(getOzoneManager(),
              omSnapshotManager, snapshotChainManager, currentSnapshotInfo, keyManager, lock)) {
        long startTime = Time.monotonicNow();
        long dirNum = 0L;
        long subDirNum = 0L;
        long subFileNum = 0L;
        int consumedSize = 0;
        List<PurgePathRequest> purgePathRequestList = new ArrayList<>();
        List<Pair<String, OmKeyInfo>> allSubDirList = new ArrayList<>();
        while (remainingBufLimit > 0) {
          KeyValue<String, OmKeyInfo> pendingDeletedDirInfo = dirSupplier.get();
          if (pendingDeletedDirInfo == null) {
            break;
          }
          boolean isDirReclaimable = reclaimableDirFilter.apply(pendingDeletedDirInfo);
          Optional<PurgePathRequest> request = prepareDeleteDirRequest(
              pendingDeletedDirInfo.getValue(),
              pendingDeletedDirInfo.getKey(), isDirReclaimable, allSubDirList,
              getOzoneManager().getKeyManager(), reclaimableFileFilter, remainingBufLimit);
          if (!request.isPresent()) {
            continue;
          }
          PurgePathRequest purgePathRequest = request.get();
          consumedSize += purgePathRequest.getSerializedSize();
          remainingBufLimit -= consumedSize;
          purgePathRequestList.add(purgePathRequest);
          // Count up the purgeDeletedDir, subDirs and subFiles
          if (purgePathRequest.hasDeletedDir() && !StringUtils.isBlank(purgePathRequest.getDeletedDir())) {
            dirNum++;
          }
          subDirNum += purgePathRequest.getMarkDeletedSubDirsCount();
          subFileNum += purgePathRequest.getDeletedSubFilesCount();
        }

        optimizeDirDeletesAndSubmitRequest(dirNum, subDirNum,
            subFileNum, allSubDirList, purgePathRequestList, snapshotTableKey,
            startTime, remainingBufLimit, getOzoneManager().getKeyManager(),
            reclaimableDirFilter, reclaimableFileFilter, expectedPreviousSnapshotId,
            runCount);
        Map<UUID, Long> exclusiveReplicatedSizeMap = reclaimableFileFilter.getExclusiveReplicatedSizeMap();
        Map<UUID, Long> exclusiveSizeMap = reclaimableFileFilter.getExclusiveSizeMap();
        List<UUID> previousPathSnapshotsInChain =
            Stream.of(exclusiveSizeMap.keySet(), exclusiveReplicatedSizeMap.keySet())
                .flatMap(Collection::stream).distinct().collect(Collectors.toList());
        for (UUID snapshot : previousPathSnapshotsInChain) {
          totalExclusiveSizeMap.compute(snapshot, (k, v) -> {
            long exclusiveSize = exclusiveSizeMap.getOrDefault(snapshot, 0L);
            long exclusiveReplicatedSize = exclusiveReplicatedSizeMap.getOrDefault(snapshot, 0L);
            if (v == null) {
              return Pair.of(exclusiveSize, exclusiveReplicatedSize);
            }
            return Pair.of(v.getLeft() + exclusiveSize, v.getRight() + exclusiveReplicatedSize);
          });
        }

        return purgePathRequestList.isEmpty();
      } catch (IOException e) {
        LOG.error("Error while running delete directories for store : {} and files background task. " +
                "Will retry at next run. ", snapshotTableKey, e);
        return false;
      }
    }

    @Override
    public BackgroundTaskResult call() {
      // Check if this is the Leader OM. If not leader, no need to execute this
      // task.
      if (shouldRun()) {
        final long run = getRunCount().incrementAndGet();
        if (snapshotId == null) {
          LOG.debug("Running DirectoryDeletingService for active object store, {}", run);
          isRunningOnAOS.set(true);
        } else {
          LOG.debug("Running DirectoryDeletingService for snapshot : {}, {}", snapshotId, run);
        }
        OmSnapshotManager omSnapshotManager = getOzoneManager().getOmSnapshotManager();
        SnapshotInfo snapInfo = null;
        try {
          snapInfo = snapshotId == null ? null :
              SnapshotUtils.getSnapshotInfo(getOzoneManager(), snapshotChainManager, snapshotId);
          if (snapInfo != null) {
            if (snapInfo.isDeepCleanedDeletedDir()) {
              LOG.info("Snapshot {} has already been deep cleaned directory. Skipping the snapshot in this iteration.",
                  snapInfo.getSnapshotId());
              return BackgroundTaskResult.EmptyTaskResult.newResult();
            }
            if (!OmSnapshotManager.areSnapshotChangesFlushedToDB(getOzoneManager().getMetadataManager(), snapInfo)) {
              LOG.info("Skipping snapshot processing since changes to snapshot {} have not been flushed to disk",
                  snapInfo);
              return BackgroundTaskResult.EmptyTaskResult.newResult();
            }
          }
          try (UncheckedAutoCloseableSupplier<OmSnapshot> omSnapshot = snapInfo == null ? null :
              omSnapshotManager.getActiveSnapshot(snapInfo.getVolumeName(), snapInfo.getBucketName(),
                  snapInfo.getName())) {
            KeyManager keyManager = snapInfo == null ? getOzoneManager().getKeyManager()
                : omSnapshot.get().getKeyManager();
            processDeletedDirsForStore(snapInfo, keyManager, ratisByteLimit, run);
          }
        } catch (IOException | ExecutionException | InterruptedException e) {
          LOG.error("Error while running delete files background task for store {}. Will retry at next run.",
              snapInfo, e);
        } finally {
          if (snapshotId == null) {
            isRunningOnAOS.set(false);
            synchronized (directoryDeletingService) {
              this.directoryDeletingService.notify();
            }
          }
        }
      }
      // By design, no one cares about the results of this call back.
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }
  }
}
