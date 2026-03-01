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

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_PATH_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_PATH_DELETING_LIMIT_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_THREAD_NUMBER_DIR_DELETION;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_THREAD_NUMBER_DIR_DELETION_DEFAULT;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.protobuf.ServiceException;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.DeleteKeysResult;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetadataManager.VolumeBucketId;
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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketNameInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgePathRequest;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.function.CheckedFunction;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 Background service responsible for purging deleted directories and files
 * in the Ozone Manager (OM) and associated snapshots.
 *
 * <p>
 * This service periodically scans the deleted directory table and submits
 * purge requests for directories and their sub-entries (subdirectories and files).
 * It operates in both the active object store (AOS) and across all deep-clean enabled
 * snapshots. The service supports parallel processing using a thread pool and
 * coordinates exclusive size calculations and cleanup status updates for
 * snapshots.
 * </p>
 *
 * <h2>Key Features</h2>
 * <ul>
 *   <li>Processes deleted directories in both the active OM and all snapshots
 *       with deep cleaning enabled.</li>
 *   <li>Uses a thread pool to parallelize deletion tasks within each store or snapshot.</li>
 *   <li>Employs filters to determine reclaimability of directories and files,
 *       ensuring safety with respect to snapshot chains.</li>
 *   <li>Tracks and updates exclusive size and replicated exclusive size for each
 *       snapshot as directories and files are reclaimed.</li>
 *   <li>Updates the "deep cleaned" flag for snapshots after a successful run.</li>
 *   <li>Handles error and race conditions gracefully, deferring work if necessary.</li>
 * </ul>
 *
 * <h2>Constructor Parameters</h2>
 * <ul>
 *   <li><b>interval</b> - How often the service runs.</li>
 *   <li><b>unit</b> - Time unit for the interval.</li>
 *   <li><b>serviceTimeout</b> - Service timeout in the given time unit.</li>
 *   <li><b>ozoneManager</b> - The OzoneManager instance.</li>
 *   <li><b>configuration</b> - Ozone configuration object.</li>
 *   <li><b>dirDeletingServiceCorePoolSize</b> - Number of parallel threads for deletion per store or snapshot.</li>
 *   <li><b>deepCleanSnapshots</b> - Whether to enable deep cleaning for snapshots.</li>
 * </ul>
 *
 * <h2>Threading and Parallelism</h2>
 * <ul>
 *   <li>Uses a configurable thread pool for parallel deletion tasks within each store/snapshot.</li>
 *   <li>Each snapshot and AOS get a separate background task for deletion.</li>
 * </ul>
 *
 * <h2>Snapshot Integration</h2>
 * <ul>
 *   <li>Iterates all snapshots in the chain if deep cleaning is enabled.</li>
 *   <li>Skips snapshots that are already deep-cleaned or not yet flushed to disk.</li>
 *   <li>Updates snapshot metadata to reflect size changes and cleaning status.</li>
 * </ul>
 *
 * <h2>Usage</h2>
 * <ul>
 *   <li>Should be scheduled as a background service in OM.</li>
 *   <li>Intended to be run only on the OM leader node.</li>
 * </ul>
 *
 * @see org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableDirFilter
 * @see org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableKeyFilter
 * @see org.apache.hadoop.ozone.om.SnapshotChainManager
 */
public class DirectoryDeletingService extends AbstractKeyDeletingService {
  private static final Logger LOG =
      LoggerFactory.getLogger(DirectoryDeletingService.class);

  // Using multi thread for DirDeletion. Multiple threads would read
  // from parent directory info from deleted directory table concurrently
  // and send deletion requests.
  private int ratisByteLimit;
  private final SnapshotChainManager snapshotChainManager;
  private final boolean deepCleanSnapshots;
  private ExecutorService deletionThreadPool;
  private final int numberOfParallelThreadsPerStore;
  private final AtomicLong deletedDirsCount;
  private final AtomicLong movedDirsCount;
  private final AtomicLong movedFilesCount;
  private final int pathLimitPerTask;

  public DirectoryDeletingService(long interval, TimeUnit unit,
      long serviceTimeout, OzoneManager ozoneManager,
      OzoneConfiguration configuration, int dirDeletingServiceCorePoolSize, boolean deepCleanSnapshots) {
    super(DirectoryDeletingService.class.getSimpleName(), interval, unit,
        dirDeletingServiceCorePoolSize, serviceTimeout, ozoneManager);
    int limit = (int) configuration.getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    this.numberOfParallelThreadsPerStore = dirDeletingServiceCorePoolSize;
    this.deletionThreadPool = new ThreadPoolExecutor(0, numberOfParallelThreadsPerStore,
        interval, unit, new LinkedBlockingDeque<>(Integer.MAX_VALUE));
    // always go to 90% of max limit for request as other header will be added
    this.ratisByteLimit = (int) (limit * 0.9);
    registerReconfigCallbacks(ozoneManager.getReconfigurationHandler());
    this.snapshotChainManager = ((OmMetadataManagerImpl)ozoneManager.getMetadataManager()).getSnapshotChainManager();
    this.deepCleanSnapshots = deepCleanSnapshots;
    this.deletedDirsCount = new AtomicLong(0);
    this.movedDirsCount = new AtomicLong(0);
    this.movedFilesCount = new AtomicLong(0);
    this.pathLimitPerTask =
        configuration.getInt(OZONE_PATH_DELETING_LIMIT_PER_TASK, OZONE_PATH_DELETING_LIMIT_PER_TASK_DEFAULT);
  }

  public void registerReconfigCallbacks(ReconfigurationHandler handler) {
    handler.registerCompleteCallback((changedKeys, newConf) -> {
      if (changedKeys.containsKey(OZONE_DIR_DELETING_SERVICE_INTERVAL) ||
          changedKeys.containsKey(OZONE_THREAD_NUMBER_DIR_DELETION)) {
        updateAndRestart((OzoneConfiguration) newConf);
      }
    });
  }

  private synchronized void updateAndRestart(OzoneConfiguration conf) {
    long newInterval = conf.getTimeDuration(OZONE_DIR_DELETING_SERVICE_INTERVAL,
        OZONE_DIR_DELETING_SERVICE_INTERVAL_DEFAULT, TimeUnit.SECONDS);
    int newCorePoolSize = conf.getInt(OZONE_THREAD_NUMBER_DIR_DELETION,
        OZONE_THREAD_NUMBER_DIR_DELETION_DEFAULT);
    LOG.info("Updating and restarting DirectoryDeletingService with interval {} {}" +
            " and core pool size {}",
        newInterval, TimeUnit.SECONDS.name().toLowerCase(), newCorePoolSize);
    shutdown();
    setInterval(newInterval, TimeUnit.SECONDS);
    setPoolSize(newCorePoolSize);
    start();
  }

  @Override
  public DeletingServiceTaskQueue getTasks() {
    DeletingServiceTaskQueue queue = new DeletingServiceTaskQueue();
    queue.add(new DirDeletingTask(null));
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
        queue.add(new DirDeletingTask(snapshotId));
      }
    }
    return queue;
  }

  @Override
  public void shutdown() {
    if (deletionThreadPool != null) {
      deletionThreadPool.shutdown();
      try {
        if (!deletionThreadPool.awaitTermination(60, TimeUnit.SECONDS)) {
          deletionThreadPool.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        deletionThreadPool.shutdownNow();
      }
    }
    super.shutdown();
  }

  @Override
  public synchronized void start() {
    if (deletionThreadPool == null || deletionThreadPool.isShutdown() || deletionThreadPool.isTerminated()) {
      this.deletionThreadPool = new ThreadPoolExecutor(0, numberOfParallelThreadsPerStore,
          super.getIntervalMillis(), TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(Integer.MAX_VALUE));
    }
    super.start();
  }

  private boolean isThreadPoolActive(ExecutorService threadPoolExecutor) {
    return threadPoolExecutor != null && !threadPoolExecutor.isShutdown() && !threadPoolExecutor.isTerminated();
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  void optimizeDirDeletesAndSubmitRequest(
      long dirNum, long subDirNum, long subFileNum,
      List<Pair<String, OmKeyInfo>> allSubDirList,
      List<PurgePathRequest> purgePathRequestList,
      String snapTableKey, long startTime,
      KeyManager keyManager,
      CheckedFunction<KeyValue<String, OmKeyInfo>, Boolean, IOException> reclaimableDirChecker,
      CheckedFunction<KeyValue<String, OmKeyInfo>, Boolean, IOException> reclaimableFileChecker,
      Map<VolumeBucketId, BucketNameInfo> bucketNameInfoMap,
      UUID expectedPreviousSnapshotId, long rnCnt, AtomicInteger remainNum) {

    // Optimization to handle delete sub-dir and keys to remove quickly
    // This case will be useful to handle when depth of directory is high
    int subdirDelNum = 0;
    int subDirRecursiveCnt = 0;
    while (subDirRecursiveCnt < allSubDirList.size() && remainNum.get() > 0) {
      try {
        Pair<String, OmKeyInfo> stringOmKeyInfoPair = allSubDirList.get(subDirRecursiveCnt++);
        Boolean subDirectoryReclaimable = reclaimableDirChecker.apply(Table.newKeyValue(stringOmKeyInfoPair.getKey(),
            stringOmKeyInfoPair.getValue()));
        Optional<PurgePathRequest> request = prepareDeleteDirRequest(
            stringOmKeyInfoPair.getValue(), stringOmKeyInfoPair.getKey(), subDirectoryReclaimable, allSubDirList,
            keyManager, reclaimableFileChecker, remainNum);
        if (!request.isPresent()) {
          continue;
        }
        PurgePathRequest requestVal = request.get();
        purgePathRequestList.add(requestVal);
        // Count up the purgeDeletedDir, subDirs and subFiles
        if (requestVal.hasDeletedDir() && !StringUtils.isBlank(requestVal.getDeletedDir())) {
          subdirDelNum++;
        }
        subDirNum += requestVal.getMarkDeletedSubDirsCount();
        subFileNum += requestVal.getDeletedSubFilesCount();
      } catch (IOException e) {
        LOG.error("Error while running delete directories and files " +
            "background task. Will retry at next run for subset.", e);
        break;
      }
    }
    if (!purgePathRequestList.isEmpty()) {
      submitPurgePathsWithBatching(purgePathRequestList, snapTableKey, expectedPreviousSnapshotId, bucketNameInfoMap);
    }

    if (dirNum != 0 || subDirNum != 0 || subFileNum != 0) {
      long subdirMoved = subDirNum - subdirDelNum;
      deletedDirsCount.addAndGet(dirNum + subdirDelNum);
      movedDirsCount.addAndGet(subdirMoved);
      movedFilesCount.addAndGet(subFileNum);
      long timeTakenInIteration = Time.monotonicNow() - startTime;
      LOG.info("Number of dirs deleted: {}, Number of sub-dir " +
              "deleted: {}, Number of sub-files moved:" +
              " {} to DeletedTable, Number of sub-dirs moved {} to " +
              "DeletedDirectoryTable, iteration elapsed: {}ms, " +
              " totalRunCount: {}",
          dirNum, subdirDelNum, subFileNum, (subDirNum - subdirDelNum),
          timeTakenInIteration, rnCnt);
      getMetrics().incrementDirectoryDeletionTotalMetrics(dirNum + subdirDelNum, subDirNum, subFileNum);
      getPerfMetrics().setDirectoryDeletingServiceLatencyMs(timeTakenInIteration);
    }
  }

  private static final class DeletedDirSupplier implements Closeable {
    private final TableIterator<String, ? extends KeyValue<String, OmKeyInfo>>
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
    public synchronized void close() {
      IOUtils.closeQuietly(deleteTableIterator);
    }
  }

  /**
   * Returns the number of dirs deleted by the background service.
   *
   * @return Long count.
   */
  @VisibleForTesting
  public long getDeletedDirsCount() {
    return deletedDirsCount.get();
  }

  /**
   * Returns the number of sub-dirs deleted by the background service.
   *
   * @return Long count.
   */
  @VisibleForTesting
  public long getMovedDirsCount() {
    return movedDirsCount.get();
  }

  /**
   * Returns the number of files moved to DeletedTable by the background
   * service.
   *
   * @return Long count.
   */
  @VisibleForTesting
  public long getMovedFilesCount() {
    return movedFilesCount.get();
  }

  private Optional<PurgePathRequest> prepareDeleteDirRequest(
      OmKeyInfo pendingDeletedDirInfo, String delDirName, boolean purgeDir,
      List<Pair<String, OmKeyInfo>> subDirList,
      KeyManager keyManager,
      CheckedFunction<Table.KeyValue<String, OmKeyInfo>, Boolean, IOException> reclaimableFileFilter,
      AtomicInteger remainNum) throws IOException {
    // step-0: Get one pending deleted directory
    if (LOG.isDebugEnabled()) {
      LOG.debug("Pending deleted dir name: {}",
          pendingDeletedDirInfo.getKeyName());
    }

    VolumeBucketId volumeBucketId = keyManager.getMetadataManager()
        .getVolumeBucketIdPairFSO(delDirName);

    // step-1: get all sub directories under the deletedDir
    int remainingNum = remainNum.get();
    DeleteKeysResult subDirDeleteResult =
        keyManager.getPendingDeletionSubDirs(volumeBucketId.getVolumeId(), volumeBucketId.getBucketId(),
            pendingDeletedDirInfo, keyInfo -> true, remainingNum);
    List<OmKeyInfo> subDirs = subDirDeleteResult.getKeysToDelete();
    subDirs = subDirs.stream()
        .map(omKeyInfo -> omKeyInfo.toBuilder().setAcls(Collections.emptyList()).build())
        .collect(Collectors.toList());
    remainNum.addAndGet(-subDirs.size());

    OMMetadataManager omMetadataManager = keyManager.getMetadataManager();
    for (OmKeyInfo dirInfo : subDirs) {
      String ozoneDbKey = omMetadataManager.getOzonePathKey(volumeBucketId.getVolumeId(),
          volumeBucketId.getBucketId(), dirInfo.getParentObjectID(), dirInfo.getFileName());
      String ozoneDeleteKey = omMetadataManager.getOzoneDeletePathKey(
          dirInfo.getObjectID(), ozoneDbKey);
      subDirList.add(Pair.of(ozoneDeleteKey, dirInfo));
      LOG.debug("Moved sub dir name: {}", dirInfo.getKeyName());
    }

    // step-2: get all sub files under the deletedDir
    // Only remove sub files if the parent directory is going to be deleted or can be reclaimed.
    remainingNum = remainNum.get();
    DeleteKeysResult subFileDeleteResult =
        keyManager.getPendingDeletionSubFiles(volumeBucketId.getVolumeId(), volumeBucketId.getBucketId(),
            pendingDeletedDirInfo, keyInfo -> purgeDir || reclaimableFileFilter.apply(keyInfo), remainingNum);
    List<OmKeyInfo> subFiles = subFileDeleteResult.getKeysToDelete();
    subFiles = subFiles.stream()
        .map(omKeyInfo -> omKeyInfo.toBuilder().setAcls(Collections.emptyList()).build())
        .collect(Collectors.toList());
    remainNum.addAndGet(-subFiles.size());

    if (LOG.isDebugEnabled()) {
      for (OmKeyInfo fileInfo : subFiles) {
        LOG.debug("Moved sub file name: {}", fileInfo.getKeyName());
      }
    }

    // step-3: If both sub-dirs and sub-files are exhausted under a parent
    // directory, only then delete the parent.
    String purgeDeletedDir =
        purgeDir && subDirDeleteResult.isProcessedKeys() && subFileDeleteResult.isProcessedKeys() ? delDirName : null;
    if (purgeDeletedDir == null && subFiles.isEmpty() && subDirs.isEmpty()) {
      return Optional.empty();
    }
    if (purgeDeletedDir != null) {
      remainNum.addAndGet(-1);
    }
    return Optional.of(
        wrapPurgeRequest(volumeBucketId.getVolumeId(), volumeBucketId.getBucketId(), purgeDeletedDir, subFiles, subDirs,
            subDirDeleteResult.getKeyRanges(), subFileDeleteResult.getKeyRanges()));
  }

  private OzoneManagerProtocolProtos.PurgePathRequest wrapPurgeRequest(
      final long volumeId,
      final long bucketId,
      final String purgeDeletedDir,
      final List<OmKeyInfo> purgeDeletedFiles,
      final List<OmKeyInfo> markDirsAsDeleted,
      List<DeleteKeysResult.ExclusiveRange> dirExclusiveRanges,
      List<DeleteKeysResult.ExclusiveRange> fileExclusiveRanges) {
    // Put all keys to be purged in a list
    PurgePathRequest.Builder purgePathsRequest = PurgePathRequest.newBuilder();
    purgePathsRequest.setVolumeId(volumeId);
    purgePathsRequest.setBucketId(bucketId);

    if (purgeDeletedDir != null) {
      purgePathsRequest.setDeletedDir(purgeDeletedDir);
    }

    for (OmKeyInfo purgeFile : purgeDeletedFiles) {
      purgePathsRequest.addDeletedSubFiles(
          purgeFile.getProtobuf(true, ClientVersion.CURRENT_VERSION));
    }

    // Add these directories to deletedDirTable, so that its sub-paths will be
    // traversed in next iteration to ensure cleanup all sub-children.
    for (OmKeyInfo dir : markDirsAsDeleted) {
      purgePathsRequest.addMarkDeletedSubDirs(
          dir.getProtobuf(ClientVersion.CURRENT_VERSION));
    }

    for (DeleteKeysResult.ExclusiveRange range : dirExclusiveRanges) {
      purgePathsRequest.addDeleteRangeSubDirs(
          HddsProtos.KeyValue.newBuilder().setKey(range.getStartKey()).setValue(range.getExclusiveEndKey()).build());
    }

    for (DeleteKeysResult.ExclusiveRange range : fileExclusiveRanges) {
      purgePathsRequest.addDeleteRangeSubFiles(
          HddsProtos.KeyValue.newBuilder().setKey(range.getStartKey()).setValue(range.getExclusiveEndKey()).build());
    }

    return purgePathsRequest.build();
  }

  private List<OzoneManagerProtocolProtos.OMResponse> submitPurgePathsWithBatching(List<PurgePathRequest> requests,
      String snapTableKey, UUID expectedPreviousSnapshotId, Map<VolumeBucketId, BucketNameInfo> bucketNameInfoMap) {

    List<OzoneManagerProtocolProtos.OMResponse> responses = new ArrayList<>();
    List<PurgePathRequest> purgePathRequestBatch = new ArrayList<>();
    long batchBytes = 0;

    for (PurgePathRequest req : requests) {
      int reqSize = req.getSerializedSize();

      // If adding this request would exceed the limit, flush the current batch first
      if (batchBytes + reqSize > ratisByteLimit && !purgePathRequestBatch.isEmpty()) {
        OzoneManagerProtocolProtos.OMResponse resp =
            submitPurgeRequest(snapTableKey, expectedPreviousSnapshotId, bucketNameInfoMap, purgePathRequestBatch);
        if (!resp.getSuccess()) {
          return Collections.emptyList();
        }
        responses.add(resp);
        purgePathRequestBatch.clear();
        batchBytes = 0;
      }

      // Add current request to batch
      purgePathRequestBatch.add(req);
      batchBytes += reqSize;
    }

    // Flush remaining batch if any
    if (!purgePathRequestBatch.isEmpty()) {
      OzoneManagerProtocolProtos.OMResponse resp =
          submitPurgeRequest(snapTableKey, expectedPreviousSnapshotId, bucketNameInfoMap, purgePathRequestBatch);
      if (!resp.getSuccess()) {
        return Collections.emptyList();
      }
      responses.add(resp);
    }

    return responses;
  }

  @VisibleForTesting
  OzoneManagerProtocolProtos.OMResponse submitPurgeRequest(String snapTableKey,
      UUID expectedPreviousSnapshotId, Map<VolumeBucketId, BucketNameInfo> bucketNameInfoMap,
      List<PurgePathRequest> pathRequests) {
    OzoneManagerProtocolProtos.PurgeDirectoriesRequest.Builder purgeDirRequest =
        OzoneManagerProtocolProtos.PurgeDirectoriesRequest.newBuilder();

    if (snapTableKey != null) {
      purgeDirRequest.setSnapshotTableKey(snapTableKey);
    }
    OzoneManagerProtocolProtos.NullableUUID.Builder expectedPreviousSnapshotNullableUUID =
        OzoneManagerProtocolProtos.NullableUUID.newBuilder();
    if (expectedPreviousSnapshotId != null) {
      expectedPreviousSnapshotNullableUUID.setUuid(HddsUtils.toProtobuf(expectedPreviousSnapshotId));
    }
    purgeDirRequest.setExpectedPreviousSnapshotID(expectedPreviousSnapshotNullableUUID.build());

    purgeDirRequest.addAllDeletedPath(pathRequests);
    purgeDirRequest.addAllBucketNameInfos(pathRequests.stream()
        .map(purgePathRequest -> new VolumeBucketId(purgePathRequest.getVolumeId(), purgePathRequest.getBucketId()))
        .distinct().map(bucketNameInfoMap::get).filter(Objects::nonNull).collect(Collectors.toList()));

    OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder().setCmdType(OzoneManagerProtocolProtos.Type.PurgeDirectories)
            .setPurgeDirectoriesRequest(purgeDirRequest).setClientId(getClientId().toString()).build();

    // Submit Purge paths request to OM. Acquire bootstrap lock when processing deletes for snapshots.
    try {
      return submitRequest(omRequest);
    } catch (ServiceException e) {
      LOG.error("PurgePaths request failed. Will retry at next run.", e);
    }
    return null;
  }

  @VisibleForTesting
  final class DirDeletingTask implements BackgroundTask {
    private final UUID snapshotId;

    DirDeletingTask(UUID snapshotId) {
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
    @VisibleForTesting
    void processDeletedDirsForStore(SnapshotInfo currentSnapshotInfo, KeyManager keyManager, long rnCnt, int remainNum)
        throws IOException, ExecutionException, InterruptedException {
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
              return processDeletedDirectories(currentSnapshotInfo, keyManager, dirSupplier,
                  expectedPreviousSnapshotId, exclusiveSizeMap, rnCnt, remainNum);
            } catch (Throwable e) {
              return false;
            }
          }, isThreadPoolActive(deletionThreadPool) ? deletionThreadPool : ForkJoinPool.commonPool());
          processedAllDeletedDirs = processedAllDeletedDirs.thenCombine(future, (a, b) -> a && b);
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
     * @param expectedPreviousSnapshotId The UUID of the previous snapshot expected in the chain.
     * @param totalExclusiveSizeMap A map for storing total exclusive size and exclusive replicated size
     *                              for each snapshot.
     * @param runCount The number of times the processing task has been executed.
     * @param remaining Number of dirs to be processed.
     * @return A boolean indicating whether the processed directory list is empty.
     */
    private boolean processDeletedDirectories(SnapshotInfo currentSnapshotInfo, KeyManager keyManager,
        DeletedDirSupplier dirSupplier, UUID expectedPreviousSnapshotId,
        Map<UUID, Pair<Long, Long>> totalExclusiveSizeMap, long runCount, int remaining) {
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
        List<PurgePathRequest> purgePathRequestList = new ArrayList<>();
        Map<VolumeBucketId, BucketNameInfo> bucketNameInfos = new HashMap<>();
        AtomicInteger remainNum = new AtomicInteger(remaining);

        List<Pair<String, OmKeyInfo>> allSubDirList = new ArrayList<>();
        while (remainNum.get() > 0) {
          KeyValue<String, OmKeyInfo> pendingDeletedDirInfo = dirSupplier.get();
          if (pendingDeletedDirInfo == null) {
            break;
          }
          OmKeyInfo deletedDirInfo = pendingDeletedDirInfo.getValue();
          VolumeBucketId volumeBucketId =
              keyManager.getMetadataManager().getVolumeBucketIdPairFSO(pendingDeletedDirInfo.getKey());
          bucketNameInfos.computeIfAbsent(volumeBucketId,
              (k) -> BucketNameInfo.newBuilder().setVolumeId(volumeBucketId.getVolumeId())
              .setBucketId(volumeBucketId.getBucketId())
              .setVolumeName(deletedDirInfo.getVolumeName())
              .setBucketName(deletedDirInfo.getBucketName())
              .build());

          boolean isDirReclaimable = reclaimableDirFilter.apply(pendingDeletedDirInfo);
          Optional<PurgePathRequest> request = prepareDeleteDirRequest(
              pendingDeletedDirInfo.getValue(),
              pendingDeletedDirInfo.getKey(), isDirReclaimable, allSubDirList,
              getOzoneManager().getKeyManager(), reclaimableFileFilter, remainNum);
          if (!request.isPresent()) {
            continue;
          }
          PurgePathRequest purgePathRequest = request.get();
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
            startTime, getOzoneManager().getKeyManager(),
            reclaimableDirFilter, reclaimableFileFilter, bucketNameInfos, expectedPreviousSnapshotId,
            runCount, remainNum);
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
          } else if (!isPreviousPurgeTransactionFlushed()) {
            return BackgroundTaskResult.EmptyTaskResult.newResult();
          }
          try (UncheckedAutoCloseableSupplier<OmSnapshot> omSnapshot = snapInfo == null ? null :
              omSnapshotManager.getActiveSnapshot(snapInfo.getVolumeName(), snapInfo.getBucketName(),
                  snapInfo.getName())) {
            KeyManager keyManager = snapInfo == null ? getOzoneManager().getKeyManager()
                : omSnapshot.get().getKeyManager();
            processDeletedDirsForStore(snapInfo, keyManager, run, pathLimitPerTask);
          }
        } catch (IOException | ExecutionException e) {
          LOG.error("Error while running delete files background task for store {}. Will retry at next run.",
              snapInfo, e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.error("Interruption running delete directory background task for store {}.",
              snapInfo, e);
        }
      }
      // By design, no one cares about the results of this call back.
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }
  }
}
