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

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK_DEFAULT;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult.EmptyTaskResult;
import org.apache.hadoop.ozone.common.DeletedBlockGroup;
import org.apache.hadoop.ozone.om.DeletingServiceMetrics;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.PendingKeysDeletion;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableKeyFilter;
import org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableRenameEntryFilter;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetSnapshotPropertyRequest;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the background service to delete keys. Scan the metadata of om
 * periodically to get the keys from DeletedTable and ask scm to delete
 * metadata accordingly, if scm returns success for keys, then clean up those
 * keys.
 */
public class KeyDeletingService extends AbstractKeyDeletingService {
  private static final Logger LOG =
      LoggerFactory.getLogger(KeyDeletingService.class);

  private int keyLimitPerTask;
  private final AtomicLong deletedKeyCount;
  private final AtomicBoolean suspended;
  private final AtomicBoolean isRunningOnAOS;
  private final boolean deepCleanSnapshots;
  private final SnapshotChainManager snapshotChainManager;
  private DeletingServiceMetrics metrics;

  public KeyDeletingService(OzoneManager ozoneManager,
      ScmBlockLocationProtocol scmClient, long serviceInterval,
      long serviceTimeout, ConfigurationSource conf, int keyDeletionCorePoolSize,
      boolean deepCleanSnapshots) {
    super(KeyDeletingService.class.getSimpleName(), serviceInterval,
        TimeUnit.MILLISECONDS, keyDeletionCorePoolSize,
        serviceTimeout, ozoneManager, scmClient);
    this.keyLimitPerTask = conf.getInt(OZONE_KEY_DELETING_LIMIT_PER_TASK,
        OZONE_KEY_DELETING_LIMIT_PER_TASK_DEFAULT);
    Preconditions.checkArgument(keyLimitPerTask >= 0,
        OZONE_KEY_DELETING_LIMIT_PER_TASK + " cannot be negative.");
    this.deletedKeyCount = new AtomicLong(0);
    this.suspended = new AtomicBoolean(false);
    this.isRunningOnAOS = new AtomicBoolean(false);
    this.deepCleanSnapshots = deepCleanSnapshots;
    this.snapshotChainManager = ((OmMetadataManagerImpl)ozoneManager.getMetadataManager()).getSnapshotChainManager();
    this.metrics = ozoneManager.getDeletionMetrics();
  }

  /**
   * Returns the number of keys deleted by the background service.
   *
   * @return Long count.
   */
  @VisibleForTesting
  public AtomicLong getDeletedKeyCount() {
    return deletedKeyCount;
  }

  public boolean isRunningOnAOS() {
    return isRunningOnAOS.get();
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new KeyDeletingTask(this, null));
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
        queue.add(new KeyDeletingTask(this, snapshotId));
      }
    }
    return queue;
  }

  private boolean shouldRun() {
    if (getOzoneManager() == null) {
      // OzoneManager can be null for testing
      return true;
    }
    return !suspended.get() && getOzoneManager().isLeaderReady();
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

  public int getKeyLimitPerTask() {
    return keyLimitPerTask;
  }

  public void setKeyLimitPerTask(int keyLimitPerTask) {
    this.keyLimitPerTask = keyLimitPerTask;
  }

  /**
   * A key deleting task scans OM DB and looking for a certain number of
   * pending-deletion keys, sends these keys along with their associated blocks
   * to SCM for deletion. Once SCM confirms keys are deleted (once SCM persisted
   * the blocks info in its deletedBlockLog), it removes these keys from the
   * DB.
   */
  private final class KeyDeletingTask implements BackgroundTask {
    private final KeyDeletingService deletingService;
    private final UUID snapshotId;

    private KeyDeletingTask(KeyDeletingService service, UUID snapshotId) {
      this.deletingService = service;
      this.snapshotId = snapshotId;
    }

    private OzoneManagerProtocolProtos.SetSnapshotPropertyRequest getSetSnapshotRequestUpdatingExclusiveSize(
        Map<UUID, Long> exclusiveSizeMap, Map<UUID, Long> exclusiveReplicatedSizeMap, UUID snapshotID) {
      OzoneManagerProtocolProtos.SnapshotSize snapshotSize = OzoneManagerProtocolProtos.SnapshotSize.newBuilder()
          .setExclusiveSize(
              exclusiveSizeMap.getOrDefault(snapshotID, 0L))
          .setExclusiveReplicatedSize(
              exclusiveReplicatedSizeMap.getOrDefault(
                  snapshotID, 0L))
          .build();

      return OzoneManagerProtocolProtos.SetSnapshotPropertyRequest.newBuilder()
          .setSnapshotKey(snapshotChainManager.getTableKey(snapshotID))
          .setSnapshotSize(snapshotSize)
          .build();
    }

    /**
     *
     * @param currentSnapshotInfo if null, deleted directories in AOS should be processed.
     * @param keyManager KeyManager of the underlying store.
     */
    private void processDeletedKeysForStore(SnapshotInfo currentSnapshotInfo, KeyManager keyManager,
        int remainNum) throws IOException, InterruptedException {
      String volume = null, bucket = null, snapshotTableKey = null;
      if (currentSnapshotInfo != null) {
        volume = currentSnapshotInfo.getVolumeName();
        bucket = currentSnapshotInfo.getBucketName();
        snapshotTableKey = currentSnapshotInfo.getTableKey();
      }

      boolean successStatus = true;
      try {
        // TODO: [SNAPSHOT] HDDS-7968. Reclaim eligible key blocks in
        //  snapshot's deletedTable when active DB's deletedTable
        //  doesn't have enough entries left.
        //  OM would have to keep track of which snapshot the key is coming
        //  from if the above would be done inside getPendingDeletionKeys().
        OmSnapshotManager omSnapshotManager = getOzoneManager().getOmSnapshotManager();
        // This is to avoid race condition b/w purge request and snapshot chain update. For AOS taking the global
        // snapshotId since AOS could process multiple buckets in one iteration. While using path
        // previous snapshotId for a snapshot since it would process only one bucket.
        UUID expectedPreviousSnapshotId = currentSnapshotInfo == null ?
            snapshotChainManager.getLatestGlobalSnapshotId() :
            SnapshotUtils.getPreviousSnapshotId(currentSnapshotInfo, snapshotChainManager);

        IOzoneManagerLock lock = getOzoneManager().getMetadataManager().getLock();

        // Purge deleted Keys in the deletedTable && rename entries in the snapshotRenamedTable which doesn't have a
        // reference in the previous snapshot.
        try (ReclaimableKeyFilter reclaimableKeyFilter = new ReclaimableKeyFilter(getOzoneManager(),
            omSnapshotManager, snapshotChainManager, currentSnapshotInfo, keyManager, lock);
             ReclaimableRenameEntryFilter renameEntryFilter = new ReclaimableRenameEntryFilter(
                 getOzoneManager(), omSnapshotManager, snapshotChainManager, currentSnapshotInfo,
                 keyManager, lock)) {
          List<String> renamedTableEntries =
              keyManager.getRenamesKeyEntries(volume, bucket, null, renameEntryFilter, remainNum).stream()
                  .map(entry -> {
                    try {
                      return entry.getKey();
                    } catch (IOException e) {
                      throw new UncheckedIOException(e);
                    }
                  }).collect(Collectors.toList());
          remainNum -= renamedTableEntries.size();

          // Get pending keys that can be deleted
          PendingKeysDeletion pendingKeysDeletion = currentSnapshotInfo == null
              ? keyManager.getPendingDeletionKeys(reclaimableKeyFilter, remainNum)
              : keyManager.getPendingDeletionKeys(volume, bucket, null, reclaimableKeyFilter, remainNum);
          List<DeletedBlockGroup> keyBlocksList = pendingKeysDeletion.getKeyBlocksList();
          //submit purge requests if there are renamed entries to be purged or keys to be purged.
          if (!renamedTableEntries.isEmpty() || keyBlocksList != null && !keyBlocksList.isEmpty()) {
            // Validating if the previous snapshot is still the same before purging the blocks.
            SnapshotUtils.validatePreviousSnapshotId(currentSnapshotInfo, snapshotChainManager,
                expectedPreviousSnapshotId);
            Pair<Integer, Boolean> purgeResult = processKeyDeletes(keyBlocksList, pendingKeysDeletion.getKeysToModify(),
                renamedTableEntries, snapshotTableKey, expectedPreviousSnapshotId);
            remainNum -= purgeResult.getKey();
            successStatus = purgeResult.getValue();
            metrics.incrNumKeysProcessed(keyBlocksList.size());
            metrics.incrNumKeysSentForPurge(purgeResult.getKey());
            if (successStatus) {
              deletedKeyCount.addAndGet(purgeResult.getKey());
            }
          }

          // Checking remainNum is greater than zero and not equal to the initial value if there were some keys to
          // reclaim. This is to check if all keys have been iterated over and all the keys necessary have been
          // reclaimed.
          if (remainNum > 0 && successStatus) {
            List<SetSnapshotPropertyRequest> setSnapshotPropertyRequests = new ArrayList<>();
            Map<UUID, Long> exclusiveReplicatedSizeMap = reclaimableKeyFilter.getExclusiveReplicatedSizeMap();
            Map<UUID, Long> exclusiveSizeMap = reclaimableKeyFilter.getExclusiveSizeMap();
            List<UUID> previousPathSnapshotsInChain =
                Stream.of(exclusiveSizeMap.keySet(), exclusiveReplicatedSizeMap.keySet())
                .flatMap(Collection::stream).distinct().collect(Collectors.toList());
            for (UUID snapshot : previousPathSnapshotsInChain) {
              setSnapshotPropertyRequests.add(getSetSnapshotRequestUpdatingExclusiveSize(exclusiveSizeMap,
                  exclusiveReplicatedSizeMap, snapshot));
            }

            // Updating directory deep clean flag of snapshot.
            if (currentSnapshotInfo != null) {
              setSnapshotPropertyRequests.add(OzoneManagerProtocolProtos.SetSnapshotPropertyRequest.newBuilder()
                  .setSnapshotKey(snapshotTableKey)
                  .setDeepCleanedDeletedKey(true)
                  .build());
            }
            submitSetSnapshotRequests(setSnapshotPropertyRequests);
          }
        }
      } catch (UncheckedIOException e) {
        throw e.getCause();
      }
    }

    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() {
      // Check if this is the Leader OM. If not leader, no need to execute this
      // task.
      if (shouldRun()) {
        final long run = getRunCount().incrementAndGet();
        if (snapshotId == null) {
          LOG.debug("Running KeyDeletingService for active object store, {}", run);
          isRunningOnAOS.set(true);
        } else {
          LOG.debug("Running KeyDeletingService for snapshot : {}, {}", snapshotId, run);
        }
        int remainNum = keyLimitPerTask;
        OmSnapshotManager omSnapshotManager = getOzoneManager().getOmSnapshotManager();
        SnapshotInfo snapInfo = null;
        try {
          snapInfo = snapshotId == null ? null :
              SnapshotUtils.getSnapshotInfo(getOzoneManager(), snapshotChainManager, snapshotId);
          if (snapInfo != null) {
            if (!OmSnapshotManager.areSnapshotChangesFlushedToDB(getOzoneManager().getMetadataManager(), snapInfo)) {
              LOG.info("Skipping snapshot processing since changes to snapshot {} have not been flushed to disk",
                  snapInfo);
              return EmptyTaskResult.newResult();
            }
            if (!snapInfo.getDeepCleanedDeletedDir()) {
              LOG.debug("Snapshot {} hasn't done deleted directory deep cleaning yet. Skipping the snapshot in this" +
                  " iteration.", snapInfo);
              return EmptyTaskResult.newResult();
            }
          }
          try (UncheckedAutoCloseableSupplier<OmSnapshot> omSnapshot = snapInfo == null ? null :
              omSnapshotManager.getActiveSnapshot(snapInfo.getVolumeName(), snapInfo.getBucketName(),
                  snapInfo.getName())) {
            KeyManager keyManager = snapInfo == null ? getOzoneManager().getKeyManager()
                : omSnapshot.get().getKeyManager();
            processDeletedKeysForStore(snapInfo, keyManager, remainNum);
          }
        } catch (IOException | InterruptedException e) {
          LOG.error("Error while running delete files background task for store {}. Will retry at next run.",
              snapInfo, e);
        } finally {
          if (snapshotId == null) {
            isRunningOnAOS.set(false);
            synchronized (deletingService) {
              this.deletingService.notify();
            }
          }
        }
      }
      // By design, no one cares about the results of this call back.
      return EmptyTaskResult.newResult();
    }
  }
}
