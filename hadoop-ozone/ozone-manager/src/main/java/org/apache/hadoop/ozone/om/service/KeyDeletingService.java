/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.protobuf.ServiceException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotSize;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetSnapshotPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult.EmptyTaskResult;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK_DEFAULT;

import org.apache.hadoop.ozone.om.PendingKeysDeletion;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.ratis.protocol.ClientId;
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

  // Use only a single thread for KeyDeletion. Multiple threads would read
  // from the same table and can send deletion requests for same key multiple
  // times.
  private static final int KEY_DELETING_CORE_POOL_SIZE = 1;

  private final KeyManager manager;

  private int keyLimitPerTask;
  private final AtomicLong deletedKeyCount;
  private final AtomicBoolean suspended;
  private final Map<String, Long> exclusiveSizeMap;
  private final Map<String, Long> exclusiveReplicatedSizeMap;
  private final Map<String, String> snapshotSeekMap;
  private final boolean deepCleanSnapshots;

  public KeyDeletingService(OzoneManager ozoneManager,
      ScmBlockLocationProtocol scmClient,
      KeyManager manager, long serviceInterval,
      long serviceTimeout, ConfigurationSource conf,
      boolean deepCleanSnapshots) {
    super(KeyDeletingService.class.getSimpleName(), serviceInterval,
        TimeUnit.MILLISECONDS, KEY_DELETING_CORE_POOL_SIZE,
        serviceTimeout, ozoneManager, scmClient);
    this.manager = manager;
    this.keyLimitPerTask = conf.getInt(OZONE_KEY_DELETING_LIMIT_PER_TASK,
        OZONE_KEY_DELETING_LIMIT_PER_TASK_DEFAULT);
    Preconditions.checkArgument(keyLimitPerTask >= 0,
        OZONE_KEY_DELETING_LIMIT_PER_TASK + " cannot be negative.");
    this.deletedKeyCount = new AtomicLong(0);
    this.suspended = new AtomicBoolean(false);
    this.exclusiveSizeMap = new HashMap<>();
    this.exclusiveReplicatedSizeMap = new HashMap<>();
    this.snapshotSeekMap = new HashMap<>();
    this.deepCleanSnapshots = deepCleanSnapshots;
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

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new KeyDeletingTask());
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
  private class KeyDeletingTask implements BackgroundTask {

    private OzoneManagerProtocolProtos.SetSnapshotPropertyRequest getSetSnapshotRequestUpdatingExclusiveSize(
        Map<String, Long> exclusiveSizeMap, Map<String, Long> exclusiveReplicatedSizeMap, String prevSnapshotKeyTable) {
      OzoneManagerProtocolProtos.SnapshotSize snapshotSize = OzoneManagerProtocolProtos.SnapshotSize.newBuilder()
          .setExclusiveSize(
              exclusiveSizeMap.getOrDefault(prevSnapshotKeyTable, 0L))
          .setExclusiveReplicatedSize(
              exclusiveReplicatedSizeMap.getOrDefault(
                  prevSnapshotKeyTable, 0L))
          .build();
      exclusiveSizeMap.remove(prevSnapshotKeyTable);
      exclusiveReplicatedSizeMap.remove(prevSnapshotKeyTable);

      return OzoneManagerProtocolProtos.SetSnapshotPropertyRequest.newBuilder()
          .setSnapshotKey(prevSnapshotKeyTable)
          .setSnapshotSize(snapshotSize)
          .build();
    }

    private OzoneManagerProtocolProtos.SetSnapshotPropertyRequest
    getSetSnapshotPropertyRequestupdatingDeepCleanSnapshotDir(String snapshotKeyTable) {
      return OzoneManagerProtocolProtos.SetSnapshotPropertyRequest.newBuilder()
          .setSnapshotKey(snapshotKeyTable)
          .setDeepCleanedDeletedKey(true)
          .build();
    }

    private void submitSetSnapshotRequest(
        List<OzoneManagerProtocolProtos.SetSnapshotPropertyRequest> setSnapshotPropertyRequests) {
      OzoneManagerProtocolProtos.OMRequest omRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
          .setCmdType(OzoneManagerProtocolProtos.Type.SetSnapshotProperty)
          .addAllSetSnapshotPropertyRequests(setSnapshotPropertyRequests)
          .setClientId(clientId.toString())
          .build();
      submitRequest(omRequest, clientId);
    }


    /**
     *
     * @param currentSnapshotInfo if null, deleted directories in AOS should be processed.
     * @param keyManager KeyManager of the underlying store.
     */
    private int processDeletedKeysForStore(SnapshotInfo currentSnapshotInfo, KeyManager keyManager,
                                            int remainNum) throws IOException {
      String volume = currentSnapshotInfo == null ? null : currentSnapshotInfo.getVolumeName();
      String bucket = currentSnapshotInfo == null ? null : currentSnapshotInfo.getBucketName();
      String snapshotTableKey = currentSnapshotInfo == null ? null : currentSnapshotInfo.getTableKey();

      String startKey = "";
      int initialRemainNum = remainNum;
      boolean successStatus = true;
      try {
        // TODO: [SNAPSHOT] HDDS-7968. Reclaim eligible key blocks in
        //  snapshot's deletedTable when active DB's deletedTable
        //  doesn't have enough entries left.
        //  OM would have to keep track of which snapshot the key is coming
        //  from if the above would be done inside getPendingDeletionKeys().
        OmSnapshotManager omSnapshotManager = getOzoneManager().getOmSnapshotManager();
        SnapshotChainManager snapshotChainManager = ((OmMetadataManagerImpl)getOzoneManager().getMetadataManager())
            .getSnapshotChainManager();
        // This is to avoid race condition b/w purge request and snapshot chain updation. For AOS taking the global
        // snapshotId since AOS could process multiple buckets in one iteration. While using path previous snapshot
        // Id for a snapshot since it would process only one bucket.
        UUID expectedPreviousSnapshotId = currentSnapshotInfo == null ?
            snapshotChainManager.getLatestGlobalSnapshotId() :
            SnapshotUtils.getPreviousSnapshotId(currentSnapshotInfo, snapshotChainManager);

        IOzoneManagerLock lock = getOzoneManager().getMetadataManager().getLock();

        // Purge deleted Keys in the deletedTable && rename entries in the snapshotRenamedTable which doesn't have a
        // reference in the previous snapshot.
        try (ReclaimableKeyFilter reclaimableKeyFilter = new ReclaimableKeyFilter(omSnapshotManager, snapshotChainManager,
            currentSnapshotInfo, keyManager.getMetadataManager(), lock);
             ReclaimableRenameEntryFilter renameEntryFilter = new ReclaimableRenameEntryFilter(
                 omSnapshotManager, snapshotChainManager, currentSnapshotInfo, keyManager.getMetadataManager(), lock)) {
          List<String> renamedTableEntries =
              keyManager.getRenamesKeyEntries(volume, bucket, startKey, renameEntryFilter, remainNum).stream()
              .map(entry -> {
                try {
                  return entry.getKey();
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              }).collect(Collectors.toList());
          remainNum -= renamedTableEntries.size();

          // Get pending keys that can be deleted
          PendingKeysDeletion pendingKeysDeletion = keyManager.getPendingDeletionKeys(volume, bucket, startKey,
              reclaimableKeyFilter, remainNum);
          List<BlockGroup> keyBlocksList = pendingKeysDeletion.getKeyBlocksList();
          //submit purge requests if there are renamed entries to be purged or keys to be purged.
          if (!renamedTableEntries.isEmpty() || keyBlocksList != null && !keyBlocksList.isEmpty()) {
             Pair<Integer, Boolean> purgeResult = processKeyDeletes(keyBlocksList, getOzoneManager().getKeyManager(),
                pendingKeysDeletion.getKeysToModify(), renamedTableEntries, snapshotTableKey, expectedPreviousSnapshotId);
            remainNum -= purgeResult.getKey();
            successStatus = purgeResult.getValue();
          }

          // Checking remainNum is greater than zero and not equal to the initial value if there were some keys to
          // reclaim. This is to check if
          if (remainNum > 0 && successStatus) {
            List<SetSnapshotPropertyRequest> setSnapshotPropertyRequests = new ArrayList<>();
            Map<String, Long> exclusiveReplicatedSizeMap = reclaimableKeyFilter.getExclusiveReplicatedSizeMap();
            Map<String, Long> exclusiveSizeMap = reclaimableKeyFilter.getExclusiveSizeMap();
            List<String> previousPathSnapshotsInChain =
                Stream.of(exclusiveSizeMap.keySet(), exclusiveReplicatedSizeMap.keySet())
                .flatMap(Collection::stream).distinct().collect(Collectors.toList());
            for (String snapshot : previousPathSnapshotsInChain) {
              setSnapshotPropertyRequests.add(getSetSnapshotRequestUpdatingExclusiveSize(exclusiveSizeMap,
                  exclusiveReplicatedSizeMap, snapshot));
            }

            //Updating directory deep clean flag of snapshot.
            if (currentSnapshotInfo != null) {
              setSnapshotPropertyRequests.add(getSetSnapshotPropertyRequestupdatingDeepCleanSnapshotDir(
                  snapshotTableKey));
            }
            submitSetSnapshotRequest(setSnapshotPropertyRequests);
          }
        }

      } catch (IOException e) {
        throw e;
      } catch (UncheckedIOException e) {
        throw e.getCause();
      }
      return remainNum;
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
        LOG.debug("Running KeyDeletingService {}", run);

        int remainNum = keyLimitPerTask;
        try {
          remainNum = processDeletedKeysForStore(null, getOzoneManager().getKeyManager(),
              remainNum);
        } catch (IOException e) {
          LOG.error("Error while running delete directories and files " +
              "background task. Will retry at next run. on active object store", e);
        }

        if (deepCleanSnapshots && remainNum > 0) {
          SnapshotChainManager chainManager =
              ((OmMetadataManagerImpl)getOzoneManager().getMetadataManager()).getSnapshotChainManager();
          OmSnapshotManager omSnapshotManager = getOzoneManager().getOmSnapshotManager();
          Iterator<UUID> iterator = null;
          try {
            iterator = chainManager.iterator(true);

          } catch (IOException e) {
            LOG.error("Error while initializing snapshot chain iterator.");
            return BackgroundTaskResult.EmptyTaskResult.newResult();
          }

          while (iterator.hasNext() && remainNum > 0) {
            UUID snapshotId =  iterator.next();
            try {
              SnapshotInfo snapInfo = SnapshotUtils.getSnapshotInfo(getOzoneManager(), chainManager, snapshotId);
              // Wait for snapshot changes to be flushed to disk.
              if (!OmSnapshotManager.areSnapshotChangesFlushedToDB(getOzoneManager().getMetadataManager(), snapInfo)) {
                LOG.info("Skipping snapshot processing since changes to snapshot {} have not been flushed to disk",
                    snapInfo);
                continue;
              }
              // Check if snapshot has been directory deep cleaned. Return if directory deep cleaning is not
              // done.
              if (!snapInfo.getDeepCleanedDeletedDir()) {
                LOG.debug("Snapshot {} hasn't done deleted directory deep cleaning yet. Skipping the snapshot in this" +
                    " iteration.", snapInfo);
                continue;
              }
              // Checking if snapshot has been key deep cleaned already.
              if (snapInfo.getDeepClean()) {
                LOG.debug("Snapshot {} has already done deleted key deep cleaning.", snapInfo);
                continue;
              }
              try (ReferenceCounted<OmSnapshot> omSnapshot = omSnapshotManager.getSnapshot(snapInfo.getVolumeName(),
                  snapInfo.getBucketName(), snapInfo.getName())) {
                remainNum = processDeletedKeysForStore(snapInfo, omSnapshot.get().getKeyManager(), remainNum);
              }

            } catch (IOException e) {
              LOG.error("Error while running delete directories and files " +
                  "background task for snapshot: {}. Will retry at next run. on active object store", snapshotId, e);
            }
          }
        }
      }
      // By design, no one cares about the results of this call back.
      return EmptyTaskResult.newResult();
    }
  }
}
