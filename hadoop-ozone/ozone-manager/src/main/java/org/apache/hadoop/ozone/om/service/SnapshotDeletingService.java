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

import static org.apache.hadoop.hdds.HddsUtils.toProtobuf;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DELETING_LIMIT_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.SNAPSHOT_GC_LOCK;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;
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
import org.apache.hadoop.ozone.om.snapshot.MultiSnapshotLocks;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveTableKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotPurgeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background Service to clean-up deleted snapshot and reclaim space.
 */
public class SnapshotDeletingService extends AbstractKeyDeletingService {
  private static final Logger LOG =
      LoggerFactory.getLogger(SnapshotDeletingService.class);

  // Use only a single thread for Snapshot Deletion. Multiple threads would read
  // from the same table and can send deletion requests for same snapshot
  // multiple times.
  private static final int SNAPSHOT_DELETING_CORE_POOL_SIZE = 1;
  private final ClientId clientId = ClientId.randomId();

  private final OzoneManager ozoneManager;
  private final OmSnapshotManager omSnapshotManager;
  private final SnapshotChainManager chainManager;
  private final OzoneConfiguration conf;
  private final AtomicLong successRunCount;
  private final int keyLimitPerTask;
  private final int snapshotDeletionPerTask;
  private final int ratisByteLimit;
  private final MultiSnapshotLocks snapshotIdLocks;
  private final List<UUID> lockIds;

  public SnapshotDeletingService(long interval, long serviceTimeout,
                                 OzoneManager ozoneManager)
      throws IOException {
    super(SnapshotDeletingService.class.getSimpleName(), interval,
        TimeUnit.MILLISECONDS, SNAPSHOT_DELETING_CORE_POOL_SIZE,
        serviceTimeout, ozoneManager);
    this.ozoneManager = ozoneManager;
    this.omSnapshotManager = ozoneManager.getOmSnapshotManager();
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl)
        ozoneManager.getMetadataManager();
    this.chainManager = omMetadataManager.getSnapshotChainManager();
    this.successRunCount = new AtomicLong(0);
    this.conf = ozoneManager.getConfiguration();
    this.snapshotDeletionPerTask = conf.getInt(SNAPSHOT_DELETING_LIMIT_PER_TASK,
        SNAPSHOT_DELETING_LIMIT_PER_TASK_DEFAULT);
    int limit = (int) conf.getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    // always go to 90% of max limit for request as other header will be added
    this.ratisByteLimit = (int) (limit * 0.9);
    this.keyLimitPerTask = conf.getInt(
        OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK,
        OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK_DEFAULT);
    IOzoneManagerLock lock = getOzoneManager().getMetadataManager().getLock();
    this.snapshotIdLocks = new MultiSnapshotLocks(lock, SNAPSHOT_GC_LOCK, true, 2);
    this.lockIds = new ArrayList<>(2);
  }

  @VisibleForTesting
  final class SnapshotDeletingTask implements BackgroundTask {

    @SuppressWarnings("checkstyle:MethodLength")
    @Override
    public BackgroundTaskResult call() throws InterruptedException {
      if (!shouldRun()) {
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }

      getRunCount().incrementAndGet();

      try {
        int remaining = keyLimitPerTask;
        Iterator<UUID> iterator = chainManager.iterator(true);
        List<String> snapshotsToBePurged = new ArrayList<>();
        long snapshotLimit = snapshotDeletionPerTask;
        while (iterator.hasNext() && snapshotLimit > 0 && remaining > 0) {
          SnapshotInfo snapInfo = SnapshotUtils.getSnapshotInfo(ozoneManager, chainManager, iterator.next());
          if (shouldIgnoreSnapshot(snapInfo)) {
            LOG.debug("Skipping Snapshot Deletion processing because " +
                "the snapshot is active or DB changes are not flushed: {}", snapInfo.getTableKey());
            continue;
          }
          LOG.info("Started Snapshot Deletion Processing for snapshot : {}", snapInfo.getTableKey());
          SnapshotInfo nextSnapshot = SnapshotUtils.getNextSnapshot(ozoneManager, chainManager, snapInfo);
          // Continue if the next snapshot is not active. This is to avoid unnecessary copies from one snapshot to
          // another.
          if (nextSnapshot != null &&
              nextSnapshot.getSnapshotStatus() != SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE) {
            LOG.info("Skipping Snapshot Deletion processing for : {} because the next snapshot is DELETED.",
                snapInfo.getTableKey());
            continue;
          }
          // nextSnapshot = null means entries would be moved to AOS.
          if (nextSnapshot == null) {
            LOG.info("Snapshot: {} entries will be moved to AOS.", snapInfo.getTableKey());
          } else {
            LOG.info("Snapshot: {} entries will be moved to next active snapshot: {}",
                snapInfo.getTableKey(), nextSnapshot.getTableKey());
          }
          lockIds.clear();
          lockIds.add(snapInfo.getSnapshotId());
          if (nextSnapshot != null) {
            lockIds.add(nextSnapshot.getSnapshotId());
          }
          // Acquire write lock on current snapshot and next snapshot in chain.
          if (!snapshotIdLocks.acquireLock(lockIds).isLockAcquired()) {
            continue;
          }
          try (UncheckedAutoCloseableSupplier<OmSnapshot> snapshot = omSnapshotManager.getSnapshot(
              snapInfo.getVolumeName(), snapInfo.getBucketName(), snapInfo.getName())) {
            KeyManager snapshotKeyManager = snapshot.get().getKeyManager();
            int moveCount = 0;
            // Get all entries from deletedKeyTable.
            List<Table.KeyValue<String, List<OmKeyInfo>>> deletedKeyEntries =
                snapshotKeyManager.getDeletedKeyEntries(snapInfo.getVolumeName(), snapInfo.getBucketName(),
                    null, (kv) -> true, remaining);
            moveCount += deletedKeyEntries.size();
            // Get all entries from deletedDirTable.
            List<Table.KeyValue<String, OmKeyInfo>> deletedDirEntries = snapshotKeyManager.getDeletedDirEntries(
                snapInfo.getVolumeName(), snapInfo.getBucketName(), remaining - moveCount);
            moveCount += deletedDirEntries.size();
            // Get all entries from snapshotRenamedTable.
            List<Table.KeyValue<String, String>> renameEntries =
                snapshotKeyManager.getRenamesKeyEntries(snapInfo.getVolumeName(), snapInfo.getBucketName(), null,
                    (kv) -> true, remaining - moveCount);
            moveCount += renameEntries.size();
            if (moveCount > 0) {
              List<SnapshotMoveKeyInfos> deletedKeys = new ArrayList<>(deletedKeyEntries.size());
              List<SnapshotMoveKeyInfos> deletedDirs = new ArrayList<>(deletedDirEntries.size());
              List<HddsProtos.KeyValue> renameKeys = new ArrayList<>(renameEntries.size());

              // Convert deletedKeyEntries to SnapshotMoveKeyInfos.
              for (Table.KeyValue<String, List<OmKeyInfo>> deletedEntry : deletedKeyEntries) {
                deletedKeys.add(SnapshotMoveKeyInfos.newBuilder().setKey(deletedEntry.getKey())
                    .addAllKeyInfos(deletedEntry.getValue()
                        .stream().map(val -> val.getProtobuf(ClientVersion.CURRENT.serialize()))
                        .collect(Collectors.toList())).build());
              }

              // Convert deletedDirEntries to SnapshotMoveKeyInfos.
              for (Table.KeyValue<String, OmKeyInfo> deletedDirEntry : deletedDirEntries) {
                deletedDirs.add(SnapshotMoveKeyInfos.newBuilder().setKey(deletedDirEntry.getKey())
                    .addKeyInfos(deletedDirEntry.getValue().getProtobuf(ClientVersion.CURRENT.serialize())).build());
              }

              // Convert renamedEntries to KeyValue.
              for (Table.KeyValue<String, String> renameEntry : renameEntries) {
                renameKeys.add(HddsProtos.KeyValue.newBuilder().setKey(renameEntry.getKey())
                    .setValue(renameEntry.getValue()).build());
              }
              int submitted = submitSnapshotMoveDeletedKeysWithBatching(snapInfo, deletedKeys, renameKeys, deletedDirs);
              remaining -= submitted;
            } else {
              snapshotsToBePurged.add(snapInfo.getTableKey());
            }
          } finally {
            snapshotIdLocks.releaseLock();
          }
          successRunCount.incrementAndGet();
          snapshotLimit--;
        }
        if (!snapshotsToBePurged.isEmpty()) {
          submitSnapshotPurgeRequest(snapshotsToBePurged);
        }
      } catch (IOException e) {
        LOG.error("Error while running Snapshot Deleting Service", e);
      }
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }

    private void submitSnapshotPurgeRequest(List<String> purgeSnapshotKeys) {
      if (!purgeSnapshotKeys.isEmpty()) {
        SnapshotPurgeRequest snapshotPurgeRequest = SnapshotPurgeRequest
            .newBuilder()
            .addAllSnapshotDBKeys(purgeSnapshotKeys)
            .build();

        OMRequest omRequest = OMRequest.newBuilder()
            .setCmdType(Type.SnapshotPurge)
            .setSnapshotPurgeRequest(snapshotPurgeRequest)
            .setClientId(clientId.toString())
            .build();

        submitOMRequest(omRequest);
      }
    }

    /**
     * Submits a single batch of snapshot move requests.
     *
     * @param snapInfo The snapshot being processed
     * @param deletedKeys List of deleted keys to move
     * @param renamedList List of renamed keys
     * @param dirsToMove List of deleted directories to move
     * @return true if submission was successful, false otherwise
     */
    private boolean submitSingleSnapshotMoveBatch(SnapshotInfo snapInfo,
                                                   List<SnapshotMoveKeyInfos> deletedKeys,
                                                   List<HddsProtos.KeyValue> renamedList,
                                                   List<SnapshotMoveKeyInfos> dirsToMove) {
      SnapshotMoveTableKeysRequest.Builder moveDeletedKeys = SnapshotMoveTableKeysRequest.newBuilder()
          .setFromSnapshotID(toProtobuf(snapInfo.getSnapshotId()));

      if (!deletedKeys.isEmpty()) {
        moveDeletedKeys.addAllDeletedKeys(deletedKeys);
      }

      if (!renamedList.isEmpty()) {
        moveDeletedKeys.addAllRenamedKeys(renamedList);
      }

      if (!dirsToMove.isEmpty()) {
        moveDeletedKeys.addAllDeletedDirs(dirsToMove);
      }

      OMRequest omRequest = OMRequest.newBuilder()
          .setCmdType(Type.SnapshotMoveTableKeys)
          .setSnapshotMoveTableKeysRequest(moveDeletedKeys.build())
          .setClientId(clientId.toString())
          .build();

      try {
        OzoneManagerProtocolProtos.OMResponse response = submitRequest(omRequest);
        if (response == null || !response.getSuccess()) {
          LOG.error("SnapshotMoveTableKeys request failed. Will retry in the next run.");
          return false;
        }
        return true;
      } catch (ServiceException e) {
        LOG.error("SnapshotMoveTableKeys request failed. Will retry in the next run", e);
        return false;
      }
    }

    /**
     * Submits snapshot move requests with batching to respect the Ratis buffer limit.
     * This method progressively builds batches while checking size limits before adding entries.
     *
     * @param snapInfo The snapshot being processed
     * @param deletedKeys List of deleted keys to move
     * @param renamedList List of renamed keys
     * @param dirsToMove List of deleted directories to move
     * @return The number of entries successfully submitted
     */
    @VisibleForTesting
    public int submitSnapshotMoveDeletedKeysWithBatching(SnapshotInfo snapInfo,
                                                           List<SnapshotMoveKeyInfos> deletedKeys,
                                                           List<HddsProtos.KeyValue> renamedList,
                                                           List<SnapshotMoveKeyInfos> dirsToMove) {
      List<SnapshotMoveKeyInfos> currentDeletedKeys = new ArrayList<>();
      List<HddsProtos.KeyValue> currentRenamedKeys = new ArrayList<>();
      List<SnapshotMoveKeyInfos> currentDeletedDirs = new ArrayList<>();
      int totalSubmitted = 0;
      int batchCount = 0;

      SnapshotMoveTableKeysRequest emptyRequest = SnapshotMoveTableKeysRequest.newBuilder()
          .setFromSnapshotID(toProtobuf(snapInfo.getSnapshotId()))
          .build();
      OMRequest baseRequest = OMRequest.newBuilder()
          .setCmdType(Type.SnapshotMoveTableKeys)
          .setSnapshotMoveTableKeysRequest(emptyRequest)
          .setClientId(clientId.toString())
          .build();
      int baseOverhead = baseRequest.getSerializedSize();
      long batchBytes = baseOverhead;

      for (SnapshotMoveKeyInfos key : deletedKeys) {
        int keySize = key.getSerializedSize();

        // If adding this key would exceed the limit, flush the current batch first
        if (batchBytes + keySize > ratisByteLimit && !currentDeletedKeys.isEmpty()) {
          batchCount++;
          LOG.debug("Submitting batch {} for snapshot {} with {} deletedKeys, {} renamedKeys, {} deletedDirs, " +
                  "size: {} bytes", batchCount, snapInfo.getTableKey(), currentDeletedKeys.size(),
              currentRenamedKeys.size(), currentDeletedDirs.size(), batchBytes);

          if (!submitSingleSnapshotMoveBatch(snapInfo, currentDeletedKeys, currentRenamedKeys, currentDeletedDirs)) {
            return totalSubmitted;
          }

          totalSubmitted += currentDeletedKeys.size();
          currentDeletedKeys.clear();
          currentRenamedKeys.clear();
          currentDeletedDirs.clear();
          batchBytes = baseOverhead;
        }

        currentDeletedKeys.add(key);
        batchBytes += keySize;
      }

      for (HddsProtos.KeyValue renameKey : renamedList) {
        int keySize = renameKey.getSerializedSize();

        // If adding this key would exceed the limit, flush the current batch first
        if (batchBytes + keySize > ratisByteLimit && 
            (!currentDeletedKeys.isEmpty() || !currentRenamedKeys.isEmpty())) {
          batchCount++;
          LOG.debug("Submitting batch {} for snapshot {} with {} deletedKeys, {} renamedKeys, {} deletedDirs, " +
                  "size: {} bytes", batchCount, snapInfo.getTableKey(), currentDeletedKeys.size(),
              currentRenamedKeys.size(), currentDeletedDirs.size(), batchBytes);

          if (!submitSingleSnapshotMoveBatch(snapInfo, currentDeletedKeys, currentRenamedKeys, currentDeletedDirs)) {
            return totalSubmitted;
          }

          totalSubmitted += currentDeletedKeys.size() + currentRenamedKeys.size();
          currentDeletedKeys.clear();
          currentRenamedKeys.clear();
          currentDeletedDirs.clear();
          batchBytes = baseOverhead;
        }

        currentRenamedKeys.add(renameKey);
        batchBytes += keySize;
      }

      for (SnapshotMoveKeyInfos dir : dirsToMove) {
        int dirSize = dir.getSerializedSize();

        // If adding this dir would exceed the limit, flush the current batch first
        if (batchBytes + dirSize > ratisByteLimit && 
            (!currentDeletedKeys.isEmpty() || !currentRenamedKeys.isEmpty() || !currentDeletedDirs.isEmpty())) {
          batchCount++;
          LOG.debug("Submitting batch {} for snapshot {} with {} deletedKeys, {} renamedKeys, {} deletedDirs, " +
                  "size: {} bytes", batchCount, snapInfo.getTableKey(), currentDeletedKeys.size(),
              currentRenamedKeys.size(), currentDeletedDirs.size(), batchBytes);

          if (!submitSingleSnapshotMoveBatch(snapInfo, currentDeletedKeys, currentRenamedKeys, currentDeletedDirs)) {
            return totalSubmitted;
          }

          totalSubmitted += currentDeletedKeys.size() + currentRenamedKeys.size() + currentDeletedDirs.size();
          currentDeletedKeys.clear();
          currentRenamedKeys.clear();
          currentDeletedDirs.clear();
          batchBytes = baseOverhead;
        }

        currentDeletedDirs.add(dir);
        batchBytes += dirSize;
      }

      // Submit the final batch if any
      if (!currentDeletedKeys.isEmpty() || !currentRenamedKeys.isEmpty() || !currentDeletedDirs.isEmpty()) {
        batchCount++;
        LOG.debug("Submitting final batch {} for snapshot {} with {} deletedKeys, {} renamedKeys, {} deletedDirs, " +
                "size: {} bytes", batchCount, snapInfo.getTableKey(), currentDeletedKeys.size(),
            currentRenamedKeys.size(), currentDeletedDirs.size(), batchBytes);

        if (!submitSingleSnapshotMoveBatch(snapInfo, currentDeletedKeys, currentRenamedKeys, currentDeletedDirs)) {
          return totalSubmitted;
        }

        totalSubmitted += currentDeletedKeys.size() + currentRenamedKeys.size() + currentDeletedDirs.size();
      }

      LOG.debug("Successfully submitted {} total entries in {} batches for snapshot {}", totalSubmitted, batchCount,
          snapInfo.getTableKey());

      return totalSubmitted;
    }

    private void submitOMRequest(OMRequest omRequest) {
      try {
        Status status = submitRequest(omRequest).getStatus();
        if (!Objects.equals(status, Status.OK)) {
          LOG.error("Request: {} failed with an status: {}. Will retry in the next run.", omRequest, status);
        }
      } catch (ServiceException e) {
        LOG.error("Request: {} fired by SnapshotDeletingService failed. Will retry in the next run", omRequest, e);
      }
    }
  }

  /**
   * Checks if a given snapshot has been deleted and all the changes made to snapshot have been flushed to disk.
   * @param snapInfo SnapshotInfo corresponding to the snapshot.
   * @return true if the snapshot is still active or changes to snapshot have not been flushed to disk otherwise false.
   * @throws IOException
   */
  @VisibleForTesting
  boolean shouldIgnoreSnapshot(SnapshotInfo snapInfo) throws IOException {
    SnapshotInfo.SnapshotStatus snapshotStatus = snapInfo.getSnapshotStatus();
    return snapshotStatus != SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED ||
        !OmSnapshotManager.areSnapshotChangesFlushedToDB(getOzoneManager().getMetadataManager(), snapInfo);
  }

  @Override
  public DeletingServiceTaskQueue getTasks() {
    DeletingServiceTaskQueue queue = new DeletingServiceTaskQueue();
    queue.add(new SnapshotDeletingTask());
    return queue;
  }

  public long getSuccessfulRunCount() {
    return successRunCount.get();
  }
}
