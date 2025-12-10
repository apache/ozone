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
  private static final int MIN_ERR_LIMIT_PER_TASK = 1000;
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

  private class SnapshotDeletingTask implements BackgroundTask {

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
                        .stream().map(val -> val.getProtobuf(ClientVersion.CURRENT_VERSION))
                        .collect(Collectors.toList())).build());
              }

              // Convert deletedDirEntries to SnapshotMoveKeyInfos.
              for (Table.KeyValue<String, OmKeyInfo> deletedDirEntry : deletedDirEntries) {
                deletedDirs.add(SnapshotMoveKeyInfos.newBuilder().setKey(deletedDirEntry.getKey())
                    .addKeyInfos(deletedDirEntry.getValue().getProtobuf(ClientVersion.CURRENT_VERSION)).build());
              }

              // Convert renamedEntries to KeyValue.
              for (Table.KeyValue<String, String> renameEntry : renameEntries) {
                renameKeys.add(HddsProtos.KeyValue.newBuilder().setKey(renameEntry.getKey())
                    .setValue(renameEntry.getValue()).build());
              }
              submitSnapshotMoveDeletedKeys(snapInfo, deletedKeys, renameKeys, deletedDirs);
              remaining -= moveCount;
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

    private void submitSnapshotMoveDeletedKeys(SnapshotInfo snapInfo,
                                               List<SnapshotMoveKeyInfos> deletedKeys,
                                               List<HddsProtos.KeyValue> renamedList,
                                               List<SnapshotMoveKeyInfos> dirsToMove) {

      SnapshotMoveTableKeysRequest.Builder moveDeletedKeysBuilder = SnapshotMoveTableKeysRequest.newBuilder()
          .setFromSnapshotID(toProtobuf(snapInfo.getSnapshotId()));

      SnapshotMoveTableKeysRequest moveDeletedKeys = moveDeletedKeysBuilder
          .addAllDeletedKeys(deletedKeys)
          .addAllRenamedKeys(renamedList)
          .addAllDeletedDirs(dirsToMove)
          .build();
      if (isBufferLimitCrossed(ratisByteLimit, 0, moveDeletedKeys.getSerializedSize())) {
        int remaining = MIN_ERR_LIMIT_PER_TASK;
        deletedKeys = deletedKeys.subList(0, Math.min(remaining, deletedKeys.size()));
        remaining -= deletedKeys.size();
        renamedList = renamedList.subList(0, Math.min(remaining, renamedList.size()));
        remaining -= renamedList.size();
        dirsToMove = dirsToMove.subList(0, Math.min(remaining, dirsToMove.size()));
        moveDeletedKeys = moveDeletedKeysBuilder
            .addAllDeletedKeys(deletedKeys)
            .addAllRenamedKeys(renamedList)
            .addAllDeletedDirs(dirsToMove)
            .build();
      }

      OMRequest omRequest = OMRequest.newBuilder()
          .setCmdType(Type.SnapshotMoveTableKeys)
          .setSnapshotMoveTableKeysRequest(moveDeletedKeys)
          .setClientId(clientId.toString())
          .build();
      submitOMRequest(omRequest);
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

  @VisibleForTesting
  public void setSuccessRunCount(long num) {
    successRunCount.getAndSet(num);
  }
}
