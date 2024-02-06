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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.IOmMetadataReader;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.om.snapshot.SnapshotCache;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotSize;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetSnapshotPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult.EmptyTaskResult;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPrefix;
import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK_DEFAULT;

import org.apache.hadoop.ozone.om.PendingKeysDeletion;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
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
  private final Set<String> completedExclusiveSizeSet;
  private final Map<String, String> snapshotSeekMap;

  public KeyDeletingService(OzoneManager ozoneManager,
      ScmBlockLocationProtocol scmClient,
      KeyManager manager, long serviceInterval,
      long serviceTimeout, ConfigurationSource conf) {
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
    this.completedExclusiveSizeSet = new HashSet<>();
    this.snapshotSeekMap = new HashMap<>();
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

        // Acquire active DB deletedTable write lock because of the
        // deletedTable read-write here to avoid interleaving with
        // the table range delete operation in createOmSnapshotCheckpoint()
        // that is called from OMSnapshotCreateResponse#addToDBBatch.
        manager.getMetadataManager().getTableLock(
            OmMetadataManagerImpl.DELETED_TABLE).writeLock().lock();
        int delCount = 0;
        try {
          // TODO: [SNAPSHOT] HDDS-7968. Reclaim eligible key blocks in
          //  snapshot's deletedTable when active DB's deletedTable
          //  doesn't have enough entries left.
          //  OM would have to keep track of which snapshot the key is coming
          //  from if the above would be done inside getPendingDeletionKeys().

          PendingKeysDeletion pendingKeysDeletion = manager
              .getPendingDeletionKeys(getKeyLimitPerTask());
          List<BlockGroup> keyBlocksList = pendingKeysDeletion
              .getKeyBlocksList();
          if (keyBlocksList != null && !keyBlocksList.isEmpty()) {
            delCount = processKeyDeletes(keyBlocksList,
                getOzoneManager().getKeyManager(),
                pendingKeysDeletion.getKeysToModify(), null);
            deletedKeyCount.addAndGet(delCount);
          }
        } catch (IOException e) {
          LOG.error("Error while running delete keys background task. Will " +
              "retry at next run.", e);
        } finally {
          // Release deletedTable write lock
          manager.getMetadataManager().getTableLock(
              OmMetadataManagerImpl.DELETED_TABLE).writeLock().unlock();
        }

        try {
          if (delCount < keyLimitPerTask) {
            processSnapshotDeepClean(delCount);
          }
        } catch (Exception e) {
          LOG.error("Error while running deep clean on snapshots. Will " +
              "retry at next run.", e);
        }

      }
      // By design, no one cares about the results of this call back.
      return EmptyTaskResult.newResult();
    }

    @SuppressWarnings("checkstyle:MethodLength")
    private void processSnapshotDeepClean(int delCount)
        throws IOException {
      OmSnapshotManager omSnapshotManager =
          getOzoneManager().getOmSnapshotManager();
      OmMetadataManagerImpl metadataManager = (OmMetadataManagerImpl)
          getOzoneManager().getMetadataManager();
      SnapshotChainManager snapChainManager = metadataManager
          .getSnapshotChainManager();
      Table<String, SnapshotInfo> snapshotInfoTable =
          getOzoneManager().getMetadataManager().getSnapshotInfoTable();
      List<String> deepCleanedSnapshots = new ArrayList<>();
      try (TableIterator<String, ? extends Table.KeyValue
          <String, SnapshotInfo>> iterator = snapshotInfoTable.iterator()) {

        while (delCount < keyLimitPerTask && iterator.hasNext()) {
          List<BlockGroup> keysToPurge = new ArrayList<>();
          HashMap<String, RepeatedOmKeyInfo> keysToModify = new HashMap<>();
          SnapshotInfo currSnapInfo = iterator.next().getValue();

          // Deep clean only on active snapshot. Deleted Snapshots will be
          // cleaned up by SnapshotDeletingService.
          if (currSnapInfo.getSnapshotStatus() != SNAPSHOT_ACTIVE ||
              currSnapInfo.getDeepClean()) {
            continue;
          }

          try (ReferenceCounted<IOmMetadataReader, SnapshotCache>
              rcCurrOmSnapshot = omSnapshotManager.checkForSnapshot(
                  currSnapInfo.getVolumeName(),
                  currSnapInfo.getBucketName(),
                  getSnapshotPrefix(currSnapInfo.getName()),
                  true)) {
            OmSnapshot currOmSnapshot = (OmSnapshot) rcCurrOmSnapshot.get();

            Table<String, RepeatedOmKeyInfo> snapDeletedTable =
                currOmSnapshot.getMetadataManager().getDeletedTable();
            Table<String, String> snapRenamedTable =
                currOmSnapshot.getMetadataManager().getSnapshotRenamedTable();

            long volumeId = metadataManager.getVolumeId(
                currSnapInfo.getVolumeName());
            // Get bucketInfo for the snapshot bucket to get bucket layout.
            String dbBucketKey = metadataManager.getBucketKey(
                currSnapInfo.getVolumeName(), currSnapInfo.getBucketName());
            OmBucketInfo bucketInfo = metadataManager.getBucketTable()
                .get(dbBucketKey);

            if (bucketInfo == null) {
              throw new IllegalStateException("Bucket " + "/" + currSnapInfo
                  .getVolumeName() + "/" + currSnapInfo.getBucketName() +
                  " is not found. BucketInfo should not be null for" +
                  " snapshotted bucket. The OM is in unexpected state.");
            }

            String snapshotBucketKey = dbBucketKey + OzoneConsts.OM_KEY_PREFIX;
            SnapshotInfo previousSnapshot = getPreviousActiveSnapshot(
                currSnapInfo, snapChainManager, omSnapshotManager);
            SnapshotInfo previousToPrevSnapshot = null;

            if (previousSnapshot != null) {
              previousToPrevSnapshot = getPreviousActiveSnapshot(
                  previousSnapshot, snapChainManager, omSnapshotManager);
            }

            Table<String, OmKeyInfo> previousKeyTable = null;
            Table<String, String> prevRenamedTable = null;
            ReferenceCounted<IOmMetadataReader, SnapshotCache>
                rcPrevOmSnapshot = null;

            // Split RepeatedOmKeyInfo and update current snapshot
            // deletedKeyTable and next snapshot deletedKeyTable.
            if (previousSnapshot != null) {
              rcPrevOmSnapshot = omSnapshotManager.checkForSnapshot(
                  previousSnapshot.getVolumeName(),
                  previousSnapshot.getBucketName(),
                  getSnapshotPrefix(previousSnapshot.getName()), true);
              OmSnapshot omPreviousSnapshot = (OmSnapshot)
                  rcPrevOmSnapshot.get();

              previousKeyTable = omPreviousSnapshot.getMetadataManager()
                  .getKeyTable(bucketInfo.getBucketLayout());
              prevRenamedTable = omPreviousSnapshot
                  .getMetadataManager().getSnapshotRenamedTable();
            }

            Table<String, OmKeyInfo> previousToPrevKeyTable = null;
            ReferenceCounted<IOmMetadataReader, SnapshotCache>
                rcPrevToPrevOmSnapshot = null;
            if (previousToPrevSnapshot != null) {
              rcPrevToPrevOmSnapshot = omSnapshotManager.checkForSnapshot(
                  previousToPrevSnapshot.getVolumeName(),
                  previousToPrevSnapshot.getBucketName(),
                  getSnapshotPrefix(previousToPrevSnapshot.getName()), true);
              OmSnapshot omPreviousToPrevSnapshot = (OmSnapshot)
                  rcPrevToPrevOmSnapshot.get();

              previousToPrevKeyTable = omPreviousToPrevSnapshot
                  .getMetadataManager()
                  .getKeyTable(bucketInfo.getBucketLayout());
            }

            try (TableIterator<String, ? extends Table.KeyValue<String,
                RepeatedOmKeyInfo>> deletedIterator = snapDeletedTable
                .iterator()) {

              String lastKeyInCurrentRun = null;
              String deletedTableSeek = snapshotSeekMap.getOrDefault(
                  currSnapInfo.getTableKey(), snapshotBucketKey);
              deletedIterator.seek(deletedTableSeek);
              // To avoid processing the last key from the previous
              // run again.
              if (!deletedTableSeek.equals(snapshotBucketKey) &&
                  deletedIterator.hasNext()) {
                deletedIterator.next();
              }

              while (deletedIterator.hasNext() && delCount < keyLimitPerTask) {
                Table.KeyValue<String, RepeatedOmKeyInfo>
                    deletedKeyValue = deletedIterator.next();
                String deletedKey = deletedKeyValue.getKey();
                lastKeyInCurrentRun = deletedKey;

                // Exit if it is out of the bucket scope.
                if (!deletedKey.startsWith(snapshotBucketKey)) {
                  break;
                }

                RepeatedOmKeyInfo repeatedOmKeyInfo =
                    deletedKeyValue.getValue();

                List<BlockGroup> blockGroupList = new ArrayList<>();
                RepeatedOmKeyInfo newRepeatedOmKeyInfo =
                    new RepeatedOmKeyInfo();
                for (OmKeyInfo keyInfo : repeatedOmKeyInfo.getOmKeyInfoList()) {
                  if (previousSnapshot != null) {
                    // Calculates the exclusive size for the previous
                    // snapshot. See Java Doc for more info.
                    calculateExclusiveSize(previousSnapshot,
                        previousToPrevSnapshot, keyInfo, bucketInfo, volumeId,
                        snapRenamedTable, previousKeyTable, prevRenamedTable,
                        previousToPrevKeyTable, exclusiveSizeMap,
                        exclusiveReplicatedSizeMap);
                  }

                  if (isKeyReclaimable(previousKeyTable, snapRenamedTable,
                      keyInfo, bucketInfo, volumeId, null)) {
                    List<BlockGroup> blocksForKeyDelete = currOmSnapshot
                        .getMetadataManager()
                        .getBlocksForKeyDelete(deletedKey);
                    if (blocksForKeyDelete != null) {
                      blockGroupList.addAll(blocksForKeyDelete);
                    }
                    delCount++;
                  } else {
                    newRepeatedOmKeyInfo.addOmKeyInfo(keyInfo);
                  }
                }

                if (newRepeatedOmKeyInfo.getOmKeyInfoList().size() > 0 &&
                    newRepeatedOmKeyInfo.getOmKeyInfoList().size() !=
                        repeatedOmKeyInfo.getOmKeyInfoList().size()) {
                  keysToModify.put(deletedKey, newRepeatedOmKeyInfo);
                }

                if (newRepeatedOmKeyInfo.getOmKeyInfoList().size() !=
                    repeatedOmKeyInfo.getOmKeyInfoList().size()) {
                  keysToPurge.addAll(blockGroupList);
                }
              }

              if (delCount < keyLimitPerTask) {
                // Deep clean is completed, we can update the SnapInfo.
                deepCleanedSnapshots.add(currSnapInfo.getTableKey());
                // exclusiveSizeList contains check is used to prevent
                // case where there is no entry in deletedTable, this
                // will throw NPE when we submit request.
                if (previousSnapshot != null && exclusiveSizeMap
                    .containsKey(previousSnapshot.getTableKey())) {
                  completedExclusiveSizeSet.add(
                      previousSnapshot.getTableKey());
                }

                snapshotSeekMap.remove(currSnapInfo.getTableKey());
              } else {
                // There are keys that still needs processing
                // we can continue from it in the next iteration
                if (lastKeyInCurrentRun != null) {
                  snapshotSeekMap.put(currSnapInfo.getTableKey(),
                      lastKeyInCurrentRun);
                }
              }

              if (!keysToPurge.isEmpty()) {
                processKeyDeletes(keysToPurge, currOmSnapshot.getKeyManager(),
                    keysToModify, currSnapInfo.getTableKey());
              }
            } finally {
              IOUtils.closeQuietly(rcPrevOmSnapshot, rcPrevToPrevOmSnapshot);
            }
          }

        }
      }

      updateDeepCleanedSnapshots(deepCleanedSnapshots);
      updateSnapshotExclusiveSize();
    }

    private void updateSnapshotExclusiveSize() {

      if (completedExclusiveSizeSet.isEmpty()) {
        return;
      }

      Iterator<String> completedSnapshotIterator =
          completedExclusiveSizeSet.iterator();
      while (completedSnapshotIterator.hasNext()) {
        ClientId clientId = ClientId.randomId();
        String dbKey = completedSnapshotIterator.next();
        SnapshotSize snapshotSize = SnapshotSize.newBuilder()
                .setExclusiveSize(exclusiveSizeMap.getOrDefault(dbKey, 0L))
                .setExclusiveReplicatedSize(
                    exclusiveReplicatedSizeMap.getOrDefault(dbKey, 0L))
                .build();
        SetSnapshotPropertyRequest setSnapshotPropertyRequest =
            SetSnapshotPropertyRequest.newBuilder()
                .setSnapshotKey(dbKey)
                .setSnapshotSize(snapshotSize)
                .build();

        OMRequest omRequest = OMRequest.newBuilder()
            .setCmdType(Type.SetSnapshotProperty)
            .setSetSnapshotPropertyRequest(setSnapshotPropertyRequest)
            .setClientId(clientId.toString())
            .build();
        submitRequest(omRequest, clientId);
        exclusiveSizeMap.remove(dbKey);
        exclusiveReplicatedSizeMap.remove(dbKey);
        completedSnapshotIterator.remove();
      }
    }

    private void updateDeepCleanedSnapshots(List<String> deepCleanedSnapshots) {
      for (String deepCleanedSnapshot: deepCleanedSnapshots) {
        ClientId clientId = ClientId.randomId();
        SetSnapshotPropertyRequest setSnapshotPropertyRequest =
            SetSnapshotPropertyRequest.newBuilder()
                .setSnapshotKey(deepCleanedSnapshot)
                .setDeepCleanedDeletedKey(true)
                .build();

        OMRequest omRequest = OMRequest.newBuilder()
            .setCmdType(Type.SetSnapshotProperty)
            .setSetSnapshotPropertyRequest(setSnapshotPropertyRequest)
            .setClientId(clientId.toString())
            .build();

        submitRequest(omRequest, clientId);
      }
    }

    public void submitRequest(OMRequest omRequest, ClientId clientId) {
      try {
        if (isRatisEnabled()) {
          OzoneManagerRatisServer server = getOzoneManager().getOmRatisServer();

          RaftClientRequest raftClientRequest = RaftClientRequest.newBuilder()
              .setClientId(clientId)
              .setServerId(server.getRaftPeerId())
              .setGroupId(server.getRaftGroupId())
              .setCallId(getRunCount().get())
              .setMessage(Message.valueOf(
                  OMRatisHelper.convertRequestToByteString(omRequest)))
              .setType(RaftClientRequest.writeRequestType())
              .build();

          server.submitRequest(omRequest, raftClientRequest);
        } else {
          getOzoneManager().getOmServerProtocol()
              .submitRequest(null, omRequest);
        }
      } catch (ServiceException e) {
        LOG.error("Snapshot deep cleaning request failed. " +
            "Will retry at next run.", e);
      }
    }
  }
}
