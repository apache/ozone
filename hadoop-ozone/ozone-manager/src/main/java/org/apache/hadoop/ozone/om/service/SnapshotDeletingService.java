/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveDeletedKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotPurgeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OBJECT_ID_RECLAIM_BLOCKS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DELETING_LIMIT_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPrefix;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;

/**
 * Background Service to clean-up deleted snapshot and reclaim space.
 */
public class SnapshotDeletingService extends BackgroundService {
  private static final Logger LOG =
      LoggerFactory.getLogger(SnapshotDeletingService.class);

  // Use only a single thread for Snapshot Deletion. Multiple threads would read
  // from the same table and can send deletion requests for same snapshot
  // multiple times.
  private static final int SNAPSHOT_DELETING_CORE_POOL_SIZE = 1;
  private final ClientId clientId = ClientId.randomId();
  private final AtomicLong runCount;

  private final OzoneManager ozoneManager;
  private final OmSnapshotManager omSnapshotManager;
  private final SnapshotChainManager chainManager;
  private final AtomicBoolean suspended;
  private final OzoneConfiguration conf;
  private final AtomicLong successRunCount;
  private final long snapshotDeletionPerTask;
  private final int keyLimitPerSnapshot;

  public SnapshotDeletingService(long interval, long serviceTimeout,
      OzoneManager ozoneManager) throws IOException {
    super(SnapshotDeletingService.class.getSimpleName(), interval,
        TimeUnit.MILLISECONDS, SNAPSHOT_DELETING_CORE_POOL_SIZE,
        serviceTimeout);
    this.ozoneManager = ozoneManager;
    this.omSnapshotManager = ozoneManager.getOmSnapshotManager();
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl)
        ozoneManager.getMetadataManager();
    this.chainManager = omMetadataManager.getSnapshotChainManager();
    this.runCount = new AtomicLong(0);
    this.successRunCount = new AtomicLong(0);
    this.suspended = new AtomicBoolean(false);
    this.conf = ozoneManager.getConfiguration();
    this.snapshotDeletionPerTask = conf
        .getLong(SNAPSHOT_DELETING_LIMIT_PER_TASK,
        SNAPSHOT_DELETING_LIMIT_PER_TASK_DEFAULT);
    this.keyLimitPerSnapshot = conf.getInt(
        OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK,
        OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK_DEFAULT);
  }

  private class SnapshotDeletingTask implements BackgroundTask {

    @Override
    public BackgroundTaskResult call() throws Exception {
      if (!shouldRun()) {
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }

      runCount.incrementAndGet();

      Table<String, SnapshotInfo> snapshotInfoTable =
          ozoneManager.getMetadataManager().getSnapshotInfoTable();
      try (TableIterator<String, ? extends Table.KeyValue
          <String, SnapshotInfo>> iterator = snapshotInfoTable.iterator()) {

        long snapshotLimit = snapshotDeletionPerTask;
        List<String> purgeSnapshotKeys = new ArrayList<>();

        while (iterator.hasNext() && snapshotLimit > 0) {
          SnapshotInfo snapInfo = iterator.next().getValue();
          SnapshotInfo.SnapshotStatus snapshotStatus =
              snapInfo.getSnapshotStatus();

          // Only Iterate in deleted snapshot
          if (!snapshotStatus.equals(
              SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED)) {
            continue;
          }

          OmSnapshot omSnapshot = (OmSnapshot) omSnapshotManager
              .checkForSnapshot(snapInfo.getVolumeName(),
                  snapInfo.getBucketName(),
                  getSnapshotPrefix(snapInfo.getName()));

          Table<String, RepeatedOmKeyInfo> snapshotDeletedTable =
              omSnapshot.getMetadataManager().getDeletedTable();

          if (snapshotDeletedTable.isEmpty()) {
            continue;
          }

          Table<String, String> renamedKeyTable =
              omSnapshot.getMetadataManager().getSnapshotRenamedKeyTable();

          long volumeId = ozoneManager.getMetadataManager()
              .getVolumeId(snapInfo.getVolumeName());
          // Get bucketInfo for the snapshot bucket to get bucket layout.
          String dbBucketKey = ozoneManager.getMetadataManager().getBucketKey(
              snapInfo.getVolumeName(), snapInfo.getBucketName());
          OmBucketInfo bucketInfo = ozoneManager.getMetadataManager()
              .getBucketTable().get(dbBucketKey);

          if (bucketInfo == null) {
            throw new OMException("Bucket " + snapInfo.getBucketName() +
                " is not found", BUCKET_NOT_FOUND);
          }

          //TODO: [SNAPSHOT] Add lock to deletedTable and Active DB.
          SnapshotInfo previousSnapshot = getPreviousSnapshot(snapInfo);
          Table<String, OmKeyInfo> previousKeyTable = null;
          OmSnapshot omPreviousSnapshot = null;

          // Split RepeatedOmKeyInfo and update current snapshot deletedKeyTable
          // and next snapshot deletedKeyTable.
          if (previousSnapshot != null) {
            omPreviousSnapshot = (OmSnapshot) omSnapshotManager
                .checkForSnapshot(previousSnapshot.getVolumeName(),
                    previousSnapshot.getBucketName(),
                    getSnapshotPrefix(previousSnapshot.getName()));

            previousKeyTable = omPreviousSnapshot
                .getMetadataManager().getKeyTable(bucketInfo.getBucketLayout());
          }

          // Move key to either next non deleted snapshot's deletedTable
          // or keep it in current snapshot deleted table.
          List<SnapshotMoveKeyInfos> toReclaimList = new ArrayList<>();
          List<SnapshotMoveKeyInfos> toNextDBList = new ArrayList<>();

          try (TableIterator<String, ? extends Table.KeyValue<String,
              RepeatedOmKeyInfo>> deletedIterator = snapshotDeletedTable
              .iterator()) {

            String snapshotBucketKey = dbBucketKey + OzoneConsts.OM_KEY_PREFIX;
            iterator.seek(snapshotBucketKey);

            int deletionCount = 0;
            while (deletedIterator.hasNext() &&
                deletionCount <= keyLimitPerSnapshot) {
              Table.KeyValue<String, RepeatedOmKeyInfo>
                  deletedKeyValue = deletedIterator.next();
              String deletedKey = deletedKeyValue.getKey();

              // Exit if it is out of the bucket scope.
              if (!deletedKey.startsWith(snapshotBucketKey)) {
                // If snapshot deletedKeyTable doesn't have any
                // entry in the snapshot scope it can be reclaimed
                // TODO: [SNAPSHOT] Check deletedDirTable to be empty.
                purgeSnapshotKeys.add(snapInfo.getTableKey());
                break;
              }

              RepeatedOmKeyInfo repeatedOmKeyInfo = deletedKeyValue.getValue();

              SnapshotMoveKeyInfos.Builder toReclaim = SnapshotMoveKeyInfos
                  .newBuilder()
                  .setKey(deletedKey);
              SnapshotMoveKeyInfos.Builder toNextDb = SnapshotMoveKeyInfos
                  .newBuilder()
                  .setKey(deletedKey);

              for (OmKeyInfo keyInfo: repeatedOmKeyInfo.getOmKeyInfoList()) {
                splitRepeatedOmKeyInfo(toReclaim, toNextDb,
                    keyInfo, previousKeyTable, renamedKeyTable,
                    bucketInfo, volumeId);
              }

              // If all the KeyInfos are reclaimable in RepeatedOmKeyInfo
              // then no need to update current snapshot deletedKeyTable.
              if (!(toReclaim.getKeyInfosCount() ==
                  repeatedOmKeyInfo.getOmKeyInfoList().size())) {
                toReclaimList.add(toReclaim.build());
              }
              toNextDBList.add(toNextDb.build());
              deletionCount++;
            }
            // Submit Move request to OM.
            submitSnapshotMoveDeletedKeys(snapInfo, toReclaimList,
                toNextDBList);
            snapshotLimit--;
            successRunCount.incrementAndGet();
          } catch (IOException ex) {
            LOG.error("Error while running Snapshot Deleting Service", ex);
          }
        }

        submitSnapshotPurgeRequest(purgeSnapshotKeys);
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

        // TODO: [SNAPSHOT] Submit request once KeyDeletingService,
        //  DirectoryDeletingService for snapshots are modified.
        // submitRequest(omRequest);
      }
    }

    private void splitRepeatedOmKeyInfo(SnapshotMoveKeyInfos.Builder toReclaim,
        SnapshotMoveKeyInfos.Builder toNextDb, OmKeyInfo keyInfo,
        Table<String, OmKeyInfo> previousKeyTable,
        Table<String, String> renamedKeyTable,
        OmBucketInfo bucketInfo, long volumeId) throws IOException {
      if (checkKeyReclaimable(previousKeyTable, renamedKeyTable,
          keyInfo, bucketInfo, volumeId)) {
        // Move to next non deleted snapshot's deleted table
        toNextDb.addKeyInfos(keyInfo.getProtobuf(
            ClientVersion.CURRENT_VERSION));
      } else {
        // Update in current db's deletedKeyTable
        toReclaim.addKeyInfos(keyInfo
            .getProtobuf(ClientVersion.CURRENT_VERSION));
      }
    }

    private void submitSnapshotMoveDeletedKeys(SnapshotInfo snapInfo,
        List<SnapshotMoveKeyInfos> toReclaimList,
        List<SnapshotMoveKeyInfos> toNextDBList) {

      SnapshotMoveDeletedKeysRequest.Builder moveDeletedKeysBuilder =
          SnapshotMoveDeletedKeysRequest.newBuilder()
              .setFromSnapshot(snapInfo.getProtobuf());

      SnapshotMoveDeletedKeysRequest moveDeletedKeys =
          moveDeletedKeysBuilder.addAllReclaimKeys(toReclaimList)
          .addAllNextDBKeys(toNextDBList).build();


      OMRequest omRequest = OMRequest.newBuilder()
          .setCmdType(Type.SnapshotMoveDeletedKeys)
          .setSnapshotMoveDeletedKeysRequest(moveDeletedKeys)
          .setClientId(clientId.toString())
          .build();

      submitRequest(omRequest);
    }

    private boolean checkKeyReclaimable(
        Table<String, OmKeyInfo> previousKeyTable,
        Table<String, String> renamedKeyTable,
        OmKeyInfo deletedKeyInfo, OmBucketInfo bucketInfo,
        long volumeId) throws IOException {

      String dbKey;
      // Handle case when the deleted snapshot is the first snapshot.
      if (previousKeyTable == null) {
        return false;
      }

      // These are uncommitted blocks wrapped into a pseudo KeyInfo
      if (deletedKeyInfo.getObjectID() == OBJECT_ID_RECLAIM_BLOCKS) {
        return false;
      }

      // Construct keyTable or fileTable DB key depending on the bucket type
      if (bucketInfo.getBucketLayout().isFileSystemOptimized()) {
        dbKey = ozoneManager.getMetadataManager().getOzonePathKey(
            volumeId,
            bucketInfo.getObjectID(),
            deletedKeyInfo.getParentObjectID(),
            deletedKeyInfo.getKeyName());
      } else {
        dbKey = ozoneManager.getMetadataManager().getOzoneKey(
            deletedKeyInfo.getVolumeName(),
            deletedKeyInfo.getBucketName(),
            deletedKeyInfo.getKeyName());
      }

      // renamedKeyTable: volumeName/bucketName/objectID -> OMRenameKeyInfo
      String dbRenameKey = ozoneManager.getMetadataManager().getRenameKey(
          deletedKeyInfo.getVolumeName(), deletedKeyInfo.getBucketName(),
          deletedKeyInfo.getObjectID());

      // Condition: key should not exist in snapshotRenamedKeyTable
      // of the current snapshot and keyTable of the previous snapshot.
      // Check key exists in renamedKeyTable of the Snapshot
      String renamedKey = renamedKeyTable.getIfExist(dbRenameKey);

      // previousKeyTable is fileTable if the bucket is FSO,
      // otherwise it is the keyTable.
      OmKeyInfo prevKeyInfo = renamedKey != null ? previousKeyTable
          .get(renamedKey) : previousKeyTable.get(dbKey);

      if (prevKeyInfo == null) {
        return false;
      }

      return prevKeyInfo.getObjectID() == deletedKeyInfo.getObjectID();
    }

    private SnapshotInfo getPreviousSnapshot(SnapshotInfo snapInfo)
        throws IOException {
      if (chainManager.hasPreviousPathSnapshot(snapInfo.getSnapshotPath(),
          snapInfo.getSnapshotID())) {
        String previousPathSnapshot = chainManager.previousPathSnapshot(
            snapInfo.getSnapshotPath(), snapInfo.getSnapshotID());
        String tableKey = chainManager.getTableKey(previousPathSnapshot);
        return omSnapshotManager.getSnapshotInfo(tableKey);
      }
      return null;
    }

    private void submitRequest(OMRequest omRequest) {
      try {
        if (isRatisEnabled()) {
          OzoneManagerRatisServer server = ozoneManager.getOmRatisServer();

          RaftClientRequest raftClientRequest = RaftClientRequest.newBuilder()
              .setClientId(clientId)
              .setServerId(server.getRaftPeerId())
              .setGroupId(server.getRaftGroupId())
              .setCallId(runCount.get())
              .setMessage(Message.valueOf(
                  OMRatisHelper.convertRequestToByteString(omRequest)))
              .setType(RaftClientRequest.writeRequestType())
              .build();

          server.submitRequest(omRequest, raftClientRequest);
        } else {
          ozoneManager.getOmServerProtocol().submitRequest(null, omRequest);
        }
      } catch (ServiceException e) {
        LOG.error("Snapshot Deleting request failed. " +
            "Will retry at next run.", e);
      }
    }

    private boolean isRatisEnabled() {
      return ozoneManager.isRatisEnabled();
    }

  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new SnapshotDeletingTask());
    return queue;
  }

  private boolean shouldRun() {
    return !suspended.get() && ozoneManager.isLeaderReady();
  }

  /**
   * Suspend the service.
   */
  @VisibleForTesting
  void suspend() {
    suspended.set(true);
  }

  /**
   * Resume the service if suspended.
   */
  @VisibleForTesting
  void resume() {
    suspended.set(false);
  }

  public long getRunCount() {
    return runCount.get();
  }

  public long getSuccessfulRunCount() {
    return successRunCount.get();
  }

  @VisibleForTesting
  public void setSuccessRunCount(long num) {
    successRunCount.getAndSet(num);
  }
}

