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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgePathRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveDeletedKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotPurgeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.util.Time;
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

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OBJECT_ID_RECLAIM_BLOCKS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DELETING_LIMIT_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPrefix;

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
  private final AtomicBoolean suspended;
  private final OzoneConfiguration conf;
  private final AtomicLong successRunCount;
  private final long snapshotDeletionPerTask;
  private final int keyLimitPerSnapshot;

  public SnapshotDeletingService(long interval, long serviceTimeout,
      OzoneManager ozoneManager, ScmBlockLocationProtocol scmClient)
      throws IOException {
    super(SnapshotDeletingService.class.getSimpleName(), interval,
        TimeUnit.MILLISECONDS, SNAPSHOT_DELETING_CORE_POOL_SIZE,
        serviceTimeout, ozoneManager, scmClient);
    this.ozoneManager = ozoneManager;
    this.omSnapshotManager = ozoneManager.getOmSnapshotManager();
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl)
        ozoneManager.getMetadataManager();
    this.chainManager = omMetadataManager.getSnapshotChainManager();
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

    @SuppressWarnings("checkstyle:MethodLength")
    @Override
    public BackgroundTaskResult call() throws Exception {
      if (!shouldRun()) {
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }

      getRunCount().incrementAndGet();

      Table<String, SnapshotInfo> snapshotInfoTable =
          ozoneManager.getMetadataManager().getSnapshotInfoTable();
      List<String> purgeSnapshotKeys = new ArrayList<>();
      try (TableIterator<String, ? extends Table.KeyValue
          <String, SnapshotInfo>> iterator = snapshotInfoTable.iterator()) {

        long snapshotLimit = snapshotDeletionPerTask;

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
                  getSnapshotPrefix(snapInfo.getName()), true);

          Table<String, RepeatedOmKeyInfo> snapshotDeletedTable =
              omSnapshot.getMetadataManager().getDeletedTable();
          Table<String, OmKeyInfo> snapshotDeletedDirTable =
              omSnapshot.getMetadataManager().getDeletedDirTable();

          Table<String, String> renamedTable =
              omSnapshot.getMetadataManager().getSnapshotRenamedTable();

          long volumeId = ozoneManager.getMetadataManager()
              .getVolumeId(snapInfo.getVolumeName());
          // Get bucketInfo for the snapshot bucket to get bucket layout.
          String dbBucketKey = ozoneManager.getMetadataManager().getBucketKey(
              snapInfo.getVolumeName(), snapInfo.getBucketName());
          OmBucketInfo bucketInfo = ozoneManager.getMetadataManager()
              .getBucketTable().get(dbBucketKey);

          if (bucketInfo == null) {
            throw new IllegalStateException("Bucket " + "/" +
                snapInfo.getVolumeName() + "/" + snapInfo.getBucketName() +
                " is not found. BucketInfo should not be null for snapshotted" +
                " bucket. The OM is in unexpected state.");
          }

          String snapshotBucketKey = dbBucketKey + OzoneConsts.OM_KEY_PREFIX;
          String dbBucketKeyForDir = ozoneManager.getMetadataManager()
              .getBucketKey(Long.toString(volumeId),
                  Long.toString(bucketInfo.getObjectID())) + OM_KEY_PREFIX;

          if (isSnapshotReclaimable(snapshotDeletedTable,
              snapshotDeletedDirTable, snapshotBucketKey, dbBucketKeyForDir)) {
            purgeSnapshotKeys.add(snapInfo.getTableKey());
            continue;
          }

          //TODO: [SNAPSHOT] Add lock to deletedTable and Active DB.
          SnapshotInfo previousSnapshot = getPreviousActiveSnapshot(snapInfo);
          Table<String, OmKeyInfo> previousKeyTable = null;
          Table<String, OmDirectoryInfo> previousDirTable = null;
          OmSnapshot omPreviousSnapshot = null;

          // Split RepeatedOmKeyInfo and update current snapshot deletedKeyTable
          // and next snapshot deletedKeyTable.
          if (previousSnapshot != null) {
            omPreviousSnapshot = (OmSnapshot) omSnapshotManager
                .checkForSnapshot(previousSnapshot.getVolumeName(),
                    previousSnapshot.getBucketName(),
                    getSnapshotPrefix(previousSnapshot.getName()), true);

            previousKeyTable = omPreviousSnapshot
                .getMetadataManager().getKeyTable(bucketInfo.getBucketLayout());
            previousDirTable = omPreviousSnapshot
                .getMetadataManager().getDirectoryTable();
          }

          // Move key to either next non deleted snapshot's deletedTable
          // or keep it in current snapshot deleted table.
          List<SnapshotMoveKeyInfos> toReclaimList = new ArrayList<>();
          List<SnapshotMoveKeyInfos> toNextDBList = new ArrayList<>();
          // A list of renamed keys/files/dirs
          List<HddsProtos.KeyValue> renamedList = new ArrayList<>();
          List<String> dirsToMove = new ArrayList<>();

          long remainNum = handleDirectoryCleanUp(snapshotDeletedDirTable,
              previousDirTable, renamedTable, dbBucketKeyForDir, snapInfo,
              omSnapshot, dirsToMove, renamedList);
          int deletionCount = 0;

          try (TableIterator<String, ? extends Table.KeyValue<String,
              RepeatedOmKeyInfo>> deletedIterator = snapshotDeletedTable
              .iterator()) {

            List<BlockGroup> keysToPurge = new ArrayList<>();
            deletedIterator.seek(snapshotBucketKey);

            while (deletedIterator.hasNext() &&
                deletionCount < remainNum) {
              Table.KeyValue<String, RepeatedOmKeyInfo>
                  deletedKeyValue = deletedIterator.next();
              String deletedKey = deletedKeyValue.getKey();

              // Exit if it is out of the bucket scope.
              if (!deletedKey.startsWith(snapshotBucketKey)) {
                // If snapshot deletedKeyTable doesn't have any
                // entry in the snapshot scope it can be reclaimed
                break;
              }

              RepeatedOmKeyInfo repeatedOmKeyInfo = deletedKeyValue.getValue();

              SnapshotMoveKeyInfos.Builder toReclaim = SnapshotMoveKeyInfos
                  .newBuilder()
                  .setKey(deletedKey);
              SnapshotMoveKeyInfos.Builder toNextDb = SnapshotMoveKeyInfos
                  .newBuilder()
                  .setKey(deletedKey);
              HddsProtos.KeyValue.Builder renamedKey = HddsProtos.KeyValue
                  .newBuilder();

              for (OmKeyInfo keyInfo : repeatedOmKeyInfo.getOmKeyInfoList()) {
                splitRepeatedOmKeyInfo(toReclaim, toNextDb, renamedKey,
                    keyInfo, previousKeyTable, renamedTable,
                    bucketInfo, volumeId);
              }

              // If all the KeyInfos are reclaimable in RepeatedOmKeyInfo
              // then no need to update current snapshot deletedKeyTable.
              if (!(toReclaim.getKeyInfosCount() ==
                  repeatedOmKeyInfo.getOmKeyInfoList().size())) {
                toReclaimList.add(toReclaim.build());
                toNextDBList.add(toNextDb.build());
              } else {
                // The key can be reclaimed here.
                List<BlockGroup> blocksForKeyDelete = omSnapshot
                    .getMetadataManager()
                    .getBlocksForKeyDelete(deletedKey);
                if (blocksForKeyDelete != null) {
                  keysToPurge.addAll(blocksForKeyDelete);
                }
              }

              if (renamedKey.hasKey() && renamedKey.hasValue()) {
                renamedList.add(renamedKey.build());
              }
              deletionCount++;
            }

            // Delete keys From deletedTable
            processKeyDeletes(keysToPurge, omSnapshot.getKeyManager(),
                snapInfo.getTableKey());
            successRunCount.incrementAndGet();
          } catch (IOException ex) {
            LOG.error("Error while running Snapshot Deleting Service for " +
                "snapshot " + snapInfo.getTableKey() + " with snapshotId " +
                snapInfo.getSnapshotID() + ". Processed " + deletionCount +
                " keys and " + (keyLimitPerSnapshot - remainNum) +
                " directories and files", ex);
          }
          snapshotLimit--;
          // Submit Move request to OM.
          submitSnapshotMoveDeletedKeys(snapInfo, toReclaimList,
              toNextDBList, renamedList, dirsToMove);
        }
      } catch (IOException e) {
        LOG.error("Error while running Snapshot Deleting Service", e);
      }
      submitSnapshotPurgeRequest(purgeSnapshotKeys);

      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }

    private boolean isSnapshotReclaimable(
        Table<String, RepeatedOmKeyInfo> snapshotDeletedTable,
        Table<String, OmKeyInfo> snapshotDeletedDirTable,
        String snapshotBucketKey, String dbBucketKeyForDir) throws IOException {

      boolean isDirTableCleanedUp = false;
      boolean isKeyTableCleanedUp  = false;
      try (TableIterator<String, ? extends Table.KeyValue<String,
          RepeatedOmKeyInfo>> iterator = snapshotDeletedTable.iterator();) {
        iterator.seek(snapshotBucketKey);
        // If the next entry doesn't start with snapshotBucketKey then
        // deletedKeyTable is already cleaned up.
        isKeyTableCleanedUp = !iterator.hasNext() || !iterator.next().getKey()
            .startsWith(snapshotBucketKey);
      }

      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
               iterator = snapshotDeletedDirTable.iterator()) {
        iterator.seek(dbBucketKeyForDir);
        // If the next entry doesn't start with dbBucketKeyForDir then
        // deletedDirTable is already cleaned up.
        isDirTableCleanedUp = !iterator.hasNext() || !iterator.next().getKey()
            .startsWith(dbBucketKeyForDir);
      }

      return (isDirTableCleanedUp || snapshotDeletedDirTable.isEmpty()) &&
          (isKeyTableCleanedUp || snapshotDeletedTable.isEmpty());
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private long handleDirectoryCleanUp(
        Table<String, OmKeyInfo> snapshotDeletedDirTable,
        Table<String, OmDirectoryInfo> previousDirTable,
        Table<String, String> renamedTable,
        String dbBucketKeyForDir, SnapshotInfo snapInfo,
        OmSnapshot omSnapshot, List<String> dirsToMove,
        List<HddsProtos.KeyValue> renamedList) {

      long dirNum = 0L;
      long subDirNum = 0L;
      long subFileNum = 0L;
      long remainNum = keyLimitPerSnapshot;
      List<PurgePathRequest> purgePathRequestList = new ArrayList<>();
      List<Pair<String, OmKeyInfo>> allSubDirList
          = new ArrayList<>(keyLimitPerSnapshot);
      try (TableIterator<String, ? extends
          Table.KeyValue<String, OmKeyInfo>> deletedDirIterator =
               snapshotDeletedDirTable.iterator()) {

        long startTime = Time.monotonicNow();
        deletedDirIterator.seek(dbBucketKeyForDir);

        while (deletedDirIterator.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> deletedDir =
              deletedDirIterator.next();

          if (isDirReclaimable(deletedDir, previousDirTable,
              renamedTable, renamedList)) {
            // Reclaim here
            PurgePathRequest request = prepareDeleteDirRequest(
                remainNum, deletedDir.getValue(), deletedDir.getKey(),
                allSubDirList, omSnapshot.getKeyManager());
            purgePathRequestList.add(request);
            remainNum = remainNum - request.getDeletedSubFilesCount();
            remainNum = remainNum - request.getMarkDeletedSubDirsCount();
            // Count up the purgeDeletedDir, subDirs and subFiles
            if (request.getDeletedDir() != null
                && !request.getDeletedDir().isEmpty()) {
              dirNum++;
            }
            subDirNum += request.getMarkDeletedSubDirsCount();
            subFileNum += request.getDeletedSubFilesCount();
          } else {
            dirsToMove.add(deletedDir.getKey());
          }
        }

        remainNum = optimizeDirDeletesAndSubmitRequest(remainNum, dirNum,
            subDirNum, subFileNum, allSubDirList, purgePathRequestList,
            snapInfo.getTableKey(), startTime);
      } catch (IOException e) {
        LOG.error("Error while running delete directories and files for " +
            "snapshot " + snapInfo.getTableKey() + " in snapshot deleting " +
            "background task. Will retry at next run.", e);
      }

      return remainNum;
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

        submitRequest(omRequest);
      }
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private void splitRepeatedOmKeyInfo(SnapshotMoveKeyInfos.Builder toReclaim,
        SnapshotMoveKeyInfos.Builder toNextDb,
        HddsProtos.KeyValue.Builder renamedKey, OmKeyInfo keyInfo,
        Table<String, OmKeyInfo> previousKeyTable,
        Table<String, String> renamedTable,
        OmBucketInfo bucketInfo, long volumeId) throws IOException {

      if (isKeyReclaimable(previousKeyTable, renamedTable,
          keyInfo, bucketInfo, volumeId, renamedKey)) {
        // Update in current db's deletedKeyTable
        toReclaim.addKeyInfos(keyInfo
            .getProtobuf(ClientVersion.CURRENT_VERSION));
      } else {
        // Move to next non deleted snapshot's deleted table
        toNextDb.addKeyInfos(keyInfo.getProtobuf(
            ClientVersion.CURRENT_VERSION));
      }
    }

    private boolean isDirReclaimable(
        Table.KeyValue<String, OmKeyInfo> deletedDir,
        Table<String, OmDirectoryInfo> previousDirTable,
        Table<String, String> renamedTable,
        List<HddsProtos.KeyValue> renamedList) throws IOException {

      if (previousDirTable == null) {
        return true;
      }

      String deletedDirDbKey = deletedDir.getKey();
      OmKeyInfo deletedDirInfo = deletedDir.getValue();
      String dbRenameKey = ozoneManager.getMetadataManager().getRenameKey(
          deletedDirInfo.getVolumeName(), deletedDirInfo.getBucketName(),
          deletedDirInfo.getObjectID());

      /*
      snapshotRenamedTable: /volumeName/bucketName/objectID ->
          /volumeId/bucketId/parentId/dirName
       */
      String dbKeyBeforeRename = renamedTable.getIfExist(dbRenameKey);
      String prevDbKey = null;

      if (dbKeyBeforeRename != null) {
        prevDbKey = dbKeyBeforeRename;
        HddsProtos.KeyValue renamedDir = HddsProtos.KeyValue
            .newBuilder()
            .setKey(dbRenameKey)
            .setValue(dbKeyBeforeRename)
            .build();
        renamedList.add(renamedDir);
      } else {
        // In OMKeyDeleteResponseWithFSO OzonePathKey is converted to
        // OzoneDeletePathKey. Changing it back to check the previous DirTable.
        prevDbKey = ozoneManager.getMetadataManager()
            .getOzoneDeletePathDirKey(deletedDirDbKey);
      }

      OmDirectoryInfo prevDirectoryInfo = previousDirTable.get(prevDbKey);
      if (prevDirectoryInfo == null) {
        return true;
      }

      return prevDirectoryInfo.getObjectID() != deletedDirInfo.getObjectID();
    }

    private boolean isKeyReclaimable(
        Table<String, OmKeyInfo> previousKeyTable,
        Table<String, String> renamedTable,
        OmKeyInfo deletedKeyInfo, OmBucketInfo bucketInfo,
        long volumeId, HddsProtos.KeyValue.Builder renamedKeyBuilder)
        throws IOException {

      String dbKey;
      // Handle case when the deleted snapshot is the first snapshot.
      if (previousKeyTable == null) {
        return true;
      }

      // These are uncommitted blocks wrapped into a pseudo KeyInfo
      if (deletedKeyInfo.getObjectID() == OBJECT_ID_RECLAIM_BLOCKS) {
        return true;
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

      /*
       snapshotRenamedTable:
       1) /volumeName/bucketName/objectID ->
                   /volumeId/bucketId/parentId/fileName (FSO)
       2) /volumeName/bucketName/objectID ->
                  /volumeName/bucketName/keyName (non-FSO)
      */
      String dbRenameKey = ozoneManager.getMetadataManager().getRenameKey(
          deletedKeyInfo.getVolumeName(), deletedKeyInfo.getBucketName(),
          deletedKeyInfo.getObjectID());

      // Condition: key should not exist in snapshotRenamedTable
      // of the current snapshot and keyTable of the previous snapshot.
      // Check key exists in renamedTable of the Snapshot
      String renamedKey = renamedTable.getIfExist(dbRenameKey);

      if (renamedKey != null) {
        renamedKeyBuilder.setKey(dbRenameKey).setValue(renamedKey);
      }
      // previousKeyTable is fileTable if the bucket is FSO,
      // otherwise it is the keyTable.
      OmKeyInfo prevKeyInfo = renamedKey != null ? previousKeyTable
          .get(renamedKey) : previousKeyTable.get(dbKey);

      if (prevKeyInfo == null) {
        return true;
      }

      return prevKeyInfo.getObjectID() != deletedKeyInfo.getObjectID();
    }

    private SnapshotInfo getPreviousActiveSnapshot(SnapshotInfo snapInfo)
        throws IOException {
      SnapshotInfo currSnapInfo = snapInfo;
      while (chainManager.hasPreviousPathSnapshot(
          currSnapInfo.getSnapshotPath(), currSnapInfo.getSnapshotID())) {

        String prevPathSnapshot = chainManager.previousPathSnapshot(
            currSnapInfo.getSnapshotPath(), currSnapInfo.getSnapshotID());
        String tableKey = chainManager.getTableKey(prevPathSnapshot);
        SnapshotInfo prevSnapInfo = omSnapshotManager.getSnapshotInfo(tableKey);
        if (prevSnapInfo.getSnapshotStatus().equals(
            SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE)) {
          return prevSnapInfo;
        }
        currSnapInfo = prevSnapInfo;
      }
      return null;
    }

    public void submitSnapshotMoveDeletedKeys(SnapshotInfo snapInfo,
        List<SnapshotMoveKeyInfos> toReclaimList,
        List<SnapshotMoveKeyInfos> toNextDBList,
        List<HddsProtos.KeyValue> renamedList,
        List<String> dirsToMove) throws InterruptedException {

      SnapshotMoveDeletedKeysRequest.Builder moveDeletedKeysBuilder =
          SnapshotMoveDeletedKeysRequest.newBuilder()
              .setFromSnapshot(snapInfo.getProtobuf());

      SnapshotMoveDeletedKeysRequest moveDeletedKeys = moveDeletedKeysBuilder
          .addAllReclaimKeys(toReclaimList)
          .addAllNextDBKeys(toNextDBList)
          .addAllRenamedKeys(renamedList)
          .addAllDeletedDirsToMove(dirsToMove)
          .build();

      OMRequest omRequest = OMRequest.newBuilder()
          .setCmdType(Type.SnapshotMoveDeletedKeys)
          .setSnapshotMoveDeletedKeysRequest(moveDeletedKeys)
          .setClientId(clientId.toString())
          .build();

      try (BootstrapStateHandler.Lock lock = new BootstrapStateHandler.Lock()) {
        submitRequest(omRequest);
      }
    }

    public void submitRequest(OMRequest omRequest) {
      try {
        if (isRatisEnabled()) {
          OzoneManagerRatisServer server = ozoneManager.getOmRatisServer();

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
          ozoneManager.getOmServerProtocol().submitRequest(null, omRequest);
        }
      } catch (ServiceException e) {
        LOG.error("Snapshot Deleting request failed. " +
            "Will retry at next run.", e);
      }
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

  public long getSuccessfulRunCount() {
    return successRunCount.get();
  }

  @VisibleForTesting
  public void setSuccessRunCount(long num) {
    successRunCount.getAndSet(num);
  }
}

