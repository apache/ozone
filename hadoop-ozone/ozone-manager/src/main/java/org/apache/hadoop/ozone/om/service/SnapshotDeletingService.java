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
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyValuePair;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveDeletedKeysRequest;
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

import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DELETING_LIMIT_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPrefix;

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

  public SnapshotDeletingService(long interval, long serviceTimeout,
      OzoneManager ozoneManager) throws IOException {
    super("SnapshotDeletingService", interval, TimeUnit.MILLISECONDS,
        SNAPSHOT_DELETING_CORE_POOL_SIZE, serviceTimeout);
    this.ozoneManager = ozoneManager;
    this.omSnapshotManager = ozoneManager.getOmSnapshotManager();
    this.chainManager = ozoneManager.getSnapshotChainManager();
    this.runCount = new AtomicLong(0);
    this.successRunCount = new AtomicLong(0);
    this.suspended = new AtomicBoolean(false);
    this.conf = ozoneManager.getConfiguration();
    this.snapshotDeletionPerTask = conf
        .getLong(SNAPSHOT_DELETING_LIMIT_PER_TASK,
        SNAPSHOT_DELETING_LIMIT_PER_TASK_DEFAULT);
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
        iterator.seekToFirst();

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
                  getSnapshotPrefix(snapInfo.getName()));

          Table<String, RepeatedOmKeyInfo> snapshotDeletedTable =
              omSnapshot.getMetadataManager().getDeletedTable();

          if (snapshotDeletedTable.getEstimatedKeyCount() == 0) {
            continue;
          }

          // Get bucketInfo for the snapshot bucket to get bucket layout.
          String dbBucketKey = ozoneManager.getMetadataManager().getBucketKey(
              snapInfo.getVolumeName(), snapInfo.getBucketName());
          OmBucketInfo bucketInfo = ozoneManager.getMetadataManager()
              .getBucketTable().get(dbBucketKey);


          chainManager.loadFromSnapshotInfoTable(
              ozoneManager.getMetadataManager());
          SnapshotInfo nextSnapshot = getNextNonDeletedSnapshot(snapInfo);

          SnapshotInfo previousSnapshot = getPreviousSnapshot(snapInfo);
          Table<String, OmKeyInfo> previousKeyTable = null;
          OmSnapshot omPreviousSnapshot = null;

          // Handle case when the deleted snapshot is the first snapshot.
          // Move deleted keys to activeDB's deletedKeyTable
          if (previousSnapshot != null) {
            omPreviousSnapshot = (OmSnapshot) omSnapshotManager
                .checkForSnapshot(previousSnapshot.getVolumeName(),
                    previousSnapshot.getBucketName(),
                    getSnapshotPrefix(previousSnapshot.getName()));

            previousKeyTable = omPreviousSnapshot
                .getMetadataManager().getKeyTable(bucketInfo.getBucketLayout());
          }

          // Move key to either next non deleted snapshot's snapshotDeletedTable
          // or move to active object store deleted table

          List<KeyValuePair> toActiveDBList = new ArrayList<>();
          List<KeyValuePair> toNextDBList = new ArrayList<>();

          try (TableIterator<String, ? extends Table.KeyValue<String,
              RepeatedOmKeyInfo>> deletedIterator = snapshotDeletedTable
              .iterator()) {

            iterator.seekToFirst();

            while (deletedIterator.hasNext()) {
              Table.KeyValue<String, RepeatedOmKeyInfo>
                  deletedKeyValue = deletedIterator.next();
              RepeatedOmKeyInfo repeatedOmKeyInfo = deletedKeyValue.getValue();

              KeyValuePair.Builder toActiveDb = KeyValuePair.newBuilder()
                  .setKey(deletedKeyValue.getKey());
              KeyValuePair.Builder toNextDb = KeyValuePair.newBuilder()
                  .setKey(deletedKeyValue.getKey());

              for (OmKeyInfo keyInfo: repeatedOmKeyInfo.getOmKeyInfoList()) {
                splitRepeatedOmKeyInfo(toActiveDb, toNextDb,
                    keyInfo, previousKeyTable);
              }

              toActiveDBList.add(toActiveDb.build());
              toNextDBList.add(toNextDb.build());

            }
            // Submit Move request to OM.
            submitSnapshotMoveDeletedKeys(snapInfo, nextSnapshot,
                toActiveDBList, toNextDBList);
            snapshotLimit--;
          } catch (IOException ex) {
            LOG.error("Error while running Snapshot Deleting Service", ex);
          }
        }

        successRunCount.incrementAndGet();
      } catch (IOException e) {
        LOG.error("Error while running Snapshot Deleting Service", e);
      }

      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }

    private void splitRepeatedOmKeyInfo(KeyValuePair.Builder toActiveDb,
        KeyValuePair.Builder toNextDb, OmKeyInfo keyInfo,
        Table<String, OmKeyInfo> previousKeyTable) throws IOException {
      if (checkKeyExistInPreviousTable(previousKeyTable, keyInfo)) {
        // Move to next non deleted snapshot's deleted table
        toNextDb.addKeyInfos(keyInfo.getProtobuf(
            ClientVersion.CURRENT_VERSION));
      } else {
        // Move to active DB Deleted Table.
        toActiveDb.addKeyInfos(keyInfo
            .getProtobuf(ClientVersion.CURRENT_VERSION));
      }
    }

    private void submitSnapshotMoveDeletedKeys(SnapshotInfo snapInfo,
        SnapshotInfo nextSnapshot, List<KeyValuePair> toActiveDBList,
        List<KeyValuePair> toNextDBList) {

      SnapshotMoveDeletedKeysRequest.Builder moveDeletedKeysBuilder =
          SnapshotMoveDeletedKeysRequest.newBuilder()
              .setFromSnapshot(snapInfo.getProtobuf());

      if (nextSnapshot != null) {
        moveDeletedKeysBuilder.setNextSnapshot(nextSnapshot.getProtobuf());
      }

      SnapshotMoveDeletedKeysRequest moveDeletedKeys =
          moveDeletedKeysBuilder.addAllActiveDBKeys(toActiveDBList)
          .addAllNextDBKeys(toNextDBList).build();


      OMRequest omRequest = OMRequest.newBuilder()
          .setCmdType(Type.SnapshotMoveDeletedKeys)
          .setSnapshotMoveDeletedKeysRequest(moveDeletedKeys)
          .setClientId(clientId.toString())
          .build();

      submitRequest(omRequest);
    }

    private boolean checkKeyExistInPreviousTable(
        Table<String, OmKeyInfo> previousKeyTable, OmKeyInfo deletedKeyInfo)
        throws IOException {

      if (previousKeyTable == null) {
        return false;
      }

      //TODO: Handle Renamed Keys
      String dbKey = ozoneManager.getMetadataManager()
          .getOzoneKey(deletedKeyInfo.getVolumeName(),
              deletedKeyInfo.getBucketName(), deletedKeyInfo.getKeyName());

      OmKeyInfo prevKeyInfo = previousKeyTable.get(dbKey);
      if (prevKeyInfo != null &&
          prevKeyInfo.getObjectID() == deletedKeyInfo.getObjectID()) {
        return true;
      }
      return false;
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

    /**
     * Get the next non deleted snapshot in the snapshot chain.
     */
    private SnapshotInfo getNextNonDeletedSnapshot(SnapshotInfo snapInfo)
        throws IOException {
      while (chainManager.hasNextPathSnapshot(snapInfo.getSnapshotPath(), 
          snapInfo.getSnapshotID())) {

        String nextPathSnapshot =
            chainManager.nextPathSnapshot(
                snapInfo.getSnapshotPath(), snapInfo.getSnapshotID());

        String tableKey = chainManager.getTableKey(nextPathSnapshot);
        SnapshotInfo nextSnapshotInfo =
            omSnapshotManager.getSnapshotInfo(tableKey);

        if (nextSnapshotInfo.getSnapshotStatus().equals(
            SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE)) {
          return nextSnapshotInfo;
        }
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
   * Suspend the service (for testing).
   */
  @VisibleForTesting
  public void suspend() {
    suspended.set(true);
  }

  /**
   * Resume the service if suspended (for testing).
   */
  @VisibleForTesting
  public void resume() {
    suspended.set(false);
  }

  public long getRunCount() {
    return runCount.get();
  }

  public long getSuccessfulRunCount() {
    return successRunCount.get();
  }
}
