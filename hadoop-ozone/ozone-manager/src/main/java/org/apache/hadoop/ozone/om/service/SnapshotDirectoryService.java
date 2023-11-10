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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ServiceException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.IOmMetadataReader;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.om.snapshot.SnapshotCache;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetSnapshotPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotSize;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPrefix;
import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.getDirectoryInfo;

/**
 * Snapshot BG Service for deleted directory deep clean and exclusive size
 * calculation for deleted directories.
 */
public class SnapshotDirectoryService extends AbstractKeyDeletingService {
  // Use only a single thread for DirDeletion. Multiple threads would read
  // or write to same tables and can send deletion requests for same key
  // multiple times.
  private static final int SNAPSHOT_DIR_CORE_POOL_SIZE = 1;

  private final AtomicBoolean suspended;
  private final Map<String, Long> exclusiveSizeMap;
  private final Map<String, Long> exclusiveReplicatedSizeMap;

  public SnapshotDirectoryService(long interval, TimeUnit unit,
                                  long serviceTimeout,
                                  OzoneManager ozoneManager,
                                  ScmBlockLocationProtocol scmClient) {
    super(SnapshotDirectoryService.class.getSimpleName(), interval, unit,
        SNAPSHOT_DIR_CORE_POOL_SIZE, serviceTimeout, ozoneManager, scmClient);
    this.suspended = new AtomicBoolean(false);
    this.exclusiveSizeMap = new HashMap<>();
    this.exclusiveReplicatedSizeMap = new HashMap<>();
  }

  private boolean shouldRun() {
    if (getOzoneManager() == null) {
      // OzoneManager can be null for testing
      return true;
    }
    return getOzoneManager().isLeaderReady() && !suspended.get();
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

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new SnapshotDirectoryService.SnapshotDirTask());
    return queue;
  }

  private class SnapshotDirTask implements BackgroundTask {

    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() {
      if (shouldRun()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Running SnapshotDirectoryService");
        }
      }

      getRunCount().incrementAndGet();
      OmSnapshotManager omSnapshotManager =
          getOzoneManager().getOmSnapshotManager();
      Table<String, SnapshotInfo> snapshotInfoTable =
          getOzoneManager().getMetadataManager().getSnapshotInfoTable();
      OmMetadataManagerImpl metadataManager = (OmMetadataManagerImpl)
          getOzoneManager().getMetadataManager();
      SnapshotChainManager snapChainManager = metadataManager
          .getSnapshotChainManager();

      try (TableIterator<String, ? extends Table.KeyValue
          <String, SnapshotInfo>> iterator = snapshotInfoTable.iterator()) {

        while (iterator.hasNext()) {
          SnapshotInfo currSnapInfo = iterator.next().getValue();

          // Expand deleted dirs only on active snapshot. Deleted Snapshots
          // will be cleaned up by SnapshotDeletingService.
          if (currSnapInfo.getSnapshotStatus() != SNAPSHOT_ACTIVE ||
              currSnapInfo.getDeepCleanedDeletedDir()) {
            continue;
          }

          long volumeId = getOzoneManager().getMetadataManager()
              .getVolumeId(currSnapInfo.getVolumeName());
          // Get bucketInfo for the snapshot bucket to get bucket layout.
          String dbBucketKey = getOzoneManager().getMetadataManager()
              .getBucketKey(currSnapInfo.getVolumeName(),
                  currSnapInfo.getBucketName());
          OmBucketInfo bucketInfo = getOzoneManager().getMetadataManager()
              .getBucketTable().get(dbBucketKey);

          if (bucketInfo == null) {
            throw new IllegalStateException("Bucket " + "/" +
                currSnapInfo.getVolumeName() + "/" + currSnapInfo
                .getBucketName() + " is not found. BucketInfo should not be " +
                "null for snapshotted bucket. The OM is in unexpected state.");
          }

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

          String dbBucketKeyForDir = getOzoneManager().getMetadataManager()
              .getBucketKey(Long.toString(volumeId),
                  Long.toString(bucketInfo.getObjectID())) + OM_KEY_PREFIX;

          try (ReferenceCounted<IOmMetadataReader, SnapshotCache>
                   rcCurrOmSnapshot = omSnapshotManager.checkForSnapshot(
              currSnapInfo.getVolumeName(),
              currSnapInfo.getBucketName(),
              getSnapshotPrefix(currSnapInfo.getName()),
              true)) {

            OmSnapshot currOmSnapshot = (OmSnapshot) rcCurrOmSnapshot.get();
            Table<String, OmKeyInfo> snapDeletedDirTable =
                currOmSnapshot.getMetadataManager().getDeletedDirTable();
            Table<String, OmDirectoryInfo> snapDirTable =
                currOmSnapshot.getMetadataManager().getDirectoryTable();
            Table<String, String> snapRenamedTable =
                currOmSnapshot.getMetadataManager().getSnapshotRenamedTable();

            try (TableIterator<String, ? extends Table.KeyValue<String,
                OmKeyInfo>> deletedDirIterator = snapDeletedDirTable
                .iterator(dbBucketKeyForDir)) {

              while (deletedDirIterator.hasNext()) {
                Table.KeyValue<String, OmKeyInfo> deletedDirInfo =
                    deletedDirIterator.next();
                String deletedDirKey = deletedDirInfo.getKey();

                // Exit if it is out of the bucket scope.
                if (!deletedDirKey.startsWith(dbBucketKeyForDir)) {
                  break;
                }

                // For each deleted directory we do an in-memory DFS and
                // do a deep clean and exclusive size calculation.
                iterateDirectoryTree(deletedDirInfo, volumeId, bucketInfo,
                    snapDirTable, previousSnapshot, previousToPrevSnapshot,
                    currOmSnapshot, snapRenamedTable, previousKeyTable,
                    prevRenamedTable, previousToPrevKeyTable,
                    dbBucketKeyForDir);
              }
              updateDeepCleanSnapshotDir(currSnapInfo.getTableKey());
              if (previousSnapshot != null) {
                updateExclusiveSize(previousSnapshot.getTableKey());
              }
            }
          }
        }
      } catch (IOException ex) {
        LOG.error("Error while running directory deep clean on snapshots." +
            " Will retry at next run.", ex);
      }
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private void iterateDirectoryTree(
      Table.KeyValue<String, OmKeyInfo> deletedDirInfo, long volumeId,
      OmBucketInfo bucketInfo,
      Table<String, OmDirectoryInfo> snapDirTable,
      SnapshotInfo previousSnapshot,
      SnapshotInfo previousToPrevSnapshot,
      OmSnapshot currOmSnapshot,
      Table<String, String> snapRenamedTable,
      Table<String, OmKeyInfo> previousKeyTable,
      Table<String, String> prevRenamedTable,
      Table<String, OmKeyInfo> previousToPrevKeyTable,
      String dbBucketKeyForDir) throws IOException {
    Stack<StackNode> stackNodes =
        new Stack<>();
    OmDirectoryInfo omDeletedDirectoryInfo =
        getDirectoryInfo(deletedDirInfo.getValue());
    String dirPathDbKey = currOmSnapshot.getMetadataManager()
        .getOzonePathKey(volumeId, bucketInfo.getObjectID(),
            omDeletedDirectoryInfo);
    // Stack Init
    StackNode topLevelDir = new StackNode();
    topLevelDir.setDirKey(dirPathDbKey);
    topLevelDir.setDirValue(omDeletedDirectoryInfo);
    stackNodes.add(topLevelDir);

    try (
        TableIterator<String, ? extends Table.KeyValue<String, OmDirectoryInfo>>
            directoryIterator = snapDirTable.iterator(dbBucketKeyForDir)) {

      while (!stackNodes.isEmpty()) {
        StackNode stackTop = stackNodes.pop();
        String seekDirInDB;
        // First process all the files in the current directory
        // and then do a DFS for directory.
        if (StringUtils.isEmpty(stackTop.getSubDirSeek())) {
          processFilesUnderDir(previousSnapshot,
              previousToPrevSnapshot,
              volumeId,
              bucketInfo,
              stackTop.getDirValue(),
              currOmSnapshot.getMetadataManager(),
              snapRenamedTable,
              previousKeyTable,
              prevRenamedTable,
              previousToPrevKeyTable);
          seekDirInDB = currOmSnapshot.getMetadataManager()
              .getOzonePathKey(volumeId, bucketInfo.getObjectID(),
                  stackTop.getDirValue().getObjectID(), "");
          directoryIterator.seek(seekDirInDB);
        } else {
          // When a leaf node is processed, we need come back in
          // the call stack and process the next directories.
          seekDirInDB = stackTop.getSubDirSeek();
          directoryIterator.seek(seekDirInDB);
          if (directoryIterator.hasNext()) {
            directoryIterator.next();
          } else {
            continue;
          }
        }

        if (directoryIterator.hasNext()) {
          Table.KeyValue<String, OmDirectoryInfo> deletedSubDirInfo =
              directoryIterator.next();
          String deletedSubDirKey = deletedSubDirInfo.getKey();

          String prefixCheck = currOmSnapshot.getMetadataManager()
              .getOzoneDeletePathDirKey(seekDirInDB);
          // Exit if it is out of the sub dir prefix scope.
          if (!deletedSubDirKey.startsWith(prefixCheck)) {
            // Add exit condition.
            continue;
          }
          stackTop.setSubDirSeek(deletedSubDirKey);
          stackNodes.add(stackTop);
          StackNode nextSubDir = new StackNode();
          nextSubDir.setDirKey(deletedSubDirInfo.getKey());
          nextSubDir.setDirValue(deletedSubDirInfo.getValue());
          stackNodes.add(nextSubDir);
        }

      }
    }
  }

  private void updateExclusiveSize(String prevSnapshotKeyTable) {
    ClientId clientId = ClientId.randomId();
    SnapshotSize snapshotSize = SnapshotSize.newBuilder()
            .setExclusiveSize(
                exclusiveSizeMap.getOrDefault(prevSnapshotKeyTable, 0L))
            .setExclusiveReplicatedSize(
                exclusiveReplicatedSizeMap.getOrDefault(
                    prevSnapshotKeyTable, 0L))
            .build();
    exclusiveSizeMap.remove(prevSnapshotKeyTable);
    exclusiveReplicatedSizeMap.remove(prevSnapshotKeyTable);
    SetSnapshotPropertyRequest
        setSnapshotPropertyRequest =
        SetSnapshotPropertyRequest.newBuilder()
            .setSnapshotKey(prevSnapshotKeyTable)
            .setSnapshotSize(snapshotSize)
            .build();

    OMRequest omRequest = OMRequest.newBuilder()
            .setCmdType(Type.SetSnapshotProperty)
            .setSetSnapshotPropertyRequest(setSnapshotPropertyRequest)
            .setClientId(clientId.toString())
            .build();

    submitRequest(omRequest, clientId);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private void processFilesUnderDir(
      SnapshotInfo previousSnapshot,
      SnapshotInfo previousToPrevSnapshot,
      long volumeId,
      OmBucketInfo bucketInfo,
      OmDirectoryInfo parentInfo,
      OMMetadataManager metadataManager,
      Table<String, String> snapRenamedTable,
      Table<String, OmKeyInfo> previousKeyTable,
      Table<String, String> prevRenamedTable,
      Table<String, OmKeyInfo> previousToPrevKeyTable)
        throws IOException {
    String seekFileInDB = metadataManager.getOzonePathKey(volumeId,
        bucketInfo.getObjectID(),
        parentInfo.getObjectID(), "");
    List<BlockGroup> blocksForKeyDelete = new ArrayList<>();

    Table fileTable = metadataManager.getFileTable();
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
             iterator = fileTable.iterator()) {
      iterator.seek(seekFileInDB);

      while (iterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> entry = iterator.next();
        OmKeyInfo fileInfo = entry.getValue();
        if (!OMFileRequest.isImmediateChild(fileInfo.getParentObjectID(),
            parentInfo.getObjectID())) {
          break;
        }

        String ozoneDeletePathKey = metadataManager
            .getOzoneDeletePathKey(fileInfo.getObjectID(), entry.getKey());
        if (isKeyReclaimable(previousKeyTable, snapRenamedTable,
            fileInfo, bucketInfo, volumeId, null)) {
          for (OmKeyLocationInfoGroup keyLocations :
              fileInfo.getKeyLocationVersions()) {
            List<BlockID> item = keyLocations.getLocationList().stream()
                .map(b -> new BlockID(b.getContainerID(), b.getLocalID()))
                .collect(Collectors.toList());
            BlockGroup keyBlocks = BlockGroup.newBuilder()
                .setKeyName(ozoneDeletePathKey)
                .addAllBlockIDs(item)
                .build();
            blocksForKeyDelete.add(keyBlocks);
          }
          // TODO: Add Retry mechanism.
          getScmClient().deleteKeyBlocks(blocksForKeyDelete);
        } else if (previousSnapshot != null) {
          calculateExclusiveSize(previousSnapshot, previousToPrevSnapshot,
              fileInfo, bucketInfo, volumeId, snapRenamedTable,
              previousKeyTable, prevRenamedTable, previousToPrevKeyTable,
              exclusiveSizeMap, exclusiveReplicatedSizeMap);
        }
      }
    }
  }

  private void updateDeepCleanSnapshotDir(String snapshotKeyTable) {
    ClientId clientId = ClientId.randomId();
    SetSnapshotPropertyRequest setSnapshotPropertyRequest =
        SetSnapshotPropertyRequest.newBuilder()
            .setSnapshotKey(snapshotKeyTable)
            .setDeepCleanedDeletedDir(true)
            .build();

    OMRequest omRequest = OMRequest.newBuilder()
            .setCmdType(Type.SetSnapshotProperty)
            .setSetSnapshotPropertyRequest(setSnapshotPropertyRequest)
            .setClientId(clientId.toString())
            .build();

    submitRequest(omRequest, clientId);
  }

  public void submitRequest(OMRequest omRequest, ClientId clientId) {
    try {
      if (isRatisEnabled()) {
        OzoneManagerRatisServer server =
            getOzoneManager().getOmRatisServer();

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

  /**
   * Stack node data for directory deep clean for snapshot.
   */
  public class StackNode {
    private String dirKey;
    private OmDirectoryInfo dirValue;
    private String subDirSeek;

    public String getDirKey() {
      return dirKey;
    }

    public void setDirKey(String dirKey) {
      this.dirKey = dirKey;
    }

    public OmDirectoryInfo getDirValue() {
      return dirValue;
    }

    public void setDirValue(OmDirectoryInfo dirValue) {
      this.dirValue = dirValue;
    }

    public String getSubDirSeek() {
      return subDirSeek;
    }

    public void setSubDirSeek(String subDirSeek) {
      this.subDirSeek = subDirSeek;
    }

    @Override
    public String toString() {
      return "StackNode{" +
          "dirKey='" + dirKey + '\'' +
          ", dirObjectId=" + dirValue.getObjectID() +
          ", subDirSeek='" + subDirSeek + '\'' +
          '}';
    }
  }
}
