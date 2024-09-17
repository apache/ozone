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
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveTableKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotPurgeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.HddsUtils.toProtobuf;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DELETING_LIMIT_PER_TASK_DEFAULT;

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
  private final SnapshotChainManager snapshotChainManager;
  private final AtomicBoolean suspended;
  private final OzoneConfiguration conf;
  private final AtomicLong successRunCount;
  private final int keyLimitPerTask;
  private final int snapshotDeletionPerTask;
  private final int ratisByteLimit;
  private final long serviceTimeout;

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
    this.snapshotChainManager = omMetadataManager.getSnapshotChainManager();
    this.successRunCount = new AtomicLong(0);
    this.suspended = new AtomicBoolean(false);
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
    this.serviceTimeout = serviceTimeout;
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
        Iterator<UUID> iterator = snapshotChainManager.iterator(true);
        List<SnapshotInfo> snapshotsToBePurged = new ArrayList<>();
        long snapshotLimit = snapshotDeletionPerTask;
        while (iterator.hasNext() && snapshotLimit > 0) {
          SnapshotInfo snapInfo = SnapshotUtils.getSnapshotInfo(ozoneManager, snapshotChainManager, iterator.next());
          // Only Iterate in deleted snapshot & only if all the changes have been flushed into disk.
          if (shouldIgnoreSnapshot(snapInfo)) {
            continue;
          }

          SnapshotInfo nextSnapshot = SnapshotUtils.getNextSnapshot(ozoneManager, snapshotChainManager, snapInfo);
          // Continue if the next snapshot is not active. This is to avoid unnecessary copies from one snapshot to
          // another.
          if (nextSnapshot != null &&
              nextSnapshot.getSnapshotStatus() != SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE) {
            continue;
          }

          // nextSnapshot = null means entries would be moved to AOS, hence ensure that KeyDeletingService &
          // DirectoryDeletingService is not running while the entries are moving.
          if (nextSnapshot == null) {
            KeyDeletingService keyDeletingService = getOzoneManager().getKeyManager().getDeletingService();
            while (keyDeletingService != null && keyDeletingService.isRunningOnAOS()) {
              keyDeletingService.wait(serviceTimeout);
            }

            DirectoryDeletingService directoryDeletingService = getOzoneManager().getKeyManager()
                .getDirDeletingService();
            while (directoryDeletingService != null && directoryDeletingService.isRunningOnAOS()) {
              directoryDeletingService.wait(serviceTimeout);
            }
          }
          try (ReferenceCounted<OmSnapshot> snapshot = omSnapshotManager.getSnapshot(
              snapInfo.getVolumeName(), snapInfo.getBucketName(), snapInfo.getName())) {
            KeyManager snapshotKeyManager = snapshot.get().getKeyManager();
            int moveCount = 0;
            // Get all entries from deletedKeyTable.
            List<Table.KeyValue<String, List<OmKeyInfo>>> deletedKeyEntries =
                snapshotKeyManager.getDeletedKeyEntries(snapInfo.getVolumeName(), snapInfo.getBucketName(),
                    null, remaining);
            moveCount += deletedKeyEntries.size();
            // Get all entries from deletedDirTable.
            List<Table.KeyValue<String, OmKeyInfo>> deletedDirEntries = snapshotKeyManager.getDeletedDirEntries(
                snapInfo.getVolumeName(), snapInfo.getBucketName(), remaining - moveCount);
            moveCount += deletedDirEntries.size();
            // Get all entries from snapshotRenamedTable.
            List<Table.KeyValue<String, String>> renameEntries = snapshotKeyManager.getRenamesKeyEntries(
                snapInfo.getVolumeName(), snapInfo.getBucketName(), null, remaining - moveCount);
            moveCount += renameEntries.size();
            if (moveCount > 0) {
              try {
                submitSnapshotMoveDeletedKeys(snapInfo, deletedKeyEntries.stream().map(kv -> {
                  try {
                    return SnapshotMoveKeyInfos.newBuilder().setKey(kv.getKey()).addAllKeyInfos(kv.getValue()
                        .stream().map(val -> val.getProtobuf(ClientVersion.CURRENT_VERSION))
                        .collect(Collectors.toList())).build();
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                }).collect(Collectors.toList()),
                    renameEntries.stream().map(kv -> {
                      try {
                        return HddsProtos.KeyValue.newBuilder().setKey(kv.getKey()).setValue(kv.getValue()).build();
                      } catch (IOException e) {
                        throw new UncheckedIOException(e);
                      }
                    }).collect(Collectors.toList()),
                    deletedDirEntries.stream()
                        .map(kv -> {
                          try {
                            return SnapshotMoveKeyInfos.newBuilder().setKey(kv.getKey())
                                .addKeyInfos(kv.getValue().getProtobuf(ClientVersion.CURRENT_VERSION)).build();
                          } catch (IOException e) {
                            throw new UncheckedIOException(e);
                          }
                        }).collect(Collectors.toList()));
                remaining -= moveCount;
              } catch (UncheckedIOException e) {
                throw e.getCause();
              }
            } else {
              snapshotsToBePurged.add(snapInfo);
            }
          }
          successRunCount.incrementAndGet();
          snapshotLimit--;
        }
        if (!snapshotsToBePurged.isEmpty()) {
          submitSnapshotPurgeRequest(snapshotsToBePurged.stream().map(SnapshotInfo::getTableKey)
              .collect(Collectors.toList()));
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

        submitRequest(omRequest);
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
          .setCmdType(Type.SnapshotMoveDeletedKeys)
          .setSnapshotMoveTableKeysRequest(moveDeletedKeys)
          .setClientId(clientId.toString())
          .build();

      try (BootstrapStateHandler.Lock lock = new BootstrapStateHandler.Lock()) {
        submitRequest(omRequest);
      }
    }

    private void submitRequest(OMRequest omRequest) {
      try {
        OzoneManagerRatisUtils.submitRequest(ozoneManager, omRequest, clientId, getRunCount().get());
      } catch (ServiceException e) {
        LOG.error("Snapshot Deleting request failed. Will retry at next run.", e);
      }
    }
  }

  @VisibleForTesting
  boolean shouldIgnoreSnapshot(SnapshotInfo snapInfo) throws IOException {
    SnapshotInfo.SnapshotStatus snapshotStatus = snapInfo.getSnapshotStatus();
    return snapshotStatus != SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED ||
        !OmSnapshotManager.areSnapshotChangesFlushedToDB(getOzoneManager().getMetadataManager(), snapInfo);
  }

  // TODO: Move this util class.
  public static boolean isBlockLocationInfoSame(OmKeyInfo prevKeyInfo,
                                                OmKeyInfo deletedKeyInfo) {

    if (prevKeyInfo == null && deletedKeyInfo == null) {
      LOG.debug("Both prevKeyInfo and deletedKeyInfo are null.");
      return true;
    }
    if (prevKeyInfo == null || deletedKeyInfo == null) {
      LOG.debug("prevKeyInfo: '{}' or deletedKeyInfo: '{}' is null.",
          prevKeyInfo, deletedKeyInfo);
      return false;
    }
    // For hsync, Though the blockLocationInfo of a key may not be same
    // at the time of snapshot and key deletion as blocks can be appended.
    // If the objectId is same then the key is same.
    if (prevKeyInfo.isHsync() && deletedKeyInfo.isHsync()) {
      return true;
    }

    if (prevKeyInfo.getKeyLocationVersions().size() !=
        deletedKeyInfo.getKeyLocationVersions().size()) {
      return false;
    }

    OmKeyLocationInfoGroup deletedOmKeyLocation =
        deletedKeyInfo.getLatestVersionLocations();
    OmKeyLocationInfoGroup prevOmKeyLocation =
        prevKeyInfo.getLatestVersionLocations();

    if (deletedOmKeyLocation == null || prevOmKeyLocation == null) {
      return false;
    }

    List<OmKeyLocationInfo> deletedLocationList =
        deletedOmKeyLocation.getLocationList();
    List<OmKeyLocationInfo> prevLocationList =
        prevOmKeyLocation.getLocationList();

    if (deletedLocationList.size() != prevLocationList.size()) {
      return false;
    }

    for (int idx = 0; idx < deletedLocationList.size(); idx++) {
      OmKeyLocationInfo deletedLocationInfo = deletedLocationList.get(idx);
      OmKeyLocationInfo prevLocationInfo = prevLocationList.get(idx);
      if (!deletedLocationInfo.hasSameBlockAs(prevLocationInfo)) {
        return false;
      }
    }

    return true;
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
