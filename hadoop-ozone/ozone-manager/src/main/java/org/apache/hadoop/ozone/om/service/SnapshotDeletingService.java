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
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgePathRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
  private final SnapshotChainManager chainManager;
  private final AtomicBoolean suspended;
  private final OzoneConfiguration conf;
  private final AtomicLong successRunCount;
  private final long snapshotDeletionPerTask;

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
  }

  private class SnapshotDeletingTask implements BackgroundTask {

    @SuppressWarnings("checkstyle:MethodLength")
    @Override
    public BackgroundTaskResult call() throws InterruptedException {
      if (!shouldRun()) {
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }

      getRunCount().incrementAndGet();
      SnapshotChainManager snapshotChainManager =
          ((OmMetadataManagerImpl)ozoneManager.getMetadataManager()).getSnapshotChainManager();

      try {
        Iterator<UUID> iterator = snapshotChainManager.iterator(true);
        long snapshotLimit = snapshotDeletionPerTask;
        while (iterator.hasNext() && snapshotLimit > 0) {
          SnapshotInfo snapInfo = SnapshotUtils.getSnapshotInfo(ozoneManager, chainManager, iterator.next());
          // Only Iterate in deleted snapshot & only if all the changes have been flushed into disk.
          if (shouldIgnoreSnapshot(snapInfo)) {
            continue;
          }

          SnapshotInfo nextSnapshot = SnapshotUtils.getNextSnapshot(ozoneManager, chainManager, snapInfo);
          // Continue if the next snapshot is not active. This is to avoid unnecessary copies from one snapshot to
          // another.
          if (nextSnapshot != null &&  nextSnapshot.getSnapshotStatus() != SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE) {
            continue;
          }

          // Wait for the next iteration if the next snapshot is still running deep cleaning or AOS is running
          // cleaning, since purge transaction will add entries and it could be processed by mistake.
          if ((nextSnapshot == null && isDeepCleaningRunningOnAOS()) ||
              (nextSnapshot != null && isSnapshotDeepCleaningServiceRunning(nextSnapshot))) {
            continue;
          }
          successRunCount.incrementAndGet();
          submitSnapshotPurgeAndMoveRequest(snapInfo, nextSnapshot);
          snapshotLimit--;
        }
      } catch (IOException e) {
        LOG.error("Error while running Snapshot Deleting Service", e);
      }
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }

    public void submitSnapshotPurgeAndMoveRequest(SnapshotInfo snapInfo, SnapshotInfo nextSnapshotInfo) {

      OzoneManagerProtocolProtos.SnapshotPurgeAndMoveRequest.Builder requestBuilder =
          OzoneManagerProtocolProtos.SnapshotPurgeAndMoveRequest.newBuilder().setSnapshotID(
              HddsUtils.toProtobuf(snapInfo.getSnapshotId()));

      // When null it means next target is the AOS.
      if (nextSnapshotInfo != null) {
        requestBuilder.setExpectedNextSnapshotID(HddsUtils.toProtobuf(nextSnapshotInfo.getSnapshotId()));
      }
      OMRequest omRequest = OMRequest.newBuilder()
          .setCmdType(Type.SnapshotPurgeAndMove)
          .setSnapshotPurgeAndMoveRequest(requestBuilder.build())
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

  @VisibleForTesting
  boolean shouldIgnoreSnapshot(SnapshotInfo snapInfo) throws IOException {
    SnapshotInfo.SnapshotStatus snapshotStatus = snapInfo.getSnapshotStatus();
    return snapshotStatus != SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED ||
        OmSnapshotManager.areSnapshotChangesFlushedToDB(getOzoneManager().getMetadataManager(), snapInfo)
        && isSnapshotDeepCleaningServiceRunning(snapInfo);
  }
  private boolean isSnapshotDeepCleaningServiceRunning(SnapshotInfo snapInfo) {
    return (!snapInfo.getDeepClean() || !snapInfo.getDeepCleanedDeletedDir());
  }

  private boolean isDeepCleaningRunningOnAOS() {
    return getOzoneManager().getKeyManager().getDeletingService().isRunningOnAOS() ||
        getOzoneManager().getKeyManager().getDirDeletingService().isRunningOnAOS();
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
