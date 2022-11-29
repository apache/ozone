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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgePathRequest;
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
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_PATH_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_PATH_DELETING_LIMIT_PER_TASK_DEFAULT;

/**
 * This is a background service to delete orphan directories and its
 * sub paths(sub-dirs and sub-files).
 *
 * <p>
 * This will scan the metadata of om periodically to get the orphan dirs from
 * DeletedDirectoryTable and find its sub paths. It will fetch all sub-files
 * from FileTable and move those to DeletedTable so that OM's
 * KeyDeletingService will cleanup those files later. It will fetch all
 * sub-directories from the DirectoryTable and move those to
 * DeletedDirectoryTable so that these will be visited in next iterations.
 *
 * <p>
 * After moving all sub-files and sub-dirs the parent orphan directory will be
 * deleted by this service. It will continue traversing until all the leaf path
 * components of an orphan directory is visited.
 */
public class DirectoryDeletingService extends BackgroundService {
  public static final Logger LOG =
      LoggerFactory.getLogger(DirectoryDeletingService.class);

  private final OzoneManager ozoneManager;
  private final AtomicLong deletedDirsCount;
  private final AtomicLong movedDirsCount;
  private final AtomicLong movedFilesCount;
  private final AtomicLong runCount;

  private static ClientId clientId = ClientId.randomId();

  // Use only a single thread for DirDeletion. Multiple threads would read
  // or write to same tables and can send deletion requests for same key
  // multiple times.
  private static final int DIR_DELETING_CORE_POOL_SIZE = 1;

  // Number of items(dirs/files) to be batched in an iteration.
  private final long pathLimitPerTask;

  public DirectoryDeletingService(long interval, TimeUnit unit,
      long serviceTimeout, OzoneManager ozoneManager,
      OzoneConfiguration configuration) {
    super("DirectoryDeletingService", interval, unit,
        DIR_DELETING_CORE_POOL_SIZE, serviceTimeout);
    this.ozoneManager = ozoneManager;
    this.deletedDirsCount = new AtomicLong(0);
    this.movedDirsCount = new AtomicLong(0);
    this.movedFilesCount = new AtomicLong(0);
    this.runCount = new AtomicLong(0);
    this.pathLimitPerTask = configuration
        .getInt(OZONE_PATH_DELETING_LIMIT_PER_TASK,
            OZONE_PATH_DELETING_LIMIT_PER_TASK_DEFAULT);
  }

  private boolean shouldRun() {
    if (ozoneManager == null) {
      // OzoneManager can be null for testing
      return true;
    }
    return ozoneManager.isLeaderReady();
  }

  private boolean isRatisEnabled() {
    if (ozoneManager == null) {
      return false;
    }
    return ozoneManager.isRatisEnabled();
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new DirectoryDeletingService.DirDeletingTask());
    return queue;
  }

  private class DirDeletingTask implements BackgroundTask {

    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      if (shouldRun()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Running DirectoryDeletingService");
        }
        runCount.incrementAndGet();
        int dirNum = 0;
        int subDirNum = 0;
        int subFileNum = 0;
        long remainNum = pathLimitPerTask;
        List<PurgePathRequest> purgePathRequestList = new ArrayList<>();

        Table.KeyValue<String, OmKeyInfo> pendingDeletedDirInfo;
        try (TableIterator<String, ? extends KeyValue<String, OmKeyInfo>>
                 deleteTableIterator = ozoneManager.getMetadataManager().
            getDeletedDirTable().iterator()) {

          long startTime = Time.monotonicNow();
          while (remainNum > 0 && deleteTableIterator.hasNext()) {
            pendingDeletedDirInfo = deleteTableIterator.next();
            // step-0: Get one pending deleted directory
            if (LOG.isDebugEnabled()) {
              LOG.debug("Pending deleted dir name: {}",
                  pendingDeletedDirInfo.getValue().getKeyName());
            }
            final String[] keys = pendingDeletedDirInfo.getKey()
                .split(OM_KEY_PREFIX);
            final long volumeId = Long.parseLong(keys[1]);
            final long bucketId = Long.parseLong(keys[2]);

            // step-1: get all sub directories under the deletedDir
            List<OmKeyInfo> subDirs = ozoneManager.getKeyManager()
                .getPendingDeletionSubDirs(volumeId, bucketId,
                    pendingDeletedDirInfo.getValue(), remainNum);
            remainNum = remainNum - subDirs.size();

            if (LOG.isDebugEnabled()) {
              for (OmKeyInfo dirInfo : subDirs) {
                LOG.debug("Moved sub dir name: {}", dirInfo.getKeyName());
              }
            }

            // step-2: get all sub files under the deletedDir
            List<OmKeyInfo> subFiles = ozoneManager.getKeyManager()
                .getPendingDeletionSubFiles(volumeId, bucketId,
                    pendingDeletedDirInfo.getValue(), remainNum);
            remainNum = remainNum - subFiles.size();

            if (LOG.isDebugEnabled()) {
              for (OmKeyInfo fileInfo : subFiles) {
                LOG.debug("Moved sub file name: {}", fileInfo.getKeyName());
              }
            }

          // step-3: Since there is a boundary condition of 'numEntries' in
          // each batch, check whether the sub paths count reached batch size
          // limit. If count reached limit then there can be some more child
          // paths to be visited and will keep the parent deleted directory
          // for one more pass.
            String purgeDeletedDir = remainNum > 0 ?
                pendingDeletedDirInfo.getKey() : null;

            PurgePathRequest request = wrapPurgeRequest(volumeId, bucketId,
                purgeDeletedDir, subFiles, subDirs);
            purgePathRequestList.add(request);

            // Count up the purgeDeletedDir, subDirs and subFiles
            if (purgeDeletedDir != null) {
              dirNum++;
            }
            subDirNum += subDirs.size();
            subFileNum += subFiles.size();
          }

          // TODO: need to handle delete with non-ratis
          if (isRatisEnabled()) {
            submitPurgePaths(purgePathRequestList);
          }

          if (dirNum != 0 || subDirNum != 0 || subFileNum != 0) {
            deletedDirsCount.addAndGet(dirNum);
            movedDirsCount.addAndGet(subDirNum);
            movedFilesCount.addAndGet(subFileNum);
            LOG.info("Number of dirs deleted: {}, Number of sub-files moved:" +
                    " {} to DeletedTable, Number of sub-dirs moved {} to " +
                    "DeletedDirectoryTable, iteration elapsed: {}ms," +
                    " totalRunCount: {}",
                dirNum, subFileNum, subDirNum,
                Time.monotonicNow() - startTime, getRunCount());
          }

        } catch (IOException e) {
          LOG.error("Error while running delete directories and files " +
              "background task. Will retry at next run.", e);
        }
      }

      // place holder by returning empty results of this call back.
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }
  }

  /**
   * Returns the number of dirs deleted by the background service.
   *
   * @return Long count.
   */
  @VisibleForTesting
  public long getDeletedDirsCount() {
    return deletedDirsCount.get();
  }

  /**
   * Returns the number of sub-dirs deleted by the background service.
   *
   * @return Long count.
   */
  @VisibleForTesting
  public long getMovedDirsCount() {
    return movedDirsCount.get();
  }

  /**
   * Returns the number of files moved to DeletedTable by the background
   * service.
   *
   * @return Long count.
   */
  @VisibleForTesting
  public long getMovedFilesCount() {
    return movedFilesCount.get();
  }

  /**
   * Returns the number of times this Background service has run.
   *
   * @return Long, run count.
   */
  @VisibleForTesting
  public long getRunCount() {
    return runCount.get();
  }

  private void submitPurgePaths(List<PurgePathRequest> requests) {
    OzoneManagerProtocolProtos.PurgeDirectoriesRequest.Builder purgeDirRequest =
        OzoneManagerProtocolProtos.PurgeDirectoriesRequest.newBuilder();
    purgeDirRequest.addAllDeletedPath(requests);

    OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.PurgeDirectories)
            .setPurgeDirectoriesRequest(purgeDirRequest)
            .setClientId(clientId.toString())
            .build();

    // Submit Purge paths request to OM
    try {
      RaftClientRequest raftClientRequest =
          createRaftClientRequestForDelete(omRequest);
      ozoneManager.getOmRatisServer().submitRequest(omRequest,
          raftClientRequest);
    } catch (ServiceException e) {
      LOG.error("PurgePaths request failed. Will retry at next run.");
    }
  }

  private PurgePathRequest wrapPurgeRequest(final long volumeId,
      final long bucketId,
      final String purgeDeletedDir,
      final List<OmKeyInfo> purgeDeletedFiles,
      final List<OmKeyInfo> markDirsAsDeleted) {
    // Put all keys to be purged in a list
    PurgePathRequest.Builder purgePathsRequest = PurgePathRequest.newBuilder();
    purgePathsRequest.setVolumeId(volumeId);
    purgePathsRequest.setBucketId(bucketId);

    if (purgeDeletedDir != null) {
      purgePathsRequest.setDeletedDir(purgeDeletedDir);
    }

    for (OmKeyInfo purgeFile : purgeDeletedFiles) {
      purgePathsRequest.addDeletedSubFiles(
          purgeFile.getProtobuf(true, ClientVersion.CURRENT_VERSION));
    }

    // Add these directories to deletedDirTable, so that its sub-paths will be
    // traversed in next iteration to ensure cleanup all sub-children.
    for (OmKeyInfo dir : markDirsAsDeleted) {
      purgePathsRequest.addMarkDeletedSubDirs(
          dir.getProtobuf(ClientVersion.CURRENT_VERSION));
    }

    return purgePathsRequest.build();
  }


  private RaftClientRequest createRaftClientRequestForDelete(
      OzoneManagerProtocolProtos.OMRequest omRequest) {
    return RaftClientRequest.newBuilder()
        .setClientId(clientId)
        .setServerId(ozoneManager.getOmRatisServer().getRaftPeerId())
        .setGroupId(ozoneManager.getOmRatisServer().getRaftGroupId())
        .setCallId(runCount.get())
        .setMessage(
            Message.valueOf(
                OMRatisHelper.convertRequestToByteString(omRequest)))
        .setType(RaftClientRequest.writeRequestType())
        .build();
  }

}
