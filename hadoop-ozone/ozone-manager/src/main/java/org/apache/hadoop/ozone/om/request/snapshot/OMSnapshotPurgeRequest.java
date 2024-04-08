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

package org.apache.hadoop.ozone.om.request.snapshot;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotPurgeResponse;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotPurgeRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.SNAPSHOT_LOCK;

/**
 * Handles OMSnapshotPurge Request.
 * This is an OM internal request. Does not need @RequireSnapshotFeatureState.
 */
public class OMSnapshotPurgeRequest extends OMClientRequest {

  private static final Logger LOG = LoggerFactory.getLogger(OMSnapshotPurgeRequest.class);

  public OMSnapshotPurgeRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, TermIndex termIndex) {
    OMMetrics omMetrics = ozoneManager.getMetrics();

    final long trxnLogIndex = termIndex.getIndex();

    OmSnapshotManager omSnapshotManager = ozoneManager.getOmSnapshotManager();
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl)
        ozoneManager.getMetadataManager();
    SnapshotChainManager snapshotChainManager =
        omMetadataManager.getSnapshotChainManager();

    OMClientResponse omClientResponse = null;

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    SnapshotPurgeRequest snapshotPurgeRequest = getOmRequest()
        .getSnapshotPurgeRequest();

    try {
      List<String> snapshotDbKeys = snapshotPurgeRequest
          .getSnapshotDBKeysList();
      Map<String, SnapshotInfo> updatedSnapInfos = new HashMap<>();
      Map<String, SnapshotInfo> updatedPathPreviousAndGlobalSnapshots =
          new HashMap<>();

      // Each snapshot purge operation does three things:
      //  1. Update the snapshot chain,
      //  2. Update the deep clean flag for the next active snapshot (So that it can be
      //     deep cleaned by the KeyDeletingService in the next run),
      //  3. Finally, purge the snapshot.
      // All of these steps have to be performed only when it acquires all the necessary
      // locks (lock on the snapshot to be purged, lock on the next active snapshot, and
      // lock on the next path and global previous snapshots). Ideally, there is no need
      // for locks for snapshot purge and can rely on OMStateMachine because OMStateMachine
      // is going to process each request sequentially.
      //
      // But there is a problem with that. After filtering unnecessary SST files for a snapshot,
      // SstFilteringService updates that snapshot's SstFilter flag. SstFilteringService cannot
      // use SetSnapshotProperty API because it runs on each OM independently and One OM does
      // not know if the snapshot has been filtered on the other OM in HA environment.
      //
      // If locks are not taken snapshot purge and SstFilteringService will cause a race condition
      // and override one's update with another.
      for (String snapTableKey : snapshotDbKeys) {
        // To acquire all the locks, a set is maintained which is keyed by snapshotTableKey.
        // snapshotTableKey is nothing but /volumeName/bucketName/snapshotName.
        // Once all the locks are acquired, it performs the three steps mentioned above and
        // release all the locks after that.
        Set<Triple<String, String, String>> lockSet = new HashSet<>(4, 1);
        try {
          if (omMetadataManager.getSnapshotInfoTable().get(snapTableKey) == null) {
            // Snapshot may have been purged in the previous iteration of SnapshotDeletingService.
            LOG.warn("The snapshot {} is not longer in snapshot table, It maybe removed in the previous " +
                "Snapshot purge request.", snapTableKey);
            continue;
          }

          acquireLock(lockSet, snapTableKey, omMetadataManager);
          SnapshotInfo fromSnapshot = omMetadataManager.getSnapshotInfoTable().get(snapTableKey);

          SnapshotInfo nextSnapshot =
              SnapshotUtils.getNextActiveSnapshot(fromSnapshot, snapshotChainManager, omSnapshotManager);

          if (nextSnapshot != null) {
            acquireLock(lockSet, nextSnapshot.getTableKey(), omMetadataManager);
          }

          // Update the chain first so that it has all the necessary locks before updating deep clean.
          updateSnapshotChainAndCache(lockSet, omMetadataManager, fromSnapshot, trxnLogIndex,
              updatedPathPreviousAndGlobalSnapshots);
          updateSnapshotInfoAndCache(nextSnapshot, omMetadataManager, trxnLogIndex, updatedSnapInfos);
          // Remove and close snapshot's RocksDB instance from SnapshotCache.
          omSnapshotManager.invalidateCacheEntry(fromSnapshot.getSnapshotId());
          // Update SnapshotInfoTable cache.
          omMetadataManager.getSnapshotInfoTable()
              .addCacheEntry(new CacheKey<>(fromSnapshot.getTableKey()), CacheValue.get(trxnLogIndex));
        } finally {
          for (Triple<String, String, String> lockKey: lockSet) {
            omMetadataManager.getLock()
                .releaseWriteLock(SNAPSHOT_LOCK, lockKey.getLeft(), lockKey.getMiddle(), lockKey.getRight());
          }
        }
      }

      omClientResponse = new OMSnapshotPurgeResponse(omResponse.build(),
          snapshotDbKeys, updatedSnapInfos,
          updatedPathPreviousAndGlobalSnapshots);

      omMetrics.incNumSnapshotPurges();
      LOG.info("Successfully executed snapshotPurgeRequest: {{}} along with updating deep clean flags for " +
              "snapshots: {} and global and previous for snapshots:{}.",
          snapshotPurgeRequest, updatedSnapInfos.keySet(), updatedPathPreviousAndGlobalSnapshots.keySet());
    } catch (IOException ex) {
      omClientResponse = new OMSnapshotPurgeResponse(
          createErrorOMResponse(omResponse, ex));
      omMetrics.incNumSnapshotPurgeFails();
      LOG.error("Failed to execute snapshotPurgeRequest:{{}}.", snapshotPurgeRequest, ex);
    }

    return omClientResponse;
  }

  private void acquireLock(Set<Triple<String, String, String>> lockSet, String snapshotTableKey,
                           OMMetadataManager omMetadataManager) throws IOException {
    SnapshotInfo snapshotInfo = omMetadataManager.getSnapshotInfoTable().get(snapshotTableKey);

    // It should not be the case that lock is required for non-existing snapshot.
    if (snapshotInfo == null) {
      LOG.error("Snapshot: '{}' doesn't not exist in snapshot table.", snapshotTableKey);
      throw new OMException("Snapshot: '{" + snapshotTableKey + "}' doesn't not exist in snapshot table.",
          OMException.ResultCodes.FILE_NOT_FOUND);
    }
    Triple<String, String, String> lockKey = Triple.of(snapshotInfo.getVolumeName(), snapshotInfo.getBucketName(),
        snapshotInfo.getName());
    if (!lockSet.contains(lockKey)) {
      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(SNAPSHOT_LOCK, lockKey.getLeft(), lockKey.getMiddle(), lockKey.getRight()));
      lockSet.add(lockKey);
    }
  }

  private void updateSnapshotInfoAndCache(SnapshotInfo snapInfo,
      OmMetadataManagerImpl omMetadataManager, long trxnLogIndex,
      Map<String, SnapshotInfo> updatedSnapInfos) throws IOException {
    if (snapInfo != null) {
      // Fetch the latest value again after acquiring lock.
      SnapshotInfo updatedSnapshotInfo = omMetadataManager.getSnapshotInfoTable().get(snapInfo.getTableKey());

      // Setting next snapshot deep clean to false, Since the
      // current snapshot is deleted. We can potentially
      // reclaim more keys in the next snapshot.
      updatedSnapshotInfo.setDeepClean(false);

      // Update table cache first
      omMetadataManager.getSnapshotInfoTable().addCacheEntry(new CacheKey<>(updatedSnapshotInfo.getTableKey()),
          CacheValue.get(trxnLogIndex, updatedSnapshotInfo));
      updatedSnapInfos.put(updatedSnapshotInfo.getTableKey(), updatedSnapshotInfo);
    }
  }

  /**
   * Removes the snapshot from the chain and updates the next snapshot's
   * previousPath and previousGlobal IDs in DB cache.
   * It also returns the pair of updated next path and global snapshots to
   * update in DB.
   */
  private void updateSnapshotChainAndCache(
      Set<Triple<String, String, String>> lockSet,
      OmMetadataManagerImpl metadataManager,
      SnapshotInfo snapInfo,
      long trxnLogIndex,
      Map<String, SnapshotInfo> updatedPathPreviousAndGlobalSnapshots
  ) throws IOException {
    if (snapInfo == null) {
      return;
    }

    SnapshotChainManager snapshotChainManager = metadataManager
        .getSnapshotChainManager();

    // If the snapshot is deleted in the previous run, then the in-memory
    // SnapshotChainManager might throw NoSuchElementException as the snapshot
    // is removed in-memory but OMDoubleBuffer has not flushed yet.
    boolean hasNextPathSnapshot;
    boolean hasNextGlobalSnapshot;
    try {
      hasNextPathSnapshot = snapshotChainManager.hasNextPathSnapshot(
          snapInfo.getSnapshotPath(), snapInfo.getSnapshotId());
      hasNextGlobalSnapshot = snapshotChainManager.hasNextGlobalSnapshot(
          snapInfo.getSnapshotId());
    } catch (NoSuchElementException ex) {
      return;
    }

    String nextPathSnapshotKey = null;

    if (hasNextPathSnapshot) {
      UUID nextPathSnapshotId = snapshotChainManager.nextPathSnapshot(
          snapInfo.getSnapshotPath(), snapInfo.getSnapshotId());
      nextPathSnapshotKey = snapshotChainManager
          .getTableKey(nextPathSnapshotId);

      // Acquire lock from the snapshot
      acquireLock(lockSet, nextPathSnapshotKey, metadataManager);
    }

    String nextGlobalSnapshotKey = null;
    if (hasNextGlobalSnapshot) {
      UUID nextGlobalSnapshotId = snapshotChainManager.nextGlobalSnapshot(snapInfo.getSnapshotId());
      nextGlobalSnapshotKey = snapshotChainManager.getTableKey(nextGlobalSnapshotId);

      // Acquire lock from the snapshot
      acquireLock(lockSet, nextGlobalSnapshotKey, metadataManager);
    }

    SnapshotInfo nextPathSnapInfo =
        nextPathSnapshotKey != null ? metadataManager.getSnapshotInfoTable().get(nextPathSnapshotKey) : null;

    SnapshotInfo nextGlobalSnapInfo =
        nextGlobalSnapshotKey != null ? metadataManager.getSnapshotInfoTable().get(nextGlobalSnapshotKey) : null;

    // Updates next path snapshot's previous snapshot ID
    if (nextPathSnapInfo != null) {
      nextPathSnapInfo.setPathPreviousSnapshotId(snapInfo.getPathPreviousSnapshotId());
      metadataManager.getSnapshotInfoTable().addCacheEntry(
          new CacheKey<>(nextPathSnapInfo.getTableKey()),
          CacheValue.get(trxnLogIndex, nextPathSnapInfo));
      updatedPathPreviousAndGlobalSnapshots
          .put(nextPathSnapInfo.getTableKey(), nextPathSnapInfo);
    }

    // Updates next global snapshot's previous snapshot ID
    // If both next global and path snapshot are same, it may overwrite
    // nextPathSnapInfo.setPathPreviousSnapshotID(), adding this check
    // will prevent it.
    if (nextGlobalSnapInfo != null && nextPathSnapInfo != null &&
        nextGlobalSnapInfo.getSnapshotId().equals(nextPathSnapInfo.getSnapshotId())) {
      nextPathSnapInfo.setGlobalPreviousSnapshotId(snapInfo.getGlobalPreviousSnapshotId());
      metadataManager.getSnapshotInfoTable().addCacheEntry(
          new CacheKey<>(nextPathSnapInfo.getTableKey()),
          CacheValue.get(trxnLogIndex, nextPathSnapInfo));
      updatedPathPreviousAndGlobalSnapshots
          .put(nextPathSnapInfo.getTableKey(), nextPathSnapInfo);
    } else if (nextGlobalSnapInfo != null) {
      nextGlobalSnapInfo.setGlobalPreviousSnapshotId(
          snapInfo.getGlobalPreviousSnapshotId());
      metadataManager.getSnapshotInfoTable().addCacheEntry(
          new CacheKey<>(nextGlobalSnapInfo.getTableKey()),
          CacheValue.get(trxnLogIndex, nextGlobalSnapInfo));
      updatedPathPreviousAndGlobalSnapshots
          .put(nextGlobalSnapInfo.getTableKey(), nextGlobalSnapInfo);
    }

    snapshotChainManager.deleteSnapshot(snapInfo);
  }
}
