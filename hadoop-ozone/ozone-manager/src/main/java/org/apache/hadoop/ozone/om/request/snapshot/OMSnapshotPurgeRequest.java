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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotPurgeResponse.OmPurgeResponse;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
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

    OMClientResponse omClientResponse;

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    SnapshotPurgeRequest snapshotPurgeRequest = getOmRequest()
        .getSnapshotPurgeRequest();

    try {
      // This is for backward compatibility. If ratis has batch purge transactions which are replayed
      // either during service upgraded or later for any other reason when OM has been upgraded to non-batch.
      if (!snapshotPurgeRequest.getSnapshotDBKeysList().isEmpty()) {
        List<OmPurgeResponse> omPurgeResponses = new ArrayList<>();
        for (String snapshotKey : snapshotPurgeRequest.getSnapshotDBKeysList()) {
          OmPurgeResponse omPurgeResponse = purgeSnapshot(snapshotKey, ozoneManager, trxnLogIndex);
          omPurgeResponses.add(omPurgeResponse);
        }
        omClientResponse = new OMSnapshotPurgeResponse(omResponse.build(), omPurgeResponses);
        LOG.info("OmPurgeResponse list: {}.", omPurgeResponses);
      } else if (snapshotPurgeRequest.hasSnapshotKey()) {
        OmPurgeResponse omPurgeResponse =
            purgeSnapshot(snapshotPurgeRequest.getSnapshotKey(), ozoneManager, trxnLogIndex);
        omClientResponse = new OMSnapshotPurgeResponse(omResponse.build(), omPurgeResponse);
        LOG.info("OmPurgeResponse: {}.", omPurgeResponse);
      } else {
        throw new OMException("Both snapshotDBKeysList and snapshotKey are null." +
            " One of them is required for snapshot purge operations.", OMException.ResultCodes.INVALID_REQUEST);
      }

      omMetrics.incNumSnapshotPurges();
      LOG.debug("Successfully executed snapshotPurgeRequest: {{}}.", snapshotPurgeRequest);
    } catch (IOException ex) {
      omClientResponse = new OMSnapshotPurgeResponse(createErrorOMResponse(omResponse, ex));
      omMetrics.incNumSnapshotPurgeFails();
      LOG.error("Failed to execute snapshotPurgeRequest:{{}}.", snapshotPurgeRequest, ex);
    }

    return omClientResponse;
  }

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
  private OmPurgeResponse purgeSnapshot(String snapshotKey,
                                        OzoneManager ozoneManager,
                                        long trxnLogIndex) throws IOException {

    OmSnapshotManager omSnapshotManager = ozoneManager.getOmSnapshotManager();
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl) ozoneManager.getMetadataManager();
    SnapshotChainManager snapshotChainManager = omMetadataManager.getSnapshotChainManager();

    if (omMetadataManager.getSnapshotInfoTable().get(snapshotKey) == null) {
      // Snapshot may have been purged in the previous iteration of SnapshotDeletingService.
      throw new OMException("Snapshot: '" + snapshotKey + "}' is no longer exist " +
          "in snapshot table. Might be removed in previous run.", OMException.ResultCodes.FILE_NOT_FOUND);
    }

    // To acquire all the locks, a set is maintained which is keyed by a triple of volumeName, bucketName and
    // snapshotName. SnapshotInfoTable key (which is /volumeName/bucketName/snapshotName) is not directly
    // because volumeName, bucketName and snapshotName can't be obtained after purging snapshot from cache.
    // Once all the necessary locks are acquired, the three steps mentioned above are performed and
    // locks are release after that.
    Set<Triple<String, String, String>> lockSet = new HashSet<>(4, 1);

    try {
      acquireLock(lockSet, snapshotKey, omMetadataManager);

      SnapshotInfo fromSnapshot = omMetadataManager.getSnapshotInfoTable().get(snapshotKey);
      SnapshotInfo nextSnapshot =
          SnapshotUtils.getNextActiveSnapshot(fromSnapshot, snapshotChainManager, omSnapshotManager);

      if (nextSnapshot != null) {
        acquireLock(lockSet, nextSnapshot.getTableKey(), omMetadataManager);
      }

      // Step 1: Update the snapshot chain.
      Pair<SnapshotInfo, SnapshotInfo> pathToGlobalSnapshotInto =
          updateSnapshotChainAndCache(lockSet, omMetadataManager, fromSnapshot, trxnLogIndex);
      SnapshotInfo nextPathSnapshotInfo = null;
      SnapshotInfo nextGlobalSnapshotInfo = null;

      if (pathToGlobalSnapshotInto != null) {
        nextPathSnapshotInfo = pathToGlobalSnapshotInto.getLeft();
        nextGlobalSnapshotInfo = pathToGlobalSnapshotInto.getRight();
      }

      // Step 2: Update the deep clean flag for the next active snapshot
      SnapshotInfo nextActiveSnapshotInfo = updateSnapshotInfoAndCache(nextSnapshot, omMetadataManager, trxnLogIndex);

      // Remove and close snapshot's RocksDB instance from SnapshotCache.
      ozoneManager.getOmSnapshotManager().invalidateCacheEntry(fromSnapshot.getSnapshotId());

      // Step 3: Purge the snapshot from SnapshotInfoTable cache.
      ozoneManager.getMetadataManager().getSnapshotInfoTable()
          .addCacheEntry(new CacheKey<>(fromSnapshot.getTableKey()), CacheValue.get(trxnLogIndex));

      return new OmPurgeResponse(snapshotKey, nextPathSnapshotInfo, nextGlobalSnapshotInfo,
          nextActiveSnapshotInfo);
    } finally {
      lockSet.forEach(lockKey -> omMetadataManager.getLock()
          .releaseWriteLock(SNAPSHOT_LOCK, lockKey.getLeft(), lockKey.getMiddle(), lockKey.getRight()));
    }
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

  private SnapshotInfo updateSnapshotInfoAndCache(SnapshotInfo snapInfo, OmMetadataManagerImpl omMetadataManager,
                                                  long trxnLogIndex) throws IOException {
    if (snapInfo == null) {
      return null;
    }

    // Fetch the latest value again after acquiring lock.
    SnapshotInfo updatedSnapshotInfo = omMetadataManager.getSnapshotInfoTable().get(snapInfo.getTableKey());

    // Setting next snapshot deep clean to false, Since the
    // current snapshot is deleted. We can potentially
    // reclaim more keys in the next snapshot.
    updatedSnapshotInfo.setDeepClean(false);

    // Update table cache first
    omMetadataManager.getSnapshotInfoTable()
        .addCacheEntry(new CacheKey<>(updatedSnapshotInfo.getTableKey()),
            CacheValue.get(trxnLogIndex, updatedSnapshotInfo));
    return updatedSnapshotInfo;
  }

  /**
   * Removes the snapshot from the chain and updates the next snapshot's
   * previousPath and previousGlobal IDs in DB cache.
   * It also returns the pair of updated next path and global snapshots to
   * update in DB.
   */
  private Pair<SnapshotInfo, SnapshotInfo> updateSnapshotChainAndCache(
      Set<Triple<String, String, String>> lockSet,
      OmMetadataManagerImpl metadataManager,
      SnapshotInfo snapInfo,
      long trxnLogIndex
  ) throws IOException {
    Table<String, SnapshotInfo> snapshotInfoTable = metadataManager.getSnapshotInfoTable();
    SnapshotChainManager snapshotChainManager = metadataManager.getSnapshotChainManager();

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
      return null;
    }

    String nextPathSnapshotKey = null;

    if (hasNextPathSnapshot) {
      UUID nextPathSnapshotId = snapshotChainManager.nextPathSnapshot(
          snapInfo.getSnapshotPath(), snapInfo.getSnapshotId());
      nextPathSnapshotKey = snapshotChainManager.getTableKey(nextPathSnapshotId);

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
        nextPathSnapshotKey != null ? snapshotInfoTable.get(nextPathSnapshotKey) : null;

    SnapshotInfo nextGlobalSnapInfo =
        nextGlobalSnapshotKey != null ? snapshotInfoTable.get(nextGlobalSnapshotKey) : null;

    // Updates next path snapshot's previous snapshot ID
    if (nextPathSnapInfo != null) {
      nextPathSnapInfo.setPathPreviousSnapshotId(snapInfo.getPathPreviousSnapshotId());
      snapshotInfoTable.addCacheEntry(new CacheKey<>(nextPathSnapInfo.getTableKey()),
          CacheValue.get(trxnLogIndex, nextPathSnapInfo));
    }

    // Updates next global snapshot's previous snapshot ID
    // If both next global and path snapshot are same, it may overwrite
    // nextPathSnapInfo.setPathPreviousSnapshotID(), adding this check
    // will prevent it.
    if (nextGlobalSnapInfo != null && nextPathSnapInfo != null &&
        Objects.equals(nextGlobalSnapInfo.getSnapshotId(), nextPathSnapInfo.getSnapshotId())) {
      nextPathSnapInfo.setGlobalPreviousSnapshotId(snapInfo.getGlobalPreviousSnapshotId());
      snapshotInfoTable.addCacheEntry(new CacheKey<>(nextPathSnapInfo.getTableKey()),
          CacheValue.get(trxnLogIndex, nextPathSnapInfo));
      nextGlobalSnapInfo = nextPathSnapInfo;
    } else if (nextGlobalSnapInfo != null) {
      nextGlobalSnapInfo.setGlobalPreviousSnapshotId(snapInfo.getGlobalPreviousSnapshotId());
      snapshotInfoTable.addCacheEntry(new CacheKey<>(nextGlobalSnapInfo.getTableKey()),
          CacheValue.get(trxnLogIndex, nextGlobalSnapInfo));
    }

    snapshotChainManager.deleteSnapshot(snapInfo);
    return Pair.of(nextPathSnapInfo, nextGlobalSnapInfo);
  }
}
