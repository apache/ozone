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

import com.google.common.base.Objects;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotPurgeAndMoveResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotPurgeResponse;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotPurgeRequest;
import org.apache.ratis.server.protocol.TermIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;

/**
 * Handles OMSnapshotPurge Request.
 * This is an OM internal request. Does not need @RequireSnapshotFeatureState.
 */
public class OMSnapshotPurgeAndMoveRequest extends OMClientRequest {

  private static final Logger LOG = LoggerFactory.getLogger(OMSnapshotPurgeAndMoveRequest.class);

  /**
   * This map contains up to date snapshotInfo and works as a local cache for OMSnapshotPurgeRequest.
   * Since purge and other updates happen in sequence inside validateAndUpdateCache, we can get updated snapshotInfo
   * from this map rather than getting form snapshotInfoTable which creates a deep copy for every get call.
   */
  private final Map<String, SnapshotInfo> updatedSnapshotInfos = new HashMap<>();

  public OMSnapshotPurgeAndMoveRequest(OMRequest omRequest) {
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
    OzoneManagerProtocolProtos.SnapshotPurgeAndMoveRequest snapshotPurgeAndMoveRequest = getOmRequest()
        .getSnapshotPurgeAndMoveRequest();

    try {
      UUID snapshotId =  HddsUtils.fromProtobuf(snapshotPurgeAndMoveRequest.getSnapshotID());
      // When value is null it basically means the next expected target would be AOS.
      UUID nextExpectedSnapshotId = snapshotPurgeAndMoveRequest.hasExpectedNextSnapshotID() ?
          HddsUtils.fromProtobuf(snapshotPurgeAndMoveRequest.getSnapshotID()) : null;
      SnapshotInfo currentSnapshot = SnapshotUtils.getSnapshotInfo(snapshotChainManager, snapshotId, omSnapshotManager);
      SnapshotInfo nextSnapshot = SnapshotUtils.getNextSnapshot(currentSnapshot, snapshotChainManager,
          omSnapshotManager);
      SnapshotInfo prevSnapshot = SnapshotUtils.getPreviousSnapshot(currentSnapshot, snapshotChainManager,
          omSnapshotManager);
      // When value is null it basically means the next target is AOS.
      UUID nextSnapshotId = Optional.ofNullable(nextSnapshot).map(SnapshotInfo::getSnapshotId).orElse(null);
      if (!Objects.equal(nextSnapshotId, nextExpectedSnapshotId)) {
        throw new OMException("Next path snapshot for " + currentSnapshot + " expected snapshot Id: "
            + nextExpectedSnapshotId + " but was " + nextSnapshotId, OMException.ResultCodes.INVALID_REQUEST);
      }

      // When next snapshot is not active. The keys can be moved to the next active snapshot to avoid unnecessary hop.
      if (nextSnapshot != null && nextSnapshot.getSnapshotStatus() == SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE) {
        throw new OMException("Next path snapshot for " + currentSnapshot + " " + nextSnapshotId + " is not active.",
            OMException.ResultCodes.INVALID_REQUEST);
      }

      // Each snapshot purge operation does three things:
      //  1. Update the deep clean flag for the next snapshot if the previous snapshot is active otherwise don't
      //  bother to run deepClean unnecessarily
      //  (So that it can be deep cleaned by the KeyDeletingService in the next run),
      //  2. Update the snapshot chain,
      //  3. Finally, purge the snapshot.
      // There is no need to take lock for snapshot purge as of now. We can simply rely on OMStateMachine
      // because it executes transaction sequentially.
      // As part of the flush the keys in the deleted table, deletedDirectoryTable and renameTable would be moved to
      // the next snapshot.
      // Step 1: Update the deep clean flag for the next active snapshot
      if (prevSnapshot == null || prevSnapshot.getSnapshotStatus() == SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE) {
        updateSnapshotInfoAndCache(nextSnapshot, omMetadataManager, termIndex);
      }
      // Step 2: Update the snapshot chain.
      updateSnapshotChainAndCache(omMetadataManager, currentSnapshot, trxnLogIndex);
      // Remove and close snapshot's RocksDB instance from SnapshotCache.
      omSnapshotManager.invalidateCacheEntry(snapshotId);
      // Step 3: Purge the snapshot from SnapshotInfoTable cache.
      omMetadataManager.getSnapshotInfoTable()
          .addCacheEntry(new CacheKey<>(currentSnapshot.getTableKey()), CacheValue.get(trxnLogIndex));

    omClientResponse = new OMSnapshotPurgeAndMoveResponse(omResponse.build(), currentSnapshot, nextSnapshot,
        updatedSnapshotInfos);
  } catch (IOException ex) {
      omClientResponse = new OMSnapshotPurgeResponse(
          createErrorOMResponse(omResponse, ex));
      omMetrics.incNumSnapshotPurgeFails();
      LOG.error("Failed to execute snapshotPurgeAndMoveRequest:{{}}.", snapshotPurgeAndMoveRequest, ex);
    }

    return omClientResponse;
  }

  private void updateSnapshotInfoAndCache(SnapshotInfo snapInfo, OmMetadataManagerImpl omMetadataManager,
                                          TermIndex termIndex) throws IOException {
    if (snapInfo != null) {
      // Setting next snapshot deep clean to false, Since the
      // current snapshot is deleted. We can potentially
      // reclaim more keys in the next snapshot.
      snapInfo.setDeepClean(false);
      snapInfo.setDeepCleanedDeletedDir(false);
      SnapshotUtils.setTransactionInfoInSnapshot(snapInfo, termIndex);

      // Update table cache first
      omMetadataManager.getSnapshotInfoTable().addCacheEntry(new CacheKey<>(snapInfo.getTableKey()),
          CacheValue.get(termIndex.getIndex(), snapInfo));
    }
  }

  /**
   * Removes the snapshot from the chain and updates the next snapshot's
   * previousPath and previousGlobal IDs in DB cache.
   * It also returns the pair of updated next path and global snapshots to
   * update in DB.
   */
  private void updateSnapshotChainAndCache(
      OmMetadataManagerImpl metadataManager,
      SnapshotInfo snapInfo,
      long trxnLogIndex
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
    }

    String nextGlobalSnapshotKey = null;
    if (hasNextGlobalSnapshot) {
      UUID nextGlobalSnapshotId = snapshotChainManager.nextGlobalSnapshot(snapInfo.getSnapshotId());
      nextGlobalSnapshotKey = snapshotChainManager.getTableKey(nextGlobalSnapshotId);
    }

    SnapshotInfo nextPathSnapInfo =
        nextPathSnapshotKey != null ? getUpdatedSnapshotInfo(nextPathSnapshotKey, metadataManager) : null;

    SnapshotInfo nextGlobalSnapInfo =
        nextGlobalSnapshotKey != null ? getUpdatedSnapshotInfo(nextGlobalSnapshotKey, metadataManager) : null;

    // Updates next path snapshot's previous snapshot ID
    if (nextPathSnapInfo != null) {
      nextPathSnapInfo.setPathPreviousSnapshotId(snapInfo.getPathPreviousSnapshotId());
      metadataManager.getSnapshotInfoTable().addCacheEntry(
          new CacheKey<>(nextPathSnapInfo.getTableKey()),
          CacheValue.get(trxnLogIndex, nextPathSnapInfo));
    }

    if (nextGlobalSnapInfo != null) {
      nextGlobalSnapInfo.setGlobalPreviousSnapshotId(
          snapInfo.getGlobalPreviousSnapshotId());
      metadataManager.getSnapshotInfoTable().addCacheEntry(
          new CacheKey<>(nextGlobalSnapInfo.getTableKey()),
          CacheValue.get(trxnLogIndex, nextGlobalSnapInfo));
    }

    snapshotChainManager.deleteSnapshot(snapInfo);
  }

  private SnapshotInfo getUpdatedSnapshotInfo(String snapshotTableKey, OMMetadataManager omMetadataManager)
      throws IOException {
    SnapshotInfo snapshotInfo = updatedSnapshotInfos.get(snapshotTableKey);

    if (snapshotInfo == null) {
      snapshotInfo = omMetadataManager.getSnapshotInfoTable().get(snapshotTableKey);
      updatedSnapshotInfos.put(snapshotTableKey, snapshotInfo);
    }
    return snapshotInfo;
  }
}
