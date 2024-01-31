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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * Handles OMSnapshotPurge Request.
 * This is an OM internal request. Does not need @RequireSnapshotFeatureState.
 */
public class OMSnapshotPurgeRequest extends OMClientRequest {

  public OMSnapshotPurgeRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, TermIndex termIndex) {
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

      // Snapshots that are purged by the SnapshotDeletingService
      // will update the next snapshot so that is can be deep cleaned
      // by the KeyDeletingService in the next run.
      for (String snapTableKey : snapshotDbKeys) {
        SnapshotInfo fromSnapshot = omMetadataManager.getSnapshotInfoTable()
            .get(snapTableKey);

        SnapshotInfo nextSnapshot = SnapshotUtils
            .getNextActiveSnapshot(fromSnapshot,
                snapshotChainManager, omSnapshotManager);

        updateSnapshotInfoAndCache(nextSnapshot, omMetadataManager,
            trxnLogIndex, updatedSnapInfos);
        updateSnapshotChainAndCache(omMetadataManager, fromSnapshot,
            trxnLogIndex, updatedPathPreviousAndGlobalSnapshots);
        ozoneManager.getOmSnapshotManager().getSnapshotCache()
            .invalidate(snapTableKey);
      }

      omClientResponse = new OMSnapshotPurgeResponse(omResponse.build(),
          snapshotDbKeys, updatedSnapInfos,
          updatedPathPreviousAndGlobalSnapshots);
    } catch (IOException ex) {
      omClientResponse = new OMSnapshotPurgeResponse(
          createErrorOMResponse(omResponse, ex));
    }

    return omClientResponse;
  }

  private void updateSnapshotInfoAndCache(SnapshotInfo snapInfo,
      OmMetadataManagerImpl omMetadataManager, long trxnLogIndex,
      Map<String, SnapshotInfo> updatedSnapInfos) {
    if (snapInfo != null) {
      // Setting next snapshot deep clean to false, Since the
      // current snapshot is deleted. We can potentially
      // reclaim more keys in the next snapshot.
      snapInfo.setDeepClean(false);

      // Update table cache first
      omMetadataManager.getSnapshotInfoTable().addCacheEntry(
          new CacheKey<>(snapInfo.getTableKey()),
          CacheValue.get(trxnLogIndex, snapInfo));
      updatedSnapInfos.put(snapInfo.getTableKey(), snapInfo);
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
      long trxnLogIndex,
      Map<String, SnapshotInfo> updatedPathPreviousAndGlobalSnapshots
  ) throws IOException {
    if (snapInfo == null) {
      return;
    }

    SnapshotChainManager snapshotChainManager = metadataManager
        .getSnapshotChainManager();
    SnapshotInfo nextPathSnapInfo = null;

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

    // Updates next path snapshot's previous snapshot ID
    if (hasNextPathSnapshot) {
      UUID nextPathSnapshotId = snapshotChainManager.nextPathSnapshot(
          snapInfo.getSnapshotPath(), snapInfo.getSnapshotId());

      String snapshotTableKey = snapshotChainManager
          .getTableKey(nextPathSnapshotId);
      nextPathSnapInfo = metadataManager.getSnapshotInfoTable()
          .get(snapshotTableKey);
      if (nextPathSnapInfo != null) {
        nextPathSnapInfo.setPathPreviousSnapshotId(
            snapInfo.getPathPreviousSnapshotId());
        metadataManager.getSnapshotInfoTable().addCacheEntry(
            new CacheKey<>(nextPathSnapInfo.getTableKey()),
            CacheValue.get(trxnLogIndex, nextPathSnapInfo));
        updatedPathPreviousAndGlobalSnapshots
            .put(nextPathSnapInfo.getTableKey(), nextPathSnapInfo);
      }
    }

    // Updates next global snapshot's previous snapshot ID
    if (hasNextGlobalSnapshot) {
      UUID nextGlobalSnapshotId =
          snapshotChainManager.nextGlobalSnapshot(snapInfo.getSnapshotId());

      String snapshotTableKey = snapshotChainManager
          .getTableKey(nextGlobalSnapshotId);

      SnapshotInfo nextGlobalSnapInfo = metadataManager.getSnapshotInfoTable()
          .get(snapshotTableKey);
      // If both next global and path snapshot are same, it may overwrite
      // nextPathSnapInfo.setPathPreviousSnapshotID(), adding this check
      // will prevent it.
      if (nextGlobalSnapInfo != null && nextPathSnapInfo != null &&
          nextGlobalSnapInfo.getSnapshotId().equals(
              nextPathSnapInfo.getSnapshotId())) {
        nextPathSnapInfo.setGlobalPreviousSnapshotId(
            snapInfo.getGlobalPreviousSnapshotId());
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
    }

    snapshotChainManager.deleteSnapshot(snapInfo);
  }
}
