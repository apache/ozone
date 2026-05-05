/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.snapshot;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.OMSystemAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotInternalMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
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

/**
 * Handles OMSnapshotPurge Request.
 * This is an OM internal request. Does not need @RequireSnapshotFeatureState.
 */
public class OMSnapshotPurgeRequest extends OMClientRequest {

  private static final Logger LOG = LoggerFactory.getLogger(OMSnapshotPurgeRequest.class);

  private static final AuditLogger AUDIT = new AuditLogger(AuditLoggerType.OMSYSTEMLOGGER);
  private static final String AUDIT_PARAM_SNAPSHOT_DB_KEYS = "snapshotsDBKeys";
  private static final String AUDIT_PARAM_SNAPSHOTS_SET_FOR_DEEP_CLEAN = "snapshotsSetForDeepClean";

  /**
   * This map contains up to date snapshotInfo and works as a local cache for OMSnapshotPurgeRequest.
   * Since purge and other updates happen in sequence inside validateAndUpdateCache, we can get updated snapshotInfo
   * from this map rather than getting form snapshotInfoTable which creates a deep copy for every get call.
   */
  private final Map<String, SnapshotInfo> updatedSnapshotInfos = new HashMap<>();

  public OMSnapshotPurgeRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    OmSnapshotInternalMetrics omSnapshotIntMetrics = ozoneManager.getOmSnapshotIntMetrics();

    final long trxnLogIndex = context.getIndex();

    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl)
        ozoneManager.getMetadataManager();
    SnapshotChainManager snapshotChainManager =
        omMetadataManager.getSnapshotChainManager();

    OMClientResponse omClientResponse = null;

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    SnapshotPurgeRequest snapshotPurgeRequest = getOmRequest()
        .getSnapshotPurgeRequest();

    List<String> snapshotDbKeys = snapshotPurgeRequest
        .getSnapshotDBKeysList();
    TransactionInfo transactionInfo = TransactionInfo.valueOf(context.getTermIndex());
    try {

      // Each snapshot purge operation does three things:
      //  1. Update the deep clean flag for the next active snapshot (So that it can be
      //     deep cleaned by the KeyDeletingService in the next run),
      //  2. Update the snapshot chain,
      //  3. Finally, purge the snapshot.
      // There is no need to take lock for snapshot purge as of now. We can simply rely on OMStateMachine
      // because it executes transaction sequentially.
      for (String snapTableKey : snapshotDbKeys) {
        SnapshotInfo fromSnapshot = getUpdatedSnapshotInfo(snapTableKey, omMetadataManager);
        if (fromSnapshot == null) {
          // Snapshot may have been purged in the previous iteration of SnapshotDeletingService.
          LOG.warn("The snapshot {} is not longer in snapshot table, It maybe removed in the previous " +
              "Snapshot purge request.", snapTableKey);
          continue;
        }
        SnapshotInfo nextSnapshot = SnapshotUtils.getNextSnapshot(ozoneManager, snapshotChainManager, fromSnapshot);
        SnapshotInfo nextToNextSnapshot = nextSnapshot == null ? null : SnapshotUtils.getNextSnapshot(ozoneManager,
            snapshotChainManager, nextSnapshot);
        // Step 1: Update the deep clean flag for the next snapshot
        updateSnapshotInfoAndCache(nextSnapshot, omMetadataManager, trxnLogIndex);
        updateSnapshotInfoAndCache(nextToNextSnapshot, omMetadataManager, trxnLogIndex);
        // Step 2: Update the snapshot chain.
        updateSnapshotChainAndCache(omMetadataManager, fromSnapshot, trxnLogIndex);
        // Step 3: Purge the snapshot from SnapshotInfoTable cache and also remove from the map.
        omMetadataManager.getSnapshotInfoTable()
            .addCacheEntry(new CacheKey<>(fromSnapshot.getTableKey()), CacheValue.get(trxnLogIndex));
        updatedSnapshotInfos.remove(fromSnapshot.getTableKey());
      }
      // Update the snapshotInfo lastTransactionInfo.
      for (SnapshotInfo snapshotInfo : updatedSnapshotInfos.values()) {
        snapshotInfo.setLastTransactionInfo(transactionInfo.toByteString());
        omMetadataManager.getSnapshotInfoTable().addCacheEntry(new CacheKey<>(snapshotInfo.getTableKey()),
            CacheValue.get(context.getIndex(), snapshotInfo));
      }

      omClientResponse = new OMSnapshotPurgeResponse(omResponse.build(), snapshotDbKeys, updatedSnapshotInfos,
          transactionInfo);

      omSnapshotIntMetrics.incNumSnapshotPurges();
      LOG.info("Successfully executed snapshotPurgeRequest: {{}} along with updating snapshots:{}.",
          snapshotPurgeRequest, updatedSnapshotInfos);
      if (LOG.isDebugEnabled()) {
        Map<String, String> auditParams = new LinkedHashMap<>();
        auditParams.put(AUDIT_PARAM_SNAPSHOT_DB_KEYS, snapshotDbKeys.toString());
        auditParams.put(AUDIT_PARAM_SNAPSHOTS_SET_FOR_DEEP_CLEAN, String.join(",", updatedSnapshotInfos.keySet()));
        AUDIT.logWriteSuccess(ozoneManager.buildAuditMessageForSuccess(OMSystemAction.SNAPSHOT_PURGE, auditParams));
      }
    } catch (IOException ex) {
      omClientResponse = new OMSnapshotPurgeResponse(
          createErrorOMResponse(omResponse, ex));
      omSnapshotIntMetrics.incNumSnapshotPurgeFails();
      LOG.error("Failed to execute snapshotPurgeRequest:{{}}.", snapshotPurgeRequest, ex);
      if (LOG.isDebugEnabled()) {
        Map<String, String> auditParams = new LinkedHashMap<>();
        auditParams.put(AUDIT_PARAM_SNAPSHOT_DB_KEYS, snapshotDbKeys.toString());
        AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.SNAPSHOT_PURGE, auditParams, ex));
      }

    }

    return omClientResponse;
  }

  private void updateSnapshotInfoAndCache(SnapshotInfo snapInfo, OmMetadataManagerImpl omMetadataManager,
                                          long trxnLogIndex) throws IOException {
    if (snapInfo != null) {
      // Setting next snapshot deep clean to false, Since the
      // current snapshot is deleted. We can potentially
      // reclaim more keys in the next snapshot.
      snapInfo.setDeepClean(false);
      snapInfo.setDeepCleanedDeletedDir(false);

      // Update table cache first
      omMetadataManager.getSnapshotInfoTable().addCacheEntry(new CacheKey<>(snapInfo.getTableKey()),
          CacheValue.get(trxnLogIndex, snapInfo));
      updatedSnapshotInfos.put(snapInfo.getTableKey(), snapInfo);
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

    if (nextPathSnapInfo != null) {
      nextPathSnapInfo.setPathPreviousSnapshotId(snapInfo.getPathPreviousSnapshotId());
      metadataManager.getSnapshotInfoTable().addCacheEntry(
          new CacheKey<>(nextPathSnapInfo.getTableKey()),
          CacheValue.get(trxnLogIndex, nextPathSnapInfo));
    }

    SnapshotInfo nextGlobalSnapInfo =
        nextGlobalSnapshotKey != null ? getUpdatedSnapshotInfo(nextGlobalSnapshotKey, metadataManager) : null;

    if (nextGlobalSnapInfo != null) {
      nextGlobalSnapInfo.setGlobalPreviousSnapshotId(snapInfo.getGlobalPreviousSnapshotId());
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
      if (snapshotInfo != null) {
        updatedSnapshotInfos.put(snapshotTableKey, snapshotInfo);
      }
    }
    return snapshotInfo;
  }
}
