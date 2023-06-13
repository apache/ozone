/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.snapshot;

import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotCreateResponse;
import org.apache.hadoop.ozone.om.snapshot.RequireSnapshotFeatureState;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateSnapshotRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateSnapshotResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

import static org.apache.hadoop.hdds.HddsUtils.fromProtobuf;
import static org.apache.hadoop.hdds.HddsUtils.toProtobuf;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.SNAPSHOT_LOCK;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.FILESYSTEM_SNAPSHOT;

/**
 * Handles CreateSnapshot Request.
 */
public class OMSnapshotCreateRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMSnapshotCreateRequest.class);

  private final String snapshotPath;
  private final String volumeName;
  private final String bucketName;
  private final String snapshotName;
  private final SnapshotInfo snapshotInfo;

  public OMSnapshotCreateRequest(OMRequest omRequest) {
    super(omRequest);
    CreateSnapshotRequest createSnapshotRequest = omRequest
        .getCreateSnapshotRequest();
    volumeName = createSnapshotRequest.getVolumeName();
    bucketName = createSnapshotRequest.getBucketName();
    UUID snapshotId = createSnapshotRequest.hasSnapshotId() ?
        fromProtobuf(createSnapshotRequest.getSnapshotId()) : null;

    String possibleName = createSnapshotRequest.getSnapshotName();
    snapshotInfo = SnapshotInfo.newInstance(volumeName,
        bucketName,
        possibleName,
        snapshotId,
        createSnapshotRequest.getCreationTime());
    snapshotName = snapshotInfo.getName();
    snapshotPath = snapshotInfo.getSnapshotPath();
  }

  @Override
  @DisallowedUntilLayoutVersion(FILESYSTEM_SNAPSHOT)
  @RequireSnapshotFeatureState(true)
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OMRequest omRequest = super.preExecute(ozoneManager);
    // Verify name
    OmUtils.validateSnapshotName(snapshotName);

    UserGroupInformation ugi = createUGI();
    String bucketOwner = ozoneManager.getBucketOwner(volumeName, bucketName,
        IAccessAuthorizer.ACLType.READ, OzoneObj.ResourceType.BUCKET);
    if (!ozoneManager.isAdmin(ugi) &&
        !ozoneManager.isOwner(ugi, bucketOwner)) {
      throw new OMException(
          "Only bucket owners and Ozone admins can create snapshots",
          OMException.ResultCodes.PERMISSION_DENIED);
    }

    return omRequest.toBuilder().setCreateSnapshotRequest(
        omRequest.getCreateSnapshotRequest().toBuilder()
            .setSnapshotId(toProtobuf(UUID.randomUUID()))
            .setCreationTime(Time.now())
            .build()).build();
  }
  
  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumSnapshotCreates();

    boolean acquiredBucketLock = false, acquiredSnapshotLock = false;
    IOException exception = null;
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl)
        ozoneManager.getMetadataManager();
    SnapshotChainManager snapshotChainManager =
        omMetadataManager.getSnapshotChainManager();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMClientResponse omClientResponse = null;
    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    String key = snapshotInfo.getTableKey();
    try {
      // Lock bucket so it doesn't
      //  get deleted while creating snapshot
      acquiredBucketLock =
          omMetadataManager.getLock().acquireReadLock(BUCKET_LOCK,
              volumeName, bucketName);

      acquiredSnapshotLock =
          omMetadataManager.getLock().acquireWriteLock(SNAPSHOT_LOCK,
              volumeName, bucketName, snapshotName);

      // Check if snapshot already exists
      if (omMetadataManager.getSnapshotInfoTable().isExist(key)) {
        LOG.debug("Snapshot '{}' already exists under '{}'", key, snapshotPath);
        throw new OMException("Snapshot already exists", FILE_ALREADY_EXISTS);
      }

      // Note down RDB latest transaction sequence number, which is used
      // as snapshot generation in the differ.
      final long dbLatestSequenceNumber =
          ((RDBStore) omMetadataManager.getStore()).getDb()
              .getLatestSequenceNumber();
      snapshotInfo.setDbTxSequenceNumber(dbLatestSequenceNumber);

      // Set previous path and global snapshot
      UUID latestPathSnapshot =
          snapshotChainManager.getLatestPathSnapshotId(snapshotPath);
      UUID latestGlobalSnapshot =
          snapshotChainManager.getLatestGlobalSnapshotId();

      snapshotInfo.setPathPreviousSnapshotId(latestPathSnapshot);
      snapshotInfo.setGlobalPreviousSnapshotId(latestGlobalSnapshot);

      snapshotChainManager.addSnapshot(snapshotInfo);

      omMetadataManager.getSnapshotInfoTable()
          .addCacheEntry(new CacheKey<>(key),
            CacheValue.get(transactionLogIndex, snapshotInfo));

      omResponse.setCreateSnapshotResponse(
          CreateSnapshotResponse.newBuilder()
          .setSnapshotInfo(snapshotInfo.getProtobuf()));
      omClientResponse = new OMSnapshotCreateResponse(
          omResponse.build(), snapshotInfo);
    } catch (IOException ex) {
      // Remove snapshot from the SnapshotChainManager in case of any failure.
      // It is possible that createSnapshot request fails after snapshot gets
      // added to snapshot chain manager because couldn't add it to cache/DB.
      // In that scenario, SnapshotChainManager#globalSnapshotId will point to
      // failed createSnapshot request's snapshotId but in actual it doesn't
      // exist in the SnapshotInfo table.
      // If it doesn't get removed, OM restart will crash on
      // SnapshotChainManager#loadFromSnapshotInfoTable because it could not
      // find the previous snapshot which doesn't exist because it was never
      // added to the SnapshotInfo table.
      if (Objects.equals(snapshotInfo.getSnapshotId(),
          snapshotChainManager.getLatestGlobalSnapshotId())) {
        removeSnapshotInfoFromSnapshotChainManager(snapshotChainManager,
            snapshotInfo);
      }
      exception = ex;
      omClientResponse = new OMSnapshotCreateResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredSnapshotLock) {
        omMetadataManager.getLock().releaseWriteLock(SNAPSHOT_LOCK, volumeName,
            bucketName, snapshotName);
      }
      if (acquiredBucketLock) {
        omMetadataManager.getLock().releaseReadLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    // Performing audit logging outside the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.CREATE_SNAPSHOT,
        snapshotInfo.toAuditMap(), exception, userInfo));
    
    if (exception == null) {
      LOG.info("Created snapshot '{}' under path '{}'",
          snapshotName, snapshotPath);
      omMetrics.incNumSnapshotActive();
    } else {
      omMetrics.incNumSnapshotCreateFails();
      LOG.error("Failed to create snapshot '{}' under path '{}'",
          snapshotName, snapshotPath);
    }
    return omClientResponse;
  }

  /**
   * Removes the snapshot from the SnapshotChainManager.
   * In case of any failure, it logs the exception as an error and swallow it.
   * Ideally, there should not be any failure in deletion.
   * If it happens, and we throw the exception, we lose the track why snapshot
   * creation failed itself.
   * Hence, to not lose that information it is better just log and swallow the
   * exception.
   */
  private void removeSnapshotInfoFromSnapshotChainManager(
      SnapshotChainManager snapshotChainManager,
      SnapshotInfo info
  ) {
    try {
      snapshotChainManager.deleteSnapshot(info);
    } catch (IOException exception) {
      LOG.error("Failed to remove snapshot: {} from SnapshotChainManager.",
          info, exception);
    }
  }
}
