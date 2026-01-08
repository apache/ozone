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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.SNAPSHOT_LOCK;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.FILESYSTEM_SNAPSHOT;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotRenameResponse;
import org.apache.hadoop.ozone.om.snapshot.RequireSnapshotFeatureState;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameSnapshotRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;

/**
 * Changes snapshot name.
 */
public class OMSnapshotRenameRequest extends OMClientRequest {

  public OMSnapshotRenameRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  @DisallowedUntilLayoutVersion(FILESYSTEM_SNAPSHOT)
  @RequireSnapshotFeatureState(true)
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OMRequest omRequest = super.preExecute(ozoneManager);

    final RenameSnapshotRequest renameSnapshotRequest =
        omRequest.getRenameSnapshotRequest();

    final String snapshotNewName = renameSnapshotRequest.getSnapshotNewName();

    OmUtils.validateSnapshotName(snapshotNewName);

    String volumeName = renameSnapshotRequest.getVolumeName();
    String bucketName = renameSnapshotRequest.getBucketName();
    // Updating the volumeName & bucketName in case the bucket is a linked bucket. We need to do this before a
    // permission check, since linked bucket permissions and source bucket permissions could be different.
    ResolvedBucket resolvedBucket = ozoneManager.resolveBucketLink(Pair.of(volumeName, bucketName), this);
    volumeName = resolvedBucket.realVolume();
    bucketName = resolvedBucket.realBucket();

    // Permission check
    if (ozoneManager.getAclsEnabled()) {
      UserGroupInformation ugi = createUGIForApi();
      String bucketOwner = ozoneManager.getBucketOwner(volumeName, bucketName,
          IAccessAuthorizer.ACLType.READ, OzoneObj.ResourceType.BUCKET);
      if (!ozoneManager.isAdmin(ugi) &&
          !ozoneManager.isOwner(ugi, bucketOwner)) {
        throw new OMException(
            "Only bucket owners and Ozone admins can rename snapshots",
            OMException.ResultCodes.PERMISSION_DENIED);
      }
    }

    // Set rename time here so OM leader and follower would have the
    // exact same timestamp.
    OMRequest.Builder omRequestBuilder = omRequest.toBuilder()
        .setRenameSnapshotRequest(
            RenameSnapshotRequest.newBuilder()
                .setVolumeName(volumeName)
                .setBucketName(bucketName)
                .setSnapshotNewName(snapshotNewName)
                .setSnapshotOldName(renameSnapshotRequest.getSnapshotOldName())
                .setRenameTime(Time.now()));

    return omRequestBuilder.build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumSnapshotRenames();

    boolean acquiredBucketLock = false;
    boolean acquiredSnapshotOldLock = false;
    boolean acquiredSnapshotNewLock = false;
    Exception exception = null;
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl)
        ozoneManager.getMetadataManager();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMClientResponse omClientResponse = null;
    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    UserInfo userInfo = getOmRequest().getUserInfo();

    final RenameSnapshotRequest request =
        getOmRequest().getRenameSnapshotRequest();

    final String volumeName = request.getVolumeName();
    final String bucketName = request.getBucketName();
    final String snapshotNewName = request.getSnapshotNewName();
    final String snapshotOldName = request.getSnapshotOldName();

    SnapshotInfo snapshotOldInfo = null;

    try {
      // Acquire bucket lock
      mergeOmLockDetails(
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
                                                       volumeName, bucketName));
      acquiredBucketLock = getOmLockDetails().isLockAcquired();

      mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLock(SNAPSHOT_LOCK,
                                                       volumeName, bucketName, snapshotOldName));
      acquiredSnapshotOldLock = getOmLockDetails().isLockAcquired();

      mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLock(SNAPSHOT_LOCK,
                                                       volumeName, bucketName, snapshotNewName));
      acquiredSnapshotNewLock = getOmLockDetails().isLockAcquired();

      // Retrieve SnapshotInfo from the table
      String snapshotNewTableKey = SnapshotInfo.getTableKey(volumeName, bucketName, snapshotNewName);

      if (omMetadataManager.getSnapshotInfoTable().isExist(snapshotNewTableKey)) {
        throw new OMException("Snapshot with name " + snapshotNewName + " already exist",
            FILE_ALREADY_EXISTS);
      }

      // Retrieve SnapshotInfo from the table
      String snapshotOldTableKey = SnapshotInfo.getTableKey(volumeName, bucketName,
                                                 snapshotOldName);
      snapshotOldInfo =
          omMetadataManager.getSnapshotInfoTable().get(snapshotOldTableKey);

      if (snapshotOldInfo == null) {
        // Snapshot does not exist
        throw new OMException("Snapshot with name " + snapshotOldName + "does not exist",
            FILE_NOT_FOUND);
      }

      switch (snapshotOldInfo.getSnapshotStatus()) {
      case SNAPSHOT_DELETED:
        throw new OMException("Snapshot is already deleted. "
            + "Pending reclamation.", FILE_NOT_FOUND);
      case SNAPSHOT_ACTIVE:
        break;
      default:
          // Unknown snapshot non-active state
        throw new OMException("Snapshot exists but no longer in active state",
            FILE_NOT_FOUND);
      }

      snapshotOldInfo.setName(snapshotNewName);

      omMetadataManager.getSnapshotInfoTable().addCacheEntry(
          new CacheKey<>(snapshotOldTableKey),
          CacheValue.get(context.getIndex()));

      omMetadataManager.getSnapshotInfoTable().addCacheEntry(
          new CacheKey<>(snapshotNewTableKey),
          CacheValue.get(context.getIndex(), snapshotOldInfo));

      omMetadataManager.getSnapshotChainManager().updateSnapshot(snapshotOldInfo);

      omResponse.setRenameSnapshotResponse(
          OzoneManagerProtocolProtos.RenameSnapshotResponse.newBuilder()
              .setSnapshotInfo(snapshotOldInfo.getProtobuf()));
      omClientResponse = new OMSnapshotRenameResponse(
          omResponse.build(), snapshotOldTableKey, snapshotNewTableKey, snapshotOldInfo);

    } catch (IOException | InvalidPathException ex) {
      omMetrics.incNumSnapshotRenameFails();
      exception = ex;
      omClientResponse = new OMSnapshotRenameResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (acquiredSnapshotNewLock) {
        mergeOmLockDetails(omMetadataManager.getLock().releaseWriteLock(SNAPSHOT_LOCK, volumeName,
                                                     bucketName, snapshotNewName));
      }
      if (acquiredSnapshotOldLock) {
        mergeOmLockDetails(omMetadataManager.getLock().releaseWriteLock(SNAPSHOT_LOCK, volumeName,
                                                     bucketName, snapshotOldName));
      }
      if (acquiredBucketLock) {
        mergeOmLockDetails(omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
                                                     bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    if (snapshotOldInfo == null) {
      // Dummy SnapshotInfo for logging and audit logging when erred
      snapshotOldInfo = SnapshotInfo.newInstance(volumeName, bucketName,
                                              snapshotOldName, null, Time.now());
    }

    // Perform audit logging outside the lock
    markForAudit(auditLogger, buildAuditMessage(OMAction.RENAME_SNAPSHOT,
                                            snapshotOldInfo.toAuditMap(), exception, userInfo));
    return omClientResponse;
  }
}
