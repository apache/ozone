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

import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotDeleteResponse;
import org.apache.hadoop.ozone.om.snapshot.RequireSnapshotFeatureState;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteSnapshotRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteSnapshotResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.SNAPSHOT_LOCK;

/**
 * Handles DeleteSnapshot Request.
 */
public class OMSnapshotDeleteRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMSnapshotDeleteRequest.class);

  public OMSnapshotDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  @RequireSnapshotFeatureState(true)
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    final OMRequest omRequest = super.preExecute(ozoneManager);

    final DeleteSnapshotRequest deleteSnapshotRequest =
        omRequest.getDeleteSnapshotRequest();

    final String snapshotName = deleteSnapshotRequest.getSnapshotName();
    // Verify snapshot name. TODO: Can remove
    OmUtils.validateSnapshotName(snapshotName);

    String volumeName = deleteSnapshotRequest.getVolumeName();
    String bucketName = deleteSnapshotRequest.getBucketName();

    // Permission check
    UserGroupInformation ugi = createUGI();
    String bucketOwner = ozoneManager.getBucketOwner(volumeName, bucketName,
        IAccessAuthorizer.ACLType.READ, OzoneObj.ResourceType.BUCKET);
    if (!ozoneManager.isAdmin(ugi) &&
        !ozoneManager.isOwner(ugi, bucketOwner)) {
      throw new OMException(
          "Only bucket owners and Ozone admins can delete snapshots",
          OMException.ResultCodes.PERMISSION_DENIED);
    }

    // Set deletion time here so OM leader and follower would have the
    // exact same timestamp.
    OMRequest.Builder omRequestBuilder = omRequest.toBuilder()
        .setDeleteSnapshotRequest(
            DeleteSnapshotRequest.newBuilder()
                .setVolumeName(volumeName)
                .setBucketName(bucketName)
                .setSnapshotName(snapshotName)
                .setDeletionTime(Time.now()));

    return omRequestBuilder.build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumSnapshotDeletes();

    boolean acquiredBucketLock = false;
    boolean acquiredSnapshotLock = false;
    IOException exception = null;
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMClientResponse omClientResponse = null;
    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    UserInfo userInfo = getOmRequest().getUserInfo();

    final DeleteSnapshotRequest request =
        getOmRequest().getDeleteSnapshotRequest();

    final String volumeName = request.getVolumeName();
    final String bucketName = request.getBucketName();
    final String snapshotName = request.getSnapshotName();
    final long deletionTime = request.getDeletionTime();

    SnapshotInfo snapshotInfo = null;

    try {
      // Acquire bucket lock
      acquiredBucketLock =
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
              volumeName, bucketName);

      acquiredSnapshotLock =
          omMetadataManager.getLock().acquireWriteLock(SNAPSHOT_LOCK,
              volumeName, bucketName, snapshotName);

      // Retrieve SnapshotInfo from the table
      String tableKey = SnapshotInfo.getTableKey(volumeName, bucketName,
          snapshotName);
      snapshotInfo =
          omMetadataManager.getSnapshotInfoTable().get(tableKey);

      if (snapshotInfo == null) {
        // Snapshot does not exist
        throw new OMException("Snapshot does not exist", FILE_NOT_FOUND);
      }

      if (!snapshotInfo.getSnapshotStatus().equals(
          SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE)) {
        // If the snapshot is not in active state, throw exception as well
        switch (snapshotInfo.getSnapshotStatus()) {
        case SNAPSHOT_DELETED:
          throw new OMException("Snapshot is already deleted. "
              + "Pending reclamation.", FILE_NOT_FOUND);
        case SNAPSHOT_RECLAIMED:
          throw new OMException("Snapshot is already deleted and reclaimed.",
              FILE_NOT_FOUND);
        default:
          // Unknown snapshot non-active state
          throw new OMException("Snapshot exists but no longer in active state",
              FILE_NOT_FOUND);
        }
      }

      // Mark snapshot as deleted
      snapshotInfo.setSnapshotStatus(
          SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED);
      snapshotInfo.setDeletionTime(deletionTime);

      // Update table cache first
      omMetadataManager.getSnapshotInfoTable().addCacheEntry(
          new CacheKey<>(tableKey),
          CacheValue.get(transactionLogIndex, snapshotInfo));

      omResponse.setDeleteSnapshotResponse(
          DeleteSnapshotResponse.newBuilder());
      omClientResponse = new OMSnapshotDeleteResponse(
          omResponse.build(), tableKey, snapshotInfo);

      // No longer need to invalidate the entry in the snapshot cache here.

    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMSnapshotDeleteResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredSnapshotLock) {
        omMetadataManager.getLock().releaseWriteLock(SNAPSHOT_LOCK, volumeName,
            bucketName, snapshotName);
      }
      if (acquiredBucketLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    if (snapshotInfo == null) {
      // Dummy SnapshotInfo for logging and audit logging when erred
      snapshotInfo = SnapshotInfo.newInstance(volumeName, bucketName,
          snapshotName, null);
    }

    // Perform audit logging outside the lock
    auditLog(auditLogger, buildAuditMessage(OMAction.DELETE_SNAPSHOT,
        snapshotInfo.toAuditMap(), exception, userInfo));

    final String snapshotPath = snapshotInfo.getSnapshotPath();
    if (exception == null) {
      LOG.info("Deleted snapshot '{}' under path '{}'",
          snapshotName, snapshotPath);
    } else {
      omMetrics.incNumSnapshotDeleteFails();
      LOG.error("Failed to delete snapshot '{}' under path '{}'",
          snapshotName, snapshotPath);
    }
    return omClientResponse;
  }

}
