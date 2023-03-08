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

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.utils.db.RDBStore;
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
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateSnapshotRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateSnapshotResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.SNAPSHOT_LOCK;


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
  private final String snapshotId;
  private final SnapshotInfo snapshotInfo;

  public OMSnapshotCreateRequest(OMRequest omRequest) {
    super(omRequest);
    CreateSnapshotRequest createSnapshotRequest = omRequest
        .getCreateSnapshotRequest();
    volumeName = createSnapshotRequest.getVolumeName();
    bucketName = createSnapshotRequest.getBucketName();
    snapshotId = createSnapshotRequest.getSnapshotId();

    String possibleName = createSnapshotRequest.getSnapshotName();
    snapshotInfo = SnapshotInfo.newInstance(volumeName,
        bucketName,
        possibleName,
        snapshotId);
    snapshotName = snapshotInfo.getName();
    snapshotPath = snapshotInfo.getSnapshotPath();
  }

  @Override
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
    return omRequest;
  }
  
  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumSnapshotCreates();
    omMetrics.incNumSnapshotActive();

    boolean acquiredBucketLock = false, acquiredSnapshotLock = false;
    IOException exception = null;
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    
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

      //Check if snapshot already exists
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

      omMetadataManager.getSnapshotInfoTable()
          .addCacheEntry(new CacheKey<>(key),
            new CacheValue<>(Optional.of(snapshotInfo), transactionLogIndex));

      omResponse.setCreateSnapshotResponse(
          CreateSnapshotResponse.newBuilder()
          .setSnapshotInfo(snapshotInfo.getProtobuf()));
      omClientResponse = new OMSnapshotCreateResponse(
          omResponse.build(), volumeName, bucketName, snapshotName);
    } catch (IOException ex) {
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
    } else {
      omMetrics.incNumSnapshotCreateFails();
      LOG.error("Failed to create snapshot '{}' under path '{}'",
          snapshotName, snapshotPath);
    }
    return omClientResponse;
  }
  
}
