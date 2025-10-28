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

package org.apache.hadoop.ozone.om.request.bucket;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.CONTAINS_SNAPSHOT;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.VOLUME_LOCK;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles DeleteBucket Request.
 */
public class OMBucketDeleteRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMBucketDeleteRequest.class);

  public OMBucketDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    super.preExecute(ozoneManager);
    DeleteBucketRequest deleteBucketRequest =
        getOmRequest().getDeleteBucketRequest();
    String volumeName = deleteBucketRequest.getVolumeName();
    String bucketName = deleteBucketRequest.getBucketName();

    // ACL check during preExecute
    if (ozoneManager.getAclsEnabled()) {
      try {
        checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.DELETE,
            volumeName, bucketName, null);
      } catch (IOException ex) {
        // Ensure audit log captures preExecute failures
        Map<String, String> auditMap = new LinkedHashMap<>();
        auditMap.put(OzoneConsts.VOLUME, volumeName);
        auditMap.put(OzoneConsts.BUCKET, bucketName);
        markForAudit(ozoneManager.getAuditLogger(),
            buildAuditMessage(OMAction.DELETE_BUCKET, auditMap, ex,
                getOmRequest().getUserInfo()));
        throw ex;
      }
    }

    return getOmRequest().toBuilder()
        .setUserInfo(getUserIfNotExists(ozoneManager))
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumBucketDeletes();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    OMRequest omRequest = getOmRequest();
    DeleteBucketRequest deleteBucketRequest =
        omRequest.getDeleteBucketRequest();
    String volumeName = deleteBucketRequest.getVolumeName();
    String bucketName = deleteBucketRequest.getBucketName();

    // Generate end user response
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    Map<String, String> auditMap = buildVolumeAuditMap(volumeName);
    auditMap.put(OzoneConsts.BUCKET, bucketName);

    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    Exception exception = null;

    boolean acquiredBucketLock = false, acquiredVolumeLock = false;
    boolean success = true;
    OMClientResponse omClientResponse = null;
    try {
      // acquire lock
      mergeOmLockDetails(
          omMetadataManager.getLock().acquireReadLock(VOLUME_LOCK, volumeName));
      acquiredVolumeLock = getOmLockDetails().isLockAcquired();
      mergeOmLockDetails(
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName,
              bucketName));
      acquiredBucketLock = getOmLockDetails().isLockAcquired();

      // No need to check volume exists here, as bucket cannot be created
      // with out volume creation. Check if bucket exists
      String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);

      OmBucketInfo omBucketInfo =
          omMetadataManager.getBucketTable().get(bucketKey);

      if (omBucketInfo == null) {
        LOG.debug("bucket: {} not found ", bucketName);
        throw new OMException("Bucket not exists", BUCKET_NOT_FOUND);
      }

      //Check if bucket is empty
      if (!omMetadataManager.isBucketEmpty(volumeName, bucketName)) {
        LOG.debug("bucket: {} is not empty ", bucketName);
        throw new OMException("Bucket is not empty",
            OMException.ResultCodes.BUCKET_NOT_EMPTY);
      }

      // Check if bucket does not contain incomplete MPUs
      if (omMetadataManager.containsIncompleteMPUs(volumeName, bucketName)) {
        LOG.debug("Volume '{}', Bucket '{}' can't be deleted when it has " +
                "incomplete multipart uploads", volumeName, bucketName);
        throw new OMException(
            String.format("Volume %s, Bucket %s can't be deleted when it " +
                "has incomplete multipart uploads", volumeName, bucketName),
            ResultCodes.BUCKET_NOT_EMPTY);
      }

      // appending '/' to end to eliminate cases where 2 buckets start with same
      // characters.
      String snapshotBucketKey = bucketKey + OzoneConsts.OM_KEY_PREFIX;

      if (bucketContainsSnapshot(omMetadataManager, snapshotBucketKey)) {
        LOG.debug("Bucket '{}' can't be deleted when it has snapshots",
            bucketName);
        throw new OMException(
            "Bucket " + bucketName + " can't be deleted when it has snapshots",
            CONTAINS_SNAPSHOT);
      }

      if (omBucketInfo.getBucketLayout().isFileSystemOptimized()) {
        omMetrics.incNumFSOBucketDeletes();
      }
      omMetrics.decNumBuckets();

      // Update table cache.
      omMetadataManager.getBucketTable().addCacheEntry(
          new CacheKey<>(bucketKey),
          CacheValue.get(transactionLogIndex));

      omResponse.setDeleteBucketResponse(
          DeleteBucketResponse.newBuilder().build());

      // update used namespace for volume
      String volumeKey = omMetadataManager.getVolumeKey(volumeName);
      OmVolumeArgs omVolumeArgs =
          omMetadataManager.getVolumeTable().getReadCopy(volumeKey);
      if (omVolumeArgs == null) {
        throw new OMException("Volume " + volumeName + " is not found",
            OMException.ResultCodes.VOLUME_NOT_FOUND);
      }
      omVolumeArgs.incrUsedNamespace(-1L);
      // Update table cache.
      omMetadataManager.getVolumeTable().addCacheEntry(
          new CacheKey<>(volumeKey),
          CacheValue.get(transactionLogIndex, omVolumeArgs));

      // Add to double buffer.
      omClientResponse = new OMBucketDeleteResponse(omResponse.build(),
          volumeName, bucketName, omVolumeArgs.copyObject());
    } catch (IOException | InvalidPathException ex) {
      success = false;
      exception = ex;
      omClientResponse = new OMBucketDeleteResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (acquiredBucketLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (acquiredVolumeLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseReadLock(VOLUME_LOCK, volumeName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    // Performing audit logging outside of the lock.
    markForAudit(auditLogger, buildAuditMessage(OMAction.DELETE_BUCKET,
        auditMap, exception, userInfo));

    // return response.
    if (success) {
      LOG.debug("Deleted bucket:{} in volume:{}", bucketName, volumeName);
      return omClientResponse;
    } else {
      omMetrics.incNumBucketDeleteFails();
      LOG.error("Delete bucket failed for bucket:{} in volume:{}", bucketName,
          volumeName, exception);
      return omClientResponse;
    }
  }

  private boolean bucketContainsSnapshot(OMMetadataManager omMetadataManager,
      String snapshotBucketKey) throws IOException {
    return bucketContainsSnapshotInCache(omMetadataManager, snapshotBucketKey)
        || bucketContainsSnapshotInTable(omMetadataManager, snapshotBucketKey);
  }

  private boolean bucketContainsSnapshotInTable(
      OMMetadataManager omMetadataManager, String snapshotBucketKey)
      throws IOException {
    try (
        TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>>
            snapshotIterator = omMetadataManager
            .getSnapshotInfoTable().iterator()) {
      snapshotIterator.seek(snapshotBucketKey);
      if (snapshotIterator.hasNext()) {
        return snapshotIterator.next().getKey().startsWith(snapshotBucketKey);
      }
    }
    return false;
  }

  private boolean bucketContainsSnapshotInCache(
      OMMetadataManager omMetadataManager, String snapshotBucketKey) {
    Iterator<Map.Entry<CacheKey<String>, CacheValue<SnapshotInfo>>> cacheIter =
        omMetadataManager.getSnapshotInfoTable().cacheIterator();
    while (cacheIter.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<SnapshotInfo>> cacheKeyValue =
          cacheIter.next();
      String key = cacheKeyValue.getKey().getCacheKey();
      // TODO: [SNAPSHOT] Revisit when delete snapshot gets implemented as entry
      //  in cache/table could be null.
      if (key.startsWith(snapshotBucketKey)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Validates bucket delete requests.
   * Handles the cases where an older client attempts to delete a bucket
   * a new bucket layout.
   * We do not want to allow this to happen, since this would cause the client
   * to be able to delete buckets it cannot understand.
   *
   * @param req - the request to validate
   * @param ctx - the validation context
   * @return the validated request
   * @throws OMException if the request is invalid
   */
  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.DeleteBucket
  )
  public static OMRequest blockBucketDeleteWithBucketLayoutFromOldClient(
      OMRequest req, ValidationContext ctx) throws IOException {
    DeleteBucketRequest request = req.getDeleteBucketRequest();

    if (request.hasBucketName() && request.hasVolumeName()) {
      BucketLayout bucketLayout = ctx.getBucketLayout(
          request.getVolumeName(), request.getBucketName());
      bucketLayout.validateSupportedOperation();
    }
    return req;
  }
}
