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

package org.apache.hadoop.ozone.om.request.bucket;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Optional;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketDeleteResponse;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

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
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
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
    IOException exception = null;

    boolean acquiredBucketLock = false, acquiredVolumeLock = false;
    boolean success = true;
    OMClientResponse omClientResponse = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.DELETE,
            volumeName, bucketName, null);
      }

      // acquire lock
      acquiredVolumeLock =
          omMetadataManager.getLock().acquireReadLock(VOLUME_LOCK, volumeName);
      acquiredBucketLock =
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
          volumeName, bucketName);

      // No need to check volume exists here, as bucket cannot be created
      // with out volume creation. Check if bucket exists
      String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);

      if (!omMetadataManager.getBucketTable().isExist(bucketKey)) {
        LOG.debug("bucket: {} not found ", bucketName);
        throw new OMException("Bucket already exist", BUCKET_NOT_FOUND);
      }

      //Check if bucket is empty
      if (!omMetadataManager.isBucketEmpty(volumeName, bucketName)) {
        LOG.debug("bucket: {} is not empty ", bucketName);
        throw new OMException("Bucket is not empty",
            OMException.ResultCodes.BUCKET_NOT_EMPTY);
      }
      omMetrics.decNumBuckets();

      // Update table cache.
      omMetadataManager.getBucketTable().addCacheEntry(
          new CacheKey<>(bucketKey),
          new CacheValue<>(Optional.absent(), transactionLogIndex));

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
          new CacheValue<>(Optional.of(omVolumeArgs), transactionLogIndex));

      // Add to double buffer.
      omClientResponse = new OMBucketDeleteResponse(omResponse.build(),
          volumeName, bucketName, omVolumeArgs.copyObject());
    } catch (IOException ex) {
      success = false;
      exception = ex;
      omClientResponse = new OMBucketDeleteResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredBucketLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
      if (acquiredVolumeLock) {
        omMetadataManager.getLock().releaseReadLock(VOLUME_LOCK, volumeName);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.DELETE_BUCKET,
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
}
