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

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.Objects;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketSetOwnerResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetBucketPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetBucketPropertyResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle set owner request for bucket.
 */
public class OMBucketSetOwnerRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMBucketSetOwnerRequest.class);

  public OMBucketSetOwnerRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager)
      throws IOException {
    long modificationTime = Time.now();
    OzoneManagerProtocolProtos.SetBucketPropertyRequest.Builder
        setBucketPropertyRequestBuilder = getOmRequest()
        .getSetBucketPropertyRequest().toBuilder()
        .setModificationTime(modificationTime);

    return getOmRequest().toBuilder()
        .setSetBucketPropertyRequest(setBucketPropertyRequestBuilder)
        .setUserInfo(getUserInfo())
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();
    SetBucketPropertyRequest setBucketPropertyRequest =
        getOmRequest().getSetBucketPropertyRequest();
    Objects.requireNonNull(setBucketPropertyRequest, "setBucketPropertyRequest == null");

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    if (!setBucketPropertyRequest.getBucketArgs().hasOwnerName()) {
      omResponse.setStatus(OzoneManagerProtocolProtos.Status.INVALID_REQUEST)
          .setSuccess(false);
      return new OMBucketSetOwnerResponse(omResponse.build());
    }

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumBucketUpdates();

    BucketArgs bucketArgs = setBucketPropertyRequest.getBucketArgs();
    OmBucketArgs omBucketArgs = OmBucketArgs.getFromProtobuf(bucketArgs);

    String volumeName = bucketArgs.getVolumeName();
    String bucketName = bucketArgs.getBucketName();
    String newOwner = bucketArgs.getOwnerName();
    String oldOwner = null;

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    Exception exception = null;
    boolean acquiredBucketLock = false, success = true;
    OMClientResponse omClientResponse = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE_ACL,
            volumeName, bucketName, null);
      }

      // acquire lock.
      mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLock(
          BUCKET_LOCK, volumeName, bucketName));
      acquiredBucketLock = getOmLockDetails().isLockAcquired();

      String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
      OmBucketInfo omBucketInfo =
          omMetadataManager.getBucketTable().get(bucketKey);
      //Check if bucket exist
      if (omBucketInfo == null) {
        LOG.debug("Bucket: {} not found ", bucketName);
        throw new OMException("Bucket doesn't exist",
            OMException.ResultCodes.BUCKET_NOT_FOUND);
      }

      // oldOwner can be null before HDDS-6171
      oldOwner = omBucketInfo.getOwner();

      if (newOwner.equals(oldOwner)) {
        LOG.warn("Bucket '{}/{}' owner is already user '{}'.",
            volumeName, bucketName, oldOwner);
        omResponse.setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setMessage("Bucket '" + volumeName + "/" + bucketName +
                "' owner is already '" + newOwner + "'.")
            .setSuccess(false);
        omResponse.setSetBucketPropertyResponse(
            SetBucketPropertyResponse.newBuilder().setResponse(false).build());
        omClientResponse = new OMBucketSetOwnerResponse(omResponse.build());
        return omClientResponse;
      }

      OmBucketInfo newOmBucketInfo = omBucketInfo.toBuilder()
          .setOwner(newOwner)
          .setModificationTime(setBucketPropertyRequest.getModificationTime())
          .setUpdateID(transactionLogIndex)
          .build();

      LOG.debug("Updating bucket owner to {} for bucket: {} in volume: {}",
          newOwner, bucketName, volumeName);

      // Update table cache.
      omMetadataManager.getBucketTable().addCacheEntry(
          new CacheKey<>(bucketKey),
          CacheValue.get(transactionLogIndex, newOmBucketInfo));

      omResponse.setSetBucketPropertyResponse(
          SetBucketPropertyResponse.newBuilder().setResponse(true).build());
      omClientResponse = new OMBucketSetOwnerResponse(
          omResponse.build(), newOmBucketInfo);
    } catch (IOException | InvalidPathException ex) {
      success = false;
      exception = ex;
      omClientResponse = new OMBucketSetOwnerResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (acquiredBucketLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    // Performing audit logging outside of the lock.
    markForAudit(auditLogger, buildAuditMessage(OMAction.SET_OWNER,
        omBucketArgs.toAuditMap(), exception, userInfo));

    // return response.
    if (success) {
      LOG.debug("Successfully changed Owner of Bucket {}/{} from {} -> {}",
          volumeName, bucketName, oldOwner, newOwner);
      return omClientResponse;
    } else {
      LOG.error("Setting Owner failed for bucket:{} in volume:{}",
          bucketName, volumeName, exception);
      omMetrics.incNumBucketUpdateFails();
      return omClientResponse;
    }
  }
}
