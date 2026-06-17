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

package org.apache.hadoop.ozone.om.request.s3.tagging;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.OzoneManagerUtils;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketSetPropertyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteBucketTaggingRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteBucketTaggingResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles DeleteBucketTagging (S3 bucket tagging).
 */
public class S3DeleteBucketTaggingRequest extends OMClientRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3DeleteBucketTaggingRequest.class);

  public S3DeleteBucketTaggingRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager)
      throws IOException {
    DeleteBucketTaggingRequest deleteBucketTaggingRequest =
        getOmRequest().getDeleteBucketTaggingRequest();
    Objects.requireNonNull(deleteBucketTaggingRequest,
        "deleteBucketTaggingRequest == null");

    BucketArgs bucketArgs = deleteBucketTaggingRequest.getBucketArgs();
    OmBucketArgs omBucketArgs = OmBucketArgs.getFromProtobuf(bucketArgs);
    String volumeName = bucketArgs.getVolumeName();
    String bucketName = bucketArgs.getBucketName();

    ResolvedBucket resolvedBucket = ozoneManager.resolveBucketLink(
        Pair.of(volumeName, bucketName), this);

    if (ozoneManager.getAclsEnabled()) {
      try {
        checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE,
            resolvedBucket.realVolume(), resolvedBucket.realBucket(), null);
      } catch (IOException ex) {
        markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
            OMAction.DELETE_BUCKET_TAGGING,
            resolvedBucket.audit(omBucketArgs.toAuditMap()), ex,
            getOmRequest().getUserInfo()));
        throw ex;
      }
    }

    DeleteBucketTaggingRequest.Builder req =
        deleteBucketTaggingRequest.toBuilder();
    req.setModificationTime(Time.now());
    req.setBucketArgs(resolvedBucket.update(bucketArgs));

    return getOmRequest().toBuilder()
        .setDeleteBucketTaggingRequest(req.build())
        .setUserInfo(getUserInfo())
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();

    DeleteBucketTaggingRequest deleteBucketTaggingRequest =
        getOmRequest().getDeleteBucketTaggingRequest();
    Objects.requireNonNull(deleteBucketTaggingRequest,
        "deleteBucketTaggingRequest == null");

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumDeleteBucketTagging();

    BucketArgs bucketArgs = deleteBucketTaggingRequest.getBucketArgs();
    OmBucketArgs omBucketArgs = OmBucketArgs.getFromProtobuf(bucketArgs);

    String volumeName = bucketArgs.getVolumeName();
    String bucketName = bucketArgs.getBucketName();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OmBucketInfo omBucketInfo = null;

    Exception exception = null;
    boolean acquiredBucketLock = false;
    boolean success = true;
    OMClientResponse omClientResponse = null;
    try {
      mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLock(
          BUCKET_LOCK, volumeName, bucketName));
      acquiredBucketLock = getOmLockDetails().isLockAcquired();

      String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
      OmBucketInfo dbBucketInfo = OzoneManagerUtils.getBucketInfo(
          omMetadataManager, volumeName, bucketName);

      omBucketInfo = dbBucketInfo.toBuilder()
          .setTags(Collections.emptyMap())
          .setUpdateID(transactionLogIndex)
          .setModificationTime(deleteBucketTaggingRequest.getModificationTime())
          .build();

      omMetadataManager.getBucketTable().addCacheEntry(
          new CacheKey<>(bucketKey),
          CacheValue.get(transactionLogIndex, omBucketInfo));

      omResponse.setDeleteBucketTaggingResponse(
          DeleteBucketTaggingResponse.newBuilder().build());
      omClientResponse = new OMBucketSetPropertyResponse(
          omResponse.build(), omBucketInfo);
    } catch (IOException ex) {
      success = false;
      exception = ex;
      omClientResponse = new OMBucketSetPropertyResponse(
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

    Map<String, String> auditMap = omBucketArgs.toAuditMap();
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.DELETE_BUCKET_TAGGING, auditMap, exception,
        getOmRequest().getUserInfo()));

    if (success) {
      LOG.debug("Delete bucket tagging for bucket:{} in volume:{}",
          bucketName, volumeName);
      return omClientResponse;
    }
    omMetrics.incNumDeleteBucketTaggingFails();
    LOG.error("Delete bucket tagging failed for bucket:{} in volume:{}",
        bucketName, volumeName, exception);
    return omClientResponse;
  }
}
