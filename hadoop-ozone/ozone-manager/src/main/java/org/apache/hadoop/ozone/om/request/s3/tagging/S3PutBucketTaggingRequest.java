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
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketSetPropertyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PutBucketTaggingRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PutBucketTaggingResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles PutBucketTagging (S3 bucket tagging).
 */
public class S3PutBucketTaggingRequest extends OMClientRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3PutBucketTaggingRequest.class);

  public S3PutBucketTaggingRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager)
      throws IOException {
    PutBucketTaggingRequest putBucketTaggingRequest =
        getOmRequest().getPutBucketTaggingRequest();
    Objects.requireNonNull(putBucketTaggingRequest, "putBucketTaggingRequest == null");

    BucketArgs bucketArgs = putBucketTaggingRequest.getBucketArgs();
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
            OMAction.PUT_BUCKET_TAGGING,
            resolvedBucket.audit(omBucketArgs.toAuditMap()), ex,
            getOmRequest().getUserInfo()));
        throw ex;
      }
    }

    PutBucketTaggingRequest.Builder req = putBucketTaggingRequest.toBuilder();
    req.setModificationTime(Time.now());
    req.setBucketArgs(resolvedBucket.update(bucketArgs));
    return getOmRequest().toBuilder()
        .setPutBucketTaggingRequest(req.build())
        .setUserInfo(getUserInfo())
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();

    PutBucketTaggingRequest putBucketTaggingRequest =
        getOmRequest().getPutBucketTaggingRequest();
    Objects.requireNonNull(putBucketTaggingRequest, "putBucketTaggingRequest == null");

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumPutBucketTagging();

    BucketArgs bucketArgs = putBucketTaggingRequest.getBucketArgs();
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

      Map<String, String> tags =
          KeyValueUtil.getFromProtobuf(bucketArgs.getTagsList());

      omBucketInfo = dbBucketInfo.toBuilder()
          .setTags(tags)
          .setUpdateID(transactionLogIndex)
          .setModificationTime(putBucketTaggingRequest.getModificationTime())
          .build();

      omMetadataManager.getBucketTable().addCacheEntry(
          new CacheKey<>(bucketKey),
          CacheValue.get(transactionLogIndex, omBucketInfo));

      omResponse.setPutBucketTaggingResponse(
          PutBucketTaggingResponse.newBuilder().build());
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
        OMAction.PUT_BUCKET_TAGGING, auditMap, exception,
        getOmRequest().getUserInfo()));

    if (success) {
      LOG.debug("Put bucket tagging for bucket:{} in volume:{}",
          bucketName, volumeName);
      return omClientResponse;
    }
    omMetrics.incNumPutBucketTaggingFails();
    LOG.error("Put bucket tagging failed for bucket:{} in volume:{}",
        bucketName, volumeName, exception);
    return omClientResponse;
  }
}
