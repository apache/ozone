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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for S3 bucket tagging write requests (put / delete).
 */
public abstract class S3BucketTaggingRequestBase extends OMClientRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3BucketTaggingRequestBase.class);

  protected S3BucketTaggingRequestBase(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    OMRequest baseRequest = super.preExecute(ozoneManager);
    BucketArgs bucketArgs = getRequestBucketArgs(baseRequest);

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
            getAuditAction(),
            resolvedBucket.audit(omBucketArgs.toAuditMap()), ex,
            getOmRequest().getUserInfo()));
        throw ex;
      }
    }
    return buildUpdatedOMRequest(baseRequest,
        resolvedBucket.update(bucketArgs), Time.now());
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();

    OMRequest omRequest = getOmRequest();
    BucketArgs bucketArgs = getRequestBucketArgs(omRequest);

    OmBucketArgs omBucketArgs = OmBucketArgs.getFromProtobuf(bucketArgs);
    String volumeName = bucketArgs.getVolumeName();
    String bucketName = bucketArgs.getBucketName();
    long modificationTime = getModificationTime(omRequest);

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OMMetrics omMetrics = ozoneManager.getMetrics();
    incRequestMetric(omMetrics);

    OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(omRequest);
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

      Map<String, String> tags = getTagsToApply(bucketArgs);
      OmBucketInfo omBucketInfo = dbBucketInfo.toBuilder()
          .setTags(tags)
          .setUpdateID(transactionLogIndex)
          .setModificationTime(modificationTime)
          .build();

      omMetadataManager.getBucketTable().addCacheEntry(
          new CacheKey<>(bucketKey),
          CacheValue.get(transactionLogIndex, omBucketInfo));
      setSuccessResponse(omResponse);
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
        getAuditAction(), auditMap, exception, getOmRequest().getUserInfo()));

    if (success) {
      LOG.debug("{} bucket tagging succeeded for bucket:{} in volume:{}",
          getOperationName(), bucketName, volumeName);
      return omClientResponse;
    }

    incRequestFailMetric(omMetrics);
    LOG.error("{} bucket tagging failed for bucket:{} in volume:{}",
        getOperationName(), bucketName, volumeName, exception);
    return omClientResponse;
  }

  protected abstract BucketArgs getRequestBucketArgs(OMRequest omRequest);

  protected abstract long getModificationTime(OMRequest omRequest);

  protected abstract OMRequest buildUpdatedOMRequest(OMRequest baseRequest,
      BucketArgs bucketArgs, long modificationTime) throws IOException;

  protected abstract Map<String, String> getTagsToApply(BucketArgs bucketArgs);

  protected abstract void setSuccessResponse(OMResponse.Builder omResponse);

  protected abstract OMAction getAuditAction();

  protected abstract void incRequestMetric(OMMetrics omMetrics);

  protected abstract void incRequestFailMetric(OMMetrics omMetrics);

  protected abstract String getOperationName();
}
