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

package org.apache.hadoop.ozone.om.request.lifecycle;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleConfiguration;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.lifecycle.OMLifecycleConfigurationSetResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleConfiguration;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetLifecycleConfigurationRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetLifecycleConfigurationResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles SetLifecycleConfiguration Request.
 */
public class OMLifecycleConfigurationSetRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMLifecycleConfigurationSetRequest.class);

  public OMLifecycleConfigurationSetRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    OMRequest omRequest = super.preExecute(ozoneManager);
    SetLifecycleConfigurationRequest request =
        omRequest.getSetLifecycleConfigurationRequest();
    LifecycleConfiguration lifecycleConfiguration =
        request.getLifecycleConfiguration();

    OmUtils.validateVolumeName(lifecycleConfiguration.getVolume(), ozoneManager.isStrictS3());
    OmUtils.validateBucketName(lifecycleConfiguration.getBucket(), ozoneManager.isStrictS3());

    String volumeName = lifecycleConfiguration.getVolume();
    String bucketName = lifecycleConfiguration.getBucket();

    ResolvedBucket resolvedBucket = ozoneManager.resolveBucketLink(
        Pair.of(volumeName, bucketName), this);

    if (ozoneManager.getAclsEnabled()) {
      checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET, OzoneObj.StoreType.OZONE,
          IAccessAuthorizer.ACLType.ALL, resolvedBucket.realVolume(), 
          resolvedBucket.realBucket(), null);
    }

    if (resolvedBucket.bucketLayout().toProto() != request.getLifecycleConfiguration().getBucketLayout()) {
      throw new OMException("Bucket layout mismatch: requested lifecycle configuration " +
          "has bucket layout " + request.getLifecycleConfiguration().getBucketLayout() + 
          " but the actual bucket has layout " + resolvedBucket.bucketLayout().toProto(),
          OMException.ResultCodes.INVALID_REQUEST);
    }

    SetLifecycleConfigurationRequest.Builder newCreateRequest =
        request.toBuilder();

    LifecycleConfiguration.Builder newLifecycleConfiguration =
        lifecycleConfiguration.toBuilder()
            .setVolume(resolvedBucket.realVolume())
            .setBucket(resolvedBucket.realBucket());

    newLifecycleConfiguration.setCreationTime(System.currentTimeMillis());
    newCreateRequest.setLifecycleConfiguration(newLifecycleConfiguration);

    return omRequest.toBuilder().setUserInfo(getUserInfo())
        .setSetLifecycleConfigurationRequest(newCreateRequest.build())
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();

    SetLifecycleConfigurationRequest setLifecycleConfigurationRequest =
        getOmRequest().getSetLifecycleConfigurationRequest();

    LifecycleConfiguration lifecycleConfiguration =
        setLifecycleConfigurationRequest.getLifecycleConfiguration();

    String volumeName = lifecycleConfiguration.getVolume();
    String bucketName = lifecycleConfiguration.getBucket();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    UserInfo userInfo = getOmRequest().getUserInfo();

    IOException exception = null;
    boolean acquiredBucketLock = false;
    OMClientResponse omClientResponse = null;
    Map<String, String> auditMap = new HashMap<>();
    try {
      mergeOmLockDetails(metadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredBucketLock = getOmLockDetails().isLockAcquired();

      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      OmBucketInfo bucketInfo = metadataManager.getBucketTable().get(bucketKey);
      if (bucketInfo == null) {
        LOG.debug("bucket: {} in volume: {} doesn't exist", bucketName,
            volumeName);
        throw new OMException("Bucket doesn't exist", BUCKET_NOT_FOUND);
      }

      OmLifecycleConfiguration.Builder lcBuilder =
          OmLifecycleConfiguration.getBuilderFromProtobuf(lifecycleConfiguration);
      lcBuilder.setUpdateID(transactionLogIndex);
      OmLifecycleConfiguration omLifecycleConfiguration;
      try {
        omLifecycleConfiguration =
            lcBuilder.setBucketObjectID(bucketInfo.getObjectID()).build();
      } catch (IllegalArgumentException e) {
        if (e.getCause() instanceof OMException) {
          throw (OMException) e.getCause();
        }
        throw e;
      }
      auditMap = omLifecycleConfiguration.toAuditMap();

      metadataManager.getLifecycleConfigurationTable().addCacheEntry(
          new CacheKey<>(bucketKey),
          CacheValue.get(transactionLogIndex, omLifecycleConfiguration));

      omResponse.setSetLifecycleConfigurationResponse(
          SetLifecycleConfigurationResponse.newBuilder().build());

      omClientResponse = new OMLifecycleConfigurationSetResponse(
          omResponse.build(), omLifecycleConfiguration);
    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMLifecycleConfigurationSetResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (acquiredBucketLock) {
        mergeOmLockDetails(metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName));
      }
    }
    if (omClientResponse != null) {
      omClientResponse.setOmLockDetails(getOmLockDetails());
    }

    // Performing audit logging outside the lock.
    markForAudit(auditLogger, buildAuditMessage(OMAction.SET_LIFECYCLE_CONFIGURATION,
        auditMap, exception, userInfo));

    if (exception == null) {
      LOG.debug("Created lifecycle configuration bucket: {} in volume: {}",
          bucketName, volumeName);
      return omClientResponse;
    } else {
      LOG.error("Lifecycle configuration creation failed for bucket:{} " +
          "in volume:{}", bucketName, volumeName, exception);
      return omClientResponse;
    }
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.CLUSTER_NEEDS_FINALIZATION,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.SetLifecycleConfiguration
  )
  public static OMRequest disallowSetLifecycleConfigurationBeforeFinalization(
      OMRequest req, ValidationContext ctx) throws OMException {
    if (!ctx.versionManager()
        .isAllowed(OMLayoutFeature.LIFECYCLE_SUPPORT)) {
      throw new OMException("Cluster does not have the Lifecycle Support"
          + " feature finalized yet. Rejecting the request to set lifecycle"
          + " configuration. Please finalize the cluster upgrade and then try again.",
          OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION);
    }
    return req;
  }
}
