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

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.lifecycle.OMLifecycleConfigurationDeleteResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteLifecycleConfigurationRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteLifecycleConfigurationResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles DeleteLifecycleConfiguration Request.
 */
public class OMLifecycleConfigurationDeleteRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMLifecycleConfigurationDeleteRequest.class);

  public OMLifecycleConfigurationDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    OMRequest request = super.preExecute(ozoneManager);
    DeleteLifecycleConfigurationRequest deleteLifecycleConfigurationRequest =
        request.getDeleteLifecycleConfigurationRequest();

    String volumeName = deleteLifecycleConfigurationRequest.getVolumeName();
    String bucketName = deleteLifecycleConfigurationRequest.getBucketName();

    // Resolve bucket link and check ACLs
    ResolvedBucket resolvedBucket = ozoneManager.resolveBucketLink(
        Pair.of(volumeName, bucketName), this);

    if (ozoneManager.getAclsEnabled()) {
      checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET, OzoneObj.StoreType.OZONE,
          IAccessAuthorizer.ACLType.ALL, resolvedBucket.realVolume(), 
          resolvedBucket.realBucket(), null);
    }

    // Update the request with resolved volume and bucket names
    DeleteLifecycleConfigurationRequest.Builder newRequest = 
        deleteLifecycleConfigurationRequest.toBuilder()
            .setVolumeName(resolvedBucket.realVolume())
            .setBucketName(resolvedBucket.realBucket());

    return request.toBuilder()
        .setUserInfo(getUserInfo())
        .setDeleteLifecycleConfigurationRequest(newRequest.build())
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();

    OMRequest omRequest = getOmRequest();
    DeleteLifecycleConfigurationRequest deleteLifecycleConfigurationRequest =
        omRequest.getDeleteLifecycleConfigurationRequest();

    String volumeName = deleteLifecycleConfigurationRequest.getVolumeName();
    String bucketName = deleteLifecycleConfigurationRequest.getBucketName();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    Map<String, String> auditMap = buildVolumeAuditMap(volumeName);
    auditMap.put(OzoneConsts.BUCKET, bucketName);

    UserInfo userInfo = getOmRequest().getUserInfo();
    IOException exception = null;
    boolean acquiredBucketLock = false;
    boolean success = true;
    OMClientResponse omClientResponse = null;

    try {
      mergeOmLockDetails(metadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredBucketLock = getOmLockDetails().isLockAcquired();

      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);

      // Check existence.
      if (!metadataManager.getLifecycleConfigurationTable().isExist(
          bucketKey)) {
        LOG.debug("lifecycle bucket: {} volume: {} not found ", bucketName,
            volumeName);
        throw new OMException("Lifecycle configurations does not exist",
            OMException.ResultCodes.LIFECYCLE_CONFIGURATION_NOT_FOUND);
      }

      // Update table cache.
      metadataManager.getLifecycleConfigurationTable().addCacheEntry(
          new CacheKey<>(bucketKey), CacheValue.get(transactionLogIndex));

      omResponse.setDeleteLifecycleConfigurationResponse(
          DeleteLifecycleConfigurationResponse.newBuilder());

      omClientResponse = new OMLifecycleConfigurationDeleteResponse(
          omResponse.build(), volumeName, bucketName);

    } catch (IOException ex) {
      success = false;
      exception = ex;
      omClientResponse = new OMLifecycleConfigurationDeleteResponse(
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
    markForAudit(auditLogger, buildAuditMessage(
        OMAction.DELETE_LIFECYCLE_CONFIGURATION, auditMap, exception, userInfo));

    if (success) {
      LOG.debug("Deleted lifecycle bucket:{} volume:{}", bucketName,
          volumeName);
      return omClientResponse;
    } else {
      LOG.error("Delete lifecycle failed for bucket:{} in volume:{}",
          bucketName, volumeName, exception);
      return omClientResponse;
    }
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.CLUSTER_NEEDS_FINALIZATION,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.DeleteLifecycleConfiguration
  )
  public static OMRequest disallowDeleteLifecycleConfigurationBeforeFinalization(
      OMRequest req, ValidationContext ctx) throws OMException {
    if (!ctx.versionManager()
        .isAllowed(OMLayoutFeature.LIFECYCLE_SUPPORT)) {
      throw new OMException("Cluster does not have the Lifecycle Support"
          + " feature finalized yet. Rejecting the request to delete lifecycle"
          + " configuration. Please finalize the cluster upgrade and then try again.",
          OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION);
    }
    return req;
  }
}
