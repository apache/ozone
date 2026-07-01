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

package org.apache.hadoop.ozone.om.request.bucket.acl;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.AclOp;
import org.apache.hadoop.ozone.om.request.util.ObjectParser;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.acl.OMBucketAclResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneObj.ObjectType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;

/**
 * Base class for Bucket acl request.
 */
public abstract class OMBucketAclRequest extends OMClientRequest {

  private final AclOp omBucketAclOp;

  public OMBucketAclRequest(OMRequest omRequest, AclOp aclOp) {
    super(omRequest);
    omBucketAclOp = aclOp;
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    OMRequest omRequest = super.preExecute(ozoneManager);

    // ACL check during preExecute
    if (ozoneManager.getAclsEnabled()) {
      try {
        ObjectParser objectParser = new ObjectParser(getPath(),
            ObjectType.BUCKET);
        ResolvedBucket resolvedBucket = ozoneManager.resolveBucketLink(
            Pair.of(objectParser.getVolume(), objectParser.getBucket()));
        String volume = resolvedBucket.realVolume();
        String bucket = resolvedBucket.realBucket();

        checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE_ACL,
            volume, bucket, null);
      } catch (IOException ex) {
        // Ensure audit log captures preExecute failures
        Map<String, String> auditMap = new LinkedHashMap<>();
        OzoneObj obj = getObject();
        auditMap.putAll(obj.toAuditMap());
        List<OzoneAcl> acls = getAcls();
        if (acls != null) {
          auditMap.put(OzoneConsts.ACL, acls.toString());
        }
        // Determine which action based on request type
        OMAction action = OMAction.SET_ACL;
        if (omRequest.hasAddAclRequest()) {
          action = OMAction.ADD_ACL;
        } else if (omRequest.hasRemoveAclRequest()) {
          action = OMAction.REMOVE_ACL;
        }
        markForAudit(ozoneManager.getAuditLogger(),
            buildAuditMessage(action, auditMap, ex,
                omRequest.getUserInfo()));
        throw ex;
      }
    }

    return omRequest;
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();

    // protobuf guarantees acls are non-null.
    List<OzoneAcl> ozoneAcls = getAcls();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumBucketUpdates();
    OmBucketInfo omBucketInfo;

    OMResponse.Builder omResponse = onInit();
    OMClientResponse omClientResponse = null;
    Exception exception = null;

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean lockAcquired = false;
    String volume = null;
    String bucket = null;
    boolean operationResult = false;
    try {
      ObjectParser objectParser = new ObjectParser(getPath(),
          ObjectType.BUCKET);
      ResolvedBucket resolvedBucket = ozoneManager.resolveBucketLink(
          Pair.of(objectParser.getVolume(), objectParser.getBucket()));
      volume = resolvedBucket.realVolume();
      bucket = resolvedBucket.realBucket();

      mergeOmLockDetails(
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volume,
              bucket));
      lockAcquired = getOmLockDetails().isLockAcquired();

      String dbBucketKey = omMetadataManager.getBucketKey(volume, bucket);
      omBucketInfo = omMetadataManager.getBucketTable().get(dbBucketKey);
      if (omBucketInfo == null) {
        throw new OMException(OMException.ResultCodes.BUCKET_NOT_FOUND);
      }

      OmBucketInfo.Builder builder = omBucketInfo.toBuilder();

      operationResult = omBucketAclOp.test(ozoneAcls, builder.acls());

      if (operationResult) {
        // Update the modification time when updating ACLs of Bucket.
        long modificationTime = omBucketInfo.getModificationTime();
        if (getOmRequest().getAddAclRequest().hasObj()) {
          modificationTime = getOmRequest().getAddAclRequest()
              .getModificationTime();
        } else if (getOmRequest().getSetAclRequest().hasObj()) {
          modificationTime = getOmRequest().getSetAclRequest()
              .getModificationTime();
        } else if (getOmRequest().getRemoveAclRequest().hasObj()) {
          modificationTime = getOmRequest().getRemoveAclRequest()
              .getModificationTime();
        }
        omBucketInfo = builder
            .setUpdateID(transactionLogIndex)
            .setModificationTime(modificationTime)
            .build();

        // update cache.
        omMetadataManager.getBucketTable().addCacheEntry(
            new CacheKey<>(dbBucketKey),
            CacheValue.get(transactionLogIndex, omBucketInfo));
      }

      omClientResponse = onSuccess(omResponse, omBucketInfo, operationResult);

    } catch (IOException | InvalidPathException ex) {
      exception = ex;
      omClientResponse = onFailure(omResponse, exception);
    } finally {
      if (lockAcquired) {
        mergeOmLockDetails(
            omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volume,
                bucket));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    OzoneObj obj = getObject();
    Map<String, String> auditMap = obj.toAuditMap();
    if (ozoneAcls != null) {
      auditMap.put(OzoneConsts.ACL, ozoneAcls.toString());
    }

    onComplete(operationResult, exception, ozoneManager.getMetrics(),
        ozoneManager.getAuditLogger(), auditMap);
    return omClientResponse;
  }

  /**
   * Get the Acls from the request.
   * @return List of OzoneAcls, for add/remove it is a single element list
   * for set it can be non-single element list.
   */
  abstract List<OzoneAcl> getAcls();

  /**
   * Get the path name from the request.
   * @return path name
   */
  abstract String getPath();

  /**
   * Get the Bucket object Info from the request.
   * @return OzoneObjInfo
   */
  abstract OzoneObj getObject();

  // TODO: Finer grain metrics can be moved to these callbacks. They can also
  // be abstracted into separate interfaces in future.
  /**
   * Get the initial om response builder with lock.
   * @return om response builder.
   */
  abstract OMResponse.Builder onInit();

  /**
   * Get the om client response on success case with lock.
   */
  abstract OMClientResponse onSuccess(
      OMResponse.Builder omResponse, OmBucketInfo omBucketInfo,
      boolean operationResult);

  /**
   * Get the om client response on failure case with lock.
   */
  OMClientResponse onFailure(OMResponse.Builder omResponse,
      Exception exception) {
    return new OMBucketAclResponse(
        createErrorOMResponse(omResponse, exception));
  }

  /**
   * Completion hook for final processing before return without lock.
   * Usually used for logging without lock and metric update.
   */
  abstract void onComplete(boolean operationResult, Exception exception,
      OMMetrics omMetrics, AuditLogger auditLogger,
      Map<String, String> auditMap);
}

