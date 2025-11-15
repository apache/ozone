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

package org.apache.hadoop.ozone.om.request.key.acl.prefix;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.PREFIX_LOCK;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.PrefixManagerImpl;
import org.apache.hadoop.ozone.om.PrefixManagerImpl.OMPrefixAclOpResult;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;

/**
 * Base class for Prefix acl request.
 */
public abstract class OMPrefixAclRequest extends OMClientRequest {

  public OMPrefixAclRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    OMRequest omRequest = super.preExecute(ozoneManager);
    
    // ACL check during preExecute
    if (ozoneManager.getAclsEnabled()) {
      try {
        PrefixManagerImpl prefixManager =
            (PrefixManagerImpl) ozoneManager.getPrefixManager();
        OzoneObj resolvedPrefixObj = prefixManager.getResolvedPrefixObj(getOzoneObj());
        prefixManager.validateOzoneObj(getOzoneObj());
        validatePrefixPath(resolvedPrefixObj.getPath());
        
        checkAcls(ozoneManager, OzoneObj.ResourceType.PREFIX,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE_ACL,
            resolvedPrefixObj.getVolumeName(), resolvedPrefixObj.getBucketName(),
            resolvedPrefixObj.getPrefixName());
      } catch (IOException ex) {
        // Ensure audit log captures preExecute failures
        Map<String, String> auditMap = new LinkedHashMap<>();
        OzoneObj obj = getOzoneObj();
        auditMap.putAll(obj.toAuditMap());
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
    final long trxnLogIndex = context.getIndex();

    OmPrefixInfo omPrefixInfo = null;

    OMResponse.Builder omResponse = onInit();
    OMClientResponse omClientResponse = null;
    Exception exception = null;

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean lockAcquired = false;
    String prefixPath = null;
    OzoneObj resolvedPrefixObj = null;
    OMPrefixAclOpResult operationResult = null;
    boolean opResult = false;
    Result result = null;

    PrefixManagerImpl prefixManager =
        (PrefixManagerImpl) ozoneManager.getPrefixManager();
    try {
      resolvedPrefixObj = prefixManager.getResolvedPrefixObj(getOzoneObj());
      prefixManager.validateOzoneObj(getOzoneObj());
      validatePrefixPath(resolvedPrefixObj.getPath());
      prefixPath = resolvedPrefixObj.getPath();

      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(PREFIX_LOCK, prefixPath));
      lockAcquired = getOmLockDetails().isLockAcquired();

      omPrefixInfo = omMetadataManager.getPrefixTable().get(prefixPath);
      if (omPrefixInfo != null) {
        omPrefixInfo.setUpdateID(trxnLogIndex);
      }

      try {
        operationResult = apply(resolvedPrefixObj, prefixManager, omPrefixInfo, trxnLogIndex);
      } catch (IOException ex) {
        // In HA case this will never happen.
        // As in add/remove/setAcl method we have logic to update database,
        // that can throw exception. But in HA case we shall not update DB.
        // The code in prefixManagerImpl is being done, because update
        // in-memory should be done after DB update for Non-HA code path.
        operationResult = new OMPrefixAclOpResult(null, false);
      }

      omPrefixInfo = operationResult.getOmPrefixInfo();
      if (omPrefixInfo == null) {
        throw new OMException(
            "No prefix info for the prefix path: " + prefixPath,
            OMException.ResultCodes.PREFIX_NOT_FOUND);
      }

      // As for remove acl list, for a prefix if after removing acl from
      // the existing acl list, if list size becomes zero, delete the
      // prefix from prefix table.
      if (getOmRequest().hasRemoveAclRequest() &&
          omPrefixInfo.getAcls().isEmpty()) {
        omMetadataManager.getPrefixTable().addCacheEntry(
            new CacheKey<>(prefixPath),
            CacheValue.get(trxnLogIndex));
      } else {
        // update cache.
        omMetadataManager.getPrefixTable().addCacheEntry(
            new CacheKey<>(prefixPath),
            CacheValue.get(trxnLogIndex, omPrefixInfo));
      }

      opResult  = operationResult.isSuccess();
      omClientResponse = onSuccess(omResponse, omPrefixInfo, opResult);
      result = Result.SUCCESS;

    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = onFailure(omResponse, exception);
    } finally {
      if (lockAcquired) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(PREFIX_LOCK, prefixPath));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    OzoneObj obj = resolvedPrefixObj;
    if (obj == null) {
      // Fall back to the prefix under link bucket
      obj = getOzoneObj();
    }

    Map<String, String> auditMap = obj.toAuditMap();
    onComplete(obj, opResult, exception, ozoneManager.getMetrics(), result,
        trxnLogIndex, ozoneManager.getAuditLogger(), auditMap);

    return omClientResponse;
  }

  private void validatePrefixPath(String prefixPath) throws OMException {
    if (!OzoneFSUtils.isValidName(prefixPath)) {
      throw new OMException("Invalid prefix path name: " + prefixPath,
          OMException.ResultCodes.INVALID_PATH_IN_ACL_REQUEST);
    }
  }

  /**
   * Get the prefix ozone object passed in the request.
   * Note: The ozone object might still refer to a prefix under a link bucket which
   *       might require to be resolved.
   * @return Prefix ozone object.
   */
  abstract OzoneObj getOzoneObj();

  // TODO: Finer grain metrics can be moved to these callbacks. They can also
  // be abstracted into separate interfaces in future.
  /**
   * Get the initial OM response builder with lock.
   * @return OM response builder.
   */
  abstract OMResponse.Builder onInit();

  /**
   * Get the OM client response on success case with lock.
   * @param omResponse OM response builder.
   * @param omPrefixInfo The updated prefix info.
   * @param operationResult The operation result. See {@link OMPrefixAclOpResult}.
   * @return OMClientResponse
   */
  abstract OMClientResponse onSuccess(
      OMResponse.Builder omResponse, OmPrefixInfo omPrefixInfo,
      boolean operationResult);

  /**
   * Get the om client response on failure case with lock.
   * @param omResponse OM response builder.
   * @param exception Exception thrown while processing the request.
   * @return OMClientResponse
   */
  abstract OMClientResponse onFailure(OMResponse.Builder omResponse,
      Exception exception);

  /**
   * Completion hook for final processing before return without lock.
   * Usually used for logging without lock and metric update.
   * @param resolvedOzoneObj Resolved prefix object in case the prefix is under a link bucket.
   *                         The original ozone object if the prefix is not under a link bucket.
   * @param operationResult The operation result. See {@link OMPrefixAclOpResult}.
   * @param exception Exception thrown while processing the request.
   * @param omMetrics OM metrics used to update the relevant metrics.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  abstract void onComplete(OzoneObj resolvedOzoneObj, boolean operationResult,
                           Exception exception, OMMetrics omMetrics, Result result, long trxnLogIndex,
                           AuditLogger auditLogger, Map<String, String> auditMap);

  /**
   * Apply the acl operation to underlying storage (prefix tree and table cache).
   * @param resolvedOzoneObj Resolved prefix object in case the prefix is under a link bucket.
   *                         The original ozone object if the prefix is not under a link bucket.
   * @param prefixManager Prefix manager used to update the underlying prefix storage.
   * @param omPrefixInfo Previous prefix info, null if there is no existing prefix info.
   * @param trxnLogIndex Transaction log index.
   * @return result of the prefix operation, see {@link OMPrefixAclOpResult}.
   * @throws IOException Exception thrown when updating the underlying prefix storage.
   */
  abstract OMPrefixAclOpResult apply(OzoneObj resolvedOzoneObj, PrefixManagerImpl prefixManager,
      OmPrefixInfo omPrefixInfo, long trxnLogIndex) throws IOException;
}

