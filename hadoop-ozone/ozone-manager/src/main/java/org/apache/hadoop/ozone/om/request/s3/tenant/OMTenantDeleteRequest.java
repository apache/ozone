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

package org.apache.hadoop.ozone.om.request.s3.tenant;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TENANT_NOT_EMPTY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TENANT_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.VOLUME_LOCK;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.MULTITENANCY_SCHEMA;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.multitenant.OzoneTenant;
import org.apache.hadoop.ozone.om.multitenant.Tenant;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantDeleteResponse;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteTenantRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteTenantResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles OMTenantDelete request.
 */
public class OMTenantDeleteRequest extends OMVolumeRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMTenantDeleteRequest.class);

  public OMTenantDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  @DisallowedUntilLayoutVersion(MULTITENANCY_SCHEMA)
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    final OMRequest omRequest = super.preExecute(ozoneManager);

    final OMMultiTenantManager multiTenantManager =
        ozoneManager.getMultiTenantManager();

    // Check Ozone cluster admin privilege
    multiTenantManager.checkAdmin();

    // First get tenant name
    final String tenantId = omRequest.getDeleteTenantRequest().getTenantId();
    Objects.requireNonNull(tenantId, "tenantId == null");

    // Check if there are any accessIds in the tenant.
    // This must be done before we attempt to delete policies from Ranger.
    if (!multiTenantManager.isTenantEmpty(tenantId)) {
      LOG.warn("tenant: '{}' is not empty. Unable to delete the tenant",
          tenantId);
      throw new OMException("Tenant '" + tenantId + "' is not empty. " +
          "All accessIds associated to this tenant must be revoked before " +
          "the tenant can be deleted. See `ozone tenant user revoke`",
          TENANT_NOT_EMPTY);
    }

    // Get tenant object by tenant name
    final Tenant tenantObj = multiTenantManager.getTenantFromDBById(tenantId);

    final OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    final OmDBTenantState dbTenantState =
        omMetadataManager.getTenantStateTable().get(tenantId);
    if (dbTenantState == null) {
      LOG.debug("tenant: '{}' does not exist", tenantId);
      throw new OMException("Tenant '" + tenantId + "' does not exist",
          TENANT_NOT_FOUND);
    }
    final String volumeName = dbTenantState.getBucketNamespaceName();
    Objects.requireNonNull(volumeName);
    if (volumeName.isEmpty()) {
      throw new OMException("Tenant '" + tenantId + "' has empty volume name",
          INVALID_REQUEST);
    }

    // Perform ACL check during preExecute (WRITE_ACL on volume if applicable)
    if (ozoneManager.getAclsEnabled()) {
      try {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE_ACL,
            volumeName, null, null);
      } catch (IOException ex) {
        // Ensure audit log captures preExecute failures
        Map<String, String> auditMap = new HashMap<>();
        auditMap.put(OzoneConsts.TENANT, tenantId);
        markForAudit(ozoneManager.getAuditLogger(),
            buildAuditMessage(OMAction.DELETE_TENANT, auditMap, ex,
                omRequest.getUserInfo()));
        throw ex;
      }
    }

    // Acquire write lock to authorizer (Ranger)
    multiTenantManager.getAuthorizerLock().tryWriteLockInOMRequest();
    try {
      // Remove policies and roles from Ranger
      // TODO: Deactivate (disable) policies instead of delete?
      multiTenantManager.getAuthorizerOp().deleteTenant(tenantObj);
    } catch (Exception e) {
      multiTenantManager.getAuthorizerLock().unlockWriteInOMRequest();
      throw e;
    }

    return omRequest;
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();

    final OMMultiTenantManager multiTenantManager =
        ozoneManager.getMultiTenantManager();

    final OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumTenantDeletes();

    OMClientResponse omClientResponse = null;
    final OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    boolean acquiredVolumeLock = false;
    final Map<String, String> auditMap = new HashMap<>();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    final DeleteTenantRequest request = getOmRequest().getDeleteTenantRequest();
    final String tenantId = request.getTenantId();
    String volumeName = null;
    boolean decVolumeRefCount = true;

    Exception exception = null;
    OmVolumeArgs omVolumeArgs = null;

    try {
      // Check tenant existence in tenantStateTable
      if (!omMetadataManager.getTenantStateTable().isExist(tenantId)) {
        LOG.debug("tenant: {} does not exist", tenantId);
        throw new OMException("Tenant '" + tenantId + "' does not exist",
            TENANT_NOT_FOUND);
      }

      // Reading the TenantStateTable without lock as we don't have or need
      // a TENANT_LOCK. The assumption is that OmDBTenantState is read-only
      // once it is set during tenant creation.
      final OmDBTenantState dbTenantState =
          omMetadataManager.getTenantStateTable().get(tenantId);
      volumeName = dbTenantState.getBucketNamespaceName();
      Objects.requireNonNull(volumeName, "volumeName == null");

      LOG.debug("Tenant '{}' has volume '{}'", tenantId, volumeName);
      // decVolumeRefCount is true if volumeName is not empty string
      decVolumeRefCount = !volumeName.isEmpty();

      // Acquire the volume lock
      mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, volumeName));
      acquiredVolumeLock = getOmLockDetails().isLockAcquired();

      // Invalidate cache entry
      omMetadataManager.getTenantStateTable().addCacheEntry(
          new CacheKey<>(tenantId),
          CacheValue.get(transactionLogIndex));

      // Decrement volume refCount
      if (decVolumeRefCount) {
        omVolumeArgs = getVolumeInfo(omMetadataManager, volumeName)
            .toBuilder()
            .decRefCount()
            .build();

        // Update omVolumeArgs
        final String dbVolumeKey = omMetadataManager.getVolumeKey(volumeName);
        omMetadataManager.getVolumeTable().addCacheEntry(
            new CacheKey<>(dbVolumeKey),
            CacheValue.get(transactionLogIndex, omVolumeArgs));

        // TODO: Set response dbVolumeKey?
      }

      // Update tenant cache
      multiTenantManager.getCacheOp().deleteTenant(new OzoneTenant(tenantId));

      // Compose response
      //
      // If decVolumeRefCount is false, return -1 to the client, otherwise
      // return the actual volume refCount. Note if the actual volume refCount
      // becomes negative somehow, omVolumeArgs.decRefCount() would have thrown
      // earlier.
      final DeleteTenantResponse.Builder deleteTenantResponse =
          DeleteTenantResponse.newBuilder()
              .setVolumeName(volumeName)
              .setVolRefCount(omVolumeArgs == null ? -1 :
                  omVolumeArgs.getRefCount());

      omClientResponse = new OMTenantDeleteResponse(
          omResponse.setDeleteTenantResponse(deleteTenantResponse).build(),
          volumeName, omVolumeArgs, tenantId);

    } catch (IOException | InvalidPathException ex) {
      exception = ex;
      omClientResponse = new OMTenantDeleteResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (acquiredVolumeLock) {
        mergeOmLockDetails(
            omMetadataManager.getLock().releaseWriteLock(VOLUME_LOCK,
                volumeName));
      }
      // Release authorizer write lock
      multiTenantManager.getAuthorizerLock().unlockWriteInOMRequest();
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    // Perform audit logging
    auditMap.put(OzoneConsts.TENANT, tenantId);
    // Audit tenant deletion
    markForAudit(ozoneManager.getAuditLogger(),
        buildAuditMessage(OMAction.DELETE_TENANT,
            auditMap, exception, getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.info("Deleted tenant '{}' and volume '{}'", tenantId, volumeName);
      omMetrics.decNumTenants();
    } else {
      LOG.error("Failed to delete tenant '{}'", tenantId, exception);
      omMetrics.incNumTenantDeleteFails();
    }
    return omClientResponse;
  }
}
