/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om.request.s3.tenant;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TENANT_NOT_EMPTY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TENANT_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.MULTITENANCY_SCHEMA;

/**
 * Handles OMTenantDelete request.
 */
public class OMTenantDeleteRequest extends OMVolumeRequest {
  public static final Logger LOG =
      LoggerFactory.getLogger(OMTenantDeleteRequest.class);

  public OMTenantDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  @DisallowedUntilLayoutVersion(MULTITENANCY_SCHEMA)
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    // Check Ozone cluster admin privilege
    OMTenantRequestHelper.checkAdmin(ozoneManager);

    // TODO: TBD: Call ozoneManager.getMultiTenantManager().deleteTenant() ?

    return getOmRequest().toBuilder().setUserInfo(getUserInfo()).build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(
      OzoneManager ozoneManager, long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

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

    IOException exception = null;
    OmVolumeArgs omVolumeArgs = null;

    try {
      // Check tenant existence in tenantStateTable
      if (!omMetadataManager.getTenantStateTable().isExist(tenantId)) {
        LOG.debug("tenant: {} does not exist", tenantId);
        throw new OMException("Tenant '" + tenantId + "' does not exist",
            TENANT_NOT_FOUND);
      }

      // Reading the TenantStateTable without lock as we don't have or need
      // a TENANT_LOCK. The assumption is that OmDBTenantInfo is read-only
      // once it is set during tenant creation.
      final OmDBTenantInfo dbTenantInfo =
          omMetadataManager.getTenantStateTable().get(tenantId);
      volumeName = dbTenantInfo.getBucketNamespaceName();
      assert (volumeName != null);

      LOG.debug("Tenant '{}' has volume '{}'", tenantId, volumeName);
      // decVolumeRefCount is true if volumeName is not empty string
      decVolumeRefCount = volumeName.length() > 0;

      // Acquire the volume lock
      acquiredVolumeLock = omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, volumeName);

      // Check if there are any accessIds in the tenant
      if (!OMTenantRequestHelper.isTenantEmpty(omMetadataManager, tenantId)) {
        LOG.warn("tenant: '{}' is not empty. Unable to delete the tenant",
            tenantId);
        throw new OMException("Tenant '" + tenantId + "' is not empty. " +
            "All accessIds associated to this tenant must be revoked before " +
            "the tenant can be deleted. See `ozone tenant user revoke`",
            TENANT_NOT_EMPTY);
      }

      // Invalidate cache entry
      omMetadataManager.getTenantStateTable().addCacheEntry(
          new CacheKey<>(tenantId),
          new CacheValue<>(Optional.absent(), transactionLogIndex));

      // Decrement volume refCount
      if (decVolumeRefCount) {
        // Check Acl
        if (ozoneManager.getAclsEnabled()) {
          checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
              OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE_ACL,
              volumeName, null, null);
        }

        omVolumeArgs = getVolumeInfo(omMetadataManager, volumeName);
        // Decrement volume ref count
        omVolumeArgs.decRefCount();

        // Update omVolumeArgs
        final String dbVolumeKey = omMetadataManager.getVolumeKey(volumeName);
        omMetadataManager.getVolumeTable().addCacheEntry(
            new CacheKey<>(dbVolumeKey),
            new CacheValue<>(Optional.of(omVolumeArgs), transactionLogIndex));

        // TODO: Set response dbVolumeKey?
      }

      // Compose response

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

    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMTenantDeleteResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredVolumeLock) {
        Preconditions.checkNotNull(volumeName);
        omMetadataManager.getLock().releaseWriteLock(VOLUME_LOCK, volumeName);
      }
    }

    // Perform audit logging
    auditMap.put(OzoneConsts.TENANT, tenantId);
    // Audit volume ref count update
    if (decVolumeRefCount) {
      auditLog(ozoneManager.getAuditLogger(),
          buildAuditMessage(OMAction.UPDATE_VOLUME,
              buildVolumeAuditMap(volumeName),
              exception, getOmRequest().getUserInfo()));
    }
    // Audit tenant deletion
    auditLog(ozoneManager.getAuditLogger(),
        buildAuditMessage(OMAction.DELETE_TENANT,
            auditMap, exception, getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.info("Deleted tenant '{}' and volume '{}'", tenantId, volumeName);
      // TODO: HDDS-6375: omMetrics.decNumTenants()
    } else {
      LOG.error("Failed to delete tenant '{}'", tenantId, exception);
      // TODO: HDDS-6375: omMetrics.incNumTenantDeleteFails()
    }
    return omClientResponse;
  }
}
