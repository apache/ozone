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
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteTenantRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.METADATA_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TENANT_NOT_EMPTY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TENANT_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_IS_REFERENCED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_EMPTY;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.TENANT_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.USER_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

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
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    // Check Ozone cluster admin privilege
    OMTenantRequestHelper.checkAdmin(ozoneManager);

    // TODO: TBD: Call ozoneManager.getMultiTenantManager().deleteTenant() ?

    return getOmRequest();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(
      OzoneManager ozoneManager, long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    OMClientResponse omClientResponse = null;
    final OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    boolean acquiredVolumeLock = false;
    boolean acquiredUserLock = false;
    boolean acquiredTenantLock = false;
    final Map<String, String> auditMap = new HashMap<>();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    final DeleteTenantRequest request = getOmRequest().getDeleteTenantRequest();
    final String tenantId = request.getTenantId();
    String volumeName = null;
    boolean deleteVolume = true;

    IOException exception = null;
    OmVolumeArgs omVolumeArgs;
    String volumeOwner = null;
    // deleteVolume is true if volumeName is not empty string
    OzoneManagerStorageProtos.PersistedUserVolumeInfo newVolumeList = null;

    try {
      // Acquire the tenant lock
      acquiredTenantLock = omMetadataManager.getLock().acquireWriteLock(
          TENANT_LOCK, tenantId);

      final OMMultiTenantManager tenantManager =
          ozoneManager.getMultiTenantManager();

      // Check tenant existence in tenantStateTable
      if (!omMetadataManager.getTenantStateTable().isExist(tenantId)) {
        LOG.debug("tenant: {} does not exist", tenantId);
        throw new OMException("Tenant '" + tenantId + "' does not exist",
            TENANT_NOT_FOUND);
      }

      final OmDBTenantInfo dbTenantInfo =
          omMetadataManager.getTenantStateTable().get(tenantId);
      volumeName = dbTenantInfo.getBucketNamespaceName();

      LOG.debug("Tenant {} has volume: {}", tenantId, volumeName);
      // TODO: deleteVolume flag might be used to impl skipVolumeDelete later
      deleteVolume = volumeName.length() > 0;

      // Check if there are any accessIds in the tenant
      if (!OMTenantRequestHelper.isTenantEmpty(tenantManager, tenantId)) {
        LOG.warn("tenant: '{}' is not empty. Unable to delete the tenant",
            tenantId);
        throw new OMException("Tenant '" + tenantId + "' is not empty. " +
            "All accessIds associated to this tenant must be revoked before " +
            "the tenant can be deleted.", TENANT_NOT_EMPTY);
      }

      // Invalidate cache entries for tenant
      omMetadataManager.getTenantStateTable().addCacheEntry(
          new CacheKey<>(tenantId),
          new CacheValue<>(Optional.absent(), transactionLogIndex));
      // TODO: Can be specified in the request args
      final String userPolicyGroupName =
          tenantId + OzoneConsts.DEFAULT_TENANT_USER_POLICY_SUFFIX;
      // TODO: Can be specified in the request args
      final String bucketPolicyGroupName =
          tenantId + OzoneConsts.DEFAULT_TENANT_BUCKET_POLICY_SUFFIX;
      omMetadataManager.getTenantPolicyTable().addCacheEntry(
          new CacheKey<>(userPolicyGroupName),
          new CacheValue<>(Optional.absent(), transactionLogIndex));
      omMetadataManager.getTenantPolicyTable().addCacheEntry(
          new CacheKey<>(bucketPolicyGroupName),
          new CacheValue<>(Optional.absent(), transactionLogIndex));

      // Release the tenant lock
      omMetadataManager.getLock().releaseWriteLock(TENANT_LOCK, tenantId);
      acquiredTenantLock = false;

      // Delete volume if the field is not empty
      if (deleteVolume) {
        // Check Acl
        if (ozoneManager.getAclsEnabled()) {
          checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
              OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.DELETE,
              volumeName, null, null);
        }

        // Acquire the volume lock
        Preconditions.checkNotNull(volumeName,
            "Volume name should have been acquired from OmDBTenantInfo. " +
                "It should be an empty string at least.");
        acquiredVolumeLock = omMetadataManager.getLock().acquireWriteLock(
            VOLUME_LOCK, volumeName);

        omVolumeArgs = getVolumeInfo(omMetadataManager, volumeName);
        // Check volume ref count
        long volRefCount = omVolumeArgs.getRefCount();
        if (volRefCount < 1L) {
          LOG.warn("Volume '{}' has a less than 1 reference count of " +
              "'{}'", volumeName, volRefCount);
          throw new OMException("Volume '" + volumeName +
              "' reference count is " + volRefCount + ", expected 1.",
              METADATA_ERROR);
        }
        if (volRefCount > 1L) {
          LOG.debug("Volume '{}' has a greater than 1 reference count of " +
              "'{}'", volumeName, volRefCount);
          throw new OMException("Volume '" + volumeName +
              "' reference count is " + volRefCount + ", expected 1. " +
              "This volume is referenced by some other Ozone features. " +
              "Please disable such other features before trying to delete " +
              "the tenant again.", VOLUME_IS_REFERENCED);
        }

        volumeOwner = omVolumeArgs.getOwnerName();
        acquiredUserLock = omMetadataManager.getLock().acquireWriteLock(
            USER_LOCK, volumeOwner);

        // Check volume emptiness
        if (!omMetadataManager.isVolumeEmpty(volumeName)) {
          LOG.debug("volume: {} is not empty", volumeName);
          throw new OMException("Tenant volume '" + volumeName +
              "' is not empty." + " Volume must be emptied before the tenant " +
              "can be deleted.", VOLUME_NOT_EMPTY);
        }

        // Actual volume deletion, follows OMVolumeDeleteRequest
        newVolumeList =
            omMetadataManager.getUserTable().get(volumeOwner);
        newVolumeList = delVolumeFromOwnerList(newVolumeList, volumeName,
            volumeOwner, transactionLogIndex);
        final String dbUserKey = omMetadataManager.getUserKey(volumeOwner);
        omMetadataManager.getUserTable().addCacheEntry(
            new CacheKey<>(dbUserKey),
            new CacheValue<>(Optional.of(newVolumeList), transactionLogIndex));
        final String dbVolumeKey = omMetadataManager.getVolumeKey(volumeName);
        omMetadataManager.getVolumeTable().addCacheEntry(
            new CacheKey<>(dbVolumeKey),
            new CacheValue<>(Optional.absent(), transactionLogIndex));

        // Release the user lock
        omMetadataManager.getLock().releaseWriteLock(USER_LOCK, volumeOwner);
        acquiredUserLock = false;

        // Release the volume lock
        omMetadataManager.getLock().releaseWriteLock(VOLUME_LOCK, volumeName);
        acquiredVolumeLock = false;

        // TODO: Set response dbVolumeKey?
      }

      omClientResponse = new OMTenantDeleteResponse(
          omResponse.build(), volumeName, volumeOwner, newVolumeList,
          tenantId, userPolicyGroupName, bucketPolicyGroupName);

    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMTenantDeleteResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredTenantLock) {
        omMetadataManager.getLock().releaseWriteLock(TENANT_LOCK, tenantId);
      }
      if (acquiredUserLock) {
        omMetadataManager.getLock().releaseWriteLock(USER_LOCK, volumeOwner);
      }
      if (acquiredVolumeLock) {
        omMetadataManager.getLock().releaseWriteLock(VOLUME_LOCK, volumeName);
      }
    }

    // Perform audit logging
    auditMap.put(OzoneConsts.TENANT, tenantId);
    // Audit volume deletion
    if (deleteVolume) {
      auditLog(ozoneManager.getAuditLogger(),
          buildAuditMessage(OMAction.DELETE_VOLUME,
              buildVolumeAuditMap(volumeName),
              exception, getOmRequest().getUserInfo()));
    }
    // Audit tenant deletion
    auditLog(ozoneManager.getAuditLogger(),
        buildAuditMessage(OMAction.DELETE_TENANT,
            auditMap, exception, getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.info("Deleted tenant '{}' and volume '{}'", tenantId, volumeName);
      // TODO: omMetrics.decNumTenants()
    } else {
      LOG.error("Failed to delete tenant '{}'", tenantId, exception);
      // TODO: omMetrics.incNumTenantDeleteFails()
    }
    return omClientResponse;
  }
}
