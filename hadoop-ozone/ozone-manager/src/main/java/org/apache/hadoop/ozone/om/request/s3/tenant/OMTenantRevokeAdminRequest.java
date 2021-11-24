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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantRevokeAdminResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantRevokeAdminRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantRevokeAdminResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.TENANT_LOCK;

/*
  Execution flow

  - preExecute
    - Check caller admin privilege
  - validateAndUpdateCache
    - Update tenantAccessIdTable
 */

/**
 * Handles OMTenantRevokeAdminRequest.
 */
public class OMTenantRevokeAdminRequest extends OMClientRequest {
  public static final Logger LOG =
      LoggerFactory.getLogger(OMTenantRevokeAdminRequest.class);

  public OMTenantRevokeAdminRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final TenantRevokeAdminRequest request =
        getOmRequest().getTenantRevokeAdminRequest();

    final String accessId = request.getAccessId();
    String tenantId = request.getTenantName();

    // If tenant name is not specified, try figuring it out from accessId.
    if (StringUtils.isEmpty(tenantId)) {
      tenantId = OMTenantRequestHelper.getTenantNameFromAccessId(
          ozoneManager.getMetadataManager(), accessId);
    }

    // Caller should be an Ozone admin or this tenant's delegated admin
    OMTenantRequestHelper.checkTenantAdmin(ozoneManager, tenantId);

    // TODO: Check tenant existence?

    OmDBAccessIdInfo accessIdInfo = ozoneManager.getMetadataManager()
        .getTenantAccessIdTable().get(accessId);

    if (accessIdInfo == null) {
      throw new OMException("accessId '" + accessId + "' not found.",
          OMException.ResultCodes.ACCESSID_NOT_FOUND);
    }

    // Check if accessId is assigned to the tenant
    if (!accessIdInfo.getTenantId().equals(tenantId)) {
      throw new OMException("accessId '" + accessId +
          "' must be assigned to tenant '" + tenantId + "' first.",
          OMException.ResultCodes.INVALID_TENANT_NAME);
    }

    // TODO: Call OMMTM to remove user from admin group of the tenant.
    // The call should remove user (not accessId) from the tenant's admin group
//    ozoneManager.getMultiTenantManager().revokeTenantAdmin();

    final OMRequest.Builder omRequestBuilder = getOmRequest().toBuilder()
        .setUserInfo(getUserInfo())
        .setTenantRevokeAdminRequest(
                // Regenerate request just in case tenantId is not provided
                //  by the client
                TenantRevokeAdminRequest.newBuilder()
                        .setTenantName(tenantId)
                        .setAccessId(request.getAccessId())
                        .build())
        .setCmdType(getOmRequest().getCmdType())
        .setClientId(getOmRequest().getClientId());

    if (getOmRequest().hasTraceID()) {
      omRequestBuilder.setTraceID(getOmRequest().getTraceID());
    }

    return omRequestBuilder.build();
  }

  @Override
  @SuppressWarnings("checkstyle:methodlength")
  public OMClientResponse validateAndUpdateCache(
      OzoneManager ozoneManager, long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    OMClientResponse omClientResponse = null;
    final OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());

    final Map<String, String> auditMap = new HashMap<>();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    final TenantRevokeAdminRequest request =
        getOmRequest().getTenantRevokeAdminRequest();
    final String accessId = request.getAccessId();
    final String tenantId = request.getTenantName();

    boolean acquiredTenantLock = false;  // TODO: use tenant lock instead, maybe
    IOException exception = null;

    final String volumeName = OMTenantRequestHelper.getTenantVolumeName(
        omMetadataManager, tenantId);

    try {
      acquiredTenantLock = omMetadataManager.getLock().acquireWriteLock(
          TENANT_LOCK, tenantId);

      final OmDBAccessIdInfo oldAccessIdInfo =
          omMetadataManager.getTenantAccessIdTable().get(accessId);

      if (oldAccessIdInfo == null) {
        throw new OMException("OmDBAccessIdInfo entry is missing for accessId '"
            + accessId + "'.", OMException.ResultCodes.METADATA_ERROR);
      }

      assert(oldAccessIdInfo.getTenantId().equals(tenantId));

      // Update tenantAccessIdTable
      final OmDBAccessIdInfo newOmDBAccessIdInfo =
          new OmDBAccessIdInfo.Builder()
          .setTenantId(oldAccessIdInfo.getTenantId())
          .setKerberosPrincipal(oldAccessIdInfo.getUserPrincipal())
          .setSharedSecret(oldAccessIdInfo.getSecretKey())
          .setIsAdmin(false)
          .setIsDelegatedAdmin(false)
          .build();
      omMetadataManager.getTenantAccessIdTable().addCacheEntry(
          new CacheKey<>(accessId),
          new CacheValue<>(Optional.of(newOmDBAccessIdInfo),
              transactionLogIndex));

      // Update tenantRoleTable?
//      final String roleName = "role_admin";
//      omMetadataManager.getTenantRoleTable().addCacheEntry(
//          new CacheKey<>(accessId),
//          new CacheValue<>(Optional.of(roleName), transactionLogIndex));

      omResponse.setTenantRevokeAdminResponse(
          TenantRevokeAdminResponse.newBuilder()
              .setSuccess(true).build());
      omClientResponse = new OMTenantRevokeAdminResponse(omResponse.build(),
          accessId, newOmDBAccessIdInfo);

    } catch (IOException ex) {
      // Error handling: do nothing to Authorizer (Ranger) here?

      exception = ex;
      // Set success flag to false
      omResponse.setTenantRevokeAdminResponse(
          TenantRevokeAdminResponse.newBuilder()
              .setSuccess(false).build());
      omClientResponse = new OMTenantRevokeAdminResponse(
          createErrorOMResponse(omResponse, ex));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(ozoneManagerDoubleBufferHelper
            .add(omClientResponse, transactionLogIndex));
      }
      if (acquiredTenantLock) {
        omMetadataManager.getLock().releaseWriteLock(TENANT_LOCK, tenantId);
      }
    }

    // Audit
    auditMap.put(OzoneConsts.TENANT, tenantId);
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.TENANT_REVOKE_ADMIN, auditMap, exception,
        getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.info("Revoked admin of accessId '{}' from tenant '{}'",
          accessId, tenantId);
      // TODO: omMetrics.incNumTenantRevokeAdmin()
    } else {
      LOG.error("Failed to revoke admin of accessId '{}' from tenant '{}': {}",
          accessId, tenantId, exception.getMessage());
      // TODO: omMetrics.incNumTenantRevokeAdminFails()
    }
    return omClientResponse;
  }
}
