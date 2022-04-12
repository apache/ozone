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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantRevokeAdminResponse;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantRevokeAdminRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantRevokeAdminResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;
import static org.apache.hadoop.ozone.om.request.s3.tenant.OMTenantRequestHelper.checkTenantAdmin;
import static org.apache.hadoop.ozone.om.request.s3.tenant.OMTenantRequestHelper.checkTenantExistence;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.MULTITENANCY_SCHEMA;

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
  @DisallowedUntilLayoutVersion(MULTITENANCY_SCHEMA)
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final TenantRevokeAdminRequest request =
        getOmRequest().getTenantRevokeAdminRequest();

    final String accessId = request.getAccessId();
    String tenantId = request.getTenantId();

    // If tenantId is not specified, infer it from the accessId
    if (StringUtils.isEmpty(tenantId)) {
      tenantId = OMTenantRequestHelper.getTenantIdFromAccessId(
          ozoneManager.getMetadataManager(), accessId);
      assert (tenantId != null);
    }

    // Sanity check
    checkTenantExistence(ozoneManager.getMetadataManager(), tenantId);

    // Caller should be an Ozone admin or this tenant's delegated admin
    checkTenantAdmin(ozoneManager, tenantId);

    OmDBAccessIdInfo accessIdInfo = ozoneManager.getMetadataManager()
        .getTenantAccessIdTable().get(accessId);

    if (accessIdInfo == null) {
      throw new OMException("accessId '" + accessId + "' not found.",
          OMException.ResultCodes.ACCESS_ID_NOT_FOUND);
    }

    // Check if accessId is assigned to the tenant
    if (!accessIdInfo.getTenantId().equals(tenantId)) {
      throw new OMException("accessId '" + accessId +
          "' must be assigned to tenant '" + tenantId + "' first.",
          OMException.ResultCodes.INVALID_TENANT_ID);
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
                        .setTenantId(tenantId)
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

    final OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumTenantRevokeAdmins();

    OMClientResponse omClientResponse = null;
    final OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());

    final Map<String, String> auditMap = new HashMap<>();
    final OMMetadataManager omMetadataManager =
        ozoneManager.getMetadataManager();

    final TenantRevokeAdminRequest request =
        getOmRequest().getTenantRevokeAdminRequest();
    final String accessId = request.getAccessId();
    final String tenantId = request.getTenantId();

    boolean acquiredVolumeLock = false;
    IOException exception = null;

    String volumeName = null;

    try {
      volumeName = OMTenantRequestHelper.getTenantVolumeName(
          omMetadataManager, tenantId);

      acquiredVolumeLock = omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, volumeName);

      final OmDBAccessIdInfo dbAccessIdInfo =
          omMetadataManager.getTenantAccessIdTable().get(accessId);

      if (dbAccessIdInfo == null) {
        throw new OMException("OmDBAccessIdInfo entry is missing for accessId '"
            + accessId + "'", OMException.ResultCodes.METADATA_ERROR);
      }

      assert (dbAccessIdInfo.getTenantId().equals(tenantId));

      // Update tenantAccessIdTable
      final OmDBAccessIdInfo newOmDBAccessIdInfo =
          new OmDBAccessIdInfo.Builder()
              .setTenantId(dbAccessIdInfo.getTenantId())
              .setUserPrincipal(dbAccessIdInfo.getUserPrincipal())
              .setIsAdmin(false)
              .setIsDelegatedAdmin(false)
              .build();
      omMetadataManager.getTenantAccessIdTable().addCacheEntry(
          new CacheKey<>(accessId),
          new CacheValue<>(Optional.of(newOmDBAccessIdInfo),
              transactionLogIndex));

      omResponse.setTenantRevokeAdminResponse(
          TenantRevokeAdminResponse.newBuilder()
              .build());
      omClientResponse = new OMTenantRevokeAdminResponse(omResponse.build(),
          accessId, newOmDBAccessIdInfo);

    } catch (IOException ex) {
      exception = ex;
      // Prepare omClientResponse
      omClientResponse = new OMTenantRevokeAdminResponse(
          createErrorOMResponse(omResponse, ex));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(ozoneManagerDoubleBufferHelper
            .add(omClientResponse, transactionLogIndex));
      }
      if (acquiredVolumeLock) {
        Preconditions.checkNotNull(volumeName);
        omMetadataManager.getLock().releaseWriteLock(VOLUME_LOCK, volumeName);
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
      omMetrics.incNumTenantRevokeAdminSuccesses();
    } else {
      LOG.error("Failed to revoke admin of accessId '{}' from tenant '{}': {}",
          accessId, tenantId, exception.getMessage());
      omMetrics.incNumTenantRevokeAdminFails();
    }
    return omClientResponse;
  }
}
