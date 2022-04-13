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
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantAssignAdminResponse;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantAssignAdminRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantAssignAdminResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.MULTITENANCY_SCHEMA;

/*
  Execution flow

  - preExecute
    - Check caller admin privilege
  - validateAndUpdateCache
    - Update tenantAccessIdTable
 */

/**
 * Handles OMTenantAssignAdminRequest.
 */
public class OMTenantAssignAdminRequest extends OMClientRequest {
  public static final Logger LOG =
      LoggerFactory.getLogger(OMTenantAssignAdminRequest.class);

  public OMTenantAssignAdminRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  @DisallowedUntilLayoutVersion(MULTITENANCY_SCHEMA)
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final TenantAssignAdminRequest request =
        getOmRequest().getTenantAssignAdminRequest();

    final String accessId = request.getAccessId();
    String tenantId = request.getTenantId();
    final OMMultiTenantManager multiTenantManager =
        ozoneManager.getMultiTenantManager();

    // If tenantId (tenant name) is not provided, infer it from the accessId
    if (StringUtils.isEmpty(tenantId)) {
      Optional<String> optionalTenantId =
          multiTenantManager.getTenantForAccessID(accessId);
      if (!optionalTenantId.isPresent()) {
        throw new OMException("OmDBAccessIdInfo is missing for accessId '" +
            accessId + "' in DB.", OMException.ResultCodes.METADATA_ERROR);
      }
      tenantId = optionalTenantId.get();
      assert (!StringUtils.isEmpty(tenantId));
    }

    multiTenantManager.checkTenantExistence(tenantId);

    // Caller should be an Ozone admin or this tenant's delegated admin
    multiTenantManager.checkTenantAdmin(tenantId, true);

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

    final boolean delegated;
    if (request.hasDelegated()) {
      delegated = request.getDelegated();
    } else {
      delegated = true;
    }
    // Call OMMTM to add user to tenant admin role
    ozoneManager.getMultiTenantManager().assignTenantAdmin(
        request.getAccessId(), delegated);

    final OMRequest.Builder omRequestBuilder = getOmRequest().toBuilder()
        .setUserInfo(getUserInfo())
        .setTenantAssignAdminRequest(
            TenantAssignAdminRequest.newBuilder()
                .setAccessId(accessId)
                .setTenantId(tenantId)
                .setDelegated(delegated)
                .build())
        .setCmdType(getOmRequest().getCmdType())
        .setClientId(getOmRequest().getClientId());

    if (getOmRequest().hasTraceID()) {
      omRequestBuilder.setTraceID(getOmRequest().getTraceID());
    }

    return omRequestBuilder.build();
  }

  @Override
  public void handleRequestFailure(OzoneManager ozoneManager) {
    final TenantAssignAdminRequest request =
        getOmRequest().getTenantAssignAdminRequest();

    try {
      ozoneManager.getMultiTenantManager().revokeTenantAdmin(
          request.getAccessId());
    } catch (Exception e) {
      // TODO: Ignore for now. See OMTenantCreateRequest#handleRequestFailure
    }
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
    final OMMetadataManager omMetadataManager =
        ozoneManager.getMetadataManager();

    final TenantAssignAdminRequest request =
        getOmRequest().getTenantAssignAdminRequest();
    final String accessId = request.getAccessId();
    final String tenantId = request.getTenantId();
    final boolean delegated = request.getDelegated();

    boolean acquiredVolumeLock = false;
    IOException exception = null;

    String volumeName = null;

    try {
      volumeName = ozoneManager.getMultiTenantManager()
          .getTenantVolumeName(tenantId);

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
              .setIsAdmin(true)
              .setIsDelegatedAdmin(delegated)
              .build();
      omMetadataManager.getTenantAccessIdTable().addCacheEntry(
          new CacheKey<>(accessId),
          new CacheValue<>(Optional.of(newOmDBAccessIdInfo),
              transactionLogIndex));

      omResponse.setTenantAssignAdminResponse(
          TenantAssignAdminResponse.newBuilder()
              .build());
      omClientResponse = new OMTenantAssignAdminResponse(omResponse.build(),
          accessId, newOmDBAccessIdInfo);

    } catch (IOException ex) {
      // Error handling
      handleRequestFailure(ozoneManager);
      exception = ex;
      // Prepare omClientResponse
      omClientResponse = new OMTenantAssignAdminResponse(
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
        OMAction.TENANT_ASSIGN_ADMIN, auditMap, exception,
        getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.info("Assigned admin to accessId '{}' in tenant '{}', "
              + "delegated: {}", accessId, tenantId, delegated);
      // TODO: HDDS-6375: omMetrics.incNumTenantAssignAdmin()
    } else {
      LOG.error("Failed to assign admin to accessId '{}' in tenant '{}', "
              + "delegated: {}: {}",
          accessId, tenantId, delegated, exception.getMessage());
      // TODO: HDDS-6375: omMetrics.incNumTenantAssignAdminFails()
    }
    return omClientResponse;
  }
}
