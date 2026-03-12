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

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.VOLUME_LOCK;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.MULTITENANCY_SCHEMA;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantAssignAdminResponse;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantAssignAdminRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantAssignAdminResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles OMTenantAssignAdminRequest.
 * Execution flow
 * - preExecute
 *   - Check caller admin privilege
 * - validateAndUpdateCache
 *   - Update tenantAccessIdTable
 */
public class OMTenantAssignAdminRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMTenantAssignAdminRequest.class);

  public OMTenantAssignAdminRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  @DisallowedUntilLayoutVersion(MULTITENANCY_SCHEMA)
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    final OMRequest omRequest = super.preExecute(ozoneManager);
    final TenantAssignAdminRequest request =
        omRequest.getTenantAssignAdminRequest();

    final String accessId = request.getAccessId();
    String tenantId = request.getTenantId();
    final OMMultiTenantManager multiTenantManager =
        ozoneManager.getMultiTenantManager();

    // If tenantId (tenant name) is not provided, infer it from the accessId
    if (StringUtils.isEmpty(tenantId)) {
      Optional<String> optionalTenantId =
          multiTenantManager.getTenantForAccessID(accessId);
      if (!optionalTenantId.isPresent()) {
        throw new OMException("accessId '" + accessId + "' is not assigned to "
            + "any tenant", OMException.ResultCodes.TENANT_NOT_FOUND);
      }
      tenantId = optionalTenantId.get();
      assert (!StringUtils.isEmpty(tenantId));
    }

    multiTenantManager.checkTenantExistence(tenantId);

    // Caller should be an Ozone admin, or a tenant delegated admin
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

    // Acquire write lock to authorizer (Ranger)
    multiTenantManager.getAuthorizerLock().tryWriteLockInOMRequest();
    try {
      // Add user to tenant admin role in Ranger.
      // User principal is inferred from the accessId given.
      // Throws if the user doesn't exist in Ranger.
      multiTenantManager.getAuthorizerOp()
          .assignTenantAdmin(accessId, delegated);
    } catch (Exception e) {
      multiTenantManager.getAuthorizerLock().unlockWriteInOMRequest();
      throw e;
    }

    final OMRequest.Builder omRequestBuilder = omRequest.toBuilder()
        .setTenantAssignAdminRequest(
            TenantAssignAdminRequest.newBuilder()
                .setAccessId(accessId)
                .setTenantId(tenantId)
                .setDelegated(delegated)
                .build());

    return omRequestBuilder.build();
  }

  @Override
  @SuppressWarnings("checkstyle:methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {

    final OMMultiTenantManager multiTenantManager =
        ozoneManager.getMultiTenantManager();

    final OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumTenantAssignAdmins();

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
    Exception exception = null;

    String volumeName = null;

    try {
      volumeName = ozoneManager.getMultiTenantManager()
          .getTenantVolumeName(tenantId);

      mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, volumeName));
      acquiredVolumeLock = getOmLockDetails().isLockAcquired();

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
          CacheValue.get(context.getIndex(), newOmDBAccessIdInfo));

      // Update tenant cache
      multiTenantManager.getCacheOp().assignTenantAdmin(accessId, delegated);

      omResponse.setTenantAssignAdminResponse(
          TenantAssignAdminResponse.newBuilder()
              .build());
      omClientResponse = new OMTenantAssignAdminResponse(omResponse.build(),
          accessId, newOmDBAccessIdInfo);

    } catch (IOException | InvalidPathException ex) {
      exception = ex;
      // Prepare omClientResponse
      omClientResponse = new OMTenantAssignAdminResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (acquiredVolumeLock) {
        Objects.requireNonNull(volumeName, "volumeName == null");
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

    // Audit
    auditMap.put(OzoneConsts.TENANT, tenantId);
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.TENANT_ASSIGN_ADMIN, auditMap, exception,
        getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.info("Assigned admin to accessId '{}' in tenant '{}', "
              + "delegated: {}", accessId, tenantId, delegated);
    } else {
      LOG.error("Failed to assign admin to accessId '{}' in tenant '{}', "
              + "delegated: {}: {}",
          accessId, tenantId, delegated, exception.getMessage());
      omMetrics.incNumTenantAssignAdminFails();
    }
    return omClientResponse;
  }
}
