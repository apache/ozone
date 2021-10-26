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
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBKerberosPrincipalInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantRevokeUserAccessIdResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantRevokeUserAccessIdRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantRevokeUserAccessIdResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.S3_SECRET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

/*
  Execution flow

  - preExecute
    - Check accessId existence
    - Get tenantName from accessId
    - Check caller Ozone admin or tenant admin privilege
    - Throw if accessId is a tenant admin
    - Call Authorizer
  - validateAndUpdateCache
    - Update DB tables
 */

/**
 * Handles OMTenantRevokeUserAccessIdRequest request.
 */
public class OMTenantRevokeUserAccessIdRequest extends OMClientRequest {
  public static final Logger LOG = LoggerFactory.getLogger(
      OMTenantRevokeUserAccessIdRequest.class);

  public OMTenantRevokeUserAccessIdRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final TenantRevokeUserAccessIdRequest request =
        getOmRequest().getTenantRevokeUserAccessIdRequest();

    final String accessId = request.getAccessId();

    // As of now, OMTenantRevokeUserAccessIdRequest does not get tenantName
    //  from the client, we just get it from the OM DB table. Uncomment
    //  below if we want the request to be similar to OMTenantRevokeAdminRequest
//    String tenantName = request.getTenantName();
//    if (tenantName == null) {
//    }

    final OMMetadataManager omMetadataManager =
        ozoneManager.getMetadataManager();
    final OmDBAccessIdInfo accessIdInfo = omMetadataManager
        .getTenantAccessIdTable().get(accessId);

    if (accessIdInfo == null) {
      // Note: This potentially leaks which accessIds exists in OM.
      throw new OMException("accessId '" + accessId + "' doesn't exist",
          OMException.ResultCodes.TENANT_USER_ACCESSID_NOT_FOUND);
    }

    final String tenantName = accessIdInfo.getTenantId();
    assert(tenantName != null);
    assert(tenantName.length() > 0);

    // Caller should be an Ozone admin or this tenant's delegated admin
    OMTenantRequestHelper.checkTenantAdmin(ozoneManager, tenantName);

    if (accessIdInfo.getIsAdmin()) {
      throw new OMException("accessId '" + accessId + "' is tenant admin of '" +
          tenantName + "'. Revoke admin first.",
          OMException.ResultCodes.PERMISSION_DENIED);
    }

    // Call OMMTM to revoke user access to tenant
    // TODO: DOUBLE CHECK destroyUser() behavior
    ozoneManager.getMultiTenantManager().revokeUserAccessId(accessId);

    final OMRequest.Builder omRequestBuilder = getOmRequest().toBuilder()
        .setUserInfo(getUserInfo())
        .setTenantRevokeUserAccessIdRequest(
            TenantRevokeUserAccessIdRequest.newBuilder()
                .setAccessId(accessId)
                .setTenantName(tenantName)
                .build())
        .setCmdType(getOmRequest().getCmdType())
        .setClientId(getOmRequest().getClientId());

    if (getOmRequest().hasTraceID()) {
      omRequestBuilder.setTraceID(getOmRequest().getTraceID());
    }

    return omRequestBuilder.build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(
      OzoneManager ozoneManager, long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    OMClientResponse omClientResponse = null;
    final OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());

    final Map<String, String> auditMap = new HashMap<>();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    final TenantRevokeUserAccessIdRequest request =
        getOmRequest().getTenantRevokeUserAccessIdRequest();
    final String accessId = request.getAccessId();
    final String tenantName = request.getTenantName();

    boolean acquiredVolumeLock = false;
    boolean acquiredS3SecretLock = false;
    IOException exception = null;

    final String volumeName = OMTenantRequestHelper.getTenantVolumeName(
        omMetadataManager, tenantName);
    String userPrincipal = null;

    try {
      acquiredVolumeLock =
          omMetadataManager.getLock().acquireWriteLock(VOLUME_LOCK, volumeName);

      // Remove from S3SecretTable. TODO: Remove later.
      acquiredS3SecretLock = omMetadataManager.getLock()
          .acquireWriteLock(S3_SECRET_LOCK, accessId);
      omMetadataManager.getS3SecretTable().addCacheEntry(
          new CacheKey<>(accessId),
          new CacheValue<>(Optional.absent(), transactionLogIndex));
      omMetadataManager.getLock().releaseWriteLock(S3_SECRET_LOCK,
          accessId);
      acquiredS3SecretLock = false;

      // Remove accessId from principalToAccessIdsTable
      OmDBAccessIdInfo omDBAccessIdInfo =
          omMetadataManager.getTenantAccessIdTable().get(accessId);
      assert(omDBAccessIdInfo != null);
      userPrincipal = omDBAccessIdInfo.getKerberosPrincipal();
      assert(userPrincipal != null);
      OmDBKerberosPrincipalInfo principalInfo = omMetadataManager
          .getPrincipalToAccessIdsTable().getIfExist(userPrincipal);
      assert(principalInfo != null);
      principalInfo.removeAccessId(accessId);
      omMetadataManager.getPrincipalToAccessIdsTable().addCacheEntry(
          new CacheKey<>(userPrincipal),
          new CacheValue<>(principalInfo.getAccessIds().size() > 0 ?
              // Invalidate (remove) the entry if accessIds set is empty
              Optional.of(principalInfo) : Optional.absent(),
              transactionLogIndex));

      // Remove from TenantAccessIdTable
      omMetadataManager.getTenantAccessIdTable().addCacheEntry(
          new CacheKey<>(accessId),
          new CacheValue<>(Optional.absent(), transactionLogIndex));

      // Remove from tenantGroupTable
      omMetadataManager.getTenantGroupTable().addCacheEntry(
          new CacheKey<>(accessId),
          new CacheValue<>(Optional.absent(), transactionLogIndex));

      // Remove from tenantRoleTable
      omMetadataManager.getTenantRoleTable().addCacheEntry(
          new CacheKey<>(accessId),
          new CacheValue<>(Optional.absent(), transactionLogIndex));

      // Generate response
      omResponse.setTenantRevokeUserAccessIdResponse(
          TenantRevokeUserAccessIdResponse.newBuilder().setSuccess(true).build()
      );
      omClientResponse = new OMTenantRevokeUserAccessIdResponse(
          omResponse.build(), accessId, userPrincipal, principalInfo);
    } catch (IOException ex) {
      // Error handling: do nothing to Authorizer here?
      exception = ex;
      // Set response success flag to false
      omResponse.setTenantRevokeUserAccessIdResponse(
          TenantRevokeUserAccessIdResponse.newBuilder()
              .setSuccess(false).build());
      omClientResponse = new OMTenantRevokeUserAccessIdResponse(
          createErrorOMResponse(omResponse, ex));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(ozoneManagerDoubleBufferHelper
            .add(omClientResponse, transactionLogIndex));
      }
      if (acquiredS3SecretLock) {
        omMetadataManager.getLock().releaseWriteLock(S3_SECRET_LOCK, accessId);
      }
      if (acquiredVolumeLock) {
        omMetadataManager.getLock().releaseWriteLock(VOLUME_LOCK, volumeName);
      }
    }

    // Audit
    auditMap.put(OzoneConsts.TENANT, tenantName);
    auditMap.put("accessId", accessId);
    auditMap.put("user", userPrincipal);
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.TENANT_REVOKE_USER_ACCESSID, auditMap, exception,
        getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.info("Revoked user '{}' accessId '{}' to tenant '{}'",
          userPrincipal, accessId, tenantName);
      // TODO: omMetrics.incNumTenantRevokeUser()
    } else {
      LOG.error("Failed to revoke user '{}' accessId '{}' to tenant '{}': {}",
          userPrincipal, accessId, tenantName, exception.getMessage());
      // TODO: omMetrics.incNumTenantRevokeUserFails()
    }
    return omClientResponse;
  }
}
