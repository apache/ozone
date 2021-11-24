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
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBKerberosPrincipalInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantAssignUserAccessIdResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Secret;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantAssignUserAccessIdRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantAssignUserAccessIdResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateGetS3SecretRequest;
import org.apache.http.auth.BasicUserPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import static org.apache.hadoop.ozone.om.helpers.OmDBKerberosPrincipalInfo.SERIALIZATION_SPLIT_KEY;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.S3_SECRET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;
import static org.apache.hadoop.ozone.om.request.s3.tenant.OMTenantRequestHelper.checkTenantAdmin;
import static org.apache.hadoop.ozone.om.request.s3.tenant.OMTenantRequestHelper.checkTenantExistence;

/*
  Ratis execution flow for OMAssignUserToTenant request:
  (might be a bit outdated)

- Client (AssignUserToTenantHandler, etc.)
  - Check admin privilege
  - Check username validity: ensure no invalid characters
  - Send request to server
- OMAssignUserToTenantRequest
  - preExecute (perform checks and init)
    - Check username validity (again), check $
      - If username is invalid, throw exception to client; else continue
    - Generate S3 secret for the new user
  - validateAndUpdateCache (update DB)
    - Permission check (checkACL need to check access key now)
    - Grab VOLUME_LOCK write lock
    - Check tenant existence
      - If tenant doesn't exist, throw exception to client; else continue
    - Check accessId existence
      - If accessId exists, throw exception to client; else continue
    - Grab S3_SECRET_LOCK write lock
    - S3SecretTable: Flush generated S3 secret
      - Key: TENANTNAME$USERNAME (equivalent to kerberosID)
      - Value: <GENERATED_SECRET>
    - Release S3_SECRET_LOCK write lock
    - New entry in tenantAccessIdTable:
      - Key: New accessId for the user in this tenant.
             Example of accessId: finance$bob@EXAMPLE.COM
      - Value: OmDBAccessIdInfo. Has tenantId, kerberosPrincipal, sharedSecret.
    - New entry or update existing entry in principalToAccessIdsTable:
      - Key: Kerberos principal of the user.
      - Value: OmDBKerberosPrincipalInfo. Has accessIds.
    - tenantGroupTable: Add this new user to the default tenant group.
      - Key: finance$bob
      - Value: finance-users
    - tenantRoleTable: TBD. No-Op for now.
    - Release VOLUME_LOCK write lock
 */

/**
 * Handles OMAssignUserToTenantRequest.
 *
 * TODO: Rename this to OMTenantAssignUserAccessIdRequest after rebase.
 */
public class OMAssignUserToTenantRequest extends OMClientRequest {
  public static final Logger LOG =
      LoggerFactory.getLogger(OMAssignUserToTenantRequest.class);

  public OMAssignUserToTenantRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final TenantAssignUserAccessIdRequest request =
        getOmRequest().getTenantAssignUserAccessIdRequest();

    final String tenantName = request.getTenantName();

    // Caller should be an Ozone admin or tenant delegated admin
    checkTenantAdmin(ozoneManager, tenantName);

    // Note: Tenant username _is_ the user principal (short name)
    final String tenantUsername = request.getTenantUsername();
    final String accessId = request.getAccessId();

    // Check tenantUsername (user principal) validity.
    // TODO: Rename tenantUsername to userPrincipal,
    //  INVALID_TENANT_USER_NAME to INVALID_TENANT_USER_PRINCIPAL, ...
    if (tenantUsername.contains(OzoneConsts.TENANT_NAME_USER_NAME_DELIMITER)) {
      throw new OMException("Invalid tenant username '" + tenantUsername +
          "'. Tenant username shouldn't contain delimiter.",
          OMException.ResultCodes.INVALID_TENANT_USER_NAME);
    }

    // Check tenant name validity.
    if (tenantName.contains(OzoneConsts.TENANT_NAME_USER_NAME_DELIMITER)) {
      throw new OMException("Invalid tenant name '" + tenantUsername +
          "'. Tenant name shouldn't contain delimiter.",
          OMException.ResultCodes.INVALID_TENANT_NAME);
    }

    // Check accessId validity.
    if (accessId.contains(SERIALIZATION_SPLIT_KEY)) {
      throw new OMException("Invalid accessId '" + accessId +
          "'. accessId should not contain '" + SERIALIZATION_SPLIT_KEY + "'",
          OMException.ResultCodes.INVALID_ACCESSID);
    }

    checkTenantExistence(ozoneManager.getMetadataManager(), tenantName);

    // Below call implies user existence check in authorizer.
    // If the user doesn't exist, Ranger return 400 and the call should throw.

    // Call OMMTM
    // Inform MultiTenantManager of user assignment so it could
    //  initialize some policies in Ranger.
    final String roleId = ozoneManager.getMultiTenantManager()
        .assignUserToTenant(new BasicUserPrincipal(tenantUsername), tenantName,
            accessId);
    if (LOG.isDebugEnabled()) {
      LOG.debug("roleId that the user is assigned to: {}", roleId);
    }

    // Generate secret. Used only when doesn't the kerberosID entry doesn't
    //  exist in DB, discarded otherwise.
    final String s3Secret = DigestUtils.sha256Hex(OmUtils.getSHADigest());

    final UpdateGetS3SecretRequest updateGetS3SecretRequest =
        UpdateGetS3SecretRequest.newBuilder()
            .setAwsSecret(s3Secret)
            .setKerberosID(accessId).build();

    final OMRequest.Builder omRequestBuilder = getOmRequest().toBuilder()
        .setUserInfo(getUserInfo())
        .setUpdateGetS3SecretRequest(updateGetS3SecretRequest)
        .setCmdType(getOmRequest().getCmdType())
        .setClientId(getOmRequest().getClientId());

    if (getOmRequest().hasTraceID()) {
      omRequestBuilder.setTraceID(getOmRequest().getTraceID());
    }

    return omRequestBuilder.build();
  }

  @Override
  public void handleRequestFailure(OzoneManager ozoneManager) {
    final TenantAssignUserAccessIdRequest request =
        getOmRequest().getTenantAssignUserAccessIdRequest();
    final String userPrincipal = request.getTenantUsername();
    final String tenantName = request.getTenantName();
    final String accessId = request.getAccessId();

    try {
      // Undo Authorizer states established in preExecute
      ozoneManager.getMultiTenantManager().revokeUserAccessId(
          request.getAccessId());
    } catch (Exception e) {
      // TODO: Ignore for now. See OMTenantCreateRequest#handleRequestFailure
      // TODO: Temporary solution for remnant tenantCache entry. Might becomes
      //  useless with Ranger thread impl. Can remove.
      ozoneManager.getMultiTenantManager().removeUserAccessIdFromCache(
          accessId, userPrincipal, tenantName);
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

    final UpdateGetS3SecretRequest updateGetS3SecretRequest =
        getOmRequest().getUpdateGetS3SecretRequest();
    final String accessId = updateGetS3SecretRequest.getKerberosID();
    final String awsSecret = updateGetS3SecretRequest.getAwsSecret();

    boolean acquiredVolumeLock = false;
    boolean acquiredS3SecretLock = false;
    Map<String, String> auditMap = new HashMap<>();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    final TenantAssignUserAccessIdRequest request =
        getOmRequest().getTenantAssignUserAccessIdRequest();
    final String tenantName = request.getTenantName();
    final String principal = request.getTenantUsername();

    assert(accessId.equals(request.getAccessId()));
    IOException exception = null;

    final String volumeName = OMTenantRequestHelper.getTenantVolumeName(
        omMetadataManager, tenantName);

    try {
      acquiredVolumeLock = omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, volumeName);

      // Expect tenant existence in tenantStateTable
      if (!omMetadataManager.getTenantStateTable().isExist(tenantName)) {
        LOG.error("tenant {} doesn't exist", tenantName);
        throw new OMException("tenant '" + tenantName + "' doesn't exist",
            OMException.ResultCodes.TENANT_NOT_FOUND);
      }

      // Expect accessId absence from tenantAccessIdTable
      if (omMetadataManager.getTenantAccessIdTable().isExist(accessId)) {
        LOG.error("accessId {} already exists", accessId);
        throw new OMException("accessId '" + accessId + "' already exists!",
            OMException.ResultCodes.TENANT_USER_ACCESSID_ALREADY_EXISTS);
      }

      OmDBKerberosPrincipalInfo principalInfo = omMetadataManager
          .getPrincipalToAccessIdsTable().getIfExist(principal);
      // Reject if the user is already assigned to the tenant
      if (principalInfo != null) {
        // If any existing accessIds are assigned to the same tenant, throw ex
        // TODO: There is room for perf improvement. add a map in OMMTM.
        for (final String existingAccId : principalInfo.getAccessIds()) {
          final OmDBAccessIdInfo accessIdInfo =
              omMetadataManager.getTenantAccessIdTable().get(existingAccId);
          if (accessIdInfo == null) {
            LOG.error("Metadata error: accessIdInfo is null for accessId '{}'. "
                + "Ignoring.", existingAccId);
            throw new NullPointerException("accessIdInfo is null");
          }
          if (tenantName.equals(accessIdInfo.getTenantId())) {
            throw new OMException("The same user is not allowed to be assigned "
                + "to the same tenant more than once. User '" + principal
                + "' is already assigned to tenant '" + tenantName + "' with "
                + "accessId '" + existingAccId + "'.",
                OMException.ResultCodes.TENANT_USER_ACCESSID_ALREADY_EXISTS);
          }
        }
      }

      // Add to S3SecretTable. TODO: Remove later.
      acquiredS3SecretLock = omMetadataManager.getLock()
          .acquireWriteLock(S3_SECRET_LOCK, accessId);

      // Expect accessId absence from S3SecretTable
      // TODO: This table might be merged with tenantAccessIdTable later.
      if (omMetadataManager.getS3SecretTable().isExist(accessId)) {
        LOG.error("accessId '{}' already exists in S3SecretTable", accessId);
        throw new OMException("accessId '" + accessId +
            "' already exists in S3SecretTable",
            OMException.ResultCodes.INVALID_REQUEST);
      }

      final S3SecretValue s3SecretValue =
          new S3SecretValue(accessId, awsSecret);
      // Add S3SecretTable cache entry
      omMetadataManager.getS3SecretTable().addCacheEntry(
          new CacheKey<>(accessId),
          new CacheValue<>(Optional.of(s3SecretValue), transactionLogIndex));

      omMetadataManager.getLock().releaseWriteLock(S3_SECRET_LOCK, accessId);
      acquiredS3SecretLock = false;

      // Add to tenantAccessIdTable
      final OmDBAccessIdInfo omDBAccessIdInfo = new OmDBAccessIdInfo.Builder()
          .setTenantId(tenantName)
          .setKerberosPrincipal(principal)
          .setSharedSecret(s3SecretValue.getAwsSecret())
          .setIsAdmin(false)
          .setIsDelegatedAdmin(false)
          .build();
      omMetadataManager.getTenantAccessIdTable().addCacheEntry(
          new CacheKey<>(accessId),
          new CacheValue<>(Optional.of(omDBAccessIdInfo), transactionLogIndex));

      // Add to principalToAccessIdsTable
      if (principalInfo == null) {
        principalInfo = new OmDBKerberosPrincipalInfo.Builder()
            .setAccessIds(new TreeSet<>(Collections.singleton(accessId)))
            .build();
      } else {
        principalInfo.addAccessId(accessId);
      }
      omMetadataManager.getPrincipalToAccessIdsTable().addCacheEntry(
          new CacheKey<>(principal),
          new CacheValue<>(Optional.of(principalInfo),
              transactionLogIndex));

      // Add to tenantGroupTable
      // TODO: DOUBLE CHECK GROUP NAME USAGE
      final String defaultGroupName =
          tenantName + OzoneConsts.DEFAULT_TENANT_USER_GROUP_SUFFIX;
      omMetadataManager.getTenantGroupTable().addCacheEntry(
          new CacheKey<>(accessId),
          new CacheValue<>(Optional.of(defaultGroupName), transactionLogIndex));

      // Add to tenantRoleTable
      // TODO: DOUBLE CHECK ROLENAME
      final String roleName = "user";
      omMetadataManager.getTenantRoleTable().addCacheEntry(
          new CacheKey<>(accessId),
          new CacheValue<>(Optional.of(roleName), transactionLogIndex));

      // Generate response
      omResponse.setTenantAssignUserAccessIdResponse(
          TenantAssignUserAccessIdResponse.newBuilder().setSuccess(true)
              .setS3Secret(S3Secret.newBuilder()
                  .setAwsSecret(awsSecret).setKerberosID(accessId))
              .build());
      omClientResponse = new OMTenantAssignUserAccessIdResponse(
          omResponse.build(), s3SecretValue, principal, defaultGroupName,
          roleName, accessId, omDBAccessIdInfo, principalInfo);
    } catch (IOException ex) {
      handleRequestFailure(ozoneManager);
      exception = ex;
      // Set response success flag to false
      omResponse.setTenantAssignUserAccessIdResponse(
          TenantAssignUserAccessIdResponse.newBuilder()
              .setSuccess(false).build());
      omClientResponse = new OMTenantAssignUserAccessIdResponse(
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
    auditMap.put("user", principal);
    auditMap.put("accessId", accessId);
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.TENANT_ASSIGN_USER_ACCESSID, auditMap, exception,
            getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.info("Assigned user '{}' to tenant '{}' with accessId '{}'",
          principal, tenantName, accessId);
      // TODO: omMetrics.incNumTenantAssignUser()
    } else {
      LOG.error("Failed to assign '{}' to tenant '{}' with accessId '{}': {}",
          principal, tenantName, accessId, exception.getMessage());
      // TODO: Check if the exception message is sufficient.
      // TODO: omMetrics.incNumTenantAssignUserFails()
    }
    return omClientResponse;
  }
}
