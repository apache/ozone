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
import org.apache.hadoop.ozone.om.multitenant.OzoneMultiTenantPrincipal;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantCreateResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMAssignUserToTenantResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssignUserToTenantRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssignUserToTenantResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Secret;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateGetS3SecretRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.S3_SECRET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

/*
  Ratis execution flow for OMTenantUserCreate

- Client (AssignUserToTenantHandler, etc.)
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
      - Key: New accessId for the user in this tenant. e.g. finance$bob ?
      - Value: OmDBAccessIdInfo. Has tenantId, kerberosPrincipal, sharedSecret.
    - New entry or update existing entry in principalToAccessIdsTable:
      - Key: Kerberos principal of the user.
      - Value: OmDBKerberosPrincipalInfo. Has accessIds.
    - tenantGroupTable: Add this new user to the default tenant group.
      - Key: finance$bob
      - Value: finance-users
    - tenantRoleTable: TBD. No-op for prototype.
    - Release VOLUME_LOCK write lock
 */

/**
 * Handles OMAssignUserToTenantRequest.
 */
public class OMAssignUserToTenantRequest extends OMVolumeRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMAssignUserToTenantRequest.class);

  public OMAssignUserToTenantRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final AssignUserToTenantRequest request =
        getOmRequest().getAssignUserToTenantRequest();
    final String tenantUsername = request.getTenantUsername();
    final String tenantName = request.getTenantName();

    // Check tenantUsername validity
    if (tenantUsername.contains(OzoneConsts.TENANT_NAME_USER_NAME_DELIMITER)) {
      throw new OMException("Invalid tenant user name " + tenantUsername +
          ". Tenant user name should not contain delimiter.",
          OMException.ResultCodes.INVALID_TENANT_USER_NAME);
    }
    // Tenant and tenant user existence check won't be performed here

    // Generate S3 secret
    final String principal = tenantName +
        OzoneConsts.TENANT_NAME_USER_NAME_DELIMITER + tenantUsername;
    final String s3Secret = DigestUtils.sha256Hex(OmUtils.getSHADigest());

    final UpdateGetS3SecretRequest updateGetS3SecretRequest =
        UpdateGetS3SecretRequest.newBuilder()
            .setAwsSecret(s3Secret)
            .setKerberosID(principal).build();

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
  @SuppressWarnings("checkstyle:methodlength")
  public OMClientResponse validateAndUpdateCache(
      OzoneManager ozoneManager, long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    OMClientResponse omClientResponse = null;
    final OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    final UpdateGetS3SecretRequest updateGetS3SecretRequest =
        getOmRequest().getUpdateGetS3SecretRequest();
    final String principal = updateGetS3SecretRequest.getKerberosID();
    final String awsSecret = updateGetS3SecretRequest.getAwsSecret();
    boolean acquiredVolumeLock = false;
    boolean acquiredS3SecretLock = false;
    OzoneMultiTenantPrincipal tenantPrincipal = null;
    Map<String, String> auditMap = new HashMap<>();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    final AssignUserToTenantRequest request =
        getOmRequest().getAssignUserToTenantRequest();
    final String tenantName = request.getTenantName();
    final String tenantUsername = request.getTenantUsername();
    final String accessId = request.getAccessId();
    final String volumeName = tenantName;  // TODO: Configurable
    IOException exception = null;
    String userId = null;

    try {
      // Check ACL: requires ozone admin or tenant admin permission
//      if (ozoneManager.getAclsEnabled()) {
//        // TODO: Call OMMultiTenantManager?
//      }

      acquiredVolumeLock = omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, volumeName);

      // Check that tenant name should exist in tenantStateTable
      if (!omMetadataManager.getTenantStateTable().isExist(tenantName)) {
        LOG.debug("tenant: {} doesn't exist", tenantName);
        throw new OMException("tenant not found",
            OMException.ResultCodes.TENANT_NOT_FOUND);
      }

      // Check that accessId should NOT exist in tenantAccessIdTable
      if (omMetadataManager.getTenantAccessIdTable().isExist(accessId)) {
        LOG.debug("accessId: {} already exists", accessId);
        throw new OMException("accessId already exists!",
            OMException.ResultCodes.TENANT_USER_ALREADY_EXISTS);
      }

      // Add to S3SecretTable.
      // TODO: dedupe - S3GetSecretRequest
      acquiredS3SecretLock = omMetadataManager.getLock()
          .acquireWriteLock(S3_SECRET_LOCK, principal);

      // Sanity check. principal should not exist in S3SecretTable
      if (omMetadataManager.getS3SecretTable().isExist(principal)) {
        LOG.error("Unexpected '{}' entry in S3SecretTable", principal);
        throw new OMException("Unexpected principal entry in S3SecretTable",
            OMException.ResultCodes.INVALID_REQUEST);
      }

      final S3SecretValue s3SecretValue =
          new S3SecretValue(principal, awsSecret);
      omMetadataManager.getS3SecretTable().addCacheEntry(
          new CacheKey<>(principal),
          new CacheValue<>(Optional.of(s3SecretValue), transactionLogIndex));

      omMetadataManager.getLock().releaseWriteLock(S3_SECRET_LOCK, principal);
      acquiredS3SecretLock = false;

      // Also inform the MultiTenantManager of the user assignment so it can
      //  initialize some policies in Ranger.
      userId = ozoneManager.getMultiTenantManager()
          .assignUserToTenant(tenantName, tenantUsername);
      LOG.debug("userId = {}", userId);

      // Add to tenantAccessIdTable
      final OmDBAccessIdInfo omDBAccessIdInfo = new OmDBAccessIdInfo.Builder()
          .setTenantName(tenantName)
          .setKerberosPrincipal(userId)  // TODO: Or principal? Ask/Double check
          .setSharedSecret(s3SecretValue.getAwsSecret())
          .build();
      omMetadataManager.getTenantAccessIdTable().addCacheEntry(
          new CacheKey<>(accessId),
          new CacheValue<>(Optional.of(omDBAccessIdInfo), transactionLogIndex));

      // Add to principalToAccessIdsTable
      OmDBKerberosPrincipalInfo omDBKerberosPrincipalInfo = omMetadataManager
          .getPrincipalToAccessIdsTable().getIfExist(userId /* TODO: CHECK */);

      // TODO: Potential TOC-TOU if we need to worry about concurrency.
      //  Won't be an issue if leader OM handles Ratis requests one-by-one.
      if (omDBKerberosPrincipalInfo == null) {
        omDBKerberosPrincipalInfo = new OmDBKerberosPrincipalInfo.Builder()
            .setAccessIds(new TreeSet<>(Collections.singleton(accessId)))
            .build();
      } else {
        omDBKerberosPrincipalInfo.addAccessId(accessId);
      }
      omMetadataManager.getPrincipalToAccessIdsTable().addCacheEntry(
          new CacheKey<>(userId),  // TODO: Or principal? Ask/Double check
          new CacheValue<>(Optional.of(omDBKerberosPrincipalInfo),
              transactionLogIndex));

      // Add to tenantGroupTable
      final String defaultGroupName =
          tenantName + OzoneConsts.DEFAULT_TENANT_USER_GROUP_SUFFIX;
      omMetadataManager.getTenantGroupTable().addCacheEntry(
          new CacheKey<>(principal),
          new CacheValue<>(Optional.of(defaultGroupName), transactionLogIndex));

      // Add to tenantRoleTable
      final String roleName = "role_admin";
      omMetadataManager.getTenantRoleTable().addCacheEntry(
          new CacheKey<>(principal),
          new CacheValue<>(Optional.of(roleName), transactionLogIndex));

      omResponse.setAssignUserToTenantResponse(
          AssignUserToTenantResponse.newBuilder().setSuccess(true)
              .setS3Secret(S3Secret.newBuilder()
                  .setAwsSecret(awsSecret).setKerberosID(principal))
              .build());
      omClientResponse = new OMAssignUserToTenantResponse(omResponse.build(),
          s3SecretValue, principal, defaultGroupName, roleName,
          accessId, omDBAccessIdInfo, userId, omDBKerberosPrincipalInfo);
    } catch (IOException ex) {
      ozoneManager.getMultiTenantManager().destroyUser(
          tenantName, tenantUsername);
      exception = ex;
      // Set response success flag to false
      omResponse.setAssignUserToTenantResponse(
          AssignUserToTenantResponse.newBuilder().setSuccess(false).build());
      omClientResponse = new OMTenantCreateResponse(
          createErrorOMResponse(omResponse, ex));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(ozoneManagerDoubleBufferHelper
            .add(omClientResponse, transactionLogIndex));
      }
      if (acquiredS3SecretLock) {
        omMetadataManager.getLock().releaseWriteLock(S3_SECRET_LOCK, principal);
      }
      if (acquiredVolumeLock) {
        omMetadataManager.getLock().releaseWriteLock(VOLUME_LOCK, volumeName);
      }
    }

    // Audit
    auditMap.put(OzoneConsts.TENANT, tenantName);
    auditLog(ozoneManager.getAuditLogger(),
        buildAuditMessage(OMAction.ASSIGN_USER_TO_TENANT, auditMap, exception,
            getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.info("Assigned user '{}' to tenant '{}'. Principal: {}",
          tenantUsername, tenantName, principal);
      // TODO: omMetrics.incNumTenantUsers()
    } else {
      LOG.error("Failed to assign user '{}' to tenant '{}': {}",
          tenantUsername, tenantName, exception.getMessage());
      // TODO: Check if the exception message is sufficient.
      // TODO: omMetrics.incNumTenantUserCreateFails()
    }
    return omClientResponse;
  }
}
