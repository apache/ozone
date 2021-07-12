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
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.multitenant.OzoneMultiTenantPrincipal;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantCreateResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantUserCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTenantUserRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTenantUserResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Secret;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateGetS3SecretRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.S3_SECRET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

/*
  Ratis execution flow for OMTenantUserCreate

- Client (UserCreateHandler , etc.)
  - Check username validity: ensure no invalid characters
  - Send request to server
- OMTenantUserCreateRequest
  - preExecute (perform checks and init)
    - Check username validity (again), check $
      - If username is invalid, throw exception to client; else continue
    - Generate S3 secret for the new user
  - validateAndUpdateCache (update DB)
    - Permission check (checkACL need to check access key now)
    - Grab VOLUME_LOCK write lock
    - Check user existence
      - If user doesn't exist, throw exception to client; else continue
    - Check tenant existence
      - If tenant doesn't exist, throw exception to client; else continue
    - Grab S3_SECRET_LOCK write lock
    - S3SecretTable: Flush generated S3 secret
      - Key: TENANTNAME$USERNAME (equivalent to kerberosID)
      - Value: <GENERATED_SECRET>
    - Release S3_SECRET_LOCK write lock
    - tenantUserTable: New entry
      - Key: Tenant user name. e.g. finance$bob, s3v$alice
      - Value: Tenant name. e.g. finance
    - tenantGroupTable: Add this new user to the default tenant group.
      - Key: finance$bob
      - Value: finance-users
    - tenantRoleTable: TBD. NoOp for prototype.
    - Release VOLUME_LOCK write lock
 */

/**
 * Handles OMTenantUserCreate request.
 */
public class OMTenantUserCreateRequest extends OMVolumeRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMTenantUserCreateRequest.class);

  public OMTenantUserCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final CreateTenantUserRequest request =
        getOmRequest().getCreateTenantUserRequest();
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
    final CreateTenantUserRequest request =
        getOmRequest().getCreateTenantUserRequest();
    final String tenantName = request.getTenantName();
    final String tenantUsername = request.getTenantUsername();
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

      // Check tenant existence in tenantStateTable
      if (!omMetadataManager.getTenantStateTable().isExist(tenantName)) {
        LOG.debug("tenant: {} does not exist", tenantName);
        throw new OMException("Tenant does not exist",
            OMException.ResultCodes.TENANT_NOT_FOUND);
      }
      // Check user existence in tenantUserTable
      if (omMetadataManager.getTenantUserTable().isExist(principal)) {
        LOG.debug("principal: {} already exists", principal);
        throw new OMException("User already exists in tenant",
            OMException.ResultCodes.TENANT_USER_ALREADY_EXISTS);
      }

      // Add to S3SecretTable. TODO: dedup S3GetSecretRequest
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

      userId = ozoneManager.getMultiTenantManager()
          .createUser(tenantName, tenantUsername);
      LOG.info("userId = {}", userId);

      // Add to tenantUserTable
      omMetadataManager.getTenantUserTable().addCacheEntry(
          new CacheKey<>(principal),
          new CacheValue<>(Optional.of(tenantName), transactionLogIndex));
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

      omResponse.setCreateTenantUserResponse(
          CreateTenantUserResponse.newBuilder().setSuccess(true)
              .setS3Secret(S3Secret.newBuilder()
                  .setAwsSecret(awsSecret).setKerberosID(principal))
              .build());
      omClientResponse = new OMTenantUserCreateResponse(omResponse.build(),
          s3SecretValue, principal, tenantName, defaultGroupName, roleName);
    } catch (IOException ex) {
      ozoneManager.getMultiTenantManager().destroyUser(
          tenantName, tenantUsername);
      exception = ex;
      // Set response success flag to false
      omResponse.setCreateTenantUserResponse(
          CreateTenantUserResponse.newBuilder().setSuccess(false).build());
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
        buildAuditMessage(OMAction.CREATE_TENANT_USER, auditMap, exception,
            getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.info("Created user: {}, in tenant: {}. Principal: {}",
          tenantUsername, tenantName, principal);
      // TODO: omMetrics.incNumTenantUsers()
    } else {
      LOG.error("Failed to create tenant user {}", tenantName, exception);
      // TODO: omMetrics.incNumTenantUserCreateFails()
    }
    return omClientResponse;
  }
}
