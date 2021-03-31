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

    // Tenant and tenant user existence check won't be done here.

    // Generate S3 secret
    // TODO: Equivalent, but not a real kerberosID: tenant$username
    final String kerberosID = tenantName +
        OzoneConsts.TENANT_NAME_USER_NAME_DELIMITER + tenantUsername;
    final String s3Secret = DigestUtils.sha256Hex(OmUtils.getSHADigest());

    final UpdateGetS3SecretRequest updateGetS3SecretRequest =
        UpdateGetS3SecretRequest.newBuilder()
            .setAwsSecret(s3Secret)
            .setKerberosID(kerberosID).build();

    // TODO: Start from OMRequest.newBuilder() if better
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
    final String kerberosID = updateGetS3SecretRequest.getKerberosID();
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
    final String fullUsername = tenantName +
        OzoneConsts.TENANT_NAME_USER_NAME_DELIMITER + tenantUsername;

    IOException exception = null;
    try {
      // Check ACL: access_key_id should be tenant admin
      if (ozoneManager.getAclsEnabled()) {
        // TODO: Use OMMultiTenantManager?
      }

      // Sanity check full user name with kerberosID in UpdateGetS3SecretRequest
      if (!fullUsername.equals(kerberosID)) {
        LOG.error("fullUsername {} mismatches kerberosID {}.",
            fullUsername, kerberosID);
        throw new OMException("fullUsername mismatches kerberosID",
            OMException.ResultCodes.INVALID_TENANT_USER_NAME);
      }

      final String dbVolumeKey = omMetadataManager.getVolumeKey(tenantName);
      acquiredVolumeLock = omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, dbVolumeKey);

      // Check tenant existence in tenantStateTable
      if (omMetadataManager.getTenantStateTable().isExist(tenantName)) {
        LOG.debug("tenant: {} already exists", tenantName);
        throw new OMException("Tenant already exists",
            OMException.ResultCodes.TENANT_ALREADY_EXISTS);
      }

      // Check user existence in tenantUserTable
      if (omMetadataManager.getTenantUserTable().isExist(fullUsername)) {
        LOG.debug("tenant full user name: {} already exists", fullUsername);
        throw new OMException("Tenant user already exists",
            OMException.ResultCodes.TENANT_USER_ALREADY_EXISTS);
      }

      // Add to S3SecretTable. TODO: dedup S3GetSecretRequest
      acquiredS3SecretLock = omMetadataManager.getLock()
          .acquireWriteLock(S3_SECRET_LOCK, kerberosID);

      // Sanity check. fullUsername secret should not exist in S3SecretTable
      if (omMetadataManager.getS3SecretTable().isExist(fullUsername)) {
        LOG.debug("Unexpected S3 secret table entry for {}", fullUsername);
        throw new OMException("S3 secret for " + fullUsername +
            " shouldn't have existed", OMException.ResultCodes.INVALID_REQUEST);
      }

      final S3SecretValue s3SecretValue =
          new S3SecretValue(kerberosID, awsSecret);
      omMetadataManager.getS3SecretTable().addCacheEntry(
          new CacheKey<>(kerberosID),
          new CacheValue<>(Optional.of(s3SecretValue), transactionLogIndex));

      omMetadataManager.getLock().releaseWriteLock(S3_SECRET_LOCK, kerberosID);
      acquiredS3SecretLock = false;

      // Add to tenantUserTable
      omMetadataManager.getTenantUserTable().addCacheEntry(
          new CacheKey<>(fullUsername),
          new CacheValue<>(Optional.of(tenantName), transactionLogIndex));
      // Add to tenantGroupTable
      final String defaultGroupName =
          tenantName + OzoneConsts.DEFAULT_TENANT_USER_GROUP_SUFFIX;
      omMetadataManager.getTenantGroupTable().addCacheEntry(
          new CacheKey<>(fullUsername),
          new CacheValue<>(Optional.of(defaultGroupName), transactionLogIndex));
      // Add to tenantRoleTable
      final String roleName = "dummy-role";
      omMetadataManager.getTenantRoleTable().addCacheEntry(
          new CacheKey<>(fullUsername),
          new CacheValue<>(Optional.of(roleName), transactionLogIndex));

      // Call OMMultiTenantManager
      //  TODO: Check usage with Prashant
      tenantPrincipal = ozoneManager.getMultiTenantManager()
          .createUser(tenantName, fullUsername /* TODO: full or short name? */);

      omResponse.setCreateTenantUserResponse(
          CreateTenantUserResponse.newBuilder().setSuccess(true)
              .setS3Secret(S3Secret.newBuilder()
                  .setAwsSecret(awsSecret).setKerberosID(kerberosID))
              .build());
      omClientResponse = new OMTenantUserCreateResponse(
          omResponse.build(), s3SecretValue,
          fullUsername, tenantName, defaultGroupName, roleName);
    } catch (IOException ex) {
      exception = ex;
      // Set response success flag to false
      omResponse.setCreateTenantUserResponse(
          CreateTenantUserResponse.newBuilder().setSuccess(false).build());
      // Cleanup any state maintained by OMMultiTenantManager
      if (tenantPrincipal != null) {
        try {  // TODO: Check usage with Prashant
          ozoneManager.getMultiTenantManager().deactivateUser(tenantPrincipal);
        } catch (Exception e) {
          // Ignore for now
        }
      }
      omClientResponse = new OMTenantCreateResponse(
          createErrorOMResponse(omResponse, ex));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(ozoneManagerDoubleBufferHelper
            .add(omClientResponse, transactionLogIndex));
      }
      if (acquiredS3SecretLock) {
        omMetadataManager.getLock().releaseWriteLock(
            S3_SECRET_LOCK, kerberosID);
      }
      if (acquiredVolumeLock) {
        omMetadataManager.getLock().releaseWriteLock(VOLUME_LOCK, tenantName);
      }
    }

    // Audit
    auditMap.put(OzoneConsts.TENANT, tenantName);
    auditLog(ozoneManager.getAuditLogger(),
        buildAuditMessage(OMAction.CREATE_TENANT_USER, auditMap, exception,
            getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.debug("Successfully created tenant user {}", tenantName);
      // TODO: add metrics, see OMVolumeCreateRequest
    } else {
      LOG.error("Failed to create tenant user {}", tenantName, exception);
      // TODO: same, add metrics
    }
    return omClientResponse;
  }
}
