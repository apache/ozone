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

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_MAXIMUM_ACCESS_ID_LENGTH;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.VOLUME_LOCK;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.MULTITENANCY_SCHEMA;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBUserPrincipalInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantAssignUserAccessIdResponse;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Secret;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantAssignUserAccessIdRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantAssignUserAccessIdResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateGetS3SecretRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles OMAssignUserToTenantRequest.
 * Execution flow (might be a bit outdated):
 * - Client (AssignUserToTenantHandler)
 *   - Check admin privilege
 *   - Check username validity: ensure no invalid characters
 *   - Send request to server
 * - OMAssignUserToTenantRequest
 *   - preExecute (perform checks and init)
 *     - Check username validity (again), check $
 *       - If username is invalid, throw exception to client; else continue
 *     - Generate S3 secret for the new user
 *   - validateAndUpdateCache (update DB)
 *     - Permission check (checkACL need to check access key now)
 *     - Grab VOLUME_LOCK write lock
 *     - Check tenant existence
 *       - If tenant doesn't exist, throw exception to client; else continue
 *     - Check accessId existence
 *       - If accessId exists, throw exception to client; else continue
 *     - Grab S3_SECRET_LOCK write lock
 *     - S3SecretTable: Flush generated S3 secret
 *       - Key: TENANTNAME$USERNAME (equivalent to kerberosID)
 *       - Value: <GENERATED_SECRET>
 *     - Release S3_SECRET_LOCK write lock
 *     - New entry in tenantAccessIdTable:
 *       - Key: New accessId for the user in this tenant.
 *              Example of accessId: finance$bob@EXAMPLE.COM
 *       - Value: OmDBAccessIdInfo. Has tenantId, kerberosPrincipal, sharedSecret.
 *     - New entry or update existing entry in principalToAccessIdsTable:
 *       - Key: User principal. Usually the short name of the Kerberos principal.
 *       - Value: OmDBUserPrincipalInfo. Has accessIds.
 *     - Release VOLUME_LOCK write lock
 *
 */
public class OMTenantAssignUserAccessIdRequest extends OMClientRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMTenantAssignUserAccessIdRequest.class);

  public OMTenantAssignUserAccessIdRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  @DisallowedUntilLayoutVersion(MULTITENANCY_SCHEMA)
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    final OMRequest omRequest = super.preExecute(ozoneManager);

    final TenantAssignUserAccessIdRequest request =
        omRequest.getTenantAssignUserAccessIdRequest();

    final String tenantId = request.getTenantId();

    final OMMultiTenantManager multiTenantManager =
        ozoneManager.getMultiTenantManager();

    // Caller should be an Ozone admin, or at least a tenant non-delegated admin
    multiTenantManager.checkTenantAdmin(tenantId, false);

    final String userPrincipal = request.getUserPrincipal();
    final String accessId = request.getAccessId();

    // Check accessId length.
    if (accessId.length() >= OZONE_MAXIMUM_ACCESS_ID_LENGTH) {
      throw new OMException(
          "accessId length exceeds the maximum length allowed",
          ResultCodes.INVALID_ACCESS_ID);
    }

    // Check userPrincipal (username) validity.
    if (userPrincipal.contains(OzoneConsts.TENANT_ID_USERNAME_DELIMITER)) {
      throw new OMException("Invalid tenant username '" + userPrincipal +
          "'. Tenant username shouldn't contain delimiter.",
          ResultCodes.INVALID_TENANT_USERNAME);
    }

    // Check tenant name validity.
    if (tenantId.contains(OzoneConsts.TENANT_ID_USERNAME_DELIMITER)) {
      throw new OMException("Invalid tenant name '" + tenantId +
          "'. Tenant name shouldn't contain delimiter.",
          ResultCodes.INVALID_TENANT_ID);
    }

    // HDDS-6366: Disallow specifying custom accessId.
    final String expectedAccessId =
        OMMultiTenantManager.getDefaultAccessId(tenantId, userPrincipal);
    if (!accessId.equals(expectedAccessId)) {
      throw new OMException("Invalid accessId '" + accessId + "'. "
          + "Specifying a custom access ID disallowed. "
          + "Expected accessId to be assigned is '" + expectedAccessId + "'",
          ResultCodes.INVALID_ACCESS_ID);
    }

    multiTenantManager.checkTenantExistence(tenantId);

    // Below call implies user existence check in authorizer.
    // If the user doesn't exist, Ranger returns 400 and the call should throw.

    // Acquire write lock to authorizer (Ranger)
    multiTenantManager.getAuthorizerLock().tryWriteLockInOMRequest();
    try {
      // Add user to tenant user role in Ranger.
      // Throws if the user doesn't exist in Ranger.
      multiTenantManager.getAuthorizerOp()
          .assignUserToTenant(userPrincipal, tenantId, accessId);
    } catch (Exception e) {
      multiTenantManager.getAuthorizerLock().unlockWriteInOMRequest();
      throw e;
    }

    // Generate secret. However, this is used only when the accessId entry
    // doesn't exist in DB and need to be created, discarded otherwise.
    final String s3Secret = DigestUtils.sha256Hex(OmUtils.getSHADigest());

    final UpdateGetS3SecretRequest updateGetS3SecretRequest =
        UpdateGetS3SecretRequest.newBuilder()
            .setKerberosID(accessId)
            .setAwsSecret(s3Secret)
            .build();

    final OMRequest.Builder omRequestBuilder = omRequest.toBuilder()
        .setUpdateGetS3SecretRequest(updateGetS3SecretRequest);

    return omRequestBuilder.build();
  }

  @Override
  @SuppressWarnings("checkstyle:methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();
    final OMMultiTenantManager multiTenantManager =
        ozoneManager.getMultiTenantManager();

    final OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumTenantAssignUsers();

    OMClientResponse omClientResponse = null;
    final OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());

    final UpdateGetS3SecretRequest updateGetS3SecretRequest =
        getOmRequest().getUpdateGetS3SecretRequest();
    final String accessId = updateGetS3SecretRequest.getKerberosID();
    final String awsSecret = updateGetS3SecretRequest.getAwsSecret();

    boolean acquiredVolumeLock = false;
    final Map<String, String> auditMap = new HashMap<>();
    final OMMetadataManager omMetadataManager =
        ozoneManager.getMetadataManager();

    final TenantAssignUserAccessIdRequest request =
        getOmRequest().getTenantAssignUserAccessIdRequest();
    final String tenantId = request.getTenantId();
    final String userPrincipal = request.getUserPrincipal();

    Preconditions.checkState(accessId.equals(request.getAccessId()));
    Exception exception = null;

    String volumeName = null;

    try {
      volumeName = ozoneManager.getMultiTenantManager()
          .getTenantVolumeName(tenantId);

      mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, volumeName));
      acquiredVolumeLock = getOmLockDetails().isLockAcquired();

      // Expect tenant existence in tenantStateTable
      if (!omMetadataManager.getTenantStateTable().isExist(tenantId)) {
        LOG.error("tenant {} doesn't exist", tenantId);
        throw new OMException("tenant '" + tenantId + "' doesn't exist",
            ResultCodes.TENANT_NOT_FOUND);
      }

      // Expect accessId absence from tenantAccessIdTable
      if (omMetadataManager.getTenantAccessIdTable().isExist(accessId)) {
        LOG.error("accessId {} already exists", accessId);
        throw new OMException("accessId '" + accessId + "' already exists!",
            ResultCodes.TENANT_USER_ACCESS_ID_ALREADY_EXISTS);
      }

      OmDBUserPrincipalInfo principalInfo = omMetadataManager
          .getPrincipalToAccessIdsTable().getIfExist(userPrincipal);
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
            throw new OMException("accessIdInfo is null",
                ResultCodes.INVALID_ACCESS_ID);
          }
          if (tenantId.equals(accessIdInfo.getTenantId())) {
            throw new OMException("The same user is not allowed to be assigned "
                + "to the same tenant more than once. User '" + userPrincipal
                + "' is already assigned to tenant '" + tenantId + "' with "
                + "accessId '" + existingAccId + "'.",
                ResultCodes.TENANT_USER_ACCESS_ID_ALREADY_EXISTS);
          }
        }
      }

      final S3SecretValue s3SecretValue = S3SecretValue.of(accessId, awsSecret, transactionLogIndex);

      // Add to tenantAccessIdTable
      final OmDBAccessIdInfo omDBAccessIdInfo = new OmDBAccessIdInfo.Builder()
          .setTenantId(tenantId)
          .setUserPrincipal(userPrincipal)
          .setIsAdmin(false)
          .setIsDelegatedAdmin(false)
          .build();
      omMetadataManager.getTenantAccessIdTable().addCacheEntry(
          new CacheKey<>(accessId),
          CacheValue.get(transactionLogIndex, omDBAccessIdInfo));

      // Add to principalToAccessIdsTable
      if (principalInfo == null) {
        principalInfo = new OmDBUserPrincipalInfo.Builder()
            .setAccessIds(new TreeSet<>(Collections.singleton(accessId)))
            .build();
      } else {
        principalInfo.addAccessId(accessId);
      }
      omMetadataManager.getPrincipalToAccessIdsTable().addCacheEntry(
          new CacheKey<>(userPrincipal),
          CacheValue.get(transactionLogIndex, principalInfo));

      // Expect accessId absence from S3SecretTable
      ozoneManager.getS3SecretManager()
          .doUnderLock(accessId, s3SecretManager -> {
            if (s3SecretManager.hasS3Secret(accessId)) {
              LOG.error("accessId '{}' already exists in S3SecretTable",
                  accessId);
              throw new OMException("accessId '" + accessId +
                  "' already exists in S3SecretTable",
                  ResultCodes.TENANT_USER_ACCESS_ID_ALREADY_EXISTS);
            }

            s3SecretManager
                .updateCache(accessId, s3SecretValue);
            return null;
          });

      // Update tenant cache
      multiTenantManager.getCacheOp()
          .assignUserToTenant(userPrincipal, tenantId, accessId);

      // Generate response
      omResponse.setTenantAssignUserAccessIdResponse(
          TenantAssignUserAccessIdResponse.newBuilder()
              .setS3Secret(S3Secret.newBuilder()
                  .setAwsSecret(awsSecret).setKerberosID(accessId))
              .build());
      omClientResponse = new OMTenantAssignUserAccessIdResponse(
          omResponse.build(), s3SecretValue, userPrincipal,
          accessId, omDBAccessIdInfo, principalInfo,
          ozoneManager.getS3SecretManager());
    } catch (IOException | InvalidPathException ex) {
      exception = ex;
      omResponse.setTenantAssignUserAccessIdResponse(
          TenantAssignUserAccessIdResponse.newBuilder().build());
      omClientResponse = new OMTenantAssignUserAccessIdResponse(
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
    auditMap.put("user", userPrincipal);
    auditMap.put("accessId", accessId);
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.TENANT_ASSIGN_USER_ACCESSID, auditMap, exception,
            getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.info("Assigned user '{}' to tenant '{}' with accessId '{}'",
          userPrincipal, tenantId, accessId);
    } else {
      LOG.error("Failed to assign '{}' to tenant '{}' with accessId '{}': {}",
          userPrincipal, tenantId, accessId, exception.getMessage());
      omMetrics.incNumTenantAssignUserFails();
    }
    return omClientResponse;
  }
}
