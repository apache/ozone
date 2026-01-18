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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TENANT_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.USER_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.USER_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.VOLUME_LOCK;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.MULTITENANCY_SCHEMA;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantCreateResponse;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTenantRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTenantResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserVolumeInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles OMTenantCreate request.
 * Extends OMVolumeRequest but not OMClientRequest since tenant creation
 *  involves volume creation.
 * Ratis execution flow for OMTenantCreate
 * - preExecute (perform checks and init)
 *   - Check tenant name validity (again)
 *     - If name is invalid, throw exception to client; else continue
 * - validateAndUpdateCache (update DB)
 *   - Grab VOLUME_LOCK write lock
 *   - Check volume existence
 *     - If tenant already exists, throw exception to client; else continue
 *   - Check tenant existence by checking tenantStateTable keys
 *     - If tenant already exists, throw exception to client; else continue
 *   - tenantStateTable: New entry
 *     - Key: tenant name. e.g. finance
 *     - Value: new OmDBTenantState for the tenant
 *       - tenantId: finance
 *       - bucketNamespaceName: finance
 *       - accountNamespaceName: finance
 *       - userPolicyGroupName: finance-users
 *       - bucketPolicyGroupName: finance-buckets
 *   - tenantPolicyTable: Generate default policies for the new tenant
 *     - K: finance-Users, V: finance-users-default
 *     - K: finance-Buckets, V: finance-buckets-default
 *   - Grab USER_LOCK write lock
 *   - Create volume finance (See OMVolumeCreateRequest)
 *   - Release VOLUME_LOCK write lock
 *   - Release USER_LOCK write lock
 *   - Queue Ranger policy sync that pushes default policies:
 *       OMMultiTenantManager#createTenant
 */
public class OMTenantCreateRequest extends OMVolumeRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMTenantCreateRequest.class);

  public OMTenantCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  @DisallowedUntilLayoutVersion(MULTITENANCY_SCHEMA)
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    final OMMultiTenantManager multiTenantManager =
        ozoneManager.getMultiTenantManager();

    // Check Ozone cluster admin privilege
    multiTenantManager.checkAdmin();

    final OMRequest omRequest = super.preExecute(ozoneManager);
    final CreateTenantRequest request = omRequest.getCreateTenantRequest();
    Objects.requireNonNull(request, "request == null");
    final String tenantId = request.getTenantId();
    // Check ACL: requires volume CREATE permission.
    if (ozoneManager.getAclsEnabled()) {
      checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
          OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.CREATE, tenantId,
          null, null);
    }
    // Check tenantId validity
    if (tenantId.contains(OzoneConsts.TENANT_ID_USERNAME_DELIMITER)) {
      throw new OMException("Invalid tenant name " + tenantId +
          ". Tenant name should not contain delimiter.",
          OMException.ResultCodes.INVALID_VOLUME_NAME);
    }

    // Check tenant existence in tenantStateTable
    if (ozoneManager.getMetadataManager().getTenantStateTable()
        .isExist(tenantId)) {
      LOG.debug("tenant: {} already exists", tenantId);
      throw new OMException("Tenant '" + tenantId + "' already exists",
          TENANT_ALREADY_EXISTS);
    }

    final String owner = getUserName();
    // Volume name defaults to tenant name if unspecified in the request
    final String volumeName = request.getVolumeName();
    // Validate volume name
    OmUtils.validateVolumeName(volumeName, ozoneManager.isStrictS3());

    final String dbVolumeKey = ozoneManager.getMetadataManager()
        .getVolumeKey(volumeName);

    // ACL check during preExecute (align with other create requests)
    if (ozoneManager.getAclsEnabled()) {
      try {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.CREATE,
            volumeName, null, null);
      } catch (IOException ex) {
        // Ensure audit log captures preExecute failures
        markForAudit(ozoneManager.getAuditLogger(),
            buildAuditMessage(OMAction.CREATE_TENANT,
                buildVolumeAuditMap(volumeName), ex,
                omRequest.getUserInfo()));
        throw ex;
      }
    }

    // Backwards compatibility with older Ozone clients that don't have this
    // field. Defaults to false.
    boolean forceCreationWhenVolumeExists =
        request.hasForceCreationWhenVolumeExists()
            && request.getForceCreationWhenVolumeExists();

    // Check volume existence
    if (!forceCreationWhenVolumeExists &&
        ozoneManager.getMetadataManager().getVolumeTable().isExist(
            dbVolumeKey)) {
      LOG.debug("volume: '{}' already exists", volumeName);
      throw new OMException("Volume already exists", VOLUME_ALREADY_EXISTS);
    }

    // TODO: Refactor this and OMVolumeCreateRequest to improve maintainability.
    final VolumeInfo volumeInfo = VolumeInfo.newBuilder()
        .setVolume(volumeName)
        .setAdminName(owner)
        .setOwnerName(owner)
        .build();

    // Generate volume modification time
    long initialTime = Time.now();
    final VolumeInfo updatedVolumeInfo = volumeInfo.toBuilder()
            .setCreationTime(initialTime)
            .setModificationTime(initialTime)
            .build();

    final String userRoleName =
        OMMultiTenantManager.getDefaultUserRoleName(tenantId);
    final String adminRoleName =
        OMMultiTenantManager.getDefaultAdminRoleName(tenantId);

    // Acquire write lock to authorizer (Ranger)
    multiTenantManager.getAuthorizerLock().tryWriteLockInOMRequest();
    try {
      // Create tenant roles and policies in Ranger.
      // If the request fails for some reason, Ranger background sync thread
      // should clean up any leftover policies and roles.
      multiTenantManager.getAuthorizerOp().createTenant(
          tenantId, userRoleName, adminRoleName);
    } catch (Exception e) {
      multiTenantManager.getAuthorizerLock().unlockWriteInOMRequest();
      throw e;
    }

    final OMRequest.Builder omRequestBuilder = omRequest.toBuilder()
        .setCreateTenantRequest(
            CreateTenantRequest.newBuilder()
                .setTenantId(tenantId)
                .setVolumeName(volumeName)
                .setUserRoleName(userRoleName)
                .setAdminRoleName(adminRoleName)
                .setForceCreationWhenVolumeExists(
                    forceCreationWhenVolumeExists))
        .setCreateVolumeRequest(
            CreateVolumeRequest.newBuilder()
                .setVolumeInfo(updatedVolumeInfo));

    return omRequestBuilder.build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();

    final OMMultiTenantManager multiTenantManager =
        ozoneManager.getMultiTenantManager();

    final OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumTenantCreates();
    omMetrics.incNumVolumeCreates();

    OMClientResponse omClientResponse = null;
    final OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OmVolumeArgs omVolumeArgs;
    boolean acquiredVolumeLock = false;
    boolean acquiredUserLock = false;
    final String owner = getOmRequest().getUserInfo().getUserName();
    Map<String, String> auditMap = new HashMap<>();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    final CreateTenantRequest request = getOmRequest().getCreateTenantRequest();
    final String tenantId = request.getTenantId();
    final String userRoleName = request.getUserRoleName();
    final String adminRoleName = request.getAdminRoleName();
    final boolean forceCreationWhenVolumeExists =
        request.getForceCreationWhenVolumeExists();

    final VolumeInfo volumeInfo =
        getOmRequest().getCreateVolumeRequest().getVolumeInfo();
    final String volumeName = volumeInfo.getVolume();
    Objects.requireNonNull(volumeName, "volumeName == null");
    Preconditions.checkState(request.getVolumeName().equals(volumeName),
        "CreateTenantRequest's volumeName value should match VolumeInfo's");
    final String dbVolumeKey = omMetadataManager.getVolumeKey(volumeName);

    Exception exception = null;

    try {
      mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, volumeName));
      acquiredVolumeLock = getOmLockDetails().isLockAcquired();

      boolean skipVolumeCreation = false;
      // Check volume existence
      if (omMetadataManager.getVolumeTable().isExist(dbVolumeKey)) {
        LOG.debug("volume: '{}' already exists", volumeName);
        if (forceCreationWhenVolumeExists) {
          LOG.warn("forceCreationWhenVolumeExists = true. Resuming "
              + "tenant creation despite volume '{}' existence", volumeName);
          skipVolumeCreation = true;
        } else {
          // forceCreationWhenVolumeExists is false, throw
          throw new OMException("Volume already exists", VOLUME_ALREADY_EXISTS);
        }
      }

      mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLock(
          USER_LOCK, owner));
      acquiredUserLock = getOmLockDetails().isLockAcquired();

      OmVolumeArgs.Builder volumeBuilder;
      PersistedUserVolumeInfo volumeList = null;
      if (!skipVolumeCreation) {
        // Create volume. TODO: dedup OMVolumeCreateRequest
        volumeBuilder = OmVolumeArgs.builderFromProtobuf(volumeInfo)
            .setQuotaInBytes(OzoneConsts.QUOTA_RESET)
            .setQuotaInNamespace(OzoneConsts.QUOTA_RESET)
            .setObjectID(ozoneManager.getObjectIdFromTxId(transactionLogIndex))
            .setUpdateID(transactionLogIndex)
            .incRefCount();
        omVolumeArgs = volumeBuilder.build();

        // Remove this check when vol ref count is also used by other features
        Preconditions.checkState(omVolumeArgs.getRefCount() == 1L,
            "refCount should have been set to 1");

        final String dbUserKey = omMetadataManager.getUserKey(owner);
        volumeList = omMetadataManager.getUserTable().get(dbUserKey);
        volumeList = addVolumeToOwnerList(volumeList, volumeName, owner,
            ozoneManager.getMaxUserVolumeCount(), transactionLogIndex);
        createVolume(omMetadataManager, omVolumeArgs, volumeList, dbVolumeKey,
            dbUserKey, transactionLogIndex);
        LOG.debug("volume: '{}' successfully created", dbVolumeKey);
      } else {
        LOG.info("Skipped volume '{}' creation. "
            + "Will only increment volume refCount", volumeName);
        volumeBuilder = getVolumeInfo(omMetadataManager, volumeName)
            .toBuilder()
            .incRefCount();
        omVolumeArgs = volumeBuilder.build();

        // Remove this check when vol ref count is also used by other features
        Preconditions.checkState(omVolumeArgs.getRefCount() == 1L,
            "refCount should have been set to 1");

        omMetadataManager.getVolumeTable().addCacheEntry(
            new CacheKey<>(dbVolumeKey),
            CacheValue.get(transactionLogIndex, omVolumeArgs));
      }

      // Audit
      auditMap = volumeBuilder.toAuditMap();

      // Check tenant existence in tenantStateTable
      if (omMetadataManager.getTenantStateTable().isExist(tenantId)) {
        LOG.debug("tenant: '{}' already exists", tenantId);
        throw new OMException("Tenant already exists", TENANT_ALREADY_EXISTS);
      }

      // Create tenant
      // Add to tenantStateTable. Redundant assignment for clarity
      final String bucketNamespaceName = volumeName;
      // Populate policy ID list
      final String bucketNamespacePolicyName =
          OMMultiTenantManager.getDefaultBucketNamespacePolicyName(tenantId);
      final String bucketPolicyName =
          OMMultiTenantManager.getDefaultBucketPolicyName(tenantId);
      final OmDBTenantState omDBTenantState = new OmDBTenantState(
          tenantId, bucketNamespaceName, userRoleName, adminRoleName,
          bucketNamespacePolicyName, bucketPolicyName);
      omMetadataManager.getTenantStateTable().addCacheEntry(
          new CacheKey<>(tenantId),
          CacheValue.get(transactionLogIndex, omDBTenantState));

      // Update tenant cache
      multiTenantManager.getCacheOp().createTenant(
          tenantId, userRoleName, adminRoleName);

      omResponse.setCreateTenantResponse(
          CreateTenantResponse.newBuilder()
              .build());
      omClientResponse = new OMTenantCreateResponse(omResponse.build(),
          omVolumeArgs, volumeList, omDBTenantState);

    } catch (IOException | InvalidPathException ex) {
      exception = ex;
      omClientResponse = new OMTenantCreateResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (acquiredUserLock) {
        mergeOmLockDetails(
            omMetadataManager.getLock().releaseWriteLock(USER_LOCK, owner));
      }
      if (acquiredVolumeLock) {
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

    // Perform audit logging
    auditMap.put(OzoneConsts.TENANT, tenantId);
    // Note auditMap contains volume creation info
    markForAudit(ozoneManager.getAuditLogger(),
        buildAuditMessage(OMAction.CREATE_TENANT, auditMap, exception,
            getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.info("Created tenant '{}' and volume '{}'", tenantId, volumeName);
      omMetrics.incNumTenants();
      omMetrics.incNumVolumes();
    } else {
      LOG.error("Failed to create tenant '{}'", tenantId, exception);
      omMetrics.incNumTenantCreateFails();
    }
    return omClientResponse;
  }

  public String getUserName() throws IOException {
    // getUserName returns:
    // - Kerberos principal when Kerberos security is enabled
    // - User's login name when security is not enabled
    // - AWS_ACCESS_KEY_ID if the original request comes from S3 Gateway.
    //    Not Applicable to TenantCreateRequest.
    final UserGroupInformation ugi = ProtobufRpcEngine.Server.getRemoteUser();
    // getShortUserName here follows RpcClient#createVolume
    // A caveat is that this assumes OM's auth_to_local is the same as
    //  the client's. Maybe move this logic to the client and pass VolumeArgs?
    if (ugi != null) {
      return ugi.getShortUserName();
    } else {
      throw new OMException("User name is null.", USER_NOT_FOUND);
    }
  }
}
