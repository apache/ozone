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
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.multitenant.AccessPolicy;
import org.apache.hadoop.ozone.om.multitenant.Tenant;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TENANT_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.USER_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.MULTITENANCY_SCHEMA;

/*
  Ratis execution flow for OMTenantCreate

- preExecute (perform checks and init)
  - Check tenant name validity (again)
    - If name is invalid, throw exception to client; else continue
- validateAndUpdateCache (update DB)
  - Grab VOLUME_LOCK write lock
  - Check volume existence
    - If tenant already exists, throw exception to client; else continue
  - Check tenant existence by checking tenantStateTable keys
    - If tenant already exists, throw exception to client; else continue
  - tenantStateTable: New entry
    - Key: tenant name. e.g. finance
    - Value: new OmDBTenantState for the tenant
      - tenantId: finance
      - bucketNamespaceName: finance
      - accountNamespaceName: finance
      - userPolicyGroupName: finance-users
      - bucketPolicyGroupName: finance-buckets
  - tenantPolicyTable: Generate default policies for the new tenant
    - K: finance-Users, V: finance-users-default
    - K: finance-Buckets, V: finance-buckets-default
  - Grab USER_LOCK write lock
  - Create volume finance (See OMVolumeCreateRequest)
  - Release VOLUME_LOCK write lock
  - Release USER_LOCK write lock
  - Queue Ranger policy sync that pushes default policies:
      OMMultiTenantManager#createTenant
 */

/**
 * Handles OMTenantCreate request.
 *
 * Extends OMVolumeRequest but not OMClientRequest since tenant creation
 *  involves volume creation.
 */
public class OMTenantCreateRequest extends OMVolumeRequest {
  public static final Logger LOG =
      LoggerFactory.getLogger(OMTenantCreateRequest.class);

  private transient Tenant tenantInContext;

  public OMTenantCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  @DisallowedUntilLayoutVersion(MULTITENANCY_SCHEMA)
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    // Check Ozone cluster admin privilege
    OMTenantRequestHelper.checkAdmin(ozoneManager);

    final CreateTenantRequest request = getOmRequest().getCreateTenantRequest();
    final String tenantId = request.getTenantId();

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

    // getUserName returns:
    // - Kerberos principal when Kerberos security is enabled
    // - User's login name when security is not enabled
    // - AWS_ACCESS_KEY_ID if the original request comes from S3 Gateway.
    //    Not Applicable to TenantCreateRequest.
    final UserGroupInformation ugi = ProtobufRpcEngine.Server.getRemoteUser();
    // getShortUserName here follows RpcClient#createVolume
    // A caveat is that this assumes OM's auth_to_local is the same as
    //  the client's. Maybe move this logic to the client and pass VolumeArgs?
    final String owner = ugi.getShortUserName();
    // Volume name defaults to tenant name if unspecified in the request
    final String volumeName = request.getVolumeName();
    // Validate volume name
    OmUtils.validateVolumeName(volumeName);
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

    // If we fail after pre-execute. handleRequestFailure() callback
    // would clean up any state maintained by the getMultiTenantManager.
    tenantInContext = ozoneManager.getMultiTenantManager()
        .createTenantAccessInAuthorizer(tenantId);

    // Get the tenant default policy, pass this along
    final String tenantDefaultPolicies = tenantInContext
        .getTenantAccessPolicies()
        .stream().map(AccessPolicy::getPolicyID)
        .collect(Collectors.joining(","));

    final OMRequest.Builder omRequestBuilder = getOmRequest().toBuilder()
        .setCreateTenantRequest(
            CreateTenantRequest.newBuilder()
                .setTenantDefaultPolicyName(tenantDefaultPolicies)
                .setTenantId(tenantId)
                .setVolumeName(volumeName))
        .setCreateVolumeRequest(
            CreateVolumeRequest.newBuilder().setVolumeInfo(updatedVolumeInfo))
        // TODO: Can the three lines below be ignored?
        .setUserInfo(getUserInfo())
        .setCmdType(getOmRequest().getCmdType())
        .setClientId(getOmRequest().getClientId());

    if (getOmRequest().hasTraceID()) {
      omRequestBuilder.setTraceID(getOmRequest().getTraceID());
    }

    return omRequestBuilder.build();
  }

  @Override
  public void handleRequestFailure(OzoneManager ozoneManager) {
    try {
      // Cleanup any state maintained by OMMultiTenantManager
      if (tenantInContext != null) {
        ozoneManager.getMultiTenantManager()
            .removeTenantAccessFromAuthorizer(tenantInContext);
      }
    } catch (Exception e) {
      // TODO: Ignore for now. Multi-Tenant Manager is responsible for
      //  cleaning up stale state eventually. The Caller is already calling
      //  this in a failure context and would throw exception anyway.
    }
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(
      OzoneManager ozoneManager, long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

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
    final VolumeInfo volumeInfo =
        getOmRequest().getCreateVolumeRequest().getVolumeInfo();
    final String volumeName = volumeInfo.getVolume();
    Preconditions.checkState(request.getVolumeName().equals(volumeName),
        "CreateTenantRequest's volumeName value should match VolumeInfo's");
    final String dbVolumeKey = omMetadataManager.getVolumeKey(volumeName);
    IOException exception = null;

    final String tenantDefaultPolicies = request.getTenantDefaultPolicyName();

    try {
      // Check ACL: requires volume CREATE permission.
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.CREATE,
            tenantId, null, null);
      }

      acquiredVolumeLock = omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, volumeName);

      // Check volume existence
      if (omMetadataManager.getVolumeTable().isExist(volumeName)) {
        LOG.debug("volume: '{}' already exists", volumeName);
        throw new OMException("Volume already exists", VOLUME_ALREADY_EXISTS);
      }

      // Create volume
      acquiredUserLock = omMetadataManager.getLock().acquireWriteLock(USER_LOCK,
          owner);

      // TODO: dedup OMVolumeCreateRequest
      omVolumeArgs = OmVolumeArgs.getFromProtobuf(volumeInfo);
      omVolumeArgs.setObjectID(
          ozoneManager.getObjectIdFromTxId(transactionLogIndex));
      omVolumeArgs.setUpdateID(transactionLogIndex,
          ozoneManager.isRatisEnabled());
      // Set volume reference count to 1
      omVolumeArgs.incRefCount();
      Preconditions.checkState(omVolumeArgs.getRefCount() == 1,
          "refCount should have been set to 1");
      // Audit
      auditMap = omVolumeArgs.toAuditMap();

      PersistedUserVolumeInfo volumeList;
      final String dbUserKey = omMetadataManager.getUserKey(owner);
      volumeList = omMetadataManager.getUserTable().get(dbUserKey);
      volumeList = addVolumeToOwnerList(volumeList, volumeName, owner,
          ozoneManager.getMaxUserVolumeCount(), transactionLogIndex);
      createVolume(omMetadataManager, omVolumeArgs, volumeList, dbVolumeKey,
          dbUserKey, transactionLogIndex);
      LOG.debug("volume: '{}' successfully created", dbVolumeKey);


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
      final String userRoleName =
          OMMultiTenantManager.getDefaultUserRoleName(tenantId);
      final String adminRoleName =
          OMMultiTenantManager.getDefaultAdminRoleName(tenantId);
      final OmDBTenantState omDBTenantState = new OmDBTenantState(
          tenantId, bucketNamespaceName, userRoleName, adminRoleName,
          bucketNamespacePolicyName, bucketPolicyName);
      omMetadataManager.getTenantStateTable().addCacheEntry(
          new CacheKey<>(tenantId),
          new CacheValue<>(Optional.of(omDBTenantState), transactionLogIndex));

      omResponse.setCreateTenantResponse(
          CreateTenantResponse.newBuilder()
              .build());
      omClientResponse = new OMTenantCreateResponse(omResponse.build(),
          omVolumeArgs, volumeList, omDBTenantState);

    } catch (IOException ex) {
      // Error handling. Clean up Ranger policies when necessary.
      if (ex instanceof OMException) {
        final OMException omEx = (OMException) ex;
        if (!omEx.getResult().equals(VOLUME_ALREADY_EXISTS) &&
            !omEx.getResult().equals(TENANT_ALREADY_EXISTS)) {
          handleRequestFailure(ozoneManager);
        }
        // Do NOT perform any clean-up if the exception is a result of
        //  volume name or tenant name already existing.
        //  Otherwise in a race condition a late-comer could wipe the
        //  policies of an existing tenant from Ranger.
      } else {
        // ALL OMs should proactively call the clean-up handler in other cases
        handleRequestFailure(ozoneManager);
      }
      // Prepare omClientResponse
      omClientResponse = new OMTenantCreateResponse(
          createErrorOMResponse(omResponse, ex));
      exception = ex;
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(ozoneManagerDoubleBufferHelper
            .add(omClientResponse, transactionLogIndex));
      }
      if (acquiredUserLock) {
        omMetadataManager.getLock().releaseWriteLock(USER_LOCK, owner);
      }
      if (acquiredVolumeLock) {
        Preconditions.checkNotNull(volumeName);
        omMetadataManager.getLock().releaseWriteLock(VOLUME_LOCK, volumeName);
      }
    }

    // Perform audit logging
    auditMap.put(OzoneConsts.TENANT, tenantId);
    // Note auditMap contains volume creation info
    auditLog(ozoneManager.getAuditLogger(),
        buildAuditMessage(OMAction.CREATE_TENANT, auditMap, exception,
            getOmRequest().getUserInfo()));
    // Log CREATE_VOLUME as well since a volume is created
    auditLog(ozoneManager.getAuditLogger(),
        buildAuditMessage(OMAction.CREATE_VOLUME, auditMap, exception,
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
}
