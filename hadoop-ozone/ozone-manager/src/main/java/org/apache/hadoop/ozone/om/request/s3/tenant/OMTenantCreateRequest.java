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
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.multitenant.Tenant;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTenantRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTenantResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.USER_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

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
    - Value: new OmDBTenantInfo for the tenant
      - tenantName: finance
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
 */
public class OMTenantCreateRequest extends OMVolumeRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMTenantCreateRequest.class);

  public OMTenantCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final CreateTenantRequest request = getOmRequest().getCreateTenantRequest();
    final String tenantName = request.getTenantName();

    // Check tenantName validity
    if (tenantName.contains(OzoneConsts.TENANT_NAME_USER_NAME_DELIMITER)) {
      throw new OMException("Invalid tenant name " + tenantName +
          ". Tenant name should not contain delimiter.",
          OMException.ResultCodes.INVALID_VOLUME_NAME);
    }

    // Generate VolumeInfo
    final VolumeInfo volumeInfo = VolumeInfo.newBuilder()
        .setVolume(tenantName)  // TODO: volume=tenant for now. configurable
//        .setOwnerName("owner")  // TODO: Fill in access_key_id
        .build();

    // Verify volume name
    OmUtils.validateVolumeName(volumeInfo.getVolume());

    // Generate volume modification time
    long initialTime = Time.now();
    final VolumeInfo updatedVolumeInfo = volumeInfo.toBuilder()
            .setCreationTime(initialTime)
            .setModificationTime(initialTime)
            .build();

    final OMRequest.Builder omRequestBuilder = getOmRequest().toBuilder()
        .setCreateTenantRequest(
            CreateTenantRequest.newBuilder().setTenantName(tenantName)
        )
        .setCreateVolumeRequest(
            CreateVolumeRequest.newBuilder().setVolumeInfo(updatedVolumeInfo)
        )
        .setUserInfo(getUserInfo())
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
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    boolean acquiredVolumeLock = false;
    boolean acquiredUserLock = false;
    Tenant tenant = null;

    // TODO: Double check volume owner existence
    // TODO: owner should be access_key_id
    final String owner = getOmRequest().getUserInfo().getUserName();
    LOG.debug("hasUserInfo: {}, owner: {}",
        getOmRequest().hasUserInfo(), owner);

    Map<String, String> auditMap = new HashMap<>();

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    CreateTenantRequest request = getOmRequest().getCreateTenantRequest();
    String tenantName = request.getTenantName();

    IOException exception = null;
    try {
      // Check ACL
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.CREATE,
            tenantName, null, null);
      }

      final String dbVolumeKey = omMetadataManager.getVolumeKey(tenantName);

      acquiredVolumeLock = omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, dbVolumeKey);

      // Check volume existence
      // In this case, volume name *is* tenant name
      if (omMetadataManager.getVolumeTable().isExist(dbVolumeKey)) {
        LOG.debug("volume: {} already exists", dbVolumeKey);
        throw new OMException("Volume already exists",
            OMException.ResultCodes.VOLUME_ALREADY_EXISTS);
      }

      // Check tenant existence in tenantStateTable
      if (omMetadataManager.getTenantStateTable().isExist(tenantName)) {
        LOG.debug("tenant: {} already exists", tenantName);
        throw new OMException("Tenant already exists",
            OMException.ResultCodes.TENANT_ALREADY_EXISTS);
      }

      // Add to tenantStateTable
      // Note: intentional redundant assignment here for clarity
      final String bucketNamespaceName = tenantName;
      final String accountNamespaceName = tenantName;
      final String userPolicyGroupName =
          tenantName + OzoneConsts.DEFAULT_TENANT_USER_POLICY_SUFFIX;
      final String bucketPolicyGroupName =
          tenantName + OzoneConsts.DEFAULT_TENANT_BUCKET_POLICY_SUFFIX;
      final OmDBTenantInfo omDBTenantInfo = new OmDBTenantInfo(
          tenantName, bucketNamespaceName, accountNamespaceName,
          userPolicyGroupName, bucketPolicyGroupName);
      omMetadataManager.getTenantStateTable().addCacheEntry(
          new CacheKey<>(tenantName),
          new CacheValue<>(Optional.of(omDBTenantInfo), transactionLogIndex)
      );

      // Call OMMultiTenantManager
      tenant = ozoneManager.getMultiTenantManager().createTenant(tenantName);

      final String tenantDefaultPolicies =
          tenant.getTenantAccessPolicies().stream()
          .map(e->e.getPolicyID()) .collect(Collectors.joining(","));

      // Add to tenantPolicyTable
      omMetadataManager.getTenantPolicyTable().addCacheEntry(
          new CacheKey<>(userPolicyGroupName),
          new CacheValue<>(Optional.of(tenantDefaultPolicies),
              transactionLogIndex)
      );

      final String bucketPolicyId =
          bucketPolicyGroupName + OzoneConsts.DEFAULT_TENANT_POLICY_ID_SUFFIX;
      omMetadataManager.getTenantPolicyTable().addCacheEntry(
          new CacheKey<>(bucketPolicyGroupName),
          new CacheValue<>(Optional.of(bucketPolicyId), transactionLogIndex)
      );

      // Create volume
      acquiredUserLock = omMetadataManager.getLock().acquireWriteLock(USER_LOCK,
          owner);

      // TODO: Dedup with OMVolumeCreateRequest?
      OzoneManagerStorageProtos.PersistedUserVolumeInfo volumeList;
      OmVolumeArgs omVolumeArgs;
      // omVolumeArgs = OmVolumeArgs.getFromProtobuf(volumeInfo);
      // TODO: Refactor this with volumeInfo from preExecute
      final VolumeInfo volumeInfo =
          getOmRequest().getCreateVolumeRequest().getVolumeInfo();
      LOG.info("Vol mod time: {}", volumeInfo.getModificationTime()); // TODO
      omVolumeArgs = OmVolumeArgs.newBuilder()
          .setAdminName(owner)
          .setOwnerName(owner)
          .setVolume(dbVolumeKey)
          .setCreationTime(volumeInfo.getCreationTime())
          .setModificationTime(volumeInfo.getModificationTime())
          .build();
      omVolumeArgs.setObjectID(
          ozoneManager.getObjectIdFromTxId(transactionLogIndex));
      omVolumeArgs.setUpdateID(transactionLogIndex,
          ozoneManager.isRatisEnabled());

      // Audit volume creation
      auditMap = omVolumeArgs.toAuditMap();

      String dbUserKey = omMetadataManager.getUserKey(owner);
      volumeList = omMetadataManager.getUserTable().get(dbUserKey);
      volumeList = addVolumeToOwnerList(volumeList, dbVolumeKey, owner,
          ozoneManager.getMaxUserVolumeCount(), transactionLogIndex);
      createVolume(omMetadataManager, omVolumeArgs, volumeList, dbVolumeKey,
          dbUserKey, transactionLogIndex);
      LOG.debug("volume:{} successfully created", dbVolumeKey);

      omResponse.setCreateTenantResponse(
          CreateTenantResponse.newBuilder().setSuccess(true).build()
      );

      omClientResponse = new OMTenantCreateResponse(
          omResponse.build(),
          omVolumeArgs, volumeList,
          omDBTenantInfo, tenantDefaultPolicies, bucketPolicyId
      );
    } catch (IOException ex) {
      exception = ex;
      // Set response success flag to false
      omResponse.setCreateTenantResponse(
          CreateTenantResponse.newBuilder().setSuccess(false).build()
      );

      // Cleanup any state maintained by OMMultiTenantManager
      if (tenant != null) {
        try {
          ozoneManager.getMultiTenantManager().destroyTenant(tenant);
        } catch (Exception e) {
          // Ignore for now. Multi-Tenant Manager is responsible for
          // cleaning up stale state eventually.
        }
      }

      omClientResponse = new OMTenantCreateResponse(
          createErrorOMResponse(omResponse, ex));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
      if (acquiredUserLock) {
        omMetadataManager.getLock().releaseWriteLock(USER_LOCK, owner);
      }
      if (acquiredVolumeLock) {
        omMetadataManager.getLock().releaseWriteLock(VOLUME_LOCK, tenantName);
      }
    }

    // Perform audit logging
    auditMap.put(OzoneConsts.TENANT, tenantName);
    // Note auditMap contains volume creation info
    auditLog(ozoneManager.getAuditLogger(),
        buildAuditMessage(OMAction.CREATE_TENANT, auditMap, exception,
            getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.debug("Successfully created tenant {}", tenantName);
      // TODO: Metrics? see OMVolumeCreateRequest
    } else {
      LOG.error("Failed to create tenant {}", tenantName, exception);
      // TODO: Metrics?
    }
    return omClientResponse;
  }
}
