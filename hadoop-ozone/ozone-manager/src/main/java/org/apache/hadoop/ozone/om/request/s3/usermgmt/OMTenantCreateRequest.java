/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.s3.usermgmt;

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantCreateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.USER_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

/**
 * Handles OMTenantCreate request.
 */
// Ref: S3GetSecretRequest extends OMClientRequest
/*
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
    - Value: new OmTenantInfo for the tenant
      - tenantName: finance
      - bucketNamespaceName: finance
      - accountNamespaceName: finance
      - userPolicyGroupName: finance-Users
      - bucketPolicyGroupName: finance-Buckets
  - tenantPolicyTable: Generate default policies for the new tenant
    - K: finance-Users, V: finance-Users-default
    - K: finance-Buckets, V: finance-Buckets-default
  - Create volume finance (See OMVolumeCreateRequest)
  - Release VOLUME_LOCK write lock
  - Queue Ranger policy sync that pushes default policies:
      OMMultiTenantManager#createTenant
 */
public class OMTenantCreateRequest extends OMVolumeRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMTenantCreateRequest.class);

  public OMTenantCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    TenantCreateRequest request = getOmRequest().getTenantCreateRequest();
    String tenantName = request.getTenantName();

    // Check tenantName validity
    if (tenantName.contains(OzoneConsts.TENANT_NAME_USER_NAME_DELIMITER)) {
      throw new OMException("Invalid tenant name " + tenantName +
          ". Tenant name should not contain delimiter.",
          OMException.ResultCodes.INVALID_VOLUME_NAME);
    }

    // .setModificationTime(0)  // TODO: Set to current time

    // TODO: Also check validity for volume name. e.g. length
    //  depending on ozone-site.xml

    // TODO: Check
    return getOmRequest().toBuilder()
        .setUserInfo(getUserInfo()).build();  // TODO: pass mod time
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
    final String owner = getOmRequest().getUserInfo().getUserName();
    LOG.debug("hasUserInfo: {}, owner: {}",
        getOmRequest().hasUserInfo(), owner);

    Map<String, String> auditMap = new HashMap<>();

    // TODO: Audit volume creation, if any

    IOException exception = null;
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    TenantCreateRequest request = getOmRequest().getTenantCreateRequest();
    String tenantName = request.getTenantName();
    try {
      // Check ACL
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.CREATE,
            tenantName, null, null);
      }

      final String dbVolumeKey = tenantName;

      acquiredVolumeLock = omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, dbVolumeKey);

      // TODO: check -. ozone debug ldb --db=

      // Check volume existence
      // In this case, volume name is tenant name.
      if (omMetadataManager.getVolumeTable().isExist(dbVolumeKey)) {
        LOG.debug("volume:{} already exists", dbVolumeKey);
        throw new OMException("Volume already exists",
            OMException.ResultCodes.VOLUME_ALREADY_EXISTS);
      }

      // Check tenant existence in tenantStateTable

      // Add to tenantStateTable
      // Intentional redundant assignment for clarity. TODO: Clean up later
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

      tenant =
            ozoneManager.getMultiTenantManager().createTenant(tenantName);
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
      OzoneManagerStorageProtos.PersistedUserVolumeInfo volumeList = null;
      OmVolumeArgs omVolumeArgs;
      // omVolumeArgs = OmVolumeArgs.getFromProtobuf(volumeInfo);
      // TODO: Double check omVolumeArgs necessary fields for volume creation.
      omVolumeArgs = OmVolumeArgs.newBuilder()
          .setAdminName(owner)
          .setOwnerName(owner)
          .setVolume(dbVolumeKey)
//          .setModificationTime(0)  // TODO: Set to current time
          .build();
      omVolumeArgs.setObjectID(
          ozoneManager.getObjectIdFromTxId(transactionLogIndex));
      omVolumeArgs.setUpdateID(transactionLogIndex,
          ozoneManager.isRatisEnabled());

      String dbUserKey = omMetadataManager.getUserKey(owner);
      volumeList = omMetadataManager.getUserTable().get(dbUserKey);
      volumeList = addVolumeToOwnerList(volumeList, dbVolumeKey, owner,
          ozoneManager.getMaxUserVolumeCount(), transactionLogIndex);
      createVolume(omMetadataManager, omVolumeArgs, volumeList, dbVolumeKey,
          dbUserKey, transactionLogIndex);
      LOG.debug("volume:{} successfully created", dbVolumeKey);

      omResponse.setTenantCreateResponse(
          TenantCreateResponse.newBuilder().setSuccess(true).build()
      );

      omClientResponse = new OMTenantCreateResponse(
          omResponse.build(),
          omVolumeArgs, volumeList,
          omDBTenantInfo, tenantDefaultPolicies, bucketPolicyId
      );
    } catch (IOException ex) {
      exception = ex;
      // Set response success flag to false
      omResponse.setTenantCreateResponse(
          TenantCreateResponse.newBuilder().setSuccess(false).build()
      );
      // Cleanup any state maintained by OMMultiTenantManager.
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

    // Audit
    auditMap.put(OzoneConsts.TENANT, tenantName);
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.TENANT_CREATE, auditMap,
        exception, getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.debug("Successfully created tenant {}", tenantName);
    } else {
      LOG.error("Failed to create tenant {}", tenantName, exception);
    }
    return omClientResponse;
  }
}
