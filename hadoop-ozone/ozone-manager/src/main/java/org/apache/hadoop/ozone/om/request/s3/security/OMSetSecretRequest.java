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

package org.apache.hadoop.ozone.om.request.s3.security;

import com.google.common.base.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.OMSetSecretResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.S3_SECRET_LOCK;
import static org.apache.hadoop.ozone.om.request.s3.tenant.OMTenantRequestHelper.isUserAccessIdPrincipalOrTenantAdmin;

/**
 * Handles SetSecret request.
 */
public class OMSetSecretRequest extends OMClientRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMSetSecretRequest.class);

  public OMSetSecretRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OMMetadataManager omMetadataManager =
        ozoneManager.getMetadataManager();

    final SetSecretRequest setSecretRequest =
        getOmRequest().getSetSecretRequest();

    final String accessId = setSecretRequest.getAccessId();

    // First check accessId existence
    final OmDBAccessIdInfo accessIdInfo = omMetadataManager
        .getTenantAccessIdTable().get(accessId);

    if (accessIdInfo == null) {
      // Check (old) S3SecretTable
      if (omMetadataManager.getS3SecretTable().get(accessId) == null) {
        throw new OMException("accessId '" + accessId + "' not found.",
            OMException.ResultCodes.ACCESSID_NOT_FOUND);
      }
    }

    // Secret should not be empty
    final String secretKey = setSecretRequest.getSecretKey();
    if (StringUtils.isEmpty(secretKey)) {
      throw new OMException("Secret key should not be empty",
              OMException.ResultCodes.INVALID_REQUEST);
    }

    final UserGroupInformation ugi = ProtobufRpcEngine.Server.getRemoteUser();
    final String username = ugi.getUserName();

    // Permission check. To pass the check, any one of the following conditions
    // shall be satisfied:
    // 1. username matches accessId exactly
    // 2. user is an OM admin
    // 3. user is assigned to a tenant under this accessId
    // 4. user is an admin of the tenant where the accessId is assigned

    if (!username.equals(accessId) && !ozoneManager.isAdmin(ugi)) {
      // Attempt to retrieve tenant info using the accessId
      if (!isUserAccessIdPrincipalOrTenantAdmin(ozoneManager, accessId, ugi)) {
        throw new OMException("Permission denied. Requested accessId '" +
                accessId + "' and user doesn't satisfy any of:\n" +
                "1) accessId match current username: '" + username + "';\n" +
                "2) is an OM admin;\n" +
                "3) user is assigned to a tenant under this accessId;\n" +
                "4) user is an admin of the tenant where the accessId is " +
                "assigned", OMException.ResultCodes.PERMISSION_DENIED);
      }
    }

    return getOmRequest();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    OMClientResponse omClientResponse = null;
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    boolean acquiredLock = false;
    IOException exception = null;
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    final SetSecretRequest setSecretRequest =
            getOmRequest().getSetSecretRequest();
    final String accessId = setSecretRequest.getAccessId();
    final String secretKey = setSecretRequest.getSecretKey();

    try {
      acquiredLock = omMetadataManager.getLock().acquireWriteLock(
              S3_SECRET_LOCK, accessId);

      // Intentionally set to final so they can only be set once.
      final S3SecretValue newS3SecretValue;
      final OmDBAccessIdInfo newDBAccessIdInfo;

      // Get accessId entry from multi-tenant TenantAccessIdTable
      final OmDBAccessIdInfo omDBAccessIdInfo =
          omMetadataManager.getTenantAccessIdTable().get(accessId);

      // Check accessId existence in TenantAccessIdTable
      if (omDBAccessIdInfo == null) {
        // accessId doesn't exist in TenantAccessIdTable, check S3SecretTable
        if (omMetadataManager.getS3SecretTable().get(accessId) == null) {
          throw new OMException("accessId '" + accessId + "' not found.",
              OMException.ResultCodes.ACCESSID_NOT_FOUND);
        }

        // accessId found in S3SecretTable. Update S3SecretTable
        LOG.debug("Updating S3SecretTable cache entry");
        // Update S3SecretTable cache entry in this case
        newS3SecretValue = new S3SecretValue(accessId, secretKey);
        newDBAccessIdInfo = null;

        omMetadataManager.getS3SecretTable().addCacheEntry(
            new CacheKey<>(accessId),
            new CacheValue<>(Optional.of(newS3SecretValue),
                transactionLogIndex));

      } else {

        // Update TenantAccessIdTable
        // Build new OmDBAccessIdInfo with updated secret
        LOG.debug("Updating TenantAccessIdTable cache entry");
        newS3SecretValue = null;
        newDBAccessIdInfo = new OmDBAccessIdInfo.Builder()
            .setTenantId(omDBAccessIdInfo.getTenantName())
            .setKerberosPrincipal(omDBAccessIdInfo.getUserPrincipal())
            .setSharedSecret(secretKey)
            .setIsAdmin(omDBAccessIdInfo.getIsAdmin())
            .setIsDelegatedAdmin(omDBAccessIdInfo.getIsDelegatedAdmin())
            .build();

        // Update TenantAccessIdTable cache entry
        omMetadataManager.getTenantAccessIdTable().addCacheEntry(
            new CacheKey<>(accessId),
            new CacheValue<>(Optional.of(newDBAccessIdInfo),
                transactionLogIndex));
      }

      // Compose response
      final SetSecretResponse.Builder setSecretResponse =
          SetSecretResponse.newBuilder()
              .setAccessId(accessId)
              .setSecretKey(secretKey);

      omClientResponse = new OMSetSecretResponse(accessId,
          newDBAccessIdInfo, newS3SecretValue,
          omResponse.setSetSecretResponse(setSecretResponse).build());

    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMSetSecretResponse(
          createErrorOMResponse(omResponse, ex));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(S3_SECRET_LOCK,
            accessId);
      }
    }

    final Map<String, String> auditMap = new HashMap<>();
    auditMap.put(OzoneConsts.S3_SETSECRET_USER, accessId);

    // audit log
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.SET_S3_SECRET, auditMap,
        exception, getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.debug("Success: SetSecret for accessKey '{}'", accessId);
    } else {
      LOG.error("Failed to SetSecret for accessKey '{}': {}",
          accessId, exception);
    }
    return omClientResponse;
  }

}
