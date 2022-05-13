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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetS3SecretRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetS3SecretResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.S3_SECRET_LOCK;

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

    final SetS3SecretRequest request =
        getOmRequest().getSetS3SecretRequest();

    final String accessId = request.getAccessId();

    // First check accessId existence
    final OmDBAccessIdInfo accessIdInfo = omMetadataManager
        .getTenantAccessIdTable().get(accessId);

    if (accessIdInfo == null) {
      // Check (old) S3SecretTable
      if (omMetadataManager.getS3SecretTable().get(accessId) == null) {
        throw new OMException("accessId '" + accessId + "' not found.",
            OMException.ResultCodes.ACCESS_ID_NOT_FOUND);
      }
    }

    // Secret should not be empty
    final String secretKey = request.getSecretKey();
    if (StringUtils.isEmpty(secretKey)) {
      throw new OMException("Secret key should not be empty",
              OMException.ResultCodes.INVALID_REQUEST);
    }

    if (secretKey.length() < OzoneConsts.S3_SECRET_KEY_MIN_LENGTH) {
      throw new OMException("Secret key length should be at least " +
          OzoneConsts.S3_SECRET_KEY_MIN_LENGTH + " characters",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    // TODO: Check if secretKey matches other requirements? e.g. combination

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
      if (!ozoneManager.getMultiTenantManager()
          .isUserAccessIdPrincipalOrTenantAdmin(accessId, ugi)) {
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

    final SetS3SecretRequest request = getOmRequest().getSetS3SecretRequest();
    final String accessId = request.getAccessId();
    final String secretKey = request.getSecretKey();

    try {
      acquiredLock = omMetadataManager.getLock().acquireWriteLock(
          S3_SECRET_LOCK, accessId);

      // Intentionally set to final so they can only be set once.
      final S3SecretValue newS3SecretValue;

      // Update legacy S3SecretTable, if the accessId entry exists
      if (omMetadataManager.getS3SecretTable().get(accessId) != null) {
        // accessId found in S3SecretTable. Update S3SecretTable
        LOG.debug("Updating S3SecretTable cache entry");
        // Update S3SecretTable cache entry in this case
        newS3SecretValue = new S3SecretValue(accessId, secretKey);

        omMetadataManager.getS3SecretTable().addCacheEntry(
            new CacheKey<>(accessId),
            new CacheValue<>(Optional.of(newS3SecretValue),
                transactionLogIndex));
      } else {
        // If S3SecretTable is not updated, throw ACCESS_ID_NOT_FOUND exception.
        throw new OMException("accessId '" + accessId + "' not found.",
            OMException.ResultCodes.ACCESS_ID_NOT_FOUND);
      }

      // Compose response
      final SetS3SecretResponse.Builder setSecretResponse =
          SetS3SecretResponse.newBuilder()
              .setAccessId(accessId)
              .setSecretKey(secretKey);

      omClientResponse = new OMSetSecretResponse(accessId, newS3SecretValue,
          omResponse.setSetS3SecretResponse(setSecretResponse).build());

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
