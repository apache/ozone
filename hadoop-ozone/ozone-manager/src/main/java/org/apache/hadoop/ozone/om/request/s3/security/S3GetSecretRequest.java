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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Optional;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.S3GetSecretResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3SecretRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3SecretResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateGetS3SecretRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Secret;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.S3_SECRET_LOCK;

/**
 * Handles GetS3Secret request.
 */
public class S3GetSecretRequest extends OMClientRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3GetSecretRequest.class);

  public S3GetSecretRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    GetS3SecretRequest s3GetSecretRequest =
        getOmRequest().getGetS3SecretRequest();

    // Generate S3 Secret to be used by OM quorum.
    String kerberosID = s3GetSecretRequest.getKerberosID();

    final UserGroupInformation ugi = ProtobufRpcEngine.Server.getRemoteUser();
    final String username = ugi.getUserName();
    // Permission check. Users need to be themselves or have admin privilege
    if (!username.equals(kerberosID) &&
        !ozoneManager.isAdmin(ugi)) {
      throw new OMException("Requested user name '" + kerberosID +
          "' doesn't match current user '" + username +
          "', nor does current user has administrator privilege.",
          OMException.ResultCodes.USER_MISMATCH);
    }

    // Generate secret. Used only when doesn't the kerberosID entry doesn't
    //  exist in DB, discarded otherwise.
    String s3Secret = DigestUtils.sha256Hex(OmUtils.getSHADigest());

    UpdateGetS3SecretRequest updateGetS3SecretRequest =
        UpdateGetS3SecretRequest.newBuilder()
            .setAwsSecret(s3Secret)
            .setKerberosID(kerberosID).build();

    // Client issues GetS3Secret request, when received by OM leader
    // it will generate s3Secret. Original GetS3Secret request is
    // converted to UpdateGetS3Secret request with the generated token
    // information. This updated request will be submitted to Ratis. In this
    // way S3Secret created by leader, will be replicated across all
    // OMs. With this approach, original GetS3Secret request from
    // client does not need any proto changes.
    OMRequest.Builder omRequest = OMRequest.newBuilder()
        .setUserInfo(getUserInfo())
        .setUpdateGetS3SecretRequest(updateGetS3SecretRequest)
        .setCmdType(getOmRequest().getCmdType())
        .setClientId(getOmRequest().getClientId());

    if (getOmRequest().hasTraceID()) {
      omRequest.setTraceID(getOmRequest().getTraceID());
    }

    return omRequest.build();

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
    UpdateGetS3SecretRequest updateGetS3SecretRequest =
        getOmRequest().getUpdateGetS3SecretRequest();
    String kerberosID = updateGetS3SecretRequest.getKerberosID();
    try {
      String awsSecret = updateGetS3SecretRequest.getAwsSecret();
      // Note: We use the same S3_SECRET_LOCK for TenantAccessIdTable.
      acquiredLock = omMetadataManager.getLock()
          .acquireWriteLock(S3_SECRET_LOCK, kerberosID);

      // Check multi-tenant table first: tenantAccessIdTable
      final S3SecretValue assignS3SecretValue;
      final OmDBAccessIdInfo omDBAccessIdInfo =
          omMetadataManager.getTenantAccessIdTable().get(kerberosID);
      if (omDBAccessIdInfo == null) {
        // Not found in TenantAccessIdTable. Fallback to S3SecretTable.
        final S3SecretValue s3SecretValue =
            omMetadataManager.getS3SecretTable().get(kerberosID);

        if (s3SecretValue == null) {
          // Still not found in S3SecretTable. Will add new entry in this case.
          assignS3SecretValue = new S3SecretValue(kerberosID, awsSecret);
          // Add cache entry first.
          omMetadataManager.getS3SecretTable().addCacheEntry(
              new CacheKey<>(kerberosID),
              new CacheValue<>(
                  Optional.of(assignS3SecretValue), transactionLogIndex));
        } else {
          // Found in S3SecretTable.
          awsSecret = s3SecretValue.getAwsSecret();
          assignS3SecretValue = null;
        }
      } else {
        // Found in TenantAccessIdTable.
        awsSecret = omDBAccessIdInfo.getSharedSecret();
        assignS3SecretValue = null;
      }

      // Compose response
      final GetS3SecretResponse.Builder getS3SecretResponse =
          GetS3SecretResponse.newBuilder().setS3Secret(
              S3Secret.newBuilder()
                  .setAwsSecret(awsSecret)
                  .setKerberosID(kerberosID));
      // If entry exists, assignS3SecretValue will be null,
      // so we won't overwrite the entry.
      omClientResponse = new S3GetSecretResponse(assignS3SecretValue,
          omResponse.setGetS3SecretResponse(getS3SecretResponse).build());

    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new S3GetSecretResponse(null,
          createErrorOMResponse(omResponse, ex));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(S3_SECRET_LOCK,
            kerberosID);
      }
    }


    Map<String, String> auditMap = new HashMap<>();
    auditMap.put(OzoneConsts.S3_GETSECRET_USER, kerberosID);

    // audit log
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.GET_S3_SECRET, auditMap,
        exception, getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.debug("Successfully generated secret for accessKey '{}'", kerberosID);
    } else {
      LOG.error("Failed to generate secret for accessKey '{}': {}", kerberosID,
          exception);
    }
    return omClientResponse;
  }

}
