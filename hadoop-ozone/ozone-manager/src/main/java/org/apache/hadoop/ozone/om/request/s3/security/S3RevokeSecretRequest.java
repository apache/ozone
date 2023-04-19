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

import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.S3RevokeSecretResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RevokeS3SecretRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles RevokeS3Secret request.
 */
public class S3RevokeSecretRequest extends OMClientRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3RevokeSecretRequest.class);

  public S3RevokeSecretRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final RevokeS3SecretRequest s3RevokeSecretRequest =
        getOmRequest().getRevokeS3SecretRequest();
    final String accessId = s3RevokeSecretRequest.getKerberosID();
    final UserGroupInformation ugi = ProtobufRpcEngine.Server.getRemoteUser();
    // Permission check
    S3SecretRequestHelper.checkAccessIdSecretOpPermission(
        ozoneManager, ugi, accessId);

    final RevokeS3SecretRequest revokeS3SecretRequest =
            RevokeS3SecretRequest.newBuilder()
                    .setKerberosID(accessId).build();

    OMRequest.Builder omRequest = OMRequest.newBuilder()
        .setRevokeS3SecretRequest(revokeS3SecretRequest)
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
    OMResponse.Builder omResponse =
            OmResponseUtil.getOMResponseBuilder(getOmRequest());
    IOException exception = null;

    final RevokeS3SecretRequest revokeS3SecretRequest =
        getOmRequest().getRevokeS3SecretRequest();
    String kerberosID = revokeS3SecretRequest.getKerberosID();
    try {
      omClientResponse = ozoneManager.getS3SecretManager()
          .doUnderLock(kerberosID, s3SecretManager -> {
            // Remove if entry exists in table
            if (s3SecretManager.hasS3Secret(kerberosID)) {
              // Invalid entry in table cache immediately
              s3SecretManager.invalidateCacheEntry(kerberosID,
                  transactionLogIndex);
              return new S3RevokeSecretResponse(kerberosID,
                  s3SecretManager,
                  omResponse.setStatus(Status.OK).build());
            } else {
              return new S3RevokeSecretResponse(null,
                  s3SecretManager,
                  omResponse.setStatus(Status.S3_SECRET_NOT_FOUND).build());
            }
          });
    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new S3RevokeSecretResponse(null,
          ozoneManager.getS3SecretManager(),
          createErrorOMResponse(omResponse, ex));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
    }

    Map<String, String> auditMap = new HashMap<>();
    auditMap.put(OzoneConsts.S3_REVOKESECRET_USER, kerberosID);
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.REVOKE_S3_SECRET, auditMap,
        exception, getOmRequest().getUserInfo()));

    if (exception == null) {
      if (omResponse.getStatus() == Status.OK) {
        LOG.info("Secret for {} is revoked.", kerberosID);
      } else {
        LOG.info("Secret for {} doesn't exist.", kerberosID);
      }
    } else {
      LOG.error("Error when revoking secret for {}.", kerberosID, exception);
    }
    return omClientResponse;
  }
}
