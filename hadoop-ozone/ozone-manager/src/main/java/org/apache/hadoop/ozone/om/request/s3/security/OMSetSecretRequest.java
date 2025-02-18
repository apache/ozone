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

package org.apache.hadoop.ozone.om.request.s3.security;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
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
      if (!ozoneManager.getS3SecretManager().hasS3Secret(accessId)) {
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

    final UserGroupInformation ugi =
        S3SecretRequestHelper.getOrCreateUgi(accessId);
    // Permission check
    S3SecretRequestHelper.checkAccessIdSecretOpPermission(
        ozoneManager, ugi, accessId);

    return getOmRequest();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    OMClientResponse omClientResponse = null;
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    IOException exception = null;

    final SetS3SecretRequest request = getOmRequest().getSetS3SecretRequest();
    final String accessId = request.getAccessId();
    final String secretKey = request.getSecretKey();

    try {
      omClientResponse = ozoneManager.getS3SecretManager()
          .doUnderLock(accessId, s3SecretManager -> {

            // Update legacy S3SecretTable, if the accessId entry exists
            if (!s3SecretManager.hasS3Secret(accessId)) {
              // If S3SecretTable is not updated,
              // throw ACCESS_ID_NOT_FOUND exception.
              throw new OMException("accessId '" + accessId + "' not found.",
                  OMException.ResultCodes.ACCESS_ID_NOT_FOUND);
            }

            // Update S3SecretTable cache entry in this case
            // Set the transactionLogIndex to be used for updating.
            final S3SecretValue newS3SecretValue = S3SecretValue.of(accessId, secretKey, context.getIndex());
            s3SecretManager.updateCache(accessId, newS3SecretValue);

            // Compose response
            final SetS3SecretResponse.Builder setSecretResponse =
                SetS3SecretResponse.newBuilder()
                    .setAccessId(accessId)
                    .setSecretKey(secretKey);

            return new OMSetSecretResponse(accessId, newS3SecretValue,
                s3SecretManager,
                omResponse.setSetS3SecretResponse(setSecretResponse).build());
          });
    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMSetSecretResponse(
          createErrorOMResponse(omResponse, ex));
    }

    final Map<String, String> auditMap = new HashMap<>();
    auditMap.put(OzoneConsts.S3_SETSECRET_USER, accessId);

    // audit log
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.SET_S3_SECRET, auditMap,
        exception, getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.debug("Success: SetSecret for accessKey '{}'", accessId);
    } else {
      LOG.error("Failed to SetSecret for accessKey '{}'", accessId, exception);
    }
    return omClientResponse;
  }

}
