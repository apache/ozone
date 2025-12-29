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
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.S3RevokeSTSTokenResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles S3RevokeSTSTokenRequest request.
 *
 * <p>This request marks an STS temporary access key id as revoked by inserting
 * it into the {@code s3RevokedStsTokenTable}. Subsequent S3 requests
 * authenticated with the same STS access key id will be rejected when the
 * revocation state has propagated.</p>
 */
public class S3RevokeSTSTokenRequest extends OMClientRequest {

  private static final Logger LOG = LoggerFactory.getLogger(S3RevokeSTSTokenRequest.class);

  public S3RevokeSTSTokenRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OzoneManagerProtocolProtos.RevokeSTSTokenRequest revokeReq =
        getOmRequest().getRevokeSTSTokenRequest();

    // Only S3/Ozone admins can revoke STS tokens by temporary access key ID.
    final OzoneManagerProtocolProtos.UserInfo userInfo = getUserInfo();
    final UserGroupInformation ugi = S3SecretRequestHelper.getOrCreateUgi(userInfo.getUserName());
    if (!ozoneManager.isS3Admin(ugi)) {
      throw new OMException("Only S3/Ozone admins can revoke STS tokens.", OMException.ResultCodes.PERMISSION_DENIED);
    }

    final OMRequest.Builder omRequest = OMRequest.newBuilder()
        .setRevokeSTSTokenRequest(revokeReq)
        .setCmdType(getOmRequest().getCmdType())
        .setClientId(getOmRequest().getClientId())
        .setUserInfo(userInfo);

    if (getOmRequest().hasTraceID()) {
      omRequest.setTraceID(getOmRequest().getTraceID());
    }

    return omRequest.build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());

    final OzoneManagerProtocolProtos.RevokeSTSTokenRequest revokeReq = getOmRequest().getRevokeSTSTokenRequest();
    final String accessKeyId = revokeReq.getAccessKeyId();

    // All actual DB mutations are done in the response's addToDBBatch().
    final OMClientResponse omClientResponse = new S3RevokeSTSTokenResponse(
        accessKeyId, omResponse.build());

    // Audit log
    final Map<String, String> auditMap = new HashMap<>();
    auditMap.put(OzoneConsts.S3_REVOKESTSTOKEN_USER, getOmRequest().getUserInfo().getUserName());
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.REVOKE_STS_TOKEN, auditMap, null, getOmRequest().getUserInfo()));

    LOG.info("Marked STS temporary access key '{}' as revoked.", accessKeyId);
    return omClientResponse;
  }
}
