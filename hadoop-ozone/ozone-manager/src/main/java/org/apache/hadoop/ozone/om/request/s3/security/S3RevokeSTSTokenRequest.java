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
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.S3RevokeSTSTokenResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.STSSecurityUtil;
import org.apache.hadoop.ozone.security.STSTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles S3RevokeSTSTokenRequest request.
 *
 * <p>This request marks an STS session token as revoked by inserting
 * it into the {@code s3RevokedStsTokenTable}. Subsequent S3 requests
 * authenticated with the same STS session token will be rejected when the
 * revocation state has propagated.</p>
 */
public class S3RevokeSTSTokenRequest extends OMClientRequest {

  private static final Logger LOG = LoggerFactory.getLogger(S3RevokeSTSTokenRequest.class);
  private static final Clock CLOCK = Clock.system(ZoneOffset.UTC);

  public S3RevokeSTSTokenRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OzoneManagerProtocolProtos.RevokeSTSTokenRequest revokeReq =
        getOmRequest().getRevokeSTSTokenRequest();

    // Get the original (long-lived) access key id from the session token
    // and enforce the same permission model that is used for S3 secret
    // operations (get/set/revoke). Only the owner of the original access
    // key (i.e. the creator of the STS token) or an S3 / tenant admin is allowed
    // to revoke its temporary STS credentials.
    final String sessionToken = revokeReq.getSessionToken();
    final STSTokenIdentifier stsTokenIdentifier = STSSecurityUtil.constructValidateAndDecryptSTSToken(
        sessionToken, ozoneManager.getSecretKeyClient(), CLOCK);
    final String originalAccessKeyId = stsTokenIdentifier.getOriginalAccessKeyId();

    final OzoneManagerProtocolProtos.UserInfo userInfo = getUserInfo();
    final UserGroupInformation ugi = S3SecretRequestHelper.getOrCreateUgi(originalAccessKeyId);
    S3SecretRequestHelper.checkAccessIdSecretOpPermission(ozoneManager, ugi, originalAccessKeyId);

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
    final String sessionToken = revokeReq.getSessionToken();

    // All actual DB mutations are done in the response's addToDBBatch().
    final OMClientResponse omClientResponse = new S3RevokeSTSTokenResponse(
        sessionToken, omResponse.build());

    // Audit log
    final Map<String, String> auditMap = new HashMap<>();
    final OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    auditMap.put(OzoneConsts.S3_REVOKESTSTOKEN_USER, userInfo.getUserName());
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.REVOKE_STS_TOKEN, auditMap, null, userInfo));

    // Update the cache immediately so subsequent validation checks see the revocation
    ozoneManager.getMetadataManager().getS3RevokedStsTokenTable().addCacheEntry(
        new CacheKey<>(sessionToken), CacheValue.get(context.getIndex(), CLOCK.millis()));

    LOG.info("Marked STS session token '{}' as revoked.", sessionToken);
    return omClientResponse;
  }
}
