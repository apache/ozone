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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;

import java.io.IOException;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.S3STSUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.S3RevokeSTSTokenResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.STSSecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles S3RevokeSTSTokenRequest request.
 *
 * <p>This request marks an STS credential pair as revoked by inserting a key
 * ({@code tempAccessKeyId|originalAccessKeyId}) into the {@code s3RevokedStsTokenTable}. Subsequent S3 requests
 * authenticated with the same STS credentials will be rejected when the revocation state has propagated.</p>
 *
 * <p>{@code tempAccessKeyId} comes directly from the request, so {@code preExecute} validates it against the
 * expected STS format.
 */
public class S3RevokeSTSTokenRequest extends OMClientRequest {

  private static final Logger LOG = LoggerFactory.getLogger(S3RevokeSTSTokenRequest.class);
  private static final Clock CLOCK = Clock.system(ZoneOffset.UTC);

  public S3RevokeSTSTokenRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OMRequest omRequest = super.preExecute(ozoneManager);
    final OzoneManagerProtocolProtos.RevokeSTSTokenRequest revokeReq =
        omRequest.getRevokeSTSTokenRequest();
    validateRevokeRequestFields(revokeReq);

    // Use the original (long-lived) access key ID from the request and enforce
    // the same permission model that is used for S3 secret
    // operations (get/set/revoke). Only the owner of the original access
    // key (i.e. the creator of the STS token) or an S3 / tenant admin is allowed
    // to revoke its temporary STS credentials.
    final String originalAccessKeyId = revokeReq.getOriginalAccessKeyId();

    final UserGroupInformation ugi = S3SecretRequestHelper.getOrCreateUgi(originalAccessKeyId);
    S3SecretRequestHelper.checkAccessIdSecretOpPermission(ozoneManager, ugi, originalAccessKeyId);

    return omRequest;
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());

    final OzoneManagerProtocolProtos.RevokeSTSTokenRequest revokeReq = getOmRequest().getRevokeSTSTokenRequest();
    final String originalAccessKeyId = revokeReq.getOriginalAccessKeyId();
    final String tempAccessKeyId = revokeReq.getTempAccessKeyId();
    final String revokedStsTokenKey = STSSecurityUtil.buildRevokedStsTokenKey(tempAccessKeyId, originalAccessKeyId);

    // All actual DB mutations are done in the response's addToDBBatch().
    final OMClientResponse omClientResponse = new S3RevokeSTSTokenResponse(revokedStsTokenKey, omResponse.build());

    // Audit log
    final Map<String, String> auditMap = new HashMap<>();
    final OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    auditMap.put(OzoneConsts.S3_REVOKESTSTOKEN_USER, userInfo.getUserName());
    auditMap.put(OzoneConsts.S3_REVOKESTSTOKEN_ORIGINAL_ACCESS_KEY_ID, originalAccessKeyId);
    auditMap.put(OzoneConsts.S3_REVOKESTSTOKEN_TEMP_ACCESS_KEY_ID, tempAccessKeyId);
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.REVOKE_STS_TOKEN, auditMap, null, userInfo));

    // Update the cache immediately so subsequent validation checks see the revocation
    ozoneManager.getMetadataManager().getS3RevokedStsTokenTable().addCacheEntry(
        new CacheKey<>(revokedStsTokenKey), CacheValue.get(context.getIndex(), CLOCK.millis()));

    LOG.info(
        "Marked STS token as revoked for originalAccessKeyId={}, tempAccessKeyId={}.", originalAccessKeyId,
        tempAccessKeyId);
    return omClientResponse;
  }

  private static void validateRevokeRequestFields(OzoneManagerProtocolProtos.RevokeSTSTokenRequest revokeReq)
      throws OMException {
    final String originalAccessKeyId = revokeReq.getOriginalAccessKeyId();
    if (StringUtils.isEmpty(originalAccessKeyId)) {
      throw new OMException("originalAccessKeyId is required for STS token revocation", INVALID_REQUEST);
    }
    if (originalAccessKeyId.length() >= OzoneConsts.OZONE_MAXIMUM_ACCESS_ID_LENGTH) {
      throw new OMException("originalAccessKeyId length is invalid: " + originalAccessKeyId.length(), INVALID_REQUEST);
    }
    // Validate the tempAccessKeyId
    S3STSUtils.validateTempAccessKeyId(revokeReq.getTempAccessKeyId());
  }
}
