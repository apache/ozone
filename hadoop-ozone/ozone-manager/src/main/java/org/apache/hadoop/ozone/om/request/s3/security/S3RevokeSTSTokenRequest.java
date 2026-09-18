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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.ACCESS_ID_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INTERNAL_ERROR;
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
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.S3RevokeSTSTokenResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RevokeSTSTokenRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles S3RevokeSTSTokenRequest request.
 *
 * <p>The client submits {@link RevokeSTSTokenRequest} with {@code originalAccessKeyId} only. On the
 * leader, {@code preExecute} captures the revocation cutoff in {@code revocationTimeMillis} and
 * replicates the updated request through Ratis so every OM applies the same cutoff.</p>
 *
 * <p>This request records a revocation cutoff for the given {@code originalAccessKeyId} in the
 * {@code s3RevokedStsTokenTable}. Subsequent S3 requests authenticated with STS tokens whose
 * {@code creationTime} is strictly before the cutoff will be rejected when the revocation state
 * has propagated.</p>
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
    final RevokeSTSTokenRequest revokeReq = omRequest.getRevokeSTSTokenRequest();
    validateRevokeRequestFields(revokeReq);

    // Use the original (long-lived) access key ID from the request and enforce
    // the same permission model that is used for S3 secret
    // operations (get/set/revoke). Only the owner of the original access
    // key (i.e. the creator of the STS token) or an S3 / tenant admin is allowed
    // to revoke its temporary STS credentials.
    final String originalAccessKeyId = revokeReq.getOriginalAccessKeyId();

    final UserGroupInformation ugi = S3SecretRequestHelper.getOrCreateUgi(originalAccessKeyId);
    S3SecretRequestHelper.checkAccessIdSecretOpPermission(ozoneManager, ugi, originalAccessKeyId);

    if (!ozoneManager.getS3SecretManager().hasS3Secret(originalAccessKeyId)) {
      throw new OMException("originalAccessKeyId does not exist: " + originalAccessKeyId, ACCESS_ID_NOT_FOUND);
    }

    final long revocationTimeMillis = CLOCK.millis();
    final RevokeSTSTokenRequest updatedRevokeReq = revokeReq.toBuilder()
        .setRevocationTimeMillis(revocationTimeMillis)
        .build();

    return omRequest.toBuilder()
        .setRevokeSTSTokenRequest(updatedRevokeReq)
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    IOException exception = null;
    OMClientResponse omClientResponse;
    final Map<String, String> auditMap = new HashMap<>();

    try {
      final RevokeSTSTokenRequest revokeReq = validateReplicatedRevokeRequestFields(getOmRequest());
      final String originalAccessKeyId = revokeReq.getOriginalAccessKeyId();
      auditMap.put(OzoneConsts.S3_REVOKESTSTOKEN_USER, originalAccessKeyId);
      final long revocationTimeMillis = revokeReq.getRevocationTimeMillis();

      // All actual DB mutations are done in the response's addToDBBatch().
      omClientResponse = new S3RevokeSTSTokenResponse(originalAccessKeyId, revocationTimeMillis, omResponse.build());

      // Update the cache immediately so subsequent validation checks see the revocation
      ozoneManager.getMetadataManager().getS3RevokedStsTokenTable().addCacheEntry(
          new CacheKey<>(originalAccessKeyId), CacheValue.get(context.getIndex(), revocationTimeMillis));

      LOG.info(
          "Marked STS tokens as revoked for originalAccessKeyId={} with cutoff time {}.",
          originalAccessKeyId, revocationTimeMillis);
    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new S3RevokeSTSTokenResponse(null, 0L, createErrorOMResponse(omResponse, ex));
    }

    // Audit log
    markForAudit(
        ozoneManager.getAuditLogger(), buildAuditMessage(
            OMAction.REVOKE_STS_TOKEN, auditMap, exception, getOmRequest().getUserInfo()));
    return omClientResponse;
  }

  private static void validateRevokeRequestFields(RevokeSTSTokenRequest revokeReq) throws OMException {
    final String originalAccessKeyId = revokeReq.getOriginalAccessKeyId();
    if (StringUtils.isEmpty(originalAccessKeyId)) {
      throw new OMException("originalAccessKeyId is required for STS token revocation", INVALID_REQUEST);
    }
    if (revokeReq.hasRevocationTimeMillis()) {
      throw new OMException("revocationTimeMillis must not be set by client", INVALID_REQUEST);
    }
  }

  private static RevokeSTSTokenRequest validateReplicatedRevokeRequestFields(OMRequest omRequest) throws OMException {
    if (!omRequest.hasRevokeSTSTokenRequest()) {
      throw new OMException("revokeSTSTokenRequest is required for STS token revocation", INTERNAL_ERROR);
    }
    final RevokeSTSTokenRequest revokeReq = omRequest.getRevokeSTSTokenRequest();
    if (StringUtils.isEmpty(revokeReq.getOriginalAccessKeyId())) {
      throw new OMException("originalAccessKeyId is required for STS token revocation", INTERNAL_ERROR);
    }
    if (!revokeReq.hasRevocationTimeMillis()) {
      throw new OMException("revocationTimeMillis is required for STS token revocation", INTERNAL_ERROR);
    }
    return revokeReq;
  }
}
