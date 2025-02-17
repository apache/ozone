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

package org.apache.hadoop.ozone.om.request.security;

import static org.apache.hadoop.ozone.om.OzoneManagerUtils.buildTokenAuditMap;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.security.OMRenewDelegationTokenResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenewDelegationTokenResponseProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateRenewDelegationTokenRequest;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle RenewDelegationToken Request.
 */
public class OMRenewDelegationTokenRequest extends OMClientRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMRenewDelegationTokenRequest.class);

  public OMRenewDelegationTokenRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    // We need to populate user info in our request object.
    OMRequest request = super.preExecute(ozoneManager);

    RenewDelegationTokenRequestProto renewDelegationTokenRequest =
        request.getRenewDelegationTokenRequest();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    Map<String, String> auditMap = null;

    long renewTime;
    try {
      Token<OzoneTokenIdentifier> token = OMPBHelper.tokenFromProto(
          renewDelegationTokenRequest.getToken());
      auditMap = buildTokenAuditMap(token);

      OzoneTokenIdentifier ozoneTokenIdentifier = OzoneTokenIdentifier.
          readProtoBuf(token.getIdentifier());
      auditMap.put(OzoneConsts.DELEGATION_TOKEN_RENEWER,
          ozoneTokenIdentifier.getRenewer() == null ? "" :
              ozoneTokenIdentifier.getRenewer().toString());

      // Call OM to renew token
      renewTime = ozoneManager.renewDelegationToken(token);
    } catch (IOException ioe) {
      markForAudit(auditLogger,
          buildAuditMessage(OMAction.RENEW_DELEGATION_TOKEN, auditMap, ioe,
              request.getUserInfo()));
      throw ioe;
    }

    RenewDelegationTokenResponseProto.Builder renewResponse =
        RenewDelegationTokenResponseProto.newBuilder();

    renewResponse.setResponse(
            org.apache.hadoop.ozone.security.proto.SecurityProtos
        .RenewDelegationTokenResponseProto.newBuilder()
        .setNewExpiryTime(renewTime));


    // Client issues RenewDelegationToken request, when received by OM leader
    // it will renew the token. Original RenewDelegationToken request is
    // converted to UpdateRenewDelegationToken request with the token and renew
    // information. This updated request will be submitted to Ratis. In this
    // way delegation token renewd by leader, will be replicated across all
    // OMs. With this approach, original RenewDelegationToken request from
    // client does not need any proto changes.

    // Create UpdateRenewDelegationTokenRequest with original request and
    // expiry time.
    OMRequest.Builder omRequest = OMRequest.newBuilder()
        .setUserInfo(getUserInfo())
        .setUpdatedRenewDelegationTokenRequest(
            UpdateRenewDelegationTokenRequest.newBuilder()
                .setRenewDelegationTokenRequest(renewDelegationTokenRequest)
                .setRenewDelegationTokenResponse(renewResponse))
        .setCmdType(getOmRequest().getCmdType())
        .setClientId(getOmRequest().getClientId());

    if (getOmRequest().hasTraceID()) {
      omRequest.setTraceID(getOmRequest().getTraceID());
    }

    return omRequest.build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {

    UpdateRenewDelegationTokenRequest updateRenewDelegationTokenRequest =
        getOmRequest().getUpdatedRenewDelegationTokenRequest();

    Token<OzoneTokenIdentifier> ozoneTokenIdentifierToken =
        OMPBHelper.tokenFromProto(updateRenewDelegationTokenRequest
            .getRenewDelegationTokenRequest().getToken());

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    Map<String, String> auditMap =
        buildTokenAuditMap(ozoneTokenIdentifierToken);

    long renewTime = updateRenewDelegationTokenRequest
        .getRenewDelegationTokenResponse().getResponse().getNewExpiryTime();

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    OMClientResponse omClientResponse = null;
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    Exception exception = null;

    try {

      OzoneTokenIdentifier ozoneTokenIdentifier = OzoneTokenIdentifier.
          readProtoBuf(ozoneTokenIdentifierToken.getIdentifier());
      auditMap.put(OzoneConsts.DELEGATION_TOKEN_RENEWER,
          ozoneTokenIdentifier.getRenewer() == null ? "" :
              ozoneTokenIdentifier.getRenewer().toString());

      // Update in memory map of token.
      ozoneManager.getDelegationTokenMgr()
          .updateRenewToken(ozoneTokenIdentifierToken, ozoneTokenIdentifier,
              renewTime);

      // Update Cache.
      omMetadataManager.getDelegationTokenTable().addCacheEntry(
          new CacheKey<>(ozoneTokenIdentifier),
          CacheValue.get(context.getIndex(), renewTime));

      omClientResponse =
          new OMRenewDelegationTokenResponse(ozoneTokenIdentifier, renewTime,
              omResponse.setRenewDelegationTokenResponse(
                  updateRenewDelegationTokenRequest
                      .getRenewDelegationTokenResponse()).build());
    } catch (IOException | InvalidPathException ex) {
      LOG.error("Error in Updating Renew DelegationToken {}",
          ozoneTokenIdentifierToken, ex);
      exception = ex;
      omClientResponse = new OMRenewDelegationTokenResponse(null, -1L,
          createErrorOMResponse(omResponse, exception));
    }

    markForAudit(auditLogger,
        buildAuditMessage(OMAction.RENEW_DELEGATION_TOKEN, auditMap, exception,
            getOmRequest().getUserInfo()));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Updated renew delegation token in-memory map: {} with expiry" +
              " time {}", ozoneTokenIdentifierToken, renewTime);
    }

    return omClientResponse;
  }

}
