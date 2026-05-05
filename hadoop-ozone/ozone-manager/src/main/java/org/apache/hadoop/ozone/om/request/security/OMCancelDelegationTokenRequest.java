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
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.security.OMCancelDelegationTokenResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CancelDelegationTokenResponseProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.proto.SecurityProtos;
import org.apache.hadoop.ozone.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle CancelDelegationToken Request.
 */
public class OMCancelDelegationTokenRequest extends OMClientRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMGetDelegationTokenRequest.class);

  public OMCancelDelegationTokenRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    // We need to populate user info in our request object.
    OMRequest request =  super.preExecute(ozoneManager);

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    Map<String, String> auditMap = null;

    try {
      Token<OzoneTokenIdentifier> token = OMPBHelper.tokenFromProto(
          request.getCancelDelegationTokenRequest().getToken());
      auditMap = buildTokenAuditMap(token);

      // Call OM to cancel token, this does check whether we can cancel token
      // or not. This does not remove token from DB/in-memory.
      ozoneManager.cancelDelegationToken(token);
      return request;

    } catch (IOException ioe) {
      markForAudit(auditLogger,
          buildAuditMessage(OMAction.CANCEL_DELEGATION_TOKEN, auditMap, ioe,
              request.getUserInfo()));
      throw ioe;
    }
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    Token<OzoneTokenIdentifier> token = getToken();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    Map<String, String> auditMap = buildTokenAuditMap(token);

    OMClientResponse omClientResponse = null;
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OzoneTokenIdentifier ozoneTokenIdentifier = null;
    Exception exception = null;

    try {
      ozoneTokenIdentifier =
          OzoneTokenIdentifier.readProtoBuf(token.getIdentifier());

      // Remove token from in-memory.
      ozoneManager.getDelegationTokenMgr().removeToken(ozoneTokenIdentifier);

      // Update Cache.
      omMetadataManager.getDelegationTokenTable().addCacheEntry(
          new CacheKey<>(ozoneTokenIdentifier),
          CacheValue.get(transactionLogIndex));

      omClientResponse =
          new OMCancelDelegationTokenResponse(ozoneTokenIdentifier,
              omResponse.setCancelDelegationTokenResponse(
                  CancelDelegationTokenResponseProto.newBuilder().setResponse(
                      SecurityProtos.CancelDelegationTokenResponseProto
                          .newBuilder())).build());
    } catch (IOException | InvalidPathException ex) {
      LOG.error("Error in cancel DelegationToken {}", ozoneTokenIdentifier, ex);
      exception = ex;
      omClientResponse = new OMCancelDelegationTokenResponse(null,
          createErrorOMResponse(omResponse, exception));
    }

    markForAudit(auditLogger,
        buildAuditMessage(OMAction.CANCEL_DELEGATION_TOKEN, auditMap, exception,
            getOmRequest().getUserInfo()));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Cancelled delegation token: {}", ozoneTokenIdentifier);
    }

    return omClientResponse;
  }

  public Token<OzoneTokenIdentifier> getToken() {
    CancelDelegationTokenRequestProto cancelDelegationTokenRequest =
        getOmRequest().getCancelDelegationTokenRequest();

    return OMPBHelper.tokenFromProto(
        cancelDelegationTokenRequest.getToken());
  }

}
