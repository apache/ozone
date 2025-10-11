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

import static org.apache.hadoop.ozone.security.OzoneTokenIdentifier.KIND_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.security.OzoneDelegationTokenSecretManager;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The class tests OMGetDelegationTokenRequest.
 */
public class TestOMGetDelegationTokenRequest extends
    TestOMDelegationTokenRequest {

  private OzoneDelegationTokenSecretManager secretManager;
  private OzoneTokenIdentifier identifier;
  private Token<OzoneTokenIdentifier> token;
  private Text tester;
  private OMRequest originalRequest;
  private OMRequest modifiedRequest;
  private OMGetDelegationTokenRequest omGetDelegationTokenRequest;
  private static final String CHECK_RESPONSE = "";

  @BeforeEach
  public void setupGetDelegationToken() throws IOException {
    secretManager = mock(OzoneDelegationTokenSecretManager.class);
    when(ozoneManager.getDelegationTokenMgr()).thenReturn(secretManager);
    when(ozoneManager.getAuditLogger()).thenReturn(new AuditLogger(
        AuditLoggerType.OMLOGGER));
    when(ozoneManager.getVersionManager()).thenReturn(
        new OMLayoutVersionManager());

    setupToken();
    setupRequest();
  }

  private void setupRequest() {
    GetDelegationTokenRequestProto getDelegationTokenRequestProto =
        GetDelegationTokenRequestProto.newBuilder()
        .setRenewer(identifier.getRenewer().toString())
        .build();

    originalRequest = OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.GetDelegationToken)
        .setGetDelegationTokenRequest(getDelegationTokenRequestProto)
        .build();

    omGetDelegationTokenRequest =
        new OMGetDelegationTokenRequest(originalRequest);

    modifiedRequest = null;
  }

  private void verifyUnchangedRequest() {
    assertEquals(originalRequest.getCmdType(), modifiedRequest.getCmdType());
    assertEquals(originalRequest.getClientId(), modifiedRequest.getClientId());
  }

  private void setupToken() {
    tester = new Text("tester");
    identifier = new OzoneTokenIdentifier(tester, tester, tester);
    identifier.setOmCertSerialId("certID");
    identifier.setOmServiceId("");

    byte[] password = RandomStringUtils
        .randomAlphabetic(10)
        .getBytes(StandardCharsets.UTF_8);
    Text service = new Text("OMTest:9862");
    token = new Token<>(identifier.getBytes(), password, KIND_NAME, service);
  }

  private OMClientResponse setValidateAndUpdateCache() throws IOException {
    modifiedRequest = omGetDelegationTokenRequest.preExecute(ozoneManager);
    OMGetDelegationTokenRequest reqPreExecuted =
        new OMGetDelegationTokenRequest(modifiedRequest);

    long txLogIndex = 1L;
    return reqPreExecuted.validateAndUpdateCache(
        ozoneManager, txLogIndex);
  }

  @Test
  public void testPreExecuteWithNonNullToken() throws Exception {
    /* Let token of ozoneManager.getDelegationToken() is nonNull. */
    when(ozoneManager.getDelegationToken(tester)).thenReturn(token);

    long tokenRenewInterval = 1000L;
    when(ozoneManager.getDelegationTokenMgr().getTokenRenewInterval())
        .thenReturn(tokenRenewInterval);

    modifiedRequest = omGetDelegationTokenRequest.preExecute(ozoneManager);
    verifyUnchangedRequest();

    long originalInterval = originalRequest.getUpdateGetDelegationTokenRequest()
        .getTokenRenewInterval();
    long renewInterval = modifiedRequest.getUpdateGetDelegationTokenRequest()
        .getTokenRenewInterval();
    assertNotEquals(originalInterval, renewInterval);
    assertEquals(tokenRenewInterval, renewInterval);

    /* In preExecute(), if the token is nonNull
     we set GetDelegationTokenResponse with response. */
    assertNotEquals(CHECK_RESPONSE,
        modifiedRequest.getUpdateGetDelegationTokenRequest()
            .getGetDelegationTokenResponse()
            .toString());
    assertNotNull(modifiedRequest
        .getUpdateGetDelegationTokenRequest()
        .getGetDelegationTokenResponse());
  }

  @Test
  public void testPreExecuteWithNullToken() throws Exception {
    /* Let token of ozoneManager.getDelegationToken() is Null. */
    when(ozoneManager.getDelegationToken(tester)).thenReturn(null);

    modifiedRequest = omGetDelegationTokenRequest.preExecute(ozoneManager);
    verifyUnchangedRequest();

    /* In preExecute(), if the token is null
     we do not set GetDelegationTokenResponse with response. */
    assertEquals(CHECK_RESPONSE,
        modifiedRequest.getUpdateGetDelegationTokenRequest()
            .getGetDelegationTokenResponse()
            .toString());
  }

  @Test
  public void testValidateAndUpdateCacheWithNonNullToken() throws Exception {
    /* Let token of ozoneManager.getDelegationToken() is nonNull. */
    when(ozoneManager.getDelegationToken(tester)).thenReturn(token);

    /* Mock OzoneDelegationTokenSecretManager#updateToken() to
     * get specific renewTime for verifying OMClientResponse returned by
     * validateAndUpdateCache(). */
    long renewTime = 1000L;
    when(secretManager.updateToken(
        any(Token.class), any(OzoneTokenIdentifier.class), anyLong()))
        .thenReturn(renewTime);

    OMClientResponse clientResponse = setValidateAndUpdateCache();

    assertEquals(renewTime, omMetadataManager.getDelegationTokenTable().get(identifier));
    assertEquals(Status.OK, clientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithNullToken() throws Exception {
    /* Let token of ozoneManager.getDelegationToken() is Null. */
    when(ozoneManager.getDelegationToken(tester)).thenReturn(null);

    OMClientResponse clientResponse = setValidateAndUpdateCache();

    boolean hasResponse = modifiedRequest.getUpdateGetDelegationTokenRequest()
        .getGetDelegationTokenResponse().hasResponse();
    assertFalse(hasResponse);

    assertNull(omMetadataManager.getDelegationTokenTable().get(identifier));
    assertEquals(Status.OK, clientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithException() throws Exception {
    /* Create a token that causes InvalidProtocolBufferException by
     * OzoneTokenIdentifier#readProtoBuf(). */
    Token<OzoneTokenIdentifier> exceptToken = new Token<>();
    when(ozoneManager.getDelegationToken(tester)).thenReturn(exceptToken);

    OMClientResponse clientResponse = setValidateAndUpdateCache();

    boolean hasResponse = modifiedRequest.getUpdateGetDelegationTokenRequest()
        .getGetDelegationTokenResponse().hasResponse();
    assertTrue(hasResponse);

    assertNotEquals(Status.OK, clientResponse.getOMResponse().getStatus());
  }
}
