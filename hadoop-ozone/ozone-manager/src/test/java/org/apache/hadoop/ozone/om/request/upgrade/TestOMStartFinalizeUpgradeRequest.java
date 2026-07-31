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

package org.apache.hadoop.ozone.om.request.upgrade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolClientSideImpl;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequestTests;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Tests for OMStartFinalizeUpgradeRequest.
 */
public class TestOMStartFinalizeUpgradeRequest extends OMKeyRequestTests {

  @BeforeEach
  public void stubPeerNodes() {
    when(ozoneManager.getPeerNodes()).thenReturn(Collections.emptyList());
  }

  @Test
  public void testPreExecuteCallsScmFinalizeUpgrade() throws IOException {
    doNothing().when(scmContainerLocationProtocol).finalizeUpgrade();

    OzoneManagerProtocolProtos.OMRequest original = buildRequest();
    OMStartFinalizeUpgradeRequest request = new OMStartFinalizeUpgradeRequest(original);

    OzoneManagerProtocolProtos.OMRequest modified = request.preExecute(ozoneManager);

    // UserInfo must have been added by the base class preExecute.
    assertNotEquals(original, modified);
    assertNotNull(modified.getUserInfo());

    // SCM must have been asked to begin finalization.
    verify(scmContainerLocationProtocol).finalizeUpgrade();
    verify(scmContainerLocationProtocol, never()).forceFinalizeUpgrade();
  }

  @Test
  public void testForcePreExecuteCallsScmForceFinalizeUpgrade() throws IOException {
    OMStartFinalizeUpgradeRequest request = new OMStartFinalizeUpgradeRequest(buildRequest(true));

    request.preExecute(ozoneManager);

    // A forced request must route to SCM's force path so SCM skips its own version checks.
    verify(scmContainerLocationProtocol).forceFinalizeUpgrade();
    verify(scmContainerLocationProtocol, never()).finalizeUpgrade();
  }

  @Test
  public void testScmFinalizeFailurePropagatesToClient() throws IOException {
    IOException scmFailure = new IOException("SCM finalize upgrade failed");
    doThrow(scmFailure).when(scmContainerLocationProtocol).finalizeUpgrade();

    OMStartFinalizeUpgradeRequest request = new OMStartFinalizeUpgradeRequest(buildRequest());

    // The exception raised by SCM must propagate out of preExecute so the OM
    // client sees the failure instead of a successful finalize.
    IOException ex = assertThrows(IOException.class, () -> request.preExecute(ozoneManager));
    assertSame(scmFailure, ex);

    verify(scmContainerLocationProtocol).finalizeUpgrade();
  }

  @Test
  public void testScmUnsupportedOperationBecomesOmNotSupportedOperation() throws IOException {
    SCMException scmFailure =
        new SCMException("SCM version mismatch", SCMException.ResultCodes.UNSUPPORTED_OPERATION);
    doThrow(scmFailure).when(scmContainerLocationProtocol).finalizeUpgrade();

    OMStartFinalizeUpgradeRequest request = new OMStartFinalizeUpgradeRequest(buildRequest());

    // An SCM UNSUPPORTED_OPERATION is re-mapped to an OM NOT_SUPPORTED_OPERATION,
    // preserving the original message and chaining the SCM exception as the cause.
    OMException ex = assertThrows(OMException.class, () -> request.preExecute(ozoneManager));
    assertEquals(OMException.ResultCodes.NOT_SUPPORTED_OPERATION, ex.getResult());
    assertEquals(scmFailure.getMessage(), ex.getMessage());
    assertSame(scmFailure, ex.getCause());

    verify(scmContainerLocationProtocol).finalizeUpgrade();
  }

  @Test
  public void testOtherScmExceptionPropagatesUnchanged() throws IOException {
    SCMException scmFailure = new SCMException("SCM is in safe mode", SCMException.ResultCodes.SAFE_MODE_EXCEPTION);
    doThrow(scmFailure).when(scmContainerLocationProtocol).finalizeUpgrade();

    OMStartFinalizeUpgradeRequest request = new OMStartFinalizeUpgradeRequest(buildRequest());

    // Only UNSUPPORTED_OPERATION is re-mapped; any other SCM exception propagates as-is.
    SCMException ex = assertThrows(SCMException.class, () -> request.preExecute(ozoneManager));
    assertSame(scmFailure, ex);

    verify(scmContainerLocationProtocol).finalizeUpgrade();
  }

  @Test
  public void testValidateAndUpdateCacheAddsFinalizationInProgressKey() throws IOException {
    doNothing().when(scmContainerLocationProtocol).finalizeUpgrade();

    assertNull(omMetadataManager.getMetaTable().get(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY),
        "key should not exist before the request");
    assertEquals(0, omMetrics.getFinalizationInProgress(),
        "metric should be 0 before the request");

    submitRequest();

    assertNotNull(omMetadataManager.getMetaTable().get(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY),
        "key should be present in the cache after validateAndUpdateCache");
    assertEquals(1, omMetrics.getFinalizationInProgress(),
        "metric should be 1 after the request");
  }

  @Test
  public void testAuditMapRecordsForceFlag() throws IOException {
    doNothing().when(scmContainerLocationProtocol).finalizeUpgrade();
    ExecutionContext context = ExecutionContext.of(1, TermIndex.INITIAL_VALUE);

    OMStartFinalizeUpgradeRequest forced = new OMStartFinalizeUpgradeRequest(buildRequest(true));
    forced.preExecute(ozoneManager);
    forced.validateAndUpdateCache(ozoneManager, context);
    assertEquals("true", forced.getAuditBuilder().getAuditMap().get("force"));

    OMStartFinalizeUpgradeRequest normal = new OMStartFinalizeUpgradeRequest(buildRequest(false));
    normal.preExecute(ozoneManager);
    normal.validateAndUpdateCache(ozoneManager, context);
    assertEquals("false", normal.getAuditBuilder().getAuditMap().get("force"));
  }

  @Test
  public void testAccessDeniedWhenUserIsNotAdmin() throws IOException {
    when(ozoneManager.isAdminAuthorizationEnabled()).thenReturn(true);
    when(ozoneManager.isAdmin(any())).thenReturn(false);

    OzoneManagerProtocolProtos.OMRequest original = buildRequest();
    OMStartFinalizeUpgradeRequest request = new OMStartFinalizeUpgradeRequest(original);
    // In the test environment there is no live RPC thread, so
    // ProtobufRpcEngine.Server.getRemoteUser() returns null and super.preExecute()
    // cannot resolve a username. setUGI() pre-seeds the identity so that
    // createUGIForApi() succeeds without needing the RPC thread-local.
    request.setUGI(UserGroupInformation.createRemoteUser("testuser"));

    // With auth in preExecute(), a non-admin is rejected before the request
    // reaches Raft or touches SCM.
    OMException ex = assertThrows(OMException.class,
        () -> request.preExecute(ozoneManager));
    assertEquals(OMException.ResultCodes.ACCESS_DENIED, ex.getResult(),
        "non-admin user should receive ACCESS_DENIED from preExecute");

    // SCM must NOT have been called — auth is checked before the SCM call.
    verify(scmContainerLocationProtocol, never()).finalizeUpgrade();
  }

  @Test
  public void testPeerVersionCheckPassesWhenNoPeers() throws IOException {
    // @BeforeEach already stubs getPeerNodes() to return an empty list.
    // preExecute must complete normally and call SCM finalize.
    doNothing().when(scmContainerLocationProtocol).finalizeUpgrade();

    OzoneManagerProtocolProtos.OMRequest original = buildRequest();
    new OMStartFinalizeUpgradeRequest(original).preExecute(ozoneManager);

    verify(scmContainerLocationProtocol).finalizeUpgrade();
  }

  @Test
  public void testPeerVersionCheckPassesWhenAllPeersMatch() throws IOException {
    doNothing().when(scmContainerLocationProtocol).finalizeUpgrade();
    when(ozoneManager.getPeerNodes()).thenReturn(Arrays.asList(buildPeer("om2"), buildPeer("om3")));
    OMAdminProtocolClientSideImpl matchingClient = peerClientWithVersion(OzoneManagerVersion.SOFTWARE_VERSION);

    try (MockedStatic<OMAdminProtocolClientSideImpl> factory =
             mockStatic(OMAdminProtocolClientSideImpl.class)) {
      factory.when(() -> OMAdminProtocolClientSideImpl.createProxyForSingleOM(any(), any(), any()))
          .thenReturn(matchingClient);

      new OMStartFinalizeUpgradeRequest(buildRequest()).preExecute(ozoneManager);
    }

    verify(scmContainerLocationProtocol).finalizeUpgrade();
  }

  @Test
  public void testPeerVersionCheckRejectsOneOlderPeer() throws IOException {
    when(ozoneManager.getPeerNodes()).thenReturn(Arrays.asList(buildPeer("om2"), buildPeer("om3")));
    OMAdminProtocolClientSideImpl matchingClient = peerClientWithVersion(OzoneManagerVersion.SOFTWARE_VERSION);
    OMAdminProtocolClientSideImpl olderClient = peerClientWithVersion(OzoneManagerVersion.HBASE_SUPPORT);

    try (MockedStatic<OMAdminProtocolClientSideImpl> factory =
             mockStatic(OMAdminProtocolClientSideImpl.class)) {
      factory.when(() -> OMAdminProtocolClientSideImpl.createProxyForSingleOM(any(), any(), any()))
          .thenReturn(matchingClient, olderClient);

      OMException ex = assertThrows(OMException.class,
          () -> new OMStartFinalizeUpgradeRequest(buildRequest()).preExecute(ozoneManager));
      assertEquals(OMException.ResultCodes.NOT_SUPPORTED_OPERATION, ex.getResult());
    }

    verify(scmContainerLocationProtocol, never()).finalizeUpgrade();
  }

  @Test
  public void testPeerVersionCheckRejectsOneUnknownFuturePeer() throws IOException {
    when(ozoneManager.getPeerNodes()).thenReturn(Arrays.asList(buildPeer("om2"), buildPeer("om3")));
    OMAdminProtocolClientSideImpl matchingClient = peerClientWithVersion(OzoneManagerVersion.SOFTWARE_VERSION);
    OMAdminProtocolClientSideImpl unknownClient = peerClientWithVersion(OzoneManagerVersion.UNKNOWN_VERSION);

    try (MockedStatic<OMAdminProtocolClientSideImpl> factory =
             mockStatic(OMAdminProtocolClientSideImpl.class)) {
      factory.when(() -> OMAdminProtocolClientSideImpl.createProxyForSingleOM(any(), any(), any()))
          .thenReturn(matchingClient, unknownClient);

      OMException ex = assertThrows(OMException.class,
          () -> new OMStartFinalizeUpgradeRequest(buildRequest()).preExecute(ozoneManager));
      assertEquals(OMException.ResultCodes.NOT_SUPPORTED_OPERATION, ex.getResult());
    }

    verify(scmContainerLocationProtocol, never()).finalizeUpgrade();
  }

  @Test
  public void testPeerVersionCheckRejectsUnreachablePeer() throws IOException {
    when(ozoneManager.getPeerNodes()).thenReturn(Collections.singletonList(buildPeer("om2")));
    OMAdminProtocolClientSideImpl unreachableClient = mock(OMAdminProtocolClientSideImpl.class);
    when(unreachableClient.getPeerUpgradeStatus()).thenThrow(new IOException("connection refused"));

    try (MockedStatic<OMAdminProtocolClientSideImpl> factory =
             mockStatic(OMAdminProtocolClientSideImpl.class)) {
      factory.when(() -> OMAdminProtocolClientSideImpl.createProxyForSingleOM(any(), any(), any()))
          .thenReturn(unreachableClient);

      OMException ex = assertThrows(OMException.class,
          () -> new OMStartFinalizeUpgradeRequest(buildRequest()).preExecute(ozoneManager));
      assertEquals(OMException.ResultCodes.NOT_SUPPORTED_OPERATION, ex.getResult());
    }

    verify(scmContainerLocationProtocol, never()).finalizeUpgrade();
  }

  @Test
  public void testForceSkipsPeerVersionCheckForUnreachablePeer() throws IOException {
    doNothing().when(scmContainerLocationProtocol).finalizeUpgrade();
    when(ozoneManager.getPeerNodes()).thenReturn(Collections.singletonList(buildPeer("om2")));
    OMAdminProtocolClientSideImpl unreachableClient = mock(OMAdminProtocolClientSideImpl.class);
    when(unreachableClient.getPeerUpgradeStatus()).thenThrow(new IOException("connection refused"));

    try (MockedStatic<OMAdminProtocolClientSideImpl> factory =
             mockStatic(OMAdminProtocolClientSideImpl.class)) {
      factory.when(() -> OMAdminProtocolClientSideImpl.createProxyForSingleOM(any(), any(), any()))
          .thenReturn(unreachableClient);

      // With force=true the peer version check is skipped, so an unreachable peer does not
      // prevent finalization and SCM is still asked to force finalization (skipping its own checks).
      new OMStartFinalizeUpgradeRequest(buildRequest(true)).preExecute(ozoneManager);
    }

    verify(unreachableClient, never()).getPeerUpgradeStatus();
    verify(scmContainerLocationProtocol).forceFinalizeUpgrade();
    verify(scmContainerLocationProtocol, never()).finalizeUpgrade();
  }

  @Test
  public void testForceSkipsPeerVersionCheckForMismatchedPeer() throws IOException {
    doNothing().when(scmContainerLocationProtocol).finalizeUpgrade();
    when(ozoneManager.getPeerNodes()).thenReturn(Collections.singletonList(buildPeer("om2")));
    OMAdminProtocolClientSideImpl olderClient = peerClientWithVersion(OzoneManagerVersion.HBASE_SUPPORT);

    try (MockedStatic<OMAdminProtocolClientSideImpl> factory =
             mockStatic(OMAdminProtocolClientSideImpl.class)) {
      factory.when(() -> OMAdminProtocolClientSideImpl.createProxyForSingleOM(any(), any(), any()))
          .thenReturn(olderClient);

      // With force=true the peer version check is skipped, so a peer running a different software
      // version does not prevent finalization and SCM is still asked to force finalization.
      new OMStartFinalizeUpgradeRequest(buildRequest(true)).preExecute(ozoneManager);
    }

    verify(olderClient, never()).getPeerUpgradeStatus();
    verify(scmContainerLocationProtocol).forceFinalizeUpgrade();
    verify(scmContainerLocationProtocol, never()).finalizeUpgrade();
  }

  private static OMNodeDetails buildPeer(String nodeId) {
    return new OMNodeDetails.Builder()
        .setOMServiceId("testService")
        .setOMNodeId(nodeId)
        .setHostAddress("127.0.0.1")
        .setRpcPort(1)
        .build();
  }

  private static OMAdminProtocolClientSideImpl peerClientWithVersion(OzoneManagerVersion version) throws IOException {
    OMAdminProtocolClientSideImpl client = mock(OMAdminProtocolClientSideImpl.class);
    when(client.getPeerUpgradeStatus()).thenReturn(version);
    return client;
  }

  private OMClientResponse submitRequest() throws IOException {
    OzoneManagerProtocolProtos.OMRequest original = buildRequest();
    OMStartFinalizeUpgradeRequest request = new OMStartFinalizeUpgradeRequest(original);
    ExecutionContext context = ExecutionContext.of(1, TermIndex.INITIAL_VALUE);

    OzoneManagerProtocolProtos.OMRequest modified = request.preExecute(ozoneManager);
    assertNotEquals(original, modified);

    return request.validateAndUpdateCache(ozoneManager, context);
  }

  private OzoneManagerProtocolProtos.OMRequest buildRequest() {
    return buildRequest(false);
  }

  private OzoneManagerProtocolProtos.OMRequest buildRequest(boolean force) {
    return OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.StartFinalizeUpgrade)
        .setClientId(ClientId.randomId().toString())
        .setStartFinalizeUpgradeRequest(OzoneManagerProtocolProtos.StartFinalizeUpgradeRequest.newBuilder()
            .setForce(force))
        .build();
  }
}
