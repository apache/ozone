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
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolClientSideImpl;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequestTests;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.upgrade.OMVersionManager;
import org.apache.hadoop.ozone.om.upgrade.OMVersionManagerTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Shared tests for the client-initiated finalize requests handled by
 * {@link OMFinalizeUpgradeRequestBase}: admin authorization, SCM finalization
 * triggering and error mapping, OM peer software version validation, and writing
 * the finalization-in-progress marker. Subclasses supply the concrete request via
 * {@link #newRequest()} and add tests for behavior unique to their command type.
 */
public abstract class TestOMStartFinalizeUpgradeRequestBase extends OMKeyRequestTests {

  /**
   * A fresh, non-forced initiate request for the command type under test.
   */
  protected abstract OMFinalizeUpgradeRequestBase newRequest();

  @BeforeEach
  public void mockPreFinalizedOM() {
    when(ozoneManager.getPeerNodes()).thenReturn(Collections.emptyList());
    // A finalize request models a cluster that still needs finalization; the parent test harness defaults to a
    // finalized version manager, which is the special already-finalized case exercised separately below.
    OMVersionManager versionManager = OMVersionManagerTestUtils.mockPreFinalizedOmVersionManager();
    when(ozoneManager.getVersionManager()).thenReturn(versionManager);
  }

  @Test
  public void testPreExecuteCallsScmFinalizeUpgrade() throws IOException {
    doNothing().when(scmBlockLocationProtocol).finalizeUpgrade();

    OMFinalizeUpgradeRequestBase request = newRequest();
    OMRequest original = request.getOmRequest();

    OMRequest modified = request.preExecute(ozoneManager);

    // UserInfo must have been added by the base class preExecute.
    assertNotEquals(original, modified);
    assertNotNull(modified.getUserInfo());

    // A non-forced initiate request must route to SCM's non-force finalize path.
    verify(scmBlockLocationProtocol).finalizeUpgrade();
    verify(scmBlockLocationProtocol, never()).forceFinalizeUpgrade();
  }

  @Test
  public void testScmFinalizeFailurePropagatesToClient() throws IOException {
    IOException scmFailure = new IOException("SCM finalize upgrade failed");
    doThrow(scmFailure).when(scmBlockLocationProtocol).finalizeUpgrade();

    OMFinalizeUpgradeRequestBase request = newRequest();

    // The exception raised by SCM must propagate out of preExecute so the OM
    // client sees the failure instead of a successful finalize.
    IOException ex = assertThrows(IOException.class, () -> request.preExecute(ozoneManager));
    assertSame(scmFailure, ex);

    verify(scmBlockLocationProtocol).finalizeUpgrade();
  }

  @Test
  public void testScmUnsupportedOperationBecomesOmNotSupportedOperation() throws IOException {
    SCMException scmFailure =
        new SCMException("SCM version mismatch", SCMException.ResultCodes.UNSUPPORTED_OPERATION);
    doThrow(scmFailure).when(scmBlockLocationProtocol).finalizeUpgrade();

    OMFinalizeUpgradeRequestBase request = newRequest();

    // An SCM UNSUPPORTED_OPERATION is re-mapped to an OM NOT_SUPPORTED_OPERATION,
    // preserving the original message and chaining the SCM exception as the cause.
    OMException ex = assertThrows(OMException.class, () -> request.preExecute(ozoneManager));
    assertEquals(OMException.ResultCodes.NOT_SUPPORTED_OPERATION, ex.getResult());
    assertEquals(scmFailure.getMessage(), ex.getMessage());
    assertSame(scmFailure, ex.getCause());

    verify(scmBlockLocationProtocol).finalizeUpgrade();
  }

  @Test
  public void testOtherScmExceptionPropagatesUnchanged() throws IOException {
    SCMException scmFailure = new SCMException("SCM is in safe mode", SCMException.ResultCodes.SAFE_MODE_EXCEPTION);
    doThrow(scmFailure).when(scmBlockLocationProtocol).finalizeUpgrade();

    OMFinalizeUpgradeRequestBase request = newRequest();

    // Only UNSUPPORTED_OPERATION is re-mapped; any other SCM exception propagates as-is.
    SCMException ex = assertThrows(SCMException.class, () -> request.preExecute(ozoneManager));
    assertSame(scmFailure, ex);

    verify(scmBlockLocationProtocol).finalizeUpgrade();
  }

  @Test
  public void testAccessDeniedWhenUserIsNotAdmin() throws IOException {
    when(ozoneManager.isAdminAuthorizationEnabled()).thenReturn(true);
    when(ozoneManager.isAdmin(any())).thenReturn(false);

    OMFinalizeUpgradeRequestBase request = newRequest();
    // In the test environment there is no live RPC thread, so
    // ProtobufRpcEngine.Server.getRemoteUser() returns null and super.preExecute()
    // cannot resolve a username. setUGI() pre-seeds the identity so that
    // createUGIForApi() succeeds without needing the RPC thread-local.
    request.setUGI(UserGroupInformation.createRemoteUser("testuser"));

    // With auth in preExecute(), a non-admin is rejected before the request
    // reaches Raft or touches SCM.
    OMException ex = assertThrows(OMException.class, () -> request.preExecute(ozoneManager));
    assertEquals(OMException.ResultCodes.ACCESS_DENIED, ex.getResult(),
        "non-admin user should receive ACCESS_DENIED from preExecute");

    // SCM must NOT have been called — auth is checked before the SCM call.
    verify(scmBlockLocationProtocol, never()).finalizeUpgrade();
  }

  @Test
  public void testPeerVersionCheckPassesWhenNoPeers() throws IOException {
    assertTrue(ozoneManager.getPeerNodes().isEmpty());
    // preExecute must complete normally and call SCM finalize.
    doNothing().when(scmBlockLocationProtocol).finalizeUpgrade();

    newRequest().preExecute(ozoneManager);

    verify(scmBlockLocationProtocol).finalizeUpgrade();
  }

  @Test
  public void testPeerVersionCheckPassesWhenAllPeersMatch() throws IOException {
    doNothing().when(scmBlockLocationProtocol).finalizeUpgrade();
    when(ozoneManager.getPeerNodes()).thenReturn(Arrays.asList(buildPeer("om2"), buildPeer("om3")));
    OMAdminProtocolClientSideImpl matchingClient = peerClientWithVersion(OzoneManagerVersion.SOFTWARE_VERSION);

    try (MockedStatic<OMAdminProtocolClientSideImpl> factory =
             mockStatic(OMAdminProtocolClientSideImpl.class)) {
      factory.when(() -> OMAdminProtocolClientSideImpl.createProxyForSingleOM(any(), any(), any()))
          .thenReturn(matchingClient);

      newRequest().preExecute(ozoneManager);
    }

    verify(scmBlockLocationProtocol).finalizeUpgrade();
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

      OMException ex = assertThrows(OMException.class, () -> newRequest().preExecute(ozoneManager));
      assertEquals(OMException.ResultCodes.NOT_SUPPORTED_OPERATION, ex.getResult());
    }

    verify(scmBlockLocationProtocol, never()).finalizeUpgrade();
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

      OMException ex = assertThrows(OMException.class, () -> newRequest().preExecute(ozoneManager));
      assertEquals(OMException.ResultCodes.NOT_SUPPORTED_OPERATION, ex.getResult());
    }

    verify(scmBlockLocationProtocol, never()).finalizeUpgrade();
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

      OMException ex = assertThrows(OMException.class, () -> newRequest().preExecute(ozoneManager));
      assertEquals(OMException.ResultCodes.NOT_SUPPORTED_OPERATION, ex.getResult());
    }

    verify(scmBlockLocationProtocol, never()).finalizeUpgrade();
  }

  @Test
  public void testValidateAndUpdateCacheAddsFinalizationInProgressKey() throws IOException {
    doNothing().when(scmBlockLocationProtocol).finalizeUpgrade();

    assertNull(omMetadataManager.getMetaTable().get(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY),
        "key should not exist before the request");
    assertEquals(0, omMetrics.getFinalizationInProgress(),
        "metric should be 0 before the request");

    OMClientResponse response = submitRequest();

    assertNotNull(omMetadataManager.getMetaTable().get(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY),
        "key should be present in the cache after validateAndUpdateCache");
    assertEquals(1, omMetrics.getFinalizationInProgress(),
        "metric should be 1 after the request");

    // Applying the response as the double buffer would must also persist the marker to the DB.
    flushResponseToDb(response);
    assertEquals("ignored", omMetadataManager.getMetaTable().getSkipCache(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY),
        "key should be persisted to the DB after the response is flushed");
  }

  @Test
  public void testValidateAndUpdateCacheSkipsMarkerWhenAlreadyFinalized() throws IOException {
    doNothing().when(scmBlockLocationProtocol).finalizeUpgrade();
    // Simulate an admin initiating finalize on a cluster that is already finalized.
    OMVersionManager finalizedVersionManager = OMVersionManagerTestUtils.mockFinalizedOmVersionManager();
    when(ozoneManager.getVersionManager()).thenReturn(finalizedVersionManager);

    assertNull(omMetadataManager.getMetaTable().get(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY),
        "key should not exist before the request");
    assertEquals(0, omMetrics.getFinalizationInProgress(),
        "metric should be 0 before the request");

    OMClientResponse response = submitRequest();

    // The marker and metric must NOT be set: OMUpgradeFinalizeService would shut down because needsFinalization()
    // is false and would never clear a marker written here, leaving it and the metric stuck.
    assertNull(omMetadataManager.getMetaTable().get(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY),
        "key must not be written when the cluster is already finalized");
    assertEquals(0, omMetrics.getFinalizationInProgress(),
        "metric must remain 0 when the cluster is already finalized");
    // Finalizing an already-finalized cluster is a successful no-op.
    assertTrue(response.getOMResponse().getSuccess(),
        "response should still report success for an already-finalized cluster");

    // Even the success response must not persist the marker when the double buffer flushes; otherwise it would be
    // orphaned in the DB.
    flushResponseToDb(response);
    assertNull(omMetadataManager.getMetaTable().getSkipCache(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY),
        "key must not be persisted to the DB when the cluster is already finalized");
  }

  /**
   * Applies the response to the DB the way the OM double buffer does on flush: status-gated addToDBBatch followed by
   * a batch commit.
   */
  private void flushResponseToDb(OMClientResponse response) throws IOException {
    try (BatchOperation batch = omMetadataManager.getStore().initBatchOperation()) {
      response.checkAndUpdateDB(omMetadataManager, batch);
      omMetadataManager.getStore().commitBatchOperation(batch);
    }
  }

  /**
   * Runs {@link #newRequest()} through preExecute and validateAndUpdateCache, returning the response.
   */
  protected OMClientResponse submitRequest() throws IOException {
    OMFinalizeUpgradeRequestBase request = newRequest();
    OMRequest original = request.getOmRequest();
    ExecutionContext context = ExecutionContext.of(1, TermIndex.INITIAL_VALUE);

    OMRequest modified = request.preExecute(ozoneManager);
    assertNotEquals(original, modified);

    return request.validateAndUpdateCache(ozoneManager, context);
  }

  protected static OMNodeDetails buildPeer(String nodeId) {
    return new OMNodeDetails.Builder()
        .setOMServiceId("testService")
        .setOMNodeId(nodeId)
        .setHostAddress("127.0.0.1")
        .setRpcPort(1)
        .build();
  }

  protected static OMAdminProtocolClientSideImpl peerClientWithVersion(OzoneManagerVersion version) throws IOException {
    OMAdminProtocolClientSideImpl client = mock(OMAdminProtocolClientSideImpl.class);
    when(client.getPeerUpgradeStatus()).thenReturn(version);
    return client;
  }
}
