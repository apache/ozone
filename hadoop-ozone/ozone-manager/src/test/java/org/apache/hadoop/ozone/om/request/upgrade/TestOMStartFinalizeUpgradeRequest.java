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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolClientSideImpl;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Tests for OMStartFinalizeUpgradeRequest, the current ({@code ozone admin upgrade finalize}) initiated
 * request. Shared initiate behavior is covered by {@link TestOMStartFinalizeUpgradeRequestBase}; the cases
 * here exercise the {@code force} flag that is unique to this request.
 */
public class TestOMStartFinalizeUpgradeRequest extends TestOMStartFinalizeUpgradeRequestBase {

  @Override
  protected OMFinalizeUpgradeRequestBase newRequest() {
    return new OMStartFinalizeUpgradeRequest(buildRequest(false));
  }

  @Test
  public void testForcePreExecuteCallsScmForceFinalizeUpgrade() throws IOException {
    OMStartFinalizeUpgradeRequest request = new OMStartFinalizeUpgradeRequest(buildRequest(true));

    request.preExecute(ozoneManager);

    // A forced request must route to SCM's force path so SCM skips its own version checks.
    verify(scmBlockLocationProtocol).forceFinalizeUpgrade();
    verify(scmBlockLocationProtocol, never()).finalizeUpgrade();
  }

  @Test
  public void testAuditMapRecordsForceFlag() throws IOException {
    doNothing().when(scmBlockLocationProtocol).finalizeUpgrade();
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
  public void testForceSkipsPeerVersionCheckForUnreachablePeer() throws IOException {
    doNothing().when(scmBlockLocationProtocol).finalizeUpgrade();
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
    verify(scmBlockLocationProtocol).forceFinalizeUpgrade();
    verify(scmBlockLocationProtocol, never()).finalizeUpgrade();
  }

  @Test
  public void testForceSkipsPeerVersionCheckForMismatchedPeer() throws IOException {
    doNothing().when(scmBlockLocationProtocol).finalizeUpgrade();
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
    verify(scmBlockLocationProtocol).forceFinalizeUpgrade();
    verify(scmBlockLocationProtocol, never()).finalizeUpgrade();
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
