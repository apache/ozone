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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.key.TestOMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;

/**
 * Tests for OMStartFinalizeUpgradeRequest.
 */
public class TestOMStartFinalizeUpgradeRequest extends TestOMKeyRequest {
  
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
  }

  @Test
  public void testValidateAndUpdateCacheAddsFinalizationInProgressKey() throws IOException {
    doNothing().when(scmContainerLocationProtocol).finalizeUpgrade();

    assertNull(omMetadataManager.getMetaTable().get(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY),
        "key should not exist before the request");

    submitRequest();

    assertNotNull(omMetadataManager.getMetaTable().get(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY),
        "key should be present in the cache after validateAndUpdateCache");
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

  private OMClientResponse submitRequest() throws IOException {
    OzoneManagerProtocolProtos.OMRequest original = buildRequest();
    OMStartFinalizeUpgradeRequest request = new OMStartFinalizeUpgradeRequest(original);
    ExecutionContext context = ExecutionContext.of(1, TermIndex.INITIAL_VALUE);

    OzoneManagerProtocolProtos.OMRequest modified = request.preExecute(ozoneManager);
    assertNotEquals(original, modified);

    return request.validateAndUpdateCache(ozoneManager, context);
  }

  private OzoneManagerProtocolProtos.OMRequest buildRequest() {
    return OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.StartFinalizeUpgrade)
        .setClientId(ClientId.randomId().toString())
        .build();
  }
}
