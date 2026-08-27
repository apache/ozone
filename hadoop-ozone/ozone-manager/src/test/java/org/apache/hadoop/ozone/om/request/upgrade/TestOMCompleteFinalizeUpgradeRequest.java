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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.ratis.TestOzoneManagerStateMachine;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequestTests;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.upgrade.OMVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;

/**
 * Tests for the OMCompleteFinalizeUpgradeRequest class.
 */
public class TestOMCompleteFinalizeUpgradeRequest extends OMKeyRequestTests {

  @Test
  public void testFinalizationInProgressKeyRemoved() throws IOException {
    OMVersionManager omVersionManager = mock(OMVersionManager.class);
    when(omVersionManager.getApparentVersion()).thenReturn(OzoneManagerVersion.DEFAULT_VERSION);
    when(ozoneManager.getVersionManager()).thenReturn(omVersionManager);

    omMetadataManager.getMetaTable().put(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY, "ignored");
    omMetadataManager.getMetaTable().addCacheEntry(
        new CacheKey<>(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY), CacheValue.get(1, "ignored"));
    omMetrics.setFinalizationInProgress(true);

    String progressKey = omMetadataManager.getMetaTable().get(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY);

    assertNotNull(progressKey);
    assertEquals(1, omMetrics.getFinalizationInProgress(),
        "metric should be 1 before finalizing");
    submitRequest();

    progressKey = omMetadataManager.getMetaTable().get(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY);
    assertNull(progressKey);
    assertEquals(0, omMetrics.getFinalizationInProgress(),
        "metric should be 0 after finalizing");
  }

  /**
   * A failed upgrade step (e.g. an upgrade action) surfaces as an UpgradeException,
   * which is an IOException but not an OMException. exceptionToResponseStatus therefore
   * maps it to INTERNAL_ERROR, and OzoneManagerStateMachine.processResponse terminates the
   * process on INTERNAL_ERROR. The INTERNAL_ERROR -> terminate half of the chain is covered by
   * {@link TestOzoneManagerStateMachine#testProcessResponseInternalErrorTerminates}.
   */
  @Test
  public void testFinalizeFailureMapsToInternalError() throws IOException {
    OMVersionManager omVersionManager = mock(OMVersionManager.class);
    when(omVersionManager.getApparentVersion()).thenReturn(OzoneManagerVersion.DEFAULT_VERSION);
    when(ozoneManager.getVersionManager()).thenReturn(omVersionManager);
    doThrow(new UpgradeException(UpgradeException.ResultCodes.FINALIZE_UPGRADE_ACTION_FAILED))
        .when(ozoneManager).finalizeUpgrade();

    OzoneManagerProtocolProtos.OMRequest omRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CompleteFinalizeUpgrade)
        .setClientId(ClientId.randomId().toString())
        .build();
    OMCompleteFinalizeUpgradeRequest request = new OMCompleteFinalizeUpgradeRequest(omRequest);
    ExecutionContext context = ExecutionContext.of(1, TermIndex.INITIAL_VALUE);
    request.preExecute(ozoneManager);

    OMClientResponse response = request.validateAndUpdateCache(ozoneManager, context);
    OMResponse omResponse = response.getOMResponse();
    assertFalse(omResponse.getSuccess());
    assertEquals(Status.INTERNAL_ERROR, omResponse.getStatus());
  }

  private void submitRequest() throws IOException {
    OzoneManagerProtocolProtos.OMRequest omRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CompleteFinalizeUpgrade)
        .setClientId(ClientId.randomId().toString())
        .build();

    OMCompleteFinalizeUpgradeRequest request = new OMCompleteFinalizeUpgradeRequest(omRequest);
    ExecutionContext context = ExecutionContext.of(1, TermIndex.INITIAL_VALUE);

    OzoneManagerProtocolProtos.OMRequest modifiedOmRequest = request.preExecute(ozoneManager);

    // Will not be equal, as UserInfo will be set.
    assertNotEquals(omRequest, modifiedOmRequest);
    request.validateAndUpdateCache(ozoneManager, context);
  }

}
