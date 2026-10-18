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

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link OMDBCheckpointServletInodeBasedXfer} behavior when this OM is not leader.
 */
class TestOMDBCheckpointServletInodeBasedXferNonLeader {

  @Test
  void processMetadataSnapshotRequestReturns503WhenNotLeader() throws Exception {
    OMDBCheckpointServletInodeBasedXfer servlet =
        spy(new OMDBCheckpointServletInodeBasedXfer());
    OzoneManager om = mock(OzoneManager.class);
    when(om.isLeader()).thenReturn(false);

    ServletContext ctx = mock(ServletContext.class);
    when(ctx.getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE)).thenReturn(om);
    doReturn(ctx).when(servlet).getServletContext();

    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);

    servlet.processMetadataSnapshotRequest(request, response, false, true);

    verify(response).sendError(eq(HttpServletResponse.SC_SERVICE_UNAVAILABLE), anyString());
  }

  @Test
  void processMetadataSnapshotRequestSetsStatusWhenSendErrorFails() throws Exception {
    OMDBCheckpointServletInodeBasedXfer servlet =
        spy(new OMDBCheckpointServletInodeBasedXfer());
    OzoneManager om = mock(OzoneManager.class);
    when(om.isLeader()).thenReturn(false);

    ServletContext ctx = mock(ServletContext.class);
    when(ctx.getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE)).thenReturn(om);
    doReturn(ctx).when(servlet).getServletContext();

    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    doThrow(new IOException("broken pipe")).when(response)
        .sendError(eq(HttpServletResponse.SC_SERVICE_UNAVAILABLE), anyString());

    servlet.processMetadataSnapshotRequest(request, response, false, true);

    verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void processMetadataSnapshotRequestDoesNotReturn503WhenLeader(boolean isLeaderReady) throws Exception {
    BootstrapStateHandler.Lock lock = mock(BootstrapStateHandler.Lock.class);
    when(lock.acquireWriteLock())
        .thenReturn(mock(UncheckedAutoCloseable.class));
    File tempDataDir = new File(System.getProperty("java.io.tmpdir"));
    OMDBCheckpointServletInodeBasedXfer servlet =
        spy(new OMDBCheckpointServletInodeBasedXfer() {
          @Override
          public BootstrapStateHandler.Lock getBootstrapStateLock() {
            return lock;
          }

          @Override
          public File getBootstrapTempData() {
            return tempDataDir;
          }
        });
    OzoneManager om = mock(OzoneManager.class);
    when(om.isLeader()).thenReturn(true);
    when(om.isLeaderReady()).thenReturn(isLeaderReady);
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);

    ServletContext ctx = mock(ServletContext.class);
    when(ctx.getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE)).thenReturn(om);
    doReturn(ctx).when(servlet).getServletContext();
    // Force a failure after leader check so this unit test can stay lightweight
    // (no full servlet/bootstrap setup) while still proving that leader requests
    // are not rejected with 503.
    doThrow(new IOException("test collect failure"))
        .when(servlet).collectDbDataToTransfer(eq(request), anySet(), any());

    servlet.processMetadataSnapshotRequest(request, response, false, true);

    verify(servlet).collectDbDataToTransfer(eq(request), anySet(), any());
    verify(response, never())
        .sendError(eq(HttpServletResponse.SC_SERVICE_UNAVAILABLE), anyString());
    verify(om).isLeader();
    verify(om, never()).isLeaderReady();
    // Internal error comes from the forced collect failure above.
    verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
  }

  @ParameterizedTest
  @EnumSource(value = RaftServerStatus.class,
      names = {"LEADER_AND_READY", "LEADER_AND_NOT_READY"})
  void isLeaderReturnsTrueForLeaderStates(RaftServerStatus raftServerStatus) {
    OzoneManager om = mock(OzoneManager.class, CALLS_REAL_METHODS);
    OzoneManagerRatisServer ratisServer = mock(OzoneManagerRatisServer.class);
    when(ratisServer.getLeaderStatus()).thenReturn(raftServerStatus);
    HddsWhiteboxTestUtils.setInternalState(om, "omRatisServer", ratisServer);

    assertTrue(om.isLeader());
  }

  @Test
  void isLeaderReturnsFalseForNonLeaderState() {
    OzoneManager om = mock(OzoneManager.class, CALLS_REAL_METHODS);
    OzoneManagerRatisServer ratisServer = mock(OzoneManagerRatisServer.class);
    when(ratisServer.getLeaderStatus()).thenReturn(RaftServerStatus.NOT_LEADER);
    HddsWhiteboxTestUtils.setInternalState(om, "omRatisServer", ratisServer);

    assertFalse(om.isLeader());
  }
}
