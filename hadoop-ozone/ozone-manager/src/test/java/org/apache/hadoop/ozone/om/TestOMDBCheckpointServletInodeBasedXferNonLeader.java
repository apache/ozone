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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

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
    OMDBCheckpointServletInodeBasedXfer servlet =
        spy(new OMDBCheckpointServletInodeBasedXfer());
    OzoneManager om = mock(OzoneManager.class);
    when(om.isLeader()).thenReturn(true);
    when(om.isLeaderReady()).thenReturn(isLeaderReady);

    ServletContext ctx = mock(ServletContext.class);
    when(ctx.getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE)).thenReturn(om);
    doReturn(ctx).when(servlet).getServletContext();
    BootstrapStateHandler.Lock lock = mock(BootstrapStateHandler.Lock.class);
    // Force a failure after leader check so this unit test can stay lightweight
    // (no full servlet/bootstrap setup) while still proving that leader requests
    // are not rejected with 503.
    doThrow(new InterruptedException("test lock failure"))
        .when(lock).acquireWriteLock();
    doReturn(lock).when(servlet).getBootstrapStateLock();

    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);

    servlet.processMetadataSnapshotRequest(request, response, false, true);

    verify(response, never())
        .sendError(eq(HttpServletResponse.SC_SERVICE_UNAVAILABLE), anyString());
    // Internal error comes from the forced lock failure above.
    verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
  }
}
