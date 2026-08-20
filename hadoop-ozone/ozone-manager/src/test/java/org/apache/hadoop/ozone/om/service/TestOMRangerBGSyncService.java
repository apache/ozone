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

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.ozone.om.ratis.utils.ProtocolMessageMetricsTestUtils.getRequestCount;
import static org.apache.hadoop.ozone.om.ratis.utils.ProtocolMessageMetricsTestUtils.getRequestTime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.multitenant.AuthorizerLock;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequestTests;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test that the {@link OMRangerBGSyncService} internal {@code SetRangerServiceVersion} request is
 * counted in the OmClientProtocol per-type metrics just like a client request.
 */
@Timeout(120)
public class TestOMRangerBGSyncService extends OMKeyRequestTests {

  @Test
  public void testSetOMDBRangerServiceVersionIncrementsMetric() throws Exception {
    OzoneManagerProtocolProtos.OMResponse respMock = mock(OzoneManagerProtocolProtos.OMResponse.class);
    OzoneManagerRatisServer ratisServerMock = mock(OzoneManagerRatisServer.class);
    // Sleep briefly inside the submission so the measured latency is reliably greater than zero.
    doAnswer(invocation -> {
      Thread.sleep(2);
      return respMock;
    }).when(ratisServerMock).submitRequest(any(), any(), anyLong());
    when(ozoneManager.getOmRatisServer()).thenReturn(ratisServerMock);
    when(ozoneManager.getThreadNamePrefix()).thenReturn("");

    OMMultiTenantManager multiTenantManager = mock(OMMultiTenantManager.class);
    when(multiTenantManager.getAuthorizerLock()).thenReturn(mock(AuthorizerLock.class));

    // accessController may be null for unit tests; the service falls back to an in-memory controller.
    OMRangerBGSyncService syncService = new OMRangerBGSyncService(ozoneManager, multiTenantManager,
        null, 10, TimeUnit.SECONDS, 10_000);

    // The metrics source is created fresh for each test, so no SetRangerServiceVersion call is recorded yet.
    assertEquals(0, getRequestCount(ozoneManager.getOmClientProtocolMetrics(),
        OzoneManagerProtocolProtos.Type.SetRangerServiceVersion));
    assertEquals(0, getRequestTime(ozoneManager.getOmClientProtocolMetrics(),
        OzoneManagerProtocolProtos.Type.SetRangerServiceVersion));

    syncService.setOMDBRangerServiceVersion(1L);

    assertThat(getRequestCount(ozoneManager.getOmClientProtocolMetrics(),
        OzoneManagerProtocolProtos.Type.SetRangerServiceVersion)).isGreaterThan(0L);
    assertThat(getRequestTime(ozoneManager.getOmClientProtocolMetrics(),
        OzoneManagerProtocolProtos.Type.SetRangerServiceVersion)).isGreaterThan(0L);
  }
}
