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

package org.apache.hadoop.ozone.om.request.s3.tenant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.framework;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMSetRangerServiceVersionResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetRangerServiceVersionRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests OMSetRangerServiceVersionRequest.
 */
public class TestSetRangerServiceVersionRequest {

  @TempDir
  private Path folder;

  private OzoneManager ozoneManager;

  @BeforeEach
  public void setUp() throws Exception {
    ozoneManager = mock(OzoneManager.class);
    when(ozoneManager.getVersionManager())
        .thenReturn(new OMLayoutVersionManager(1));

    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());
    OmMetadataManagerImpl omMetadataManager = new OmMetadataManagerImpl(conf,
        ozoneManager);
    when(ozoneManager.getMetadataManager())
        .thenReturn(omMetadataManager);
    OMPerformanceMetrics omPerformanceMetrics = mock(OMPerformanceMetrics.class);
    when(ozoneManager.getPerfMetrics()).thenReturn(omPerformanceMetrics);
  }

  @AfterEach
  public void tearDown() throws Exception {
    framework().clearInlineMocks();
  }

  private OMRequest createRangerSyncRequest(long rangerServiceVersion) {

    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.SetRangerServiceVersion)
        .setSetRangerServiceVersionRequest(
            SetRangerServiceVersionRequest.newBuilder()
                .setRangerServiceVersion(rangerServiceVersion)
                .build())
        .build();
  }

  @Test
  public void testRequest() throws IOException {

    long txLogIndex = 1;

    // Run preExecute
    OMSetRangerServiceVersionRequest request =
        new OMSetRangerServiceVersionRequest(
            new OMSetRangerServiceVersionRequest(
                createRangerSyncRequest(10L)).preExecute(ozoneManager));

    // Run validateAndUpdateCaches
    OMClientResponse clientResponse = request.validateAndUpdateCache(
            ozoneManager, txLogIndex);

    // Check response type and cast
    assertInstanceOf(OMSetRangerServiceVersionResponse.class, clientResponse);
    final OMSetRangerServiceVersionResponse omSetRangerServiceVersionResponse =
        (OMSetRangerServiceVersionResponse) clientResponse;

    // Verify response
    String verStr = omSetRangerServiceVersionResponse.getNewServiceVersion();
    assertEquals(10L, Long.parseLong(verStr));
  }
}
