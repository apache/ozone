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

package org.apache.hadoop.ozone.om.request.lifecycle;

import static org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager.maxLayoutVersion;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetLifecycleServiceStatusRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

/**
 * Tests OMLifecycleSetServiceStatusRequest.
 */
public class TestOMLifecycleSetServiceStatusRequest {

  @Test
  public void testPreExecuteAdminCheck() throws Exception {
    OzoneManager ozoneManager = mock(OzoneManager.class);
    OMLayoutVersionManager versionManager = mock(OMLayoutVersionManager.class);
    when(versionManager.getMetadataLayoutVersion()).thenReturn(maxLayoutVersion());
    when(ozoneManager.getVersionManager()).thenReturn(versionManager);

    when(ozoneManager.isAdminAuthorizationEnabled()).thenReturn(true);
    when(ozoneManager.isAdmin(any(UserGroupInformation.class))).thenReturn(false);

    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.SetLifecycleServiceStatus)
        .setClientId(UUID.randomUUID().toString())
        .setSetLifecycleServiceStatusRequest(SetLifecycleServiceStatusRequest.newBuilder()
            .setSuspend(true)
            .build())
        .build();

    OMLifecycleSetServiceStatusRequest request = new OMLifecycleSetServiceStatusRequest(omRequest);
    request.setUGI(UserGroupInformation.getCurrentUser());

    OMException exception = assertThrows(OMException.class, () -> {
      request.preExecute(ozoneManager);
    });

    assertEquals(OMException.ResultCodes.ACCESS_DENIED, exception.getResult());
    assertTrue(exception.getMessage().contains("Superuser privilege is required"));

    when(ozoneManager.isAdmin(any(UserGroupInformation.class))).thenReturn(true);
    assertDoesNotThrow(() -> request.preExecute(ozoneManager));

    when(ozoneManager.isAdminAuthorizationEnabled()).thenReturn(false);
    when(ozoneManager.isAdmin(any(UserGroupInformation.class))).thenReturn(false);
    assertDoesNotThrow(() -> request.preExecute(ozoneManager));
  }
}
