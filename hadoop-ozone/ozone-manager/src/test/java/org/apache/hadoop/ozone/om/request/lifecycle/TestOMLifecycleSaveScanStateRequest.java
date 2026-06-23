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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleScanState;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleScanState;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SaveLifecycleScanStateRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

/**
 * Tests OMLifecycleSaveScanStateRequest.
 */
public class TestOMLifecycleSaveScanStateRequest {

  @Test
  public void testPreExecuteAdminCheck() throws Exception {
    OzoneManager ozoneManager = mock(OzoneManager.class);
    
    // Test when ACLs are enabled but user is not admin
    when(ozoneManager.getAclsEnabled()).thenReturn(true);
    when(ozoneManager.isAdmin(any(UserGroupInformation.class))).thenReturn(false);

    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.SaveLifecycleScanState)
        .setClientId(UUID.randomUUID().toString())
        .setSaveLifecycleScanStateRequest(SaveLifecycleScanStateRequest.newBuilder()
            .setState(LifecycleScanState.newBuilder().setBucketKey("dummy").setScanStartTime(1L).build())
            .build())
        .build();

    OMLifecycleSaveScanStateRequest request = new OMLifecycleSaveScanStateRequest(omRequest);
    request.setUGI(UserGroupInformation.getCurrentUser());

    OMException exception = assertThrows(OMException.class, () -> {
      request.preExecute(ozoneManager);
    });

    assertEquals(OMException.ResultCodes.ACCESS_DENIED, exception.getResult());
    assertTrue(exception.getMessage().contains("Superuser privilege is required"));
    
    // Test when user is admin
    when(ozoneManager.isAdmin(any(UserGroupInformation.class))).thenReturn(true);
    OMRequest preExecuted = request.preExecute(ozoneManager);
    assertEquals(omRequest, preExecuted);
    
    // Test when ACLs are disabled
    when(ozoneManager.getAclsEnabled()).thenReturn(false);
    when(ozoneManager.isAdmin(any(UserGroupInformation.class))).thenReturn(false);
    preExecuted = request.preExecute(ozoneManager);
    assertEquals(omRequest, preExecuted);
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    OzoneManager ozoneManager = mock(OzoneManager.class);
    OMMetadataManager omMetadataManager = mock(OMMetadataManager.class);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    
    Table<String, OmLifecycleScanState> table = mock(Table.class);
    when(omMetadataManager.getLifecycleScanStateTable()).thenReturn(table);

    LifecycleScanState stateProto = LifecycleScanState.newBuilder()
        .setBucketKey("/vol1/bucket1")
        .setScanStartTime(123456789L)
        .setLastScannedKey("key1")
        .build();

    SaveLifecycleScanStateRequest saveRequest = SaveLifecycleScanStateRequest.newBuilder()
        .setState(stateProto)
        .build();

    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.SaveLifecycleScanState)
        .setClientId(UUID.randomUUID().toString())
        .setSaveLifecycleScanStateRequest(saveRequest)
        .build();

    OMLifecycleSaveScanStateRequest request = new OMLifecycleSaveScanStateRequest(omRequest);

    OMRequest preExecuted = request.preExecute(ozoneManager);
    assertEquals(omRequest, preExecuted);

    OMClientResponse response = request.validateAndUpdateCache(ozoneManager, 100L);
    assertNotNull(response);
    assertEquals(OzoneManagerProtocolProtos.Status.OK, response.getOMResponse().getStatus());
  }
}
