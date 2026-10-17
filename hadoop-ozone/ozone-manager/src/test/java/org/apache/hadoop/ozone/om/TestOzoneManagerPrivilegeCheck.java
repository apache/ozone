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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Test OzoneManager operations that depend on the RPC user of the call.
 */
class TestOzoneManagerPrivilegeCheck {

  private OzoneManager om;

  @BeforeEach
  void setUp() throws Exception {
    om = mock(OzoneManager.class);
    when(om.isSecurityEnabled()).thenReturn(true);
    when(om.isAdminAuthorizationEnabled()).thenReturn(true);
    when(om.isLeaderReady()).thenReturn(true);
    // Every user is an admin. any(Class) does not match null.
    when(om.isAdmin(any(UserGroupInformation.class))).thenReturn(true);

    when(om.getQuotaRepairStatus()).thenCallRealMethod();
    when(om.getDBUpdates(any())).thenCallRealMethod();
    when(om.triggerRangerBGSync(anyBoolean())).thenCallRealMethod();
    when(om.triggerSnapshotDefrag(anyBoolean())).thenCallRealMethod();
    when(om.getDelegationToken(any())).thenCallRealMethod();
    when(om.renewDelegationToken(any())).thenCallRealMethod();
    doCallRealMethod().when(om).cancelDelegationToken(any());
  }

  @AfterEach
  void tearDown() {
    Server.getCurCall().remove();
  }

  @Test
  void testAdminOperationsWithoutRpcUser() {
    assertAll(
        () -> assertPermissionDenied(() -> om.getQuotaRepairStatus()),
        () -> assertPermissionDenied(() -> om.getDBUpdates(null)),
        () -> assertPermissionDenied(() -> om.triggerRangerBGSync(true)),
        () -> assertPermissionDenied(() -> om.triggerSnapshotDefrag(true)));
  }

  @Test
  void testDelegationTokenOperationsWithoutRpcUser() {
    assertAll(
        () -> assertPermissionDenied(() -> om.getDelegationToken(new Text("renewer"))),
        () -> assertPermissionDenied(() -> om.renewDelegationToken(null)),
        () -> assertPermissionDenied(() -> om.cancelDelegationToken(null)));
  }

  @Test
  void testAdminOperationWithRpcUser() {
    Server.Call call = mock(Server.Call.class);
    when(call.getRemoteUser()).thenReturn(UserGroupInformation.createRemoteUser("alice"));
    Server.getCurCall().set(call);

    assertDoesNotThrow(() -> om.getQuotaRepairStatus());
  }

  private static void assertPermissionDenied(Executable operation) {
    OMException e = assertThrows(OMException.class, operation);
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, e.getResult());
  }
}
