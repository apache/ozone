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

package org.apache.hadoop.ozone.om.request.s3.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.hadoop.ipc.ExternalCall;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link S3RevokeSTSTokenRequest}.
 */
public class TestS3RevokeSTSTokenRequest {

  @AfterEach
  public void tearDown() {
    Server.getCurCall().remove();
  }

  @Test
  public void testPreExecuteFailsForNonAdmin() throws Exception {
    // Verify that preExecute rejects non-admin users.
    final String tempAccessKeyId = "ASIA12345678";
    final UserGroupInformation nonAdminUgi = UserGroupInformation.createRemoteUser("non-admin-user");
    Server.getCurCall().set(new StubCall(nonAdminUgi));

    final OMException ex;
    try (OzoneManager ozoneManager = mock(OzoneManager.class)) {
      when(ozoneManager.isS3Admin(any(UserGroupInformation.class)))
          .thenReturn(false);

      final OzoneManagerProtocolProtos.RevokeSTSTokenRequest revokeRequest =
          OzoneManagerProtocolProtos.RevokeSTSTokenRequest.newBuilder()
              .setAccessKeyId(tempAccessKeyId)
              .build();

      final OMRequest omRequest = OMRequest.newBuilder()
          .setClientId(UUID.randomUUID().toString())
          .setCmdType(Type.RevokeSTSToken)
          .setRevokeSTSTokenRequest(revokeRequest)
          .build();

      final OMClientRequest omClientRequest = new S3RevokeSTSTokenRequest(omRequest);

      ex = assertThrows(OMException.class, () -> omClientRequest.preExecute(ozoneManager));
    }
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, ex.getResult());
  }

  @Test
  public void testPreExecuteSucceedsForAdmin() throws Exception {
    // Verify that preExecute allows S3/Ozone admins to revoke STS tokens.
    final String tempAccessKeyId = "ASIA4567891230";
    final UserGroupInformation adminUgi = UserGroupInformation.createRemoteUser("admin-user");
    Server.getCurCall().set(new StubCall(adminUgi));

    final OzoneManager ozoneManager = mock(OzoneManager.class);
    when(ozoneManager.isS3Admin(any(UserGroupInformation.class)))
        .thenReturn(true);

    final OzoneManagerProtocolProtos.RevokeSTSTokenRequest revokeRequest =
        OzoneManagerProtocolProtos.RevokeSTSTokenRequest.newBuilder()
            .setAccessKeyId(tempAccessKeyId)
            .build();

    final OMRequest omRequest = OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.RevokeSTSToken)
        .setRevokeSTSTokenRequest(revokeRequest)
        .build();

    final OMClientRequest omClientRequest = new S3RevokeSTSTokenRequest(omRequest);
    final OMRequest result = omClientRequest.preExecute(ozoneManager);
    assertEquals(Type.RevokeSTSToken, result.getCmdType());
  }

  /**
   * Stub used to inject a remote user into the ProtobufRpcEngine.Server.getRemoteUser() thread-local.
   */
  private static final class StubCall extends ExternalCall<String> {
    private final UserGroupInformation ugi;

    StubCall(UserGroupInformation ugi) {
      super(null);
      this.ugi = ugi;
    }

    @Override
    public UserGroupInformation getRemoteUser() {
      return ugi;
    }
  }
}
