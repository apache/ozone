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

package org.apache.hadoop.ozone.om.ratis;

import static org.apache.hadoop.ipc_.ProcessingDetails.Timing.LOCKSHARED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.RpcConstants;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.ozone.security.STSTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TestOMThreadContext {

  @AfterEach
  void tearDown() {
    Server.getCurCall().remove();
    OzoneManager.setS3Auth(null);
    OzoneManager.setStsTokenIdentifier(null);
  }

  @Test
  void readContextAppliesRequestAndRestoresPreviousContext() throws Exception {
    OzoneManager ozoneManager = secureOzoneManager();
    S3Authentication requestS3Auth = S3Authentication.newBuilder().setAccessId("request-access-id").build();
    OMRequest request = requestWithContext("request-user", requestS3Auth);
    OMThreadContext context = OMThreadContext.forRead(request, ozoneManager);

    Server.Call previousCall = createCall("previous-user");
    S3Authentication previousS3Auth = S3Authentication.newBuilder().setAccessId("previous-access-id").build();
    STSTokenIdentifier previousStsToken = mock(STSTokenIdentifier.class);
    Server.getCurCall().set(previousCall);
    OzoneManager.setS3Auth(previousS3Auth);
    OzoneManager.setStsTokenIdentifier(previousStsToken);

    try (OMThreadContext.Scope ignored = context.applyToCurrentThread()) {
      assertEquals("request-user", Server.getRemoteUser().getUserName());
      assertEquals("127.0.0.1", Server.getRemoteIp().getHostAddress());
      assertEquals(requestS3Auth, OzoneManager.getS3Auth());
      assertNull(OzoneManager.getStsTokenIdentifier());
    }

    assertSame(previousCall, Server.getCurCall().get());
    assertSame(previousS3Auth, OzoneManager.getS3Auth());
    assertSame(previousStsToken, OzoneManager.getStsTokenIdentifier());
  }

  @Test
  void writeContextAppliesRequestAndRestoresPreviousContext() throws Exception {
    OzoneManager ozoneManager = secureOzoneManager();
    S3Authentication requestS3Auth = S3Authentication.newBuilder().setAccessId("request-access-id").build();
    OMThreadContext context = OMThreadContext.forWrite(requestWithContext("request-user", requestS3Auth), ozoneManager);

    Server.Call previousCall = createCall("previous-user");
    S3Authentication previousS3Auth = S3Authentication.newBuilder().setAccessId("previous-access-id").build();
    STSTokenIdentifier previousStsToken = mock(STSTokenIdentifier.class);
    Server.getCurCall().set(previousCall);
    OzoneManager.setS3Auth(previousS3Auth);
    OzoneManager.setStsTokenIdentifier(previousStsToken);

    try (OMThreadContext.Scope ignored = context.applyToCurrentThread()) {
      assertNull(Server.getCurCall().get());
      assertEquals(requestS3Auth, OzoneManager.getS3Auth());
      assertNull(OzoneManager.getStsTokenIdentifier());
    }

    assertSame(previousCall, Server.getCurCall().get());
    assertSame(previousS3Auth, OzoneManager.getS3Auth());
    assertSame(previousStsToken, OzoneManager.getStsTokenIdentifier());
  }

  @Test
  void eachReadScopeUsesFreshCallAndLockDetails() throws Exception {
    OMThreadContext context = OMThreadContext.forRead(
        requestWithContext("request-user", null), secureOzoneManager());
    OMResponse response = OMResponse.newBuilder()
        .setCmdType(Type.ServiceList)
        .setStatus(Status.OK)
        .setSuccess(true)
        .build();

    Server.Call firstCall;
    try (OMThreadContext.Scope scope = context.applyToCurrentThread()) {
      firstCall = Server.getCurCall().get();
      firstCall.getProcessingDetails().add(LOCKSHARED, 11, TimeUnit.NANOSECONDS);
      assertEquals(11, scope.addLockDetails(response).getOmLockDetails().getReadLockNanos());
    }

    try (OMThreadContext.Scope scope = context.applyToCurrentThread()) {
      assertNotSame(firstCall, Server.getCurCall().get());
      assertSame(response, scope.addLockDetails(response));
    }
  }

  @Test
  void closeIsIdempotent() throws Exception {
    Server.Call previousCall = createCall("previous-user");
    Server.getCurCall().set(previousCall);
    OMThreadContext.Scope scope = OMThreadContext.forRead(
        requestWithContext("request-user", null), secureOzoneManager()).applyToCurrentThread();

    scope.close();
    scope.close();

    assertSame(previousCall, Server.getCurCall().get());
  }

  @Test
  void closeRejectsDifferentThread() throws Exception {
    Server.Call previousCall = createCall("previous-user");
    Server.getCurCall().set(previousCall);
    OMThreadContext.Scope scope = OMThreadContext.forRead(
        requestWithContext("request-user", null), secureOzoneManager()).applyToCurrentThread();

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<Void> close = executor.submit(() -> {
        scope.close();
        return null;
      });
      ExecutionException exception = assertThrows(ExecutionException.class, close::get);
      assertInstanceOf(IllegalStateException.class, exception.getCause());
      assertEquals("OM thread context must be closed on the thread where it was applied",
          exception.getCause().getMessage());
    } finally {
      executor.shutdownNow();
      scope.close();
    }

    assertSame(previousCall, Server.getCurCall().get());
  }

  private static OzoneManager secureOzoneManager() {
    OzoneManager ozoneManager = mock(OzoneManager.class);
    when(ozoneManager.isSecurityEnabled()).thenReturn(true);
    return ozoneManager;
  }

  private static OMRequest requestWithContext(String userName, S3Authentication s3Authentication) {
    OMRequest.Builder builder = OMRequest.newBuilder()
        .setCmdType(Type.ServiceList)
        .setClientId("client-id")
        .setUserInfo(UserInfo.newBuilder()
            .setUserName(userName)
            .setHostName("localhost")
            .setRemoteAddress("127.0.0.1"));
    if (s3Authentication != null) {
      builder.setS3Authentication(s3Authentication);
    }
    return builder.build();
  }

  private static Server.Call createCall(String userName) throws Exception {
    UserGroupInformation user = UserGroupInformation.createRemoteUser(userName);
    InetAddress address = InetAddress.getByAddress("localhost", new byte[] {127, 0, 0, 1});
    return new Server.Call(RpcConstants.INVALID_CALL_ID, RpcConstants.INVALID_RETRY_COUNT,
        null, null, RPC.RpcKind.RPC_PROTOCOL_BUFFER, RpcConstants.DUMMY_CLIENT_ID) {
      @Override
      public UserGroupInformation getRemoteUser() {
        return user;
      }

      @Override
      public InetAddress getHostInetAddress() {
        return address;
      }
    };
  }
}
