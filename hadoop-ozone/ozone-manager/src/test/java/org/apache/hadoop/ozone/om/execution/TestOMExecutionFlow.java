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

package org.apache.hadoop.ozone.om.execution;

import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.om.OzoneAclUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.ozone.security.STSTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TestOMExecutionFlow {

  @AfterEach
  void tearDown() {
    Server.getCurCall().remove();
    OzoneManager.setS3Auth(null);
    OzoneManager.setStsTokenIdentifier(null);
  }

  @Test
  void readRequestUsesAuthenticatedCallerContext() throws Exception {
    OzoneManager ozoneManager = mock(OzoneManager.class);
    OzoneManagerRatisServer ratisServer = mock(OzoneManagerRatisServer.class);
    AtomicReference<OMRequest> submittedRequest = new AtomicReference<>();
    when(ozoneManager.getOmRatisServer()).thenReturn(ratisServer);
    when(ratisServer.submitRequest(any(OMRequest.class), eq(false))).thenAnswer(invocation -> {
      submittedRequest.set(invocation.getArgument(0));
      return successResponse();
    });

    UserGroupInformation realUser = UserGroupInformation.createRemoteUser("kerberos-user");
    realUser.setAuthenticationMethod(KERBEROS);
    UserGroupInformation remoteUser = UserGroupInformation.createProxyUser("authenticated-user", realUser);
    Credentials credentials = new Credentials();
    credentials.addSecretKey(new Text("credential-key"), new byte[] {1, 2, 3});
    remoteUser.addCredentials(credentials);
    remoteUser.addToken(new Token<TokenIdentifier>(
        new byte[] {4}, new byte[] {5}, new Text("token-kind"), new Text("token-service")));
    InetAddress remoteAddress = InetAddress.getByAddress("client.example.com", new byte[] {10, 20, 30, 40});
    Server.getCurCall().set(createCall(remoteUser, remoteAddress));

    OMRequest request = OMRequest.newBuilder()
        .setCmdType(Type.ServiceList)
        .setClientId("client-id")
        .setUserInfo(UserInfo.newBuilder()
            .setUserName("forged-user")
            .setHostName("forged-host")
            .setRemoteAddress("192.0.2.1"))
        .build();

    new OMExecutionFlow(ozoneManager).submit(request, false);

    UserInfo userInfo = submittedRequest.get().getUserInfo();
    assertEquals("authenticated-user", userInfo.getUserName());
    assertEquals("client.example.com", userInfo.getHostName());
    assertEquals("10.20.30.40", userInfo.getRemoteAddress());
  }

  @Test
  void readRequestCarriesResolvedStsContext() throws Exception {
    OzoneManager ozoneManager = mock(OzoneManager.class);
    OzoneManagerRatisServer ratisServer = mock(OzoneManagerRatisServer.class);
    AtomicReference<OMRequest> submittedRequest = new AtomicReference<>();
    when(ozoneManager.getOmRatisServer()).thenReturn(ratisServer);
    when(ozoneManager.isSecurityEnabled()).thenReturn(true);
    when(ratisServer.submitRequest(any(OMRequest.class), eq(false))).thenAnswer(invocation -> {
      submittedRequest.set(invocation.getArgument(0));
      return successResponse();
    });

    S3Authentication s3Authentication = S3Authentication.newBuilder()
        .setAccessId("temp-access-id")
        .setSessionToken("session-token")
        .setS3Action("GetObject")
        .build();
    STSTokenIdentifier stsToken = mock(STSTokenIdentifier.class);
    UUID secretKeyId = UUID.randomUUID();
    when(stsToken.getSessionPolicy()).thenReturn("session-policy");
    when(stsToken.getRoleArn()).thenReturn("role-arn");
    when(stsToken.getOriginalAccessKeyId()).thenReturn("original-access-id");
    when(stsToken.getTempAccessKeyId()).thenReturn("temp-access-id");
    when(stsToken.getSecretKeyId()).thenReturn(secretKeyId);
    OzoneManager.setS3Auth(s3Authentication);
    OzoneManager.setStsTokenIdentifier(stsToken);

    OMRequest request = OMRequest.newBuilder()
        .setCmdType(Type.ServiceList)
        .setClientId("client-id")
        .setS3Authentication(s3Authentication)
        .setUserInfo(UserInfo.newBuilder().setUserName("forged-user"))
        .build();

    new OMExecutionFlow(ozoneManager).submit(request, false);

    OMRequest submitted = submittedRequest.get();
    assertEquals(OzoneAclUtils.accessIdToUserPrincipal("original-access-id"),
        submitted.getUserInfo().getUserName());
    assertEquals("session-policy", submitted.getS3Authentication().getResolvedStsSessionPolicy());
    assertEquals("role-arn", submitted.getS3Authentication().getResolvedStsRoleArn());
    assertEquals("original-access-id", submitted.getS3Authentication().getResolvedStsOriginalAccessKeyId());
    assertEquals("temp-access-id", submitted.getS3Authentication().getResolvedStsTempAccessKeyId());
    assertEquals(secretKeyId.toString(), submitted.getS3Authentication().getResolvedStsSecretKeyId());
  }

  private static Server.Call createCall(UserGroupInformation user, InetAddress remoteAddress) {
    return new Server.Call(1, 0, null, null, RPC.RpcKind.RPC_PROTOCOL_BUFFER, new byte[0]) {
      @Override
      public UserGroupInformation getRemoteUser() {
        return user;
      }

      @Override
      public InetAddress getHostInetAddress() {
        return remoteAddress;
      }
    };
  }

  private static OMResponse successResponse() {
    return OMResponse.newBuilder()
        .setCmdType(Type.ServiceList)
        .setStatus(Status.OK)
        .setSuccess(true)
        .build();
  }
}
