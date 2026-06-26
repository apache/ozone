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

package org.apache.hadoop.ozone.om.request;

import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.newBucketInfoBuilder;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.newCreateBucketRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mockStatic;

import java.util.UUID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketCreateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Tests that {@link OMClientRequest} does not silently fall back to the OM
 * starter/login user when a request carries no user information (HDDS-15467).
 */
public class TestOMClientRequestUserInfoFallback {

  private OMRequest newBucketRequest(UserInfo userInfo) {
    BucketInfo.Builder bucketInfo = newBucketInfoBuilder(
        UUID.randomUUID().toString(), UUID.randomUUID().toString())
        .setIsVersionEnabled(true)
        .setStorageType(StorageTypeProto.DISK);
    OMRequest.Builder builder = newCreateBucketRequest(bucketInfo);
    if (userInfo != null) {
      builder.setUserInfo(userInfo);
    }
    return builder.build();
  }

  /**
   * With no RPC/gRPC context and no UserInfo on the request, getUserInfo() must
   * not manufacture an identity from the OM starter user; createUGI() then
   * fails closed instead of silently escalating.
   */
  @Test
  public void noFallbackToServerUserWhenUserInfoMissing() throws Exception {
    try (MockedStatic<Server> mockedRpcServer = mockStatic(Server.class)) {
      mockedRpcServer.when(Server::getRemoteUser).thenReturn(null);
      mockedRpcServer.when(Server::getRemoteIp).thenReturn(null);

      OMRequest omRequest = newBucketRequest(null);
      OMClientRequest request = new OMBucketCreateRequest(omRequest);

      UserInfo userInfo = request.getUserInfo();
      assertFalse(userInfo.hasUserName());
      assertFalse(userInfo.hasRemoteAddress());

      OMClientRequest withUserInfo = new OMBucketCreateRequest(
          omRequest.toBuilder().setUserInfo(userInfo).build());
      assertThrows(AuthenticationException.class, withUserInfo::createUGI);
    }
  }

  /**
   * An internal service (e.g. the Trash emptier) populates its own UserInfo.
   * With no RPC/gRPC context, getUserInfo() must preserve that identity rather
   * than replacing it with the OM starter user.
   */
  @Test
  public void internalServiceUserInfoIsPreserved() throws Exception {
    try (MockedStatic<Server> mockedRpcServer = mockStatic(Server.class)) {
      mockedRpcServer.when(Server::getRemoteUser).thenReturn(null);
      mockedRpcServer.when(Server::getRemoteIp).thenReturn(null);

      UserInfo serviceUserInfo = UserInfo.newBuilder()
          .setUserName("trash-service-user")
          .setHostName("om-host")
          .setRemoteAddress("10.0.0.9")
          .build();

      OMClientRequest request =
          new OMBucketCreateRequest(newBucketRequest(serviceUserInfo));

      UserInfo result = request.getUserInfo();
      assertEquals("trash-service-user", result.getUserName());
      assertEquals("10.0.0.9", result.getRemoteAddress());
      assertEquals("om-host", result.getHostName());
    }
  }
}
