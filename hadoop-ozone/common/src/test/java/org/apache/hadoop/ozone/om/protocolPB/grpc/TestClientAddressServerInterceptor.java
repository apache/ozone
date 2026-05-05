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

package org.apache.hadoop.ozone.om.protocolPB.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

/**
 * Test OM GRPC server interceptor to define client ip and hostname.
 */
public class TestClientAddressServerInterceptor {

  @Test
  public void testClientAddressEntriesInHeaders() {
    try (MockedStatic<Contexts> contextsMockedStatic =
             mockStatic(Contexts.class)) {
      // given
      ServerInterceptor serverInterceptor =
          new ClientAddressServerInterceptor();
      ServerCall serverCall = mock(ServerCall.class);
      ServerCallHandler serverCallHandler = mock(ServerCallHandler.class);
      Metadata headers = mock(Metadata.class);
      when(headers.get(GrpcClientConstants.CLIENT_HOSTNAME_METADATA_KEY))
          .thenReturn("host.example.com");
      when(headers.get(GrpcClientConstants.CLIENT_IP_ADDRESS_METADATA_KEY))
          .thenReturn("173.56.23.4");

      // when
      serverInterceptor.interceptCall(serverCall, headers, serverCallHandler);

      // then
      ArgumentCaptor<Context> contextArgumentCaptor =
          ArgumentCaptor.forClass(Context.class);
      contextsMockedStatic.verify(
          () -> {
            Contexts.interceptCall(contextArgumentCaptor.capture(),
                eq(serverCall), eq(headers), eq(serverCallHandler));
          }
      );
      Context context = contextArgumentCaptor.getValue();
      context.attach();
      assertEquals("host.example.com",
          GrpcClientConstants.CLIENT_HOSTNAME_CTX_KEY.get());
      assertEquals("173.56.23.4",
          GrpcClientConstants.CLIENT_IP_ADDRESS_CTX_KEY.get());
    }
  }

}
