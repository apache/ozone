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

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Test OM GRPC client interceptor to define client ip and hostname headers.
 */
public class TestClientAddressClientInterceptor {

  @Test
  public void testClientAddressEntriesInRequestHeaders() {
    try (MockedStatic<Context> grpcContextStaticMock =
             mockStatic(Context.class)) {
      // given
      Context.Key<String> ipAddressContextKey = mock(Context.Key.class);
      when(ipAddressContextKey.get()).thenReturn("172.43.3.2");
      grpcContextStaticMock.when(() -> Context.key("CLIENT_IP_ADDRESS"))
          .thenReturn(ipAddressContextKey);

      Context.Key<String> hostnameContextKey = mock(Context.Key.class);
      when(hostnameContextKey.get()).thenReturn("host.example.com");
      grpcContextStaticMock.when(() -> Context.key("CLIENT_HOSTNAME"))
          .thenReturn(hostnameContextKey);

      ClientInterceptor interceptor = new ClientAddressClientInterceptor();
      Channel channel = mock(Channel.class);
      MethodDescriptor methodDescriptor = mock(MethodDescriptor.class);
      CallOptions callOptions = mock(CallOptions.class);
      ClientCall delegate = mock(ClientCall.class);
      when(channel.newCall(eq(methodDescriptor), eq(callOptions)))
          .thenReturn(delegate);

      // when
      ClientCall clientCall = interceptor.interceptCall(methodDescriptor,
          callOptions, channel);
      Metadata metadata = mock(Metadata.class);
      clientCall.start(mock(ClientCall.Listener.class), metadata);

      // then
      verify(metadata).put(GrpcClientConstants.CLIENT_HOSTNAME_METADATA_KEY,
          hostnameContextKey.get());
      verify(metadata).put(GrpcClientConstants.CLIENT_IP_ADDRESS_METADATA_KEY,
          ipAddressContextKey.get());
    }
  }

}
