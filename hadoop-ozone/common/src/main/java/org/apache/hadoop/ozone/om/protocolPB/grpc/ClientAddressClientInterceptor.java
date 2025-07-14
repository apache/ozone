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

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * GRPC client side interceptor to provide client hostname and IP address.
 */
public class ClientAddressClientInterceptor implements ClientInterceptor {
  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions,
      Channel channel) {
    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
        channel.newCall(methodDescriptor, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        String ipAddress = GrpcClientConstants.CLIENT_HOSTNAME_CTX_KEY.get();
        if (ipAddress != null) {
          headers.put(GrpcClientConstants.CLIENT_HOSTNAME_METADATA_KEY,
              ipAddress);
        }
        String hostname = GrpcClientConstants.CLIENT_IP_ADDRESS_CTX_KEY.get();
        if (GrpcClientConstants.CLIENT_IP_ADDRESS_CTX_KEY.get() != null) {
          headers.put(GrpcClientConstants.CLIENT_IP_ADDRESS_METADATA_KEY,
              hostname);
        }
        super.start(responseListener, headers);
      }
    };
  }
}
