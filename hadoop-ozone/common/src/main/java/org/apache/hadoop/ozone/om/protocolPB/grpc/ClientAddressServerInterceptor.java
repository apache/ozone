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

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/**
 * GRPC server side interceptor to retrieve the client IP and hostname.
 */
public class ClientAddressServerInterceptor implements ServerInterceptor {
  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers,
      ServerCallHandler<ReqT, RespT> next) {
    String clientHostname =
        headers.get(GrpcClientConstants.CLIENT_HOSTNAME_METADATA_KEY);
    String clientIpAddress =
        headers.get(GrpcClientConstants.CLIENT_IP_ADDRESS_METADATA_KEY);

    Context ctx = Context.current()
        .withValue(GrpcClientConstants.CLIENT_HOSTNAME_CTX_KEY,
            clientHostname)
        .withValue(GrpcClientConstants.CLIENT_IP_ADDRESS_CTX_KEY,
            clientIpAddress);
    return Contexts.interceptCall(ctx, call, headers, next);
  }
}
