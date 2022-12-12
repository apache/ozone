/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.grpc.metrics;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ForwardingServerCall;

import java.nio.charset.StandardCharsets;

/**
 * Interceptor to gather metrics based on grpc server response.
 */
public class GrpcMetricsServerResponseInterceptor implements ServerInterceptor {

  private final GrpcMetrics grpcMetrics;
  private long bytesSent;

  public GrpcMetricsServerResponseInterceptor(
      GrpcMetrics grpcMetrics) {
    super();
    this.grpcMetrics = grpcMetrics;
    this.bytesSent = 0;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> serverCall, Metadata headers,
      ServerCallHandler<ReqT, RespT> serverCallHandler) {

    return serverCallHandler.startCall(
        new ForwardingServerCall
            .SimpleForwardingServerCall<ReqT, RespT>(serverCall) {
          @Override
          public void sendMessage(RespT message) {
            final byte[] messageBytes =
                message.toString().getBytes(StandardCharsets.UTF_8);

            bytesSent += messageBytes.length;

            if (bytesSent > 0) {
              grpcMetrics.setSentBytes(bytesSent);
            }
            super.sendMessage(message);
          }
        }, headers);
  }
}