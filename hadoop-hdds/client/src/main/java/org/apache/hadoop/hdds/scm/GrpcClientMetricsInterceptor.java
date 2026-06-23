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

package org.apache.hadoop.hdds.scm;

import org.apache.ratis.thirdparty.io.grpc.CallOptions;
import org.apache.ratis.thirdparty.io.grpc.Channel;
import org.apache.ratis.thirdparty.io.grpc.ClientCall;
import org.apache.ratis.thirdparty.io.grpc.ClientInterceptor;
import org.apache.ratis.thirdparty.io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import org.apache.ratis.thirdparty.io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.Status;

/**
 * Interceptor to capture gRPC level metrics.
 */
public class GrpcClientMetricsInterceptor implements ClientInterceptor {

  private final XceiverClientMetrics metrics;

  public GrpcClientMetricsInterceptor(XceiverClientMetrics metrics) {
    this.metrics = metrics;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions,
      Channel next) {

    return new SimpleForwardingClientCall<ReqT, RespT>(
        next.newCall(method, callOptions)) {

      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        super.start(new SimpleForwardingClientCallListener<RespT>(responseListener) {
          @Override
          public void onClose(Status status, Metadata trailers) {
            if (!status.isOk()) {
              if (status.getCode() == Status.Code.UNAUTHENTICATED) {
                metrics.incGrpcAuthenticationFailures();
              } else if (status.getCode() == Status.Code.UNAVAILABLE ||
                  status.getCode() == Status.Code.DEADLINE_EXCEEDED) {
                metrics.incGrpcConnectionFailures();
              }
            }
            super.onClose(status, trailers);
          }
        }, headers);
      }
    };
  }
}
