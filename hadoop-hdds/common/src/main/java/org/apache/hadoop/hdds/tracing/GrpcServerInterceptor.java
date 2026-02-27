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

package org.apache.hadoop.hdds.tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.thirdparty.io.grpc.ServerCall.Listener;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor;

/**
 * Interceptor to add the tracing id to the outgoing call header.
 */
public class GrpcServerInterceptor implements ServerInterceptor {

  @Override
  public <ReqT, RespT> Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers,
      ServerCallHandler<ReqT, RespT> next) {

    return new SimpleForwardingServerCallListener<ReqT>(
        next.startCall(call, headers)) {
      @Override
      public void onMessage(ReqT message) {

        Span span = TracingUtil
            .importAndCreateSpan(
                call.getMethodDescriptor().getFullMethodName(),
                headers.get(GrpcClientInterceptor.TRACING_HEADER));
        try (Scope ignored = span.makeCurrent()) {
          super.onMessage(message);
        } finally {
          span.end();
        }
      }
    };
  }
}
