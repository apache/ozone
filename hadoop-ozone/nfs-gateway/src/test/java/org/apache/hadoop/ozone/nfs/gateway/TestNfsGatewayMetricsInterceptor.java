/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.nfs.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayGrpc;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.GetattrRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.GetattrResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.Result;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TestNfsGatewayMetricsInterceptor {
  @AfterEach
  void unregisterMetrics() {
    NfsGatewayMetrics.unregister();
  }

  @Test
  @SuppressWarnings("unchecked")
  void recordsNfsAndGrpcErrorsAndLatency() {
    NfsGatewayMetrics metrics = NfsGatewayMetrics.create();
    NfsGatewayMetricsInterceptor interceptor = new NfsGatewayMetricsInterceptor(metrics);
    AtomicReference<ServerCall<GetattrRequest, GetattrResponse>> forwarded = new AtomicReference<>();
    ServerCall<GetattrRequest, GetattrResponse> call = mock(ServerCall.class);
    when(call.getMethodDescriptor()).thenReturn(NfsGatewayGrpc.getGetattrMethod());
    ServerCallHandler<GetattrRequest, GetattrResponse> next = (serverCall, headers) -> {
      forwarded.set(serverCall);
      return new ServerCall.Listener<GetattrRequest>() { };
    };

    interceptor.interceptCall(call, new Metadata(), next);
    forwarded.get().sendMessage(GetattrResponse.newBuilder()
        .setResult(Result.newBuilder().setNfsStatus(13)).build());
    forwarded.get().close(Status.OK, new Metadata());

    interceptor.interceptCall(call, new Metadata(), next);
    forwarded.get().close(Status.UNAVAILABLE, new Metadata());

    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    metrics.getMetrics(collector, true);
    String snapshot = collector.getRecords().toString();
    assertThat(snapshot).contains("name=GetattrRequests, description=Number of Getattr RPCs}, value=2")
        .contains("name=GetattrNfsErrors, description=NFS error responses for Getattr}, value=1")
        .contains("name=GetattrRpcErrors, description=gRPC failures for Getattr}, value=1")
        .contains("name=GetattrLatencyNsNumOps, description=Number of ops for getattr RPC latency in nanoseconds}, "
            + "value=2");
  }
}
