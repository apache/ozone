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

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.concurrent.atomic.AtomicBoolean;

/** Records per-operation latency and error counts for the authenticated backend RPCs. */
final class NfsGatewayMetricsInterceptor implements ServerInterceptor {
  private final NfsGatewayMetrics metrics;

  NfsGatewayMetricsInterceptor(NfsGatewayMetrics metrics) {
    this.metrics = metrics;
  }

  @Override
  public <RequestT, ResponseT> ServerCall.Listener<RequestT> interceptCall(ServerCall<RequestT, ResponseT> call,
      Metadata headers, ServerCallHandler<RequestT, ResponseT> next) {
    String operation = call.getMethodDescriptor().getBareMethodName();
    long startNanos = System.nanoTime();
    AtomicBoolean completed = new AtomicBoolean();
    metrics.recordRequest(operation);
    ServerCall<RequestT, ResponseT> measuredCall = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
      @Override
      public void sendMessage(ResponseT message) {
        Integer nfsStatus = getNfsStatus(message);
        if (nfsStatus != null) {
          metrics.recordNfsStatus(operation, nfsStatus);
        }
        super.sendMessage(message);
      }

      @Override
      public void close(Status status, Metadata trailers) {
        recordCompletion(status);
        super.close(status, trailers);
      }

      private void recordCompletion(Status status) {
        if (completed.compareAndSet(false, true)) {
          metrics.recordCompletion(operation, System.nanoTime() - startNanos, !status.isOk());
        }
      }
    };
    ServerCall.Listener<RequestT> listener = next.startCall(measuredCall, headers);
    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
      @Override
      public void onCancel() {
        if (completed.compareAndSet(false, true)) {
          metrics.recordCompletion(operation, System.nanoTime() - startNanos, true);
        }
        super.onCancel();
      }
    };
  }

  private static Integer getNfsStatus(Object response) {
    if (!(response instanceof Message)) {
      return null;
    }
    Message message = (Message) response;
    FieldDescriptor resultField = message.getDescriptorForType().findFieldByName("result");
    if (resultField == null || !message.hasField(resultField)) {
      return null;
    }
    Object result = message.getField(resultField);
    if (!(result instanceof Message)) {
      return null;
    }
    FieldDescriptor statusField = ((Message) result).getDescriptorForType().findFieldByName("nfs_status");
    return statusField == null ? null : (Integer) ((Message) result).getField(statusField);
  }
}
