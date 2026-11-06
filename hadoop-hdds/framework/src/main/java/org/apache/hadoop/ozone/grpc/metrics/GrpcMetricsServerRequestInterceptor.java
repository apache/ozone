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

package org.apache.hadoop.ozone.grpc.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Descriptors;
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.util.function.LongSupplier;

/**
 * Interceptor to gather metrics based on grpc server request.
 *
 * <p>One instance serves every call on the server, so the timestamps of a call are kept on that call's listener.
 * Queue and processing times are recorded in nanoseconds.
 */
public class GrpcMetricsServerRequestInterceptor implements ServerInterceptor {

  private static final String CMD_TYPE_FIELD = "cmdType";

  private final GrpcMetrics grpcMetrics;
  private final LongSupplier nanoClock;

  public GrpcMetricsServerRequestInterceptor(
      GrpcMetrics grpcMetrics) {
    this(grpcMetrics, System::nanoTime);
  }

  @VisibleForTesting
  GrpcMetricsServerRequestInterceptor(GrpcMetrics grpcMetrics, LongSupplier nanoClock) {
    this.grpcMetrics = grpcMetrics;
    this.nanoClock = nanoClock;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> serverCall, Metadata headers,
      ServerCallHandler<ReqT, RespT> serverCallHandler) {

    // received time
    final long receivedTime = nanoClock.getAsLong();

    return new SimpleForwardingServerCallListener<ReqT>(
        serverCallHandler.startCall(serverCall, headers)) {

      // gRPC calls the listener of one call serially, so these need no synchronization
      private long startTime;
      private boolean messageReceived;

      @Override
      public void onMessage(ReqT message) {
        // start time
        startTime = nanoClock.getAsLong();
        messageReceived = true;

        long messageSize = 0;
        if (message instanceof AbstractMessage) {
          AbstractMessage parsedMessage = (AbstractMessage) message;
          messageSize += parsedMessage.getSerializedSize();
          // Read the field directly instead of rendering the whole request to text
          Descriptors.FieldDescriptor cmdType =
              parsedMessage.getDescriptorForType().findFieldByName(CMD_TYPE_FIELD);
          if (cmdType != null && parsedMessage.hasField(cmdType)) {
            grpcMetrics.setRequestType(parsedMessage.getField(cmdType).toString());
          }
        } else {
          grpcMetrics.incrUnknownMessagesReceived();
        }

        grpcMetrics.incrReceivedBytes(messageSize);

        super.onMessage(message);
      }

      @Override
      public void onComplete() {
        super.onComplete();
        // a call that completes without a request has no start time
        if (!messageReceived) {
          return;
        }
        // end time
        long endTime = nanoClock.getAsLong();

        // set metrics queue time
        grpcMetrics.addGrpcQueueTime(startTime - receivedTime);

        // set metrics processing time
        grpcMetrics.addGrpcProcessingTime(endTime - startTime);
      }
    };
  }
}
