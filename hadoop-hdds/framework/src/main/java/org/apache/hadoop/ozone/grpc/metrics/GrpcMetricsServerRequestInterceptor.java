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

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Descriptors;
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/**
 * Interceptor to gather metrics based on grpc server request.
 */
public class GrpcMetricsServerRequestInterceptor implements ServerInterceptor {

  private static final String CMD_TYPE_FIELD = "cmdType";

  private final GrpcMetrics grpcMetrics;
  private long receivedTime;
  private long startTime;
  private long endTime;

  public GrpcMetricsServerRequestInterceptor(
      GrpcMetrics grpcMetrics) {
    super();
    this.grpcMetrics = grpcMetrics;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> serverCall, Metadata headers,
      ServerCallHandler<ReqT, RespT> serverCallHandler) {

    // received time
    receivedTime = System.nanoTime();

    return new SimpleForwardingServerCallListener<ReqT>(
        serverCallHandler.startCall(serverCall, headers)) {

      @Override
      public void onMessage(ReqT message) {
        // start time
        startTime = System.nanoTime();

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
        // end time
        endTime = System.nanoTime();

        int queueTime = (int) (startTime - receivedTime);
        int processingTime = (int) (endTime - startTime);

        // set metrics queue time
        grpcMetrics.addGrpcQueueTime(queueTime);

        // set metrics processing time
        grpcMetrics.addGrpcProcessingTime(processingTime);
      }
    };
  }
}
