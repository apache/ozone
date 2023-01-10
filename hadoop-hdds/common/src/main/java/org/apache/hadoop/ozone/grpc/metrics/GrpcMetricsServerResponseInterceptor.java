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

import com.google.protobuf.AbstractMessage;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ForwardingServerCall;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

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

            long messageSize = 0;
            // message is always an instance of AbstractMessage
            if (message instanceof AbstractMessage) {
              AbstractMessage parsedMessage = (AbstractMessage) message;
              messageSize += parsedMessage.getSerializedSize();
            } else {
              ByteArrayOutputStream byteArrayOutputStream =
                  new ByteArrayOutputStream();
              try {
                ObjectOutputStream outputStream =
                    new ObjectOutputStream(byteArrayOutputStream);
                outputStream.writeObject(message);
                outputStream.flush();
                outputStream.close();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }

              InputStream inputStream = new ByteArrayInputStream(
                  byteArrayOutputStream.toByteArray());
              ByteBuffer buffer;
              try {
                buffer = ByteBuffer.wrap(IOUtils.toByteArray(inputStream));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              messageSize += buffer.remaining();
            }

            bytesSent += messageSize;

            if (bytesSent > 0) {
              grpcMetrics.setSentBytes(bytesSent);
            }
            super.sendMessage(message);
          }
        }, headers);
  }
}