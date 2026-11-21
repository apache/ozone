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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.ratis.thirdparty.io.grpc.CallOptions;
import org.apache.ratis.thirdparty.io.grpc.Channel;
import org.apache.ratis.thirdparty.io.grpc.Drainable;
import org.apache.ratis.thirdparty.io.grpc.KnownLength;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition;
import org.apache.ratis.thirdparty.io.grpc.ServiceDescriptor;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.stub.ClientCalls;
import org.apache.ratis.thirdparty.io.grpc.stub.ServerCalls;

/**
 * The gRPC binding of the OM service used by the S3 gateway, on Ratis's shaded gRPC.
 * <p>
 * {@link OMRequest} and {@link OMResponse} are unshaded protobuf messages, which the shaded {@code ProtoUtils}
 * marshallers don't accept, so this class takes the place of the stub that protoc-gen-grpc-java generated. The service
 * and method names, and the bytes on the wire, are the same as with the generated stub, so old and new clients and
 * servers interoperate.
 */
public final class OzoneManagerServiceGrpcBinding {

  public static final String SERVICE_NAME = "hadoop.ozone.OzoneManagerService";

  private static final MethodDescriptor<OMRequest, OMResponse> SUBMIT_REQUEST_METHOD =
      MethodDescriptor.<OMRequest, OMResponse>newBuilder()
          .setType(MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "submitRequest"))
          .setSampledToLocalTracing(true)
          .setRequestMarshaller(marshaller(OMRequest.parser()))
          .setResponseMarshaller(marshaller(OMResponse.parser()))
          .build();

  private static final ServiceDescriptor SERVICE_DESCRIPTOR =
      ServiceDescriptor.newBuilder(SERVICE_NAME).addMethod(SUBMIT_REQUEST_METHOD).build();

  private OzoneManagerServiceGrpcBinding() {
  }

  public static MethodDescriptor<OMRequest, OMResponse> getSubmitRequestMethod() {
    return SUBMIT_REQUEST_METHOD;
  }

  /** The server side: the service definition that calls {@code submitRequest} for every request. */
  public static ServerServiceDefinition bindService(ServerCalls.UnaryMethod<OMRequest, OMResponse> submitRequest) {
    return ServerServiceDefinition.builder(SERVICE_DESCRIPTOR)
        .addMethod(SUBMIT_REQUEST_METHOD, ServerCalls.asyncUnaryCall(submitRequest))
        .build();
  }

  /** The client side: a blocking call, as the generated blocking stub made it. */
  public static OMResponse submitRequest(Channel channel, OMRequest request) {
    return ClientCalls.blockingUnaryCall(channel, SUBMIT_REQUEST_METHOD, CallOptions.DEFAULT, request);
  }

  /** A marshaller for unshaded protobuf messages, writing the same bytes as {@code ProtoUtils.marshaller}. */
  static <T extends MessageLite> MethodDescriptor.Marshaller<T> marshaller(Parser<T> parser) {
    return new MethodDescriptor.Marshaller<T>() {
      @Override
      public InputStream stream(T message) {
        return new MessageInputStream(message);
      }

      @Override
      public T parse(InputStream stream) {
        try {
          return parser.parseFrom(stream);
        } catch (InvalidProtocolBufferException e) {
          throw Status.INTERNAL.withDescription("Invalid protobuf byte sequence").withCause(e).asRuntimeException();
        }
      }
    };
  }

  /** A metadata marshaller for unshaded protobuf messages, as {@code ProtoUtils.metadataMarshaller}. */
  static <T extends MessageLite> Metadata.BinaryMarshaller<T> metadataMarshaller(Parser<T> parser) {
    return new Metadata.BinaryMarshaller<T>() {
      @Override
      public byte[] toBytes(T value) {
        return value.toByteArray();
      }

      @Override
      public T parseBytes(byte[] serialized) {
        try {
          return parser.parseFrom(serialized);
        } catch (InvalidProtocolBufferException e) {
          throw new IllegalArgumentException(e);
        }
      }
    };
  }

  /**
   * Serializes the message lazily. gRPC drains it straight into its frame ({@link Drainable}), sized by
   * {@link #available()} ({@link KnownLength}), so the common path makes no intermediate copy.
   */
  private static final class MessageInputStream extends InputStream implements Drainable, KnownLength {
    private MessageLite message;
    private ByteArrayInputStream serialized;

    MessageInputStream(MessageLite message) {
      this.message = message;
    }

    @Override
    public int drainTo(OutputStream target) throws IOException {
      int written = 0;
      if (message != null) {
        written = message.getSerializedSize();
        message.writeTo(target);
        message = null;
      } else if (serialized != null) {
        byte[] rest = new byte[serialized.available()];
        written = serialized.read(rest, 0, rest.length);
        target.write(rest, 0, Math.max(written, 0));
        serialized = null;
      }
      return Math.max(written, 0);
    }

    @Override
    public int read() {
      return toByteStream() == null ? -1 : serialized.read();
    }

    @Override
    public int read(byte[] b, int off, int len) {
      return toByteStream() == null ? -1 : serialized.read(b, off, len);
    }

    @Override
    public int available() {
      if (message != null) {
        return message.getSerializedSize();
      }
      return serialized == null ? 0 : serialized.available();
    }

    private ByteArrayInputStream toByteStream() {
      if (message != null) {
        serialized = new ByteArrayInputStream(message.toByteArray());
        message = null;
      }
      return serialized;
    }
  }
}
