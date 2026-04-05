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

package org.apache.hadoop.ozone.om;

import com.google.protobuf.MessageLite;
import com.google.protobuf.RpcController;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.util.UUIDUtil;
import org.apache.ratis.thirdparty.io.grpc.BindableService;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.stub.ServerCalls;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Grpc Service for handling S3 gateway OzoneManagerProtocol client requests.
 * Implements BindableService directly using shaded gRPC with a custom byte-stream
 * marshaller so that vanilla protobuf OMRequest/OMResponse can be used without
 * shading OzoneManagerProtocolProtos (which imports HddsProtos vanilla types).
 */
public class OzoneManagerServiceGrpc implements BindableService {
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerServiceGrpc.class);

  static final String SERVICE_NAME = "hadoop.ozone.OzoneManagerService";

  private static final MethodDescriptor<OMRequest, OMResponse>
      SUBMIT_REQUEST_DESCRIPTOR =
      MethodDescriptor.<OMRequest, OMResponse>newBuilder()
          .setType(MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(
              MethodDescriptor.generateFullMethodName(SERVICE_NAME, "submitRequest"))
          .setRequestMarshaller(proto2Marshaller(OMRequest::parseFrom))
          .setResponseMarshaller(proto2Marshaller(OMResponse::parseFrom))
          .build();

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;
  private final OzoneManagerProtocolServerSideTranslatorPB omTranslator;

  OzoneManagerServiceGrpc(
      OzoneManagerProtocolServerSideTranslatorPB omTranslator) {
    this.omTranslator = omTranslator;
  }

  @Override
  public ServerServiceDefinition bindService() {
    return ServerServiceDefinition.builder(SERVICE_NAME)
        .addMethod(SUBMIT_REQUEST_DESCRIPTOR,
            ServerCalls.asyncUnaryCall(this::submitRequest))
        .build();
  }

  private void submitRequest(OMRequest request,
      StreamObserver<OMResponse> responseObserver) {
    LOG.debug("OzoneManagerServiceGrpc: OzoneManagerServiceImplBase " +
        "processing s3g client submit request - for command {}",
        request.getCmdType().name());
    AtomicInteger callCount = new AtomicInteger(0);

    org.apache.hadoop.ipc_.Server.getCurCall().set(new Server.Call(1,
        callCount.incrementAndGet(),
        null,
        null,
        RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        getClientId()));
    // TODO: currently require setting the Server class for each request
    // with thread context (Server.Call()) that includes retries
    // and importantly random ClientId.  This is currently necessary for
    // Om Ratis Server to create createWriteRaftClientRequest.
    // Look to remove Server class requirement for issuing ratis transactions
    // for OMRequests.  Test through successful ratis-enabled OMRequest
    // handling without dependency on hadoop IPC based Server.
    try {
      OMResponse omResponse = this.omTranslator.
          submitRequest(NULL_RPC_CONTROLLER, request);
      responseObserver.onNext(omResponse);
    } catch (Throwable e) {
      LOG.error("Failed to submit request", e);
      IOException ex = new IOException(e.getCause());
      responseObserver.onError(
          Status.INTERNAL.withDescription(ex.getMessage())
              .asRuntimeException());
      return;
    }
    responseObserver.onCompleted();
  }

  private static byte[] getClientId() {
    return UUIDUtil.randomUUIDBytes();
  }

  private static <T extends MessageLite> MethodDescriptor.Marshaller<T> proto2Marshaller(
      Proto2Parser<T> parser) {
    return new MethodDescriptor.Marshaller<T>() {
      @Override
      public InputStream stream(T value) {
        return new ByteArrayInputStream(value.toByteArray());
      }

      @Override
      public T parse(InputStream stream) {
        try {
          return parser.parse(stream);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    };
  }

  @FunctionalInterface
  private interface Proto2Parser<T> {
    T parse(InputStream stream) throws IOException;
  }
}
