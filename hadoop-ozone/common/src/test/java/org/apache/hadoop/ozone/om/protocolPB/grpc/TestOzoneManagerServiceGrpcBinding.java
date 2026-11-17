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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMGrpcErrorProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ratis.thirdparty.io.grpc.Drainable;
import org.apache.ratis.thirdparty.io.grpc.KnownLength;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OzoneManagerServiceGrpcBinding}. The method name and the bytes on the wire must stay those of the
 * stub that protoc-gen-grpc-java generated before HDDS-14949, so that old and new S3 gateways and OMs interoperate.
 */
public class TestOzoneManagerServiceGrpcBinding {
  private static final int MAX_MESSAGE_SIZE = 8 * 1024 * 1024;

  private static final OMRequest REQUEST = OMRequest.newBuilder()
      .setCmdType(Type.ListVolume).setClientId("client-1").setTraceID("trace-1").build();

  private static OMResponse response(int messageLength) {
    return OMResponse.newBuilder().setCmdType(Type.ListVolume).setStatus(Status.OK)
        .setMessage(StringUtils.repeat('x', messageLength)).build();
  }

  @Test
  public void testMethodIsTheGeneratedStubsMethod() {
    MethodDescriptor<OMRequest, OMResponse> method = OzoneManagerServiceGrpcBinding.getSubmitRequestMethod();
    // the full name of OzoneManagerServiceGrpc.getSubmitRequestMethod() in the generated stub
    assertEquals("hadoop.ozone.OzoneManagerService/submitRequest", method.getFullMethodName());
    assertEquals(MethodDescriptor.MethodType.UNARY, method.getType());
  }

  @Test
  public void testMarshallerWritesTheMessageBytes() throws IOException {
    MethodDescriptor<OMRequest, OMResponse> method = OzoneManagerServiceGrpcBinding.getSubmitRequestMethod();
    OMResponse response = response(1000);

    // drained, as gRPC writes a frame
    assertArrayEquals(REQUEST.toByteArray(), drain(method.streamRequest(REQUEST)));
    assertArrayEquals(response.toByteArray(), drain(method.streamResponse(response)));
    // read byte by byte, and read after a partial read
    assertArrayEquals(response.toByteArray(), readAll(method.streamResponse(response)));
    InputStream partial = method.streamResponse(response);
    byte[] first = new byte[10];
    assertEquals(10, partial.read(first, 0, 10));
    ByteArrayOutputStream rest = new ByteArrayOutputStream();
    rest.write(first);
    ((Drainable) partial).drainTo(rest);
    assertArrayEquals(response.toByteArray(), rest.toByteArray());
    assertEquals(-1, partial.read());

    InputStream sized = method.streamResponse(response);
    assertEquals(response.getSerializedSize(), sized.available());
    assertEquals(response.getSerializedSize(), ((KnownLength) sized).available());
  }

  @Test
  public void testMarshallerParsesWhatItWrites() {
    MethodDescriptor<OMRequest, OMResponse> method = OzoneManagerServiceGrpcBinding.getSubmitRequestMethod();
    OMResponse response = response(1000);
    assertEquals(REQUEST, method.parseRequest(method.streamRequest(REQUEST)));
    assertEquals(response, method.parseResponse(method.streamResponse(response)));

    StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
        () -> method.parseRequest(new ByteArrayInputStream(new byte[] {(byte) 0xff, (byte) 0xff})));
    assertEquals(org.apache.ratis.thirdparty.io.grpc.Status.Code.INTERNAL, e.getStatus().getCode());
  }

  @Test
  public void testMetadataMarshaller() {
    Metadata.BinaryMarshaller<OMGrpcErrorProto> marshaller =
        OzoneManagerServiceGrpcBinding.metadataMarshaller(OMGrpcErrorProto.parser());
    OMGrpcErrorProto error = OMGrpcError.fromException(new OMNotLeaderException("om1 is not the leader", "om2"))
        .getProtobuf();
    assertArrayEquals(error.toByteArray(), marshaller.toBytes(error));
    assertEquals(error, marshaller.parseBytes(marshaller.toBytes(error)));
    assertThrows(IllegalArgumentException.class, () -> marshaller.parseBytes(new byte[] {(byte) 0xff}));
  }

  @Test
  public void testCallOverLoopback() throws Exception {
    Server server = NettyServerBuilder.forAddress(new InetSocketAddress("localhost", 0))
        .addService(OzoneManagerServiceGrpcBinding.bindService((request, observer) -> {
          if (request.getClientId().equals("notleader")) {
            observer.onError(OMGrpcError.toStatusRuntimeException(new OMNotLeaderException("not leader", "om2")));
            return;
          }
          observer.onNext(response(Integer.parseInt(request.getClientId())));
          observer.onCompleted();
        }))
        .build().start();
    ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", server.getPort())
        .usePlaintext().maxInboundMessageSize(MAX_MESSAGE_SIZE).build();
    try {
      // just under the limit; the response carries a few bytes besides the message
      int fits = MAX_MESSAGE_SIZE - 64;
      assertEquals(fits, OzoneManagerServiceGrpcBinding.submitRequest(channel, request(String.valueOf(fits)))
          .getMessage().length());

      StatusRuntimeException tooLarge = assertThrows(StatusRuntimeException.class,
          () -> OzoneManagerServiceGrpcBinding.submitRequest(channel, request(String.valueOf(MAX_MESSAGE_SIZE))));
      assertEquals(org.apache.ratis.thirdparty.io.grpc.Status.Code.RESOURCE_EXHAUSTED,
          tooLarge.getStatus().getCode());

      StatusRuntimeException notLeader = assertThrows(StatusRuntimeException.class,
          () -> OzoneManagerServiceGrpcBinding.submitRequest(channel, request("notleader")));
      OMGrpcError error = OMGrpcError.fromStatusRuntimeException(notLeader);
      assertEquals(OMGrpcError.Kind.NOT_LEADER, error.getKind());
      assertEquals("om2", error.getSuggestedLeaderNodeId());
    } finally {
      channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
      server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  private static OMRequest request(String clientId) {
    return REQUEST.toBuilder().setClientId(clientId).build();
  }

  private static byte[] drain(InputStream stream) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ((Drainable) stream).drainTo(out);
    return out.toByteArray();
  }

  private static byte[] readAll(InputStream stream) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int b = stream.read(); b != -1; b = stream.read()) {
      out.write(b);
    }
    return out.toByteArray();
  }
}
