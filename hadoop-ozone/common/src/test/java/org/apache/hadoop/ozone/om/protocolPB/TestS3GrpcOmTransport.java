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

package org.apache.hadoop.ozone.om.protocolPB;

import static org.apache.hadoop.ozone.ClientVersion.CURRENT_VERSION;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_GRPC_PORT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.ServerCalls;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for GrpcOmTransport client.
 */
public class TestS3GrpcOmTransport {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestS3GrpcOmTransport.class);

  private static final String SERVICE_NAME = "hadoop.ozone.OzoneManagerService";

  private static final MethodDescriptor<OMRequest, OMResponse>
      SUBMIT_REQUEST_METHOD = MethodDescriptor.<OMRequest, OMResponse>newBuilder()
      .setType(MethodDescriptor.MethodType.UNARY)
      .setFullMethodName(MethodDescriptor.generateFullMethodName(
          SERVICE_NAME, "submitRequest"))
      .setRequestMarshaller(new Proto2Marshaller<>(OMRequest::parseFrom))
      .setResponseMarshaller(new Proto2Marshaller<>(OMResponse::parseFrom))
      .build();

  private static final String LEADER_OM_NODE_ID = "TestOM";
  private static final String OM_SERVICE_ID = "om-service-test";
  private static final String OM_NODE_ID = "om0";

  private final OMResponse omResponse = OMResponse.newBuilder()
      .setSuccess(true)
      .setStatus(org.apache.hadoop.ozone.protocol
          .proto.OzoneManagerProtocolProtos.Status.OK)
      .setLeaderOMNodeId(LEADER_OM_NODE_ID)
      .setCmdType(Type.AllocateBlock)
      .build();

  private boolean doFailover = false;
  private boolean completeFailover = true;
  private int failoverCount;

  private OzoneConfiguration conf;

  private String omServiceId;
  private UserGroupInformation ugi;
  private Server server;

  private final ServerCalls.UnaryMethod<OMRequest, OMResponse> submitRequestImpl =
      mock(ServerCalls.UnaryMethod.class,
          delegatesTo(new ServerCalls.UnaryMethod<OMRequest, OMResponse>() {
            @Override
            public void invoke(OMRequest request,
                               StreamObserver<OMResponse> responseObserver) {
              try {
                if (doFailover) {
                  if (completeFailover) {
                    doFailover = false;
                  }
                  failoverCount++;
                  throw createNotLeaderException();
                } else {
                  responseObserver.onNext(omResponse);
                  responseObserver.onCompleted();
                }
              } catch (Throwable e) {
                IOException ex = new IOException(e.getCause());
                responseObserver.onError(Status.INTERNAL
                    .withDescription(ex.getMessage())
                    .asRuntimeException());
              }
            }
          }));

  private GrpcOmTransport client;

  private ServiceException createNotLeaderException() {
    RaftPeerId raftPeerId = RaftPeerId.getRaftPeerId("testNodeId");

    // TODO: Set suggest leaderID. Right now, client is not using suggest
    // leaderID. Need to fix this.
    OMNotLeaderException notLeaderException =
        new OMNotLeaderException(raftPeerId);
    LOG.debug(notLeaderException.getMessage());
    return new ServiceException(notLeaderException);
  }

  @BeforeEach
  public void setUp() throws Exception {
    failoverCount = 0;

    ServerServiceDefinition service = ServerServiceDefinition.builder(SERVICE_NAME)
        .addMethod(SUBMIT_REQUEST_METHOD,
            ServerCalls.asyncUnaryCall(submitRequestImpl))
        .build();

    server = NettyServerBuilder.forPort(0)
        .directExecutor()
        .addService(service)
        .build()
        .start();

    omServiceId = OM_SERVICE_ID;
    conf = new OzoneConfiguration();
    conf.set(OZONE_OM_SERVICE_IDS_KEY, omServiceId);
    conf.set(ConfUtils.addKeySuffixes(OZONE_OM_NODES_KEY, omServiceId), OM_NODE_ID);
    conf.set(ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, omServiceId, OM_NODE_ID), "localhost");
    conf.setInt(ConfUtils.addKeySuffixes(OZONE_OM_GRPC_PORT_KEY, omServiceId, OM_NODE_ID),
        server.getPort());
    ugi = UserGroupInformation.getCurrentUser();
    doFailover = false;
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (client != null) {
      client.close();
      client = null;
    }
    if (server != null) {
      server.shutdownNow();
      server.awaitTermination();
      server = null;
    }
  }

  @Test
  public void testSubmitRequestToServer() throws Exception {
    ServiceListRequest req = ServiceListRequest.newBuilder().build();

    final OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(Type.ServiceList)
        .setVersion(CURRENT_VERSION)
        .setClientId("test")
        .setServiceListRequest(req)
        .build();

    client = new GrpcOmTransport(conf, ugi, omServiceId);

    final OMResponse resp = client.submitRequest(omRequest);
    assertEquals(resp.getStatus(), org.apache.hadoop.ozone.protocol
        .proto.OzoneManagerProtocolProtos.Status.OK);
    assertEquals(resp.getLeaderOMNodeId(), LEADER_OM_NODE_ID);
  }

  @Test
  public void testGrpcFailoverProxy() throws Exception {
    ServiceListRequest req = ServiceListRequest.newBuilder().build();

    final OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(Type.ServiceList)
        .setVersion(CURRENT_VERSION)
        .setClientId("test")
        .setServiceListRequest(req)
        .build();

    client = new GrpcOmTransport(conf, ugi, omServiceId);

    doFailover = true;
    // first invocation generates a NotALeaderException
    // failover is performed and request is internally retried
    // second invocation request to server succeeds
    final OMResponse resp = client.submitRequest(omRequest);
    assertEquals(resp.getStatus(), org.apache.hadoop.ozone.protocol
        .proto.OzoneManagerProtocolProtos.Status.OK);
    assertEquals(resp.getLeaderOMNodeId(), LEADER_OM_NODE_ID);
  }

  @Test
  public void testGrpcFailoverProxyExhaustRetry() throws Exception {
    final int expectedFailoverCount = 1;
    ServiceListRequest req = ServiceListRequest.newBuilder().build();

    final OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(Type.ServiceList)
        .setVersion(CURRENT_VERSION)
        .setClientId("test")
        .setServiceListRequest(req)
        .build();

    conf.setInt(OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, 0);
    client = new GrpcOmTransport(conf, ugi, omServiceId);

    doFailover = true;
    // first invocation generates a NotALeaderException
    // failover is performed and request is internally retried
    // OMFailoverProvider returns Fail retry due to #attempts >
    // max failovers

    assertThrows(Exception.class, () -> client.submitRequest(omRequest));
    assertEquals(expectedFailoverCount, failoverCount);
  }

  @Test
  public void testGrpcFailoverProxyCalculatesFailoverCountPerRequest() throws Exception {
    final int maxFailoverAttempts = 2;
    final int expectedRequest2FailoverAttemptsCount = 1;
    doFailover = true;
    completeFailover = false;
    conf.setInt(OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, maxFailoverAttempts);
    conf.setLong(OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY, 50);
    client = new GrpcOmTransport(conf, ugi, omServiceId);

    assertThrows(Exception.class, () -> client.submitRequest(arbitraryOmRequest()));
    assertEquals(maxFailoverAttempts, failoverCount);

    failoverCount = 0;
    completeFailover = true;
    //No exception this time
    client.submitRequest(arbitraryOmRequest());

    assertEquals(expectedRequest2FailoverAttemptsCount, failoverCount);
  }

  @Test
  public void testGrpcFailoverExceedMaxMesgLen() throws Exception {
    ServiceListRequest req = ServiceListRequest.newBuilder().build();

    final OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(Type.ServiceList)
        .setVersion(CURRENT_VERSION)
        .setClientId("test")
        .setServiceListRequest(req)
        .build();

    conf.setInt(OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH, 1);
    client = new GrpcOmTransport(conf, ugi, omServiceId);

    doFailover = true;
    // GrpcOMFailoverProvider returns Fail retry due to mesg response
    // len > 0, causing RESOURCE_EXHAUSTED exception.
    // This exception should cause failover to NOT retry,
    // rather to fail.
    assertThrows(Exception.class, () -> client.submitRequest(omRequest));
  }

  private static OMRequest arbitraryOmRequest() {
    ServiceListRequest req = ServiceListRequest.newBuilder().build();
    return OMRequest.newBuilder()
        .setCmdType(Type.ServiceList)
        .setVersion(CURRENT_VERSION)
        .setClientId("test")
        .setServiceListRequest(req)
        .build();
  }

  private static final class Proto2Marshaller<T> implements MethodDescriptor.Marshaller<T> {
    private final Parser<T> parser;

    private Proto2Marshaller(Parser<T> parser) {
      this.parser = parser;
    }

    @Override
    public InputStream stream(T value) {
      if (value instanceof com.google.protobuf.Message) {
        return ((com.google.protobuf.Message) value).toByteString().newInput();
      }
      if (value instanceof com.google.protobuf.MessageLite) {
        return ((com.google.protobuf.MessageLite) value).toByteString().newInput();
      }
      throw new IllegalArgumentException("Unsupported protobuf type: " + value.getClass());
    }

    @Override
    public T parse(InputStream stream) {
      try {
        return parser.parse(stream);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @FunctionalInterface
    private interface Parser<T> {
      T parse(InputStream stream) throws IOException;
    }
  }
}
