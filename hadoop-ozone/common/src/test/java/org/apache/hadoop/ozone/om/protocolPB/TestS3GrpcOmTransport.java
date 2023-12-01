/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.ozone.om.protocolPB;

import static org.apache.hadoop.ozone.ClientVersion.CURRENT_VERSION;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.ManagedChannel;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerServiceGrpc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

import com.google.protobuf.ServiceException;
import org.apache.ratis.protocol.RaftPeerId;

import static org.junit.jupiter.api.Assertions.fail;
import static org.apache.hadoop.ozone.om.OMConfigKeys
    .OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH;

/**
 * Tests for GrpcOmTransport client.
 */
public class TestS3GrpcOmTransport {
  private final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final Logger LOG =
      LoggerFactory.getLogger(TestS3GrpcOmTransport.class);

  private final String leaderOMNodeId = "TestOM";

  private final OMResponse omResponse = OMResponse.newBuilder()
      .setSuccess(true)
      .setStatus(org.apache.hadoop.ozone.protocol
          .proto.OzoneManagerProtocolProtos.Status.OK)
      .setLeaderOMNodeId(leaderOMNodeId)
      .setCmdType(Type.AllocateBlock)
      .build();

  private boolean doFailover = false;

  private OzoneConfiguration conf;

  private String omServiceId;
  private UserGroupInformation ugi;
  private ManagedChannel channel;

  private String serverName;


  private ServiceException createNotLeaderException() {
    RaftPeerId raftPeerId = RaftPeerId.getRaftPeerId("testNodeId");

    // TODO: Set suggest leaderID. Right now, client is not using suggest
    // leaderID. Need to fix this.
    OMNotLeaderException notLeaderException =
        new OMNotLeaderException(raftPeerId);
    LOG.debug(notLeaderException.getMessage());
    return new ServiceException(notLeaderException);
  }

  private final OzoneManagerServiceGrpc.OzoneManagerServiceImplBase
      serviceImpl =
      mock(OzoneManagerServiceGrpc.OzoneManagerServiceImplBase.class,
          delegatesTo(
              new OzoneManagerServiceGrpc.OzoneManagerServiceImplBase() {
                @Override
                public void submitRequest(org.apache.hadoop.ozone.protocol.proto
                                              .OzoneManagerProtocolProtos
                                              .OMRequest request,
                                          io.grpc.stub.StreamObserver<org.apache
                                              .hadoop.ozone.protocol.proto
                                              .OzoneManagerProtocolProtos
                                              .OMResponse>
                                              responseObserver) {
                  try {
                    if (doFailover) {
                      doFailover = false;
                      throw createNotLeaderException();
                    } else {
                      responseObserver.onNext(omResponse);
                      responseObserver.onCompleted();
                    }
                  } catch (Throwable e) {
                    IOException ex = new IOException(e.getCause());
                    responseObserver.onError(io.grpc.Status
                        .INTERNAL
                        .withDescription(ex.getMessage())
                        .asRuntimeException());
                  }
                }
              }));

  private GrpcOmTransport client;

  @BeforeEach
  public void setUp() throws Exception {
    // Generate a unique in-process server name.
    serverName = InProcessServerBuilder.generateName();

    // Create a server, add service, start,
    // and register for automatic graceful shutdown.
    grpcCleanup.register(InProcessServerBuilder
        .forName(serverName)
        .directExecutor()
        .addService(serviceImpl)
        .build()
        .start());

    // Create a client channel and register for automatic graceful shutdown.
    channel = grpcCleanup.register(
        InProcessChannelBuilder.forName(serverName).directExecutor().build());

    omServiceId = "";
    conf = new OzoneConfiguration();
    ugi = UserGroupInformation.getCurrentUser();
    doFailover = false;
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
    client.startClient(channel);

    final OMResponse resp = client.submitRequest(omRequest);
    Assertions.assertEquals(resp.getStatus(), org.apache.hadoop.ozone.protocol
        .proto.OzoneManagerProtocolProtos.Status.OK);
    Assertions.assertEquals(resp.getLeaderOMNodeId(), leaderOMNodeId);
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
    client.startClient(channel);

    doFailover = true;
    // first invocation generates a NotALeaderException
    // failover is performed and request is internally retried
    // second invocation request to server succeeds
    final OMResponse resp = client.submitRequest(omRequest);
    Assertions.assertEquals(resp.getStatus(), org.apache.hadoop.ozone.protocol
        .proto.OzoneManagerProtocolProtos.Status.OK);
    Assertions.assertEquals(resp.getLeaderOMNodeId(), leaderOMNodeId);
  }

  @Test
  public void testGrpcFailoverProxyExhaustRetry() throws Exception {
    ServiceListRequest req = ServiceListRequest.newBuilder().build();

    final OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(Type.ServiceList)
        .setVersion(CURRENT_VERSION)
        .setClientId("test")
        .setServiceListRequest(req)
        .build();

    conf.setInt(OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, 0);
    client = new GrpcOmTransport(conf, ugi, omServiceId);
    client.startClient(channel);

    doFailover = true;
    // first invocation generates a NotALeaderException
    // failover is performed and request is internally retried
    // OMFailoverProvider returns Fail retry due to #attempts >
    // max failovers

    try {
      final OMResponse resp = client.submitRequest(omRequest);
      fail();
    } catch (Exception e) {
      Assertions.assertTrue(true);
    }
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
    int maxSize = conf.getInt(OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH,
        OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT);
    channel = grpcCleanup.register(
        InProcessChannelBuilder
            .forName(serverName)
            .maxInboundMetadataSize(maxSize)
            .directExecutor().build());
    client.startClient(channel);

    doFailover = true;
    // GrpcOMFailoverProvider returns Fail retry due to mesg response
    // len > 0, causing RESOURCE_EXHAUSTED exception.
    // This exception should cause failover to NOT retry,
    // rather to fail.
    try {
      final OMResponse resp = client.submitRequest(omRequest);
      fail();
    } catch (Exception e) {
      Assertions.assertTrue(true);
    }
  }
}
