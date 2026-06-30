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
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_GRPC_PORT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

import com.google.protobuf.ServiceException;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReadConsistencyHint;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReadConsistencyProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerServiceGrpc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for GrpcOmTransport client.
 */
public class TestS3GrpcOmTransport {
  private final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final Logger LOG =
      LoggerFactory.getLogger(TestS3GrpcOmTransport.class);

  private static final String LEADER_OM_NODE_ID = "TestOM";

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
  private ManagedChannel channel;

  private String serverName;

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
                    responseObserver.onError(io.grpc.Status
                        .INTERNAL
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
    client.startClient(channel);

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
    client.startClient(channel);

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
    client.startClient(channel);

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
    assertThrows(Exception.class, () -> client.submitRequest(omRequest));
  }

  @Test
  public void testFollowerReadDoesNotFailoverFromKnownLeader() throws Exception {
    conf.setBoolean(OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY, true);
    configureHaOmService("om0", "om1");

    AtomicInteger leaderRequestCount = new AtomicInteger();
    AtomicInteger followerRequestCount = new AtomicInteger();
    AtomicReference<OMRequest> leaderRequest = new AtomicReference<>();

    client = new GrpcOmTransport(conf, ugi, omServiceId);
    client.startClient("om0", createNodeChannel("om0",
        leaderRequestCount, leaderRequest));
    client.startClient("om1", createNodeChannel("om1",
        followerRequestCount, new AtomicReference<>()));
    client.changeLeaderProxyForTest("om0");
    client.changeFollowerReadInitialProxy("om0");

    OMRequest request = OMRequest.newBuilder()
        .setCmdType(Type.ListVolume)
        .setVersion(CURRENT_VERSION)
        .setClientId("test")
        .build();

    client.submitRequest(request);

    assertEquals(1, leaderRequestCount.get());
    assertEquals(0, followerRequestCount.get());
    assertEquals(ReadConsistencyProto.LINEARIZABLE_ALLOW_FOLLOWER,
        leaderRequest.get().getReadConsistencyHint().getReadConsistency());
  }

  @Test
  public void testFollowerReadDoesNotRouteWriteRequestToFollower() throws Exception {
    conf.setBoolean(OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY, true);
    configureHaOmService("om0", "om1");

    AtomicInteger leaderRequestCount = new AtomicInteger();
    AtomicInteger followerRequestCount = new AtomicInteger();
    AtomicReference<OMRequest> leaderRequest = new AtomicReference<>();

    client = new GrpcOmTransport(conf, ugi, omServiceId);
    client.startClient("om0", createNodeChannel("om0",
        leaderRequestCount, leaderRequest));
    client.startClient("om1", createNodeChannel("om1",
        followerRequestCount, new AtomicReference<>()));
    client.changeLeaderProxyForTest("om0");
    client.changeFollowerReadInitialProxy("om1");

    client.submitRequest(OMRequest.newBuilder()
        .setCmdType(Type.CreateVolume)
        .setVersion(CURRENT_VERSION)
        .setClientId("test")
        .build());

    assertEquals(1, leaderRequestCount.get());
    assertEquals(0, followerRequestCount.get());
    assertEquals(ReadConsistencyProto.DEFAULT,
        leaderRequest.get().getReadConsistencyHint().getReadConsistency());
  }

  @Test
  public void testFollowerReadKeepsExistingConsistencyHint() throws Exception {
    conf.setBoolean(OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY, true);
    configureHaOmService("om0", "om1");

    AtomicInteger followerRequestCount = new AtomicInteger();
    AtomicReference<OMRequest> followerRequest = new AtomicReference<>();

    client = new GrpcOmTransport(conf, ugi, omServiceId);
    client.startClient("om0", createNodeChannel("om0",
        new AtomicInteger(), new AtomicReference<>()));
    client.startClient("om1", createNodeChannel("om1",
        followerRequestCount, followerRequest));
    client.changeLeaderProxyForTest("om0");
    client.changeFollowerReadInitialProxy("om1");

    client.submitRequest(OMRequest.newBuilder()
        .setCmdType(Type.ListVolume)
        .setVersion(CURRENT_VERSION)
        .setClientId("test")
        .setReadConsistencyHint(ReadConsistencyHint.newBuilder()
            .setReadConsistency(ReadConsistencyProto.LOCAL_LEASE)
            .build())
        .build());

    assertEquals(1, followerRequestCount.get());
    assertEquals(ReadConsistencyProto.LOCAL_LEASE,
        followerRequest.get().getReadConsistencyHint().getReadConsistency());
  }

  @Test
  public void testFollowerReadFallsBackToLeaderOnNotLeaderException() throws Exception {
    conf.setBoolean(OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY, true);
    configureHaOmService("om0", "om1");

    AtomicInteger leaderRequestCount = new AtomicInteger();
    AtomicInteger followerRequestCount = new AtomicInteger();
    AtomicReference<OMRequest> leaderRequest = new AtomicReference<>();

    client = new GrpcOmTransport(conf, ugi, omServiceId);
    client.startClient("om0", createNodeChannel("om0",
        leaderRequestCount, leaderRequest));
    client.startClient("om1", createNotLeaderNodeChannel(followerRequestCount));
    client.changeLeaderProxyForTest("om0");
    client.changeFollowerReadInitialProxy("om1");

    client.submitRequest(OMRequest.newBuilder()
        .setCmdType(Type.ListVolume)
        .setVersion(CURRENT_VERSION)
        .setClientId("test")
        .build());

    assertEquals(1, followerRequestCount.get());
    assertEquals(1, leaderRequestCount.get());
    assertEquals(ReadConsistencyProto.DEFAULT,
        leaderRequest.get().getReadConsistencyHint().getReadConsistency());
  }

  @Test
  public void testFollowerReadRejectsInvalidFollowerReadConsistency() {
    conf.setBoolean(OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY, true);
    conf.set(OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_DEFAULT_CONSISTENCY_KEY, "DEFAULT");
    configureHaOmService("om0", "om1");

    assertThrows(IllegalStateException.class,
        () -> new GrpcOmTransport(conf, ugi, omServiceId));
  }

  @Test
  public void testFollowerReadRejectsInvalidLeaderReadConsistency() {
    conf.setBoolean(OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY, true);
    conf.set(OzoneConfigKeys.OZONE_CLIENT_LEADER_READ_DEFAULT_CONSISTENCY_KEY,
        "LINEARIZABLE_ALLOW_FOLLOWER");
    configureHaOmService("om0", "om1");

    assertThrows(IllegalStateException.class,
        () -> new GrpcOmTransport(conf, ugi, omServiceId));
  }

  private void configureHaOmService(String... nodeIds) {
    omServiceId = "om-service-test";
    conf.set(OZONE_OM_SERVICE_IDS_KEY, omServiceId);
    conf.set(ConfUtils.addKeySuffixes(OZONE_OM_NODES_KEY, omServiceId),
        String.join(",", nodeIds));
    for (int i = 0; i < nodeIds.length; i++) {
      conf.set(ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, omServiceId,
          nodeIds[i]), "localhost");
      conf.setInt(ConfUtils.addKeySuffixes(OZONE_OM_GRPC_PORT_KEY, omServiceId,
          nodeIds[i]), 19880 + i);
    }
  }

  private ManagedChannel createNodeChannel(String nodeId,
      AtomicInteger requestCount, AtomicReference<OMRequest> lastRequest)
      throws IOException {
    String nodeServerName = InProcessServerBuilder.generateName();
    grpcCleanup.register(InProcessServerBuilder
        .forName(nodeServerName)
        .directExecutor()
        .addService(new OzoneManagerServiceGrpc.OzoneManagerServiceImplBase() {
          @Override
          public void submitRequest(OMRequest request,
              StreamObserver<OMResponse> responseObserver) {
            requestCount.incrementAndGet();
            lastRequest.set(request);
            responseObserver.onNext(OMResponse.newBuilder()
                .setSuccess(true)
                .setStatus(org.apache.hadoop.ozone.protocol
                    .proto.OzoneManagerProtocolProtos.Status.OK)
                .setLeaderOMNodeId(nodeId)
                .setCmdType(request.getCmdType())
                .build());
            responseObserver.onCompleted();
          }
        })
        .build()
        .start());
    return grpcCleanup.register(
        InProcessChannelBuilder.forName(nodeServerName).directExecutor().build());
  }

  private ManagedChannel createNotLeaderNodeChannel(AtomicInteger requestCount)
      throws IOException {
    String nodeServerName = InProcessServerBuilder.generateName();
    grpcCleanup.register(InProcessServerBuilder
        .forName(nodeServerName)
        .directExecutor()
        .addService(new OzoneManagerServiceGrpc.OzoneManagerServiceImplBase() {
          @Override
          public void submitRequest(OMRequest request,
              StreamObserver<OMResponse> responseObserver) {
            requestCount.incrementAndGet();
            try {
              throw createNotLeaderException();
            } catch (Throwable e) {
              IOException ex = new IOException(e.getCause());
              responseObserver.onError(io.grpc.Status
                  .INTERNAL
                  .withDescription(ex.getMessage())
                  .asRuntimeException());
            }
          }
        })
        .build()
        .start());
    return grpcCleanup.register(
        InProcessChannelBuilder.forName(nodeServerName).directExecutor().build());
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
}
