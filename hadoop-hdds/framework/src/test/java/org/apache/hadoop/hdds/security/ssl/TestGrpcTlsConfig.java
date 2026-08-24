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

package org.apache.hadoop.hdds.security.ssl;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_PROVIDER;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.SUCCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc.XceiverClientProtocolServiceImplBase;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc.XceiverClientProtocolServiceStub;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClientTestImpl;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.ClientAuth;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests TLS protocol version and cipher suite enforcement on gRPC endpoints.
 */
public class TestGrpcTlsConfig {
  private CertificateClientTestImpl caClient;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_GRPC_TLS_PROVIDER, "JDK");
    caClient = new CertificateClientTestImpl(conf);
  }

  @Test
  public void testTls13ServerAcceptsTls13Client() throws Exception {
    Server server = null;
    ManagedChannel channel = null;
    try {
      server = setupServer(new String[]{"TLSv1.3"}, null);
      server.start();
      channel = setupClient(server.getPort(), new String[]{"TLSv1.3"}, null);
      XceiverClientProtocolServiceStub asyncStub = XceiverClientProtocolServiceGrpc.newStub(channel);
      ContainerCommandResponseProto response = sendRequest(asyncStub);
      assertEquals(SUCCESS, response.getResult());
    } finally {
      shutdown(channel, server);
    }
  }

  @Test
  public void testTls13ServerRejectsTls12Client() throws Exception {
    Server server = null;
    ManagedChannel channel = null;
    try {
      server = setupServer(new String[]{"TLSv1.3"}, null);
      server.start();
      channel = setupClient(server.getPort(),  new String[]{"TLSv1.2"}, null);
      XceiverClientProtocolServiceStub asyncStub =  XceiverClientProtocolServiceGrpc.newStub(channel);
      assertThrows(ExecutionException.class, () -> sendRequest(asyncStub));
    } finally {
      shutdown(channel, server);
    }
  }

  @Test
  public void testCipherMatchSucceeds() throws Exception {
    Server server = null;
    ManagedChannel channel = null;
    try {
      String[] ciphers = {"TLS_AES_256_GCM_SHA384"};
      server = setupServer(new String[]{"TLSv1.3"}, ciphers);
      server.start();
      channel = setupClient(server.getPort(), new String[]{"TLSv1.3"}, ciphers);
      XceiverClientProtocolServiceStub asyncStub = XceiverClientProtocolServiceGrpc.newStub(channel);
      ContainerCommandResponseProto response = sendRequest(asyncStub);
      assertEquals(SUCCESS, response.getResult());
    } finally {
      shutdown(channel, server);
    }
  }

  @Test
  public void testCipherMismatchFails() throws Exception {
    Server server = null;
    ManagedChannel channel = null;
    try {
      server = setupServer(new String[]{"TLSv1.3"}, new String[]{"TLS_AES_256_GCM_SHA384"});
      server.start();
      channel = setupClient(server.getPort(), new String[]{"TLSv1.3"}, new String[]{"TLS_AES_128_GCM_SHA256"});
      XceiverClientProtocolServiceStub asyncStub = XceiverClientProtocolServiceGrpc.newStub(channel);
      assertThrows(ExecutionException.class, () -> sendRequest(asyncStub));
    } finally {
      shutdown(channel, server);
    }
  }

  @Test
  public void testDefaultConfigAcceptsConnection() throws Exception {
    Server server = null;
    ManagedChannel channel = null;
    try {
      server = setupServer(null, null);
      server.start();
      channel = setupClient(server.getPort(), null, null);
      XceiverClientProtocolServiceStub asyncStub = XceiverClientProtocolServiceGrpc.newStub(channel);
      ContainerCommandResponseProto response = sendRequest(asyncStub);
      assertEquals(SUCCESS, response.getResult());
    } finally {
      shutdown(channel, server);
    }
  }

  private Server setupServer(String[] protocols, String[] ciphers)
      throws Exception {
    NettyServerBuilder nettyServerBuilder = NettyServerBuilder.forPort(0).addService(new GrpcService());
    SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(caClient.getKeyManager());
    sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
    sslContextBuilder.trustManager(caClient.getTrustManager());
    sslContextBuilder = GrpcSslContexts.configure(sslContextBuilder, SslProvider.JDK);
    if (protocols != null) {
      sslContextBuilder.protocols(protocols);
    }
    if (ciphers != null) {
      sslContextBuilder.ciphers(Arrays.asList(ciphers));
    }
    nettyServerBuilder.sslContext(sslContextBuilder.build());
    return nettyServerBuilder.build();
  }

  private ManagedChannel setupClient(int port, String[] protocols, String[] ciphers) throws Exception {
    NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress("localhost", port);
    SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
    sslContextBuilder.trustManager(caClient.getTrustManager());
    sslContextBuilder.keyManager(caClient.getKeyManager());
    sslContextBuilder = GrpcSslContexts.configure(sslContextBuilder, SslProvider.JDK);
    if (protocols != null) {
      sslContextBuilder.protocols(protocols);
    }
    if (ciphers != null) {
      sslContextBuilder.ciphers(Arrays.asList(ciphers));
    }
    channelBuilder.useTransportSecurity().sslContext(sslContextBuilder.build());
    return channelBuilder.build();
  }

  private ContainerCommandResponseProto sendRequest(
      XceiverClientProtocolServiceStub stub) throws Exception {
    DatanodeDetails dn = DatanodeDetails.newBuilder().setUuid(UUID.randomUUID()).build();
    List<DatanodeDetails> nodes = new ArrayList<>();
    nodes.add(dn);
    Pipeline pipeline = Pipeline.newBuilder().setId(PipelineID.randomId())
        .setReplicationConfig(RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE))
        .setState(Pipeline.PipelineState.OPEN)
        .setNodes(nodes).build();

    ContainerCommandRequestProto request = ContainerTestHelper.getCreateContainerRequest(0, pipeline);
    final CompletableFuture<ContainerCommandResponseProto> replyFuture = new CompletableFuture<>();
    final StreamObserver<ContainerCommandRequestProto> requestObserver =
        stub.send(new StreamObserver<ContainerCommandResponseProto>() {
          @Override
          public void onNext(ContainerCommandResponseProto value) {
            replyFuture.complete(value);
          }

          @Override
          public void onError(Throwable t) {
            replyFuture.completeExceptionally(t);
          }

          @Override
          public void onCompleted() {
          }
        });
    requestObserver.onNext(request);
    requestObserver.onCompleted();
    return replyFuture.get();
  }

  private void shutdown(ManagedChannel channel, Server server) {
    if (channel != null) {
      channel.shutdownNow();
    }
    if (server != null) {
      server.shutdownNow();
    }
  }

  private static class GrpcService
      extends XceiverClientProtocolServiceImplBase {

    @Override
    public StreamObserver<ContainerCommandRequestProto> send(
        StreamObserver<ContainerCommandResponseProto> responseObserver) {
      return new StreamObserver<ContainerCommandRequestProto>() {

        @Override
        public void onNext(ContainerCommandRequestProto request) {
          ContainerCommandResponseProto resp =
              ContainerCommandResponseProto.newBuilder()
                  .setCmdType(ContainerProtos.Type.CreateContainer)
                  .setResult(SUCCESS)
                  .build();
          responseObserver.onNext(resp);
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onCompleted() {
          responseObserver.onCompleted();
        }
      };
    }
  }
}
