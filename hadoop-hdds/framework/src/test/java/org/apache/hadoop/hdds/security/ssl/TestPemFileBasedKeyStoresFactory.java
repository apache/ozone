/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.security.ssl;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc.XceiverClientProtocolServiceStub;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc.XceiverClientProtocolServiceImplBase;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.security.SecurityConfig;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.net.ssl.SSLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.SUCCESS;

/**
 * Test PemFileBasedKeyStoresFactory.
 */
public class TestPemFileBasedKeyStoresFactory {
  private CertificateClientTestImpl caClient;
  private SecurityConfig secConf;
  private static final int RELOAD_INTERVAL = 2000;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    caClient = new CertificateClientTestImpl(conf);
    secConf = new SecurityConfig(conf);
  }

  @ValueSource(booleans = {true, false})
  @ParameterizedTest
  public void testInit(boolean clientAuth) throws Exception {
    KeyStoresFactory keyStoresFactory = new PemFileBasedKeyStoresFactory(
        secConf, caClient);
    try {
      keyStoresFactory.init(KeyStoresFactory.Mode.CLIENT, clientAuth);
      Assertions.assertEquals(clientAuth, keyStoresFactory.getKeyManagers()[0]
          instanceof ReloadingX509KeyManager);
      Assertions.assertTrue(keyStoresFactory.getTrustManagers()[0]
          instanceof ReloadingX509TrustManager);
    } finally {
      keyStoresFactory.destroy();
    }

    try {
      keyStoresFactory.init(KeyStoresFactory.Mode.SERVER, clientAuth);
      Assertions.assertTrue(keyStoresFactory.getKeyManagers()[0]
          instanceof ReloadingX509KeyManager);
      Assertions.assertTrue(keyStoresFactory.getTrustManagers()[0]
          instanceof ReloadingX509TrustManager);
    } finally {
      keyStoresFactory.destroy();
    }
  }

  @Test
  public void testConnectionWithCertReload() throws Exception {
    KeyStoresFactory serverFactory = null;
    KeyStoresFactory clientFactory = null;
    Server server = null;
    ManagedChannel channel = null;
    try {
      // create server
      serverFactory = new PemFileBasedKeyStoresFactory(secConf, caClient);
      serverFactory.init(KeyStoresFactory.Mode.SERVER, true);
      server = setupServer(serverFactory);
      server.start();

      // create client
      clientFactory = new PemFileBasedKeyStoresFactory(secConf, caClient);
      clientFactory.init(KeyStoresFactory.Mode.CLIENT, true);
      channel = setupClient(clientFactory, server.getPort());
      XceiverClientProtocolServiceStub asyncStub =
          XceiverClientProtocolServiceGrpc.newStub(channel);

      // send command
      ContainerCommandResponseProto responseProto = sendRequest(asyncStub);
      Assertions.assertEquals(SUCCESS, responseProto.getResult());

      // Renew certificate
      caClient.renewKey();
      Thread.sleep(RELOAD_INTERVAL);

      // send command again
      responseProto = sendRequest(asyncStub);
      Assertions.assertEquals(SUCCESS, responseProto.getResult());
    } finally {
      if (channel != null) {
        channel.shutdownNow();
      }
      if (server != null) {
        server.shutdownNow();
      }
      if (clientFactory != null) {
        clientFactory.destroy();
      }
      if (serverFactory != null) {
        serverFactory.destroy();
      }
    }
  }

  private ContainerCommandResponseProto sendRequest(
      XceiverClientProtocolServiceStub stub) throws Exception {
    DatanodeDetails dn = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID()).build();
    List<DatanodeDetails> nodes = new ArrayList<>();
    nodes.add(dn);
    Pipeline pipeline = Pipeline.newBuilder().setId(PipelineID.randomId())
        .setReplicationConfig(RatisReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.ONE))
        .setState(Pipeline.PipelineState.OPEN)
        .setNodes(nodes).build();

    ContainerCommandRequestProto request = ContainerTestHelper
        .getCreateContainerRequest(0, pipeline);
    final CompletableFuture<ContainerCommandResponseProto> replyFuture =
        new CompletableFuture<>();
    final StreamObserver<ContainerCommandRequestProto> requestObserver =
        stub.send(new StreamObserver<ContainerCommandResponseProto>() {
          @Override
          public void onNext(ContainerCommandResponseProto value) {
            replyFuture.complete(value);
          }
          @Override
          public void onError(Throwable t) {
          }
          @Override
          public void onCompleted() {
          }
        });
    requestObserver.onNext(request);
    requestObserver.onCompleted();
    return replyFuture.get();
  }

  private ManagedChannel setupClient(KeyStoresFactory factory, int port)
      throws SSLException {
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress("localhost", port);

    SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
    sslContextBuilder.trustManager(factory.getTrustManagers()[0]);
    sslContextBuilder.keyManager(factory.getKeyManagers()[0]);
    channelBuilder.useTransportSecurity().sslContext(sslContextBuilder.build());
    return channelBuilder.build();
  }

  private Server setupServer(KeyStoresFactory factory) throws SSLException {
    NettyServerBuilder nettyServerBuilder = NettyServerBuilder.forPort(0)
        .addService(new GrpcService());
    SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(
        factory.getKeyManagers()[0]);
    sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
    sslContextBuilder.trustManager(factory.getTrustManagers()[0]);
    sslContextBuilder = GrpcSslContexts.configure(
        sslContextBuilder, secConf.getGrpcSslProvider());
    nettyServerBuilder.sslContext(sslContextBuilder.build());
    return nettyServerBuilder.build();
  }

  /**
   * Test Class to provide a server side service.
   */
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
