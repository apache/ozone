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

package org.apache.hadoop.ozone.container.replication;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerRequest;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerResponse;
import org.apache.hadoop.hdds.protocol.datanode.proto.IntraDatanodeProtocolServiceGrpc;
import org.apache.hadoop.hdds.protocol.datanode.proto.IntraDatanodeProtocolServiceGrpc.IntraDatanodeProtocolServiceStub;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.ClientAuth;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client to push container data to another datanode via gRPC.
 */
public class GrpcReplicationClient implements AutoCloseable {

  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcReplicationClient.class);

  private final ManagedChannel channel;

  private final IntraDatanodeProtocolServiceStub client;

  private final AtomicBoolean closed = new AtomicBoolean();
  private final String debugString;

  public GrpcReplicationClient(
      String host, int port,
      SecurityConfig secConfig, CertificateClient certClient)
      throws IOException {
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE)
            .proxyDetector(uri -> null);

    if (secConfig.isSecurityEnabled() && secConfig.isGrpcTlsEnabled()) {
      channelBuilder.useTransportSecurity();

      SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
      if (certClient != null) {
        sslContextBuilder
            .trustManager(certClient.getTrustManager())
            .clientAuth(ClientAuth.REQUIRE)
            .keyManager(certClient.getKeyManager());
      }
      if (secConfig.useTestCert()) {
        channelBuilder.overrideAuthority("localhost");
      }
      channelBuilder.sslContext(sslContextBuilder.build());
    }
    channel = channelBuilder.build();
    client = IntraDatanodeProtocolServiceGrpc.newStub(channel);
    debugString = getClass().getSimpleName()
        + "{" + host + ":" + port + "}"
        + "@" + Integer.toHexString(hashCode());
    LOG.debug("{}: created", this);
  }

  public StreamObserver<SendContainerRequest> upload(
      StreamObserver<SendContainerResponse> responseObserver) {
    return client.upload(responseObserver);
  }

  private void shutdown() {
    if (!closed.getAndSet(true)) {
      LOG.debug("{}: shutdown", this);
      channel.shutdown();
      try {
        channel.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.error("failed to shutdown replication channel", e);
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void close() throws Exception {
    shutdown();
  }

  @Override
  public String toString() {
    return debugString;
  }
}
