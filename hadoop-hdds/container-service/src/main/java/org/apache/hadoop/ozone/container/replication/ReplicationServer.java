/**
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
package org.apache.hadoop.ozone.container.replication;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.tracing.GrpcServerInterceptor;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;

import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.ServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptors;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationServer {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReplicationServer.class);

  private Server server;

  private SecurityConfig secConf;

  private CertificateClient caClient;

  private ContainerController controller;

  private int port = 1111;

  public ReplicationServer(
      ContainerController controller,
      ReplicationConfig replicationConfig,
      SecurityConfig secConf,
      CertificateClient caClient
  ) {
    this.secConf = secConf;
    this.caClient = caClient;
    this.controller = controller;
    this.port = replicationConfig.getPort();
    init();
  }

  public void init() {
    NettyServerBuilder nettyServerBuilder =
        ((NettyServerBuilder) ServerBuilder.forPort(port))
            .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE);

    GrpcServerInterceptor tracingInterceptor = new GrpcServerInterceptor();
    nettyServerBuilder
        .addService(ServerInterceptors.intercept(new GrpcReplicationService(
            new OnDemandContainerReplicationSource(controller)
        ), tracingInterceptor));

    if (secConf.isSecurityEnabled()) {
      try {
        SslContextBuilder sslClientContextBuilder = SslContextBuilder.forServer(
            caClient.getPrivateKey(), caClient.getCertificate());
        SslContextBuilder sslContextBuilder = GrpcSslContexts.configure(
            sslClientContextBuilder, secConf.getGrpcSslProvider());
        nettyServerBuilder.sslContext(sslContextBuilder.build());
      } catch (Exception ex) {
        LOG.error(
            "Unable to setup TLS for secure datanode replication GRPC "
                + "endpoint.", ex);
      }
    }

    server = nettyServerBuilder.build();
  }

  public void start() throws IOException {
    server.start();

    port = server.getPort();

    if (port == 0) {
      LOG.info("{} is started using port {}", getClass().getSimpleName(), port);
    }
  }

  public void stop() {
    try {
      server.shutdown().awaitTermination(10L, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      LOG.warn("{} couldn't be stopped gracefully", getClass().getSimpleName());
    }
  }

  public int getPort() {
    return port;
  }

  @ConfigGroup(prefix = "datanode.replication")
  public static final class ReplicationConfig {

    @Config(key = "port", defaultValue = "9886", description = "Port used for"
        + " the server2server replication server", tags = {
        ConfigTag.MANAGEMENT})
    private int port;

    public int getPort() {
      return port;
    }

    public ReplicationConfig setPort(int port) {
      this.port = port;
      return this;
    }
  }

}
