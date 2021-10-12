/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.replication;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.tracing.GrpcServerInterceptor;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;

import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptors;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.Epoll;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.EpollServerDomainSocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.ClientAuth;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Separated network server for server2server container replication.
 */
public class ReplicationServer {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReplicationServer.class);

  private Server server;

  private SecurityConfig secConf;

  private CertificateClient caClient;

  private ContainerController controller;

  private int port;

  private int poolSize;

  private int queueLimit;

  private ThreadPoolExecutor executor;

  private EventLoopGroup eventLoopGroup;

  public ReplicationServer(
      ContainerController controller,
      ReplicationConfig replicationConfig,
      SecurityConfig secConf, DatanodeConfiguration dnConfig,
      CertificateClient caClient) {
    this.secConf = secConf;
    this.caClient = caClient;
    this.controller = controller;
    this.port = replicationConfig.getPort();
    this.poolSize = dnConfig.getReplicationMaxStreams();
    this.queueLimit = dnConfig.getReplicationQueueLimit();
    init();
  }

  public void init() {
    executor = new ThreadPoolExecutor(
        poolSize, poolSize, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(queueLimit),
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("ReplicationServerExecutor-%d")
            .build());

    Class<? extends ServerChannel> channelType;

    if (Epoll.isAvailable()) {
      eventLoopGroup = new EpollEventLoopGroup(poolSize * 2);
      channelType = EpollServerDomainSocketChannel.class;
    } else {
      eventLoopGroup = new NioEventLoopGroup(poolSize * 2);
      channelType = NioServerSocketChannel.class;
    }

    NettyServerBuilder nettyServerBuilder = NettyServerBuilder.forPort(port)
        .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE)
        .workerEventLoopGroup(eventLoopGroup)
        .bossEventLoopGroup(eventLoopGroup)
        .channelType(channelType)
        .executor(executor)
        .addService(ServerInterceptors.intercept(new GrpcReplicationService(
            new OnDemandContainerReplicationSource(controller)
        ), new GrpcServerInterceptor()));

    if (secConf.isSecurityEnabled()) {
      try {
        SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(
            caClient.getPrivateKey(), caClient.getCertificate());

        sslContextBuilder = GrpcSslContexts.configure(
            sslContextBuilder, secConf.getGrpcSslProvider());

        sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
        sslContextBuilder.trustManager(HAUtils.buildCAX509List(caClient,
            secConf.getConfiguration()));

        nettyServerBuilder.sslContext(sslContextBuilder.build());
      } catch (IOException ex) {
        throw new IllegalArgumentException(
            "Unable to setup TLS for secure datanode replication GRPC "
                + "endpoint.", ex);
      }
    }

    server = nettyServerBuilder.build();
  }

  public void start() throws IOException {
    server.start();

    if (port == 0) {
      LOG.info("{} is started using port {}", getClass().getSimpleName(),
          server.getPort());
    }

    port = server.getPort();
  }

  public void stop() {
    try {
      eventLoopGroup.shutdownGracefully().sync();
      executor.shutdown();
      executor.awaitTermination(5L, TimeUnit.SECONDS);
      server.shutdown().awaitTermination(5L, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      LOG.warn("{} couldn't be stopped gracefully", getClass().getSimpleName());
      Thread.currentThread().interrupt();
    }
  }

  public int getPort() {
    return port;
  }

  /**
   * Replication-related configuration.
   */
  @ConfigGroup(prefix = "hdds.datanode.replication")
  public static final class ReplicationConfig {

    @Config(key = "port", defaultValue = "9886", description = "Port used for"
        + " the server2server replication server", tags = {
        ConfigTag.MANAGEMENT})
    private int port;

    public int getPort() {
      return port;
    }

    public ReplicationConfig setPort(int portParam) {
      this.port = portParam;
      return this;
    }
  }

}
