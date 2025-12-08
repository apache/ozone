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

package org.apache.hadoop.ozone.container.common.transport.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.net.BindException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.tracing.GrpcServerInterceptor;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptors;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.ServerChannel;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.Epoll;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.EpollServerSocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a Grpc server endpoint that acts as the communication layer for
 * Ozone containers.
 */
public final class XceiverServerGrpc implements XceiverServerSpi {
  private static final Logger
      LOG = LoggerFactory.getLogger(XceiverServerGrpc.class);
  private int port;
  private DatanodeID id;
  private Server server;
  private final ContainerDispatcher storageContainer;
  private boolean isStarted;
  private DatanodeDetails datanodeDetails;
  private ThreadPoolExecutor readExecutors;
  private EventLoopGroup eventLoopGroup;

  /**
   * Constructs a Grpc server class.
   *
   * @param conf - Configuration
   */
  public XceiverServerGrpc(DatanodeDetails datanodeDetails,
      ConfigurationSource conf,
      ContainerDispatcher dispatcher, CertificateClient caClient) {
    Objects.requireNonNull(conf, "conf == null");

    this.id = datanodeDetails.getID();
    this.datanodeDetails = datanodeDetails;
    this.port = conf.getInt(OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT,
        OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT_DEFAULT);

    if (conf.getBoolean(OzoneConfigKeys.HDDS_CONTAINER_IPC_RANDOM_PORT,
        OzoneConfigKeys.HDDS_CONTAINER_IPC_RANDOM_PORT_DEFAULT)) {
      this.port = 0;
    }

    final int threadCountPerDisk =
        conf.getObject(DatanodeConfiguration.class).getNumReadThreadPerVolume();
    final int numberOfDisks =
        HddsServerUtil.getDatanodeStorageDirs(conf).size();
    final int poolSize = threadCountPerDisk * numberOfDisks;

    readExecutors = new ThreadPoolExecutor(poolSize, poolSize,
        60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat(datanodeDetails.threadNamePrefix() +
                "ChunkReader-%d")
            .build());

    ThreadFactory factory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(datanodeDetails.threadNamePrefix() +
            "ChunkReader-ELG-%d")
        .build();

    Class<? extends ServerChannel> channelType;
    if (Epoll.isAvailable()) {
      eventLoopGroup = new EpollEventLoopGroup(poolSize / 10, factory);
      channelType = EpollServerSocketChannel.class;
    } else {
      eventLoopGroup = new NioEventLoopGroup(poolSize / 10, factory);
      channelType = NioServerSocketChannel.class;
    }

    LOG.info("GrpcServer channel type {}", channelType.getSimpleName());
    GrpcXceiverService xceiverService = new GrpcXceiverService(dispatcher);
    NettyServerBuilder nettyServerBuilder = NettyServerBuilder.forPort(port)
        .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE)
        .bossEventLoopGroup(eventLoopGroup)
        .workerEventLoopGroup(eventLoopGroup)
        .channelType(channelType)
        .executor(readExecutors)
        .addService(ServerInterceptors.intercept(
            xceiverService.bindServiceWithZeroCopy(),
            new GrpcServerInterceptor()));

    SecurityConfig secConf = new SecurityConfig(conf);
    if (secConf.isSecurityEnabled() && secConf.isGrpcTlsEnabled()) {
      try {
        SslContextBuilder sslClientContextBuilder = SslContextBuilder.forServer(
            caClient.getKeyManager());
        SslContextBuilder sslContextBuilder = GrpcSslContexts.configure(
            sslClientContextBuilder, secConf.getGrpcSslProvider());
        nettyServerBuilder.sslContext(sslContextBuilder.build());
      } catch (Exception ex) {
        LOG.error("Unable to setup TLS for secure datanode GRPC endpoint.", ex);
      }
    }
    server = nettyServerBuilder.build();
    storageContainer = dispatcher;
  }

  @Override
  public int getIPCPort() {
    return this.port;
  }

  /**
   * Returns the Replication type supported by this end-point.
   *
   * @return enum -- {Stand_Alone, Ratis, Grpc, Chained}
   */
  @Override
  public HddsProtos.ReplicationType getServerType() {
    return HddsProtos.ReplicationType.STAND_ALONE;
  }

  @Override
  public void start() throws IOException {
    if (!isStarted) {
      try {
        server.start();
      } catch (IOException e) {
        LOG.error("Error while starting the server", e);
        if (e.getMessage().contains("Failed to bind to address")) {
          throw new BindException(e.getMessage());
        } else {
          throw e;
        }
      }
      int realPort = server.getPort();

      if (port == 0) {
        LOG.info("{} {} is started using port {}", getClass().getSimpleName(),
            this.id, realPort);
        port = realPort;
      }

      //register the real port to the datanode details.
      datanodeDetails.setPort(DatanodeDetails.newStandalonePort(realPort));

      isStarted = true;
    }
  }

  @Override
  public void stop() {
    if (isStarted) {
      try {
        readExecutors.shutdown();
        readExecutors.awaitTermination(5L, TimeUnit.SECONDS);
        server.shutdown();
        server.awaitTermination(5, TimeUnit.SECONDS);
        eventLoopGroup.shutdownGracefully().sync();
      } catch (InterruptedException e) {
        LOG.error("failed to shutdown XceiverServerGrpc", e);
        Thread.currentThread().interrupt();
      }
      isStarted = false;
    }
  }

  @Override
  public void submitRequest(ContainerCommandRequestProto request,
      HddsProtos.PipelineID pipelineID) throws IOException {
    Span span = TracingUtil
        .importAndCreateSpan(
            "XceiverServerGrpc." + request.getCmdType().name(),
            request.getTraceID());
    try (Scope ignore = span.makeCurrent()) {
      ContainerProtos.ContainerCommandResponseProto response =
          storageContainer.dispatch(request, null);
      if (response.getResult() != ContainerProtos.Result.SUCCESS) {
        throw new StorageContainerException(response.getMessage(),
            response.getResult());
      }
    } finally {
      span.end();
    }
  }

  @Override
  public boolean isExist(HddsProtos.PipelineID pipelineId) {
    return id.toPipelineID().getProtobuf().equals(pipelineId);
  }

  @Override
  public List<PipelineReport> getPipelineReport() {
    return Collections.singletonList(PipelineReport.newBuilder()
        .setPipelineID(id.toPipelineID().getProtobuf())
        .build());
  }
}
