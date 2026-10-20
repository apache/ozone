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

package org.apache.hadoop.ozone.om.jobworker;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.io.IOException;
import java.util.OptionalInt;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerCommandType;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ozone.conf.OMJobworkerConfiguration;
import org.apache.hadoop.ozone.grpc.metrics.GrpcMetrics;
import org.apache.hadoop.ozone.grpc.metrics.GrpcMetricsServerRequestInterceptor;
import org.apache.hadoop.ozone.grpc.metrics.GrpcMetricsServerResponseInterceptor;
import org.apache.hadoop.ozone.grpc.metrics.GrpcMetricsServerTransportFilter;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.jobworker.command.OMJobworkerCommandManager;
import org.apache.hadoop.ozone.util.RemoteAddressInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC server that manages network resources and transport for Jobworker communication.
 */
public class JobworkerGrpcServer {
  private static final Logger LOG = LoggerFactory.getLogger(JobworkerGrpcServer.class);

  private final GrpcMetrics jobworkerGrpcMetrics;
  private Server server;
  private int port;
  private final int grpcExecutorSize;
  private final int bossGroupSize;
  private final int workerGroupSize;
  private final String threadNamePrefix;
  private ThreadPoolExecutor readExecutors;
  private EventLoopGroup bossEventLoopGroup;
  private EventLoopGroup workerEventLoopGroup;
  private final int maxInboundLength;
  private final ProtocolMessageMetrics<JobworkerCommandType> protocolMessageMetrics;
  private final String serviceName;
  private final OMNodeDetails omNodeDetails;

  public JobworkerGrpcServer(OzoneConfiguration config,
      JobworkerProtocolServerImpl jobworkerServerImpl, OMNodeDetails nodeDetails,
      OMJobworkerCommandManager jobworkerCommandManager) {
    omNodeDetails = nodeDetails;
    serviceName = nodeDetails == null ? "Jobworker" : "Jobworker-" + nodeDetails.threadNamePrefix();
    OMJobworkerConfiguration jobworkerServiceConfig = config.getObject(OMJobworkerConfiguration.class);
    port = getGrpcPort(config, jobworkerServiceConfig);
    grpcExecutorSize = jobworkerServiceConfig.getGrpcExecutorThreadNum();
    maxInboundLength = jobworkerServiceConfig.getGrpcMaximumInboundLength();
    bossGroupSize = jobworkerServiceConfig.getGrpcBossGroupSize();
    workerGroupSize = jobworkerServiceConfig.getGrpcWorkerGroupSize();
    threadNamePrefix = serviceName;
    protocolMessageMetrics = getProtocolMessageMetrics(config);
    jobworkerGrpcMetrics = GrpcMetrics.create(config, serviceName);
    try {
      init(jobworkerServerImpl, jobworkerCommandManager);
    } catch (RuntimeException e) {
      stop();
      throw e;
    }
  }

  private void init(JobworkerProtocolServerImpl jobworkerServerImpl,
      OMJobworkerCommandManager jobworkerCommandManager) {
    readExecutors = new ThreadPoolExecutor(grpcExecutorSize, grpcExecutorSize,
        60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat(threadNamePrefix + "RpcExecutor-%d")
            .build());

    Class<? extends ServerChannel> channelType;
    if (Epoll.isAvailable()) {
      bossEventLoopGroup = new EpollEventLoopGroup(bossGroupSize, new ThreadFactoryBuilder()
          .setDaemon(true).setNameFormat(threadNamePrefix + "RpcBoss-ELG-%d").build());
      workerEventLoopGroup = new EpollEventLoopGroup(workerGroupSize, new ThreadFactoryBuilder()
          .setDaemon(true).setNameFormat(threadNamePrefix + "RpcWorker-ELG-%d").build());
      channelType =  EpollServerSocketChannel.class;
    } else {
      bossEventLoopGroup = new NioEventLoopGroup(bossGroupSize, new ThreadFactoryBuilder()
          .setDaemon(true).setNameFormat(threadNamePrefix + "RpcBoss-ELG-%d").build());
      workerEventLoopGroup = new NioEventLoopGroup(workerGroupSize, new ThreadFactoryBuilder()
          .setDaemon(true).setNameFormat(threadNamePrefix + "RpcWorker-ELG-%d").build());
      channelType = NioServerSocketChannel.class;
    }
    LOG.info("GrpcServer channel type {}", channelType.getSimpleName());

    NettyServerBuilder nettyServerBuilder = NettyServerBuilder.forPort(port)
        .bossEventLoopGroup(bossEventLoopGroup)
        .maxInboundMessageSize(maxInboundLength)
        .workerEventLoopGroup(workerEventLoopGroup)
        .channelType(channelType)
        .executor(readExecutors)
        .addService(ServerInterceptors.intercept(
            new JobworkerGrpcRequestHandler(jobworkerServerImpl, protocolMessageMetrics, jobworkerCommandManager),
            new GrpcMetricsServerRequestInterceptor(jobworkerGrpcMetrics),
            new GrpcMetricsServerResponseInterceptor(jobworkerGrpcMetrics)))
        .addTransportFilter(new GrpcMetricsServerTransportFilter(jobworkerGrpcMetrics))
        .intercept(new RemoteAddressInterceptor());
    server = nettyServerBuilder.build();
  }

  public void start() throws IOException {
    server.start();
    port = server.getPort();
    LOG.info("Job Worker gRPC Server started on port {}", port);
  }

  public void stop() {
    LOG.info("Stopping Job Worker gRPC Server...");

    if (readExecutors != null) {
      readExecutors.shutdown();
      try {
        if (!readExecutors.awaitTermination(5, TimeUnit.SECONDS)) {
          LOG.warn("gRPC readExecutors did not terminate gracefully within 5 seconds");
          readExecutors.shutdownNow();
        }
      } catch (InterruptedException e) {
        readExecutors.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    if (server != null) {
      try {
        server.shutdown();
        if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
          LOG.warn("Netty server did not terminate gracefully within 10 seconds");
          server.shutdownNow();
        }
      } catch (InterruptedException e) {
        server.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    if (bossEventLoopGroup != null) {
      bossEventLoopGroup.shutdownGracefully();
    }
    if (workerEventLoopGroup != null) {
      workerEventLoopGroup.shutdownGracefully();
    }
    try {
      if (bossEventLoopGroup != null) {
        bossEventLoopGroup.terminationFuture().sync();
      }
      if (workerEventLoopGroup != null) {
        workerEventLoopGroup.terminationFuture().sync();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    if (jobworkerGrpcMetrics != null) {
      jobworkerGrpcMetrics.unRegister();
    }
    protocolMessageMetrics.unregister();

    LOG.info("Job Worker gRPC Server stopped");
  }

  public int getPort() {
    return port;
  }

  /**
   * Get the ProtocolMessageMetrics for this server.
   * @return ProtocolMessageMetrics
   */
  private ProtocolMessageMetrics<JobworkerCommandType> getProtocolMessageMetrics(OzoneConfiguration conf) {
    return ProtocolMessageMetrics
        .create("OMJobworkerGrpc-" + serviceName, "OM Jobworker Grpc protocol",
            JobworkerCommandType.class);
  }

  private int getGrpcPort(OzoneConfiguration conf,
                          OMJobworkerConfiguration jobworkerServiceConfig) {
    if (omNodeDetails == null) {
      return jobworkerServiceConfig.getGrpcPort();
    }

    String haPortKey = ConfUtils.addKeySuffixes(OMJobworkerConfiguration.getGrpcPortKey(),
        omNodeDetails.getServiceId(), omNodeDetails.getNodeId());
    OptionalInt haPort = HddsUtils.getNumberFromConfigKeys(conf, haPortKey,
        OMJobworkerConfiguration.getGrpcPortKey());
    if (haPort.isPresent()) {
      return haPort.getAsInt();
    } else {
      return jobworkerServiceConfig.getGrpcPort();
    }
  }
}
