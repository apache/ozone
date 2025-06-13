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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_PROVIDER;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_PROVIDER_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_GRPC_BOSSGROUP_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_GRPC_BOSSGROUP_SIZE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_GRPC_READ_THREAD_NUM_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_GRPC_READ_THREAD_NUM_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_GRPC_WORKERGROUP_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_GRPC_WORKERGROUP_SIZE_KEY;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.IOException;
import java.util.OptionalInt;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.ozone.grpc.metrics.GrpcMetrics;
import org.apache.hadoop.ozone.grpc.metrics.GrpcMetricsServerRequestInterceptor;
import org.apache.hadoop.ozone.grpc.metrics.GrpcMetricsServerResponseInterceptor;
import org.apache.hadoop.ozone.grpc.metrics.GrpcMetricsServerTransportFilter;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransport;
import org.apache.hadoop.ozone.om.protocolPB.grpc.ClientAddressServerInterceptor;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.security.OzoneDelegationTokenSecretManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Separated network server for gRPC transport OzoneManagerService s3g-&gt;OM.
 */
public class GrpcOzoneManagerServer {
  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcOzoneManagerServer.class);

  private final GrpcMetrics omS3gGrpcMetrics;
  private Server server;
  private int port;
  private final int maxSize;
  private final String threadNamePrefix;

  private ThreadPoolExecutor readExecutors;
  private EventLoopGroup bossEventLoopGroup;
  private EventLoopGroup workerEventLoopGroup;

  public GrpcOzoneManagerServer(OzoneConfiguration config,
                                OzoneManagerProtocolServerSideTranslatorPB
                                    omTranslator,
                                OzoneDelegationTokenSecretManager
                                    delegationTokenMgr,
                                CertificateClient caClient,
                                String threadPrefix) {
    maxSize = config.getInt(OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH,
        OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT);
    OptionalInt haPort = HddsUtils.getNumberFromConfigKeys(config,
        ConfUtils.addKeySuffixes(
            OMConfigKeys.OZONE_OM_GRPC_PORT_KEY,
            config.get(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY),
            config.get(OMConfigKeys.OZONE_OM_NODE_ID_KEY)),
        OMConfigKeys.OZONE_OM_GRPC_PORT_KEY);
    if (haPort.isPresent()) {
      this.port = haPort.getAsInt();
    } else {
      this.port = config.getObject(
          GrpcOmTransport.GrpcOmTransportConfig.class).
          getPort();
    }
    this.threadNamePrefix = threadPrefix;
    this.omS3gGrpcMetrics = GrpcMetrics.create(config);

    init(omTranslator,
        delegationTokenMgr,
        config,
        caClient);
  }

  public void init(OzoneManagerProtocolServerSideTranslatorPB omTranslator,
                   OzoneDelegationTokenSecretManager delegationTokenMgr,
                   OzoneConfiguration omServerConfig,
                   CertificateClient caClient) {

    int poolSize = omServerConfig.getInt(OZONE_OM_GRPC_READ_THREAD_NUM_KEY,
        OZONE_OM_GRPC_READ_THREAD_NUM_DEFAULT);

    int bossGroupSize = omServerConfig.getInt(OZONE_OM_GRPC_BOSSGROUP_SIZE_KEY,
        OZONE_OM_GRPC_BOSSGROUP_SIZE_DEFAULT);

    int workerGroupSize =
        omServerConfig.getInt(OZONE_OM_GRPC_WORKERGROUP_SIZE_KEY,
            OZONE_OM_GRPC_WORKERGROUP_SIZE_DEFAULT);

    readExecutors = new ThreadPoolExecutor(poolSize, poolSize,
        60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat(threadNamePrefix + "OmRpcReader-%d")
            .build());

    ThreadFactory bossFactory = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat(threadNamePrefix + "OmRpcBoss-ELG-%d")
        .build();
    bossEventLoopGroup = new NioEventLoopGroup(bossGroupSize, bossFactory);

    ThreadFactory workerFactory = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat(threadNamePrefix + "OmRpcWorker-ELG-%d")
        .build();
    workerEventLoopGroup =
        new NioEventLoopGroup(workerGroupSize, workerFactory);

    NettyServerBuilder nettyServerBuilder = NettyServerBuilder.forPort(port)
        .maxInboundMessageSize(maxSize)
        .bossEventLoopGroup(bossEventLoopGroup)
        .workerEventLoopGroup(workerEventLoopGroup)
        .channelType(NioServerSocketChannel.class)
        .executor(readExecutors)
        .addService(ServerInterceptors.intercept(
            new OzoneManagerServiceGrpc(omTranslator),
            new ClientAddressServerInterceptor(),
            new GrpcMetricsServerResponseInterceptor(omS3gGrpcMetrics),
            new GrpcMetricsServerRequestInterceptor(omS3gGrpcMetrics)))
        .addTransportFilter(
            new GrpcMetricsServerTransportFilter(omS3gGrpcMetrics));

    SecurityConfig secConf = new SecurityConfig(omServerConfig);
    if (secConf.isSecurityEnabled() && secConf.isGrpcTlsEnabled()) {
      try {
        SslContextBuilder sslClientContextBuilder =
            SslContextBuilder.forServer(caClient.getKeyManager());
        SslContextBuilder sslContextBuilder = GrpcSslContexts.configure(
            sslClientContextBuilder,
            SslProvider.valueOf(omServerConfig.get(HDDS_GRPC_TLS_PROVIDER,
                HDDS_GRPC_TLS_PROVIDER_DEFAULT)));
        nettyServerBuilder.sslContext(sslContextBuilder.build());
      } catch (Exception ex) {
        LOG.error("Unable to setup TLS for secure Om S3g GRPC channel.", ex);
      }
    }

    server = nettyServerBuilder.build();
  }

  public void start() throws IOException {
    server.start();
    LOG.info("{} is started using port {}", getClass().getSimpleName(),
        server.getPort());
    port = server.getPort();
  }

  public void stop() {
    try {
      readExecutors.shutdown();
      readExecutors.awaitTermination(5L, TimeUnit.SECONDS);
      server.shutdown().awaitTermination(10L, TimeUnit.SECONDS);
      bossEventLoopGroup.shutdownGracefully().sync();
      workerEventLoopGroup.shutdownGracefully().sync();
      LOG.info("Server {} is shutdown", getClass().getSimpleName());
    } catch (InterruptedException ex) {
      LOG.warn("{} couldn't be stopped gracefully", getClass().getSimpleName());
    } finally {
      omS3gGrpcMetrics.unRegister();
    }
  }

  public int getPort() {
    return port;
  }
}
