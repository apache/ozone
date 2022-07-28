/*
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
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.PostConstruct;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.tracing.GrpcServerInterceptor;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;

import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptors;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.ClientAuth;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.conf.ConfigTag.DATANODE;
import static org.apache.hadoop.hdds.conf.ConfigTag.MANAGEMENT;

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
    NettyServerBuilder nettyServerBuilder = NettyServerBuilder.forPort(port)
        .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE)
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
      server.shutdown().awaitTermination(10L, TimeUnit.SECONDS);
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
  @ConfigGroup(prefix = ReplicationConfig.PREFIX)
  public static final class ReplicationConfig {

    public static final String PREFIX = "hdds.datanode.replication";
    public static final String STREAMS_LIMIT_KEY = "streams.limit";

    public static final String REPLICATION_STREAMS_LIMIT_KEY =
        PREFIX + "." + STREAMS_LIMIT_KEY;

    public static final int REPLICATION_MAX_STREAMS_DEFAULT = 10;

    /**
     * The maximum number of replication commands a single datanode can execute
     * simultaneously.
     */
    @Config(key = STREAMS_LIMIT_KEY,
        type = ConfigType.INT,
        defaultValue = "10",
        tags = {DATANODE},
        description = "The maximum number of replication commands a single " +
            "datanode can execute simultaneously"
    )
    private int replicationMaxStreams = REPLICATION_MAX_STREAMS_DEFAULT;

    @Config(key = "port", defaultValue = "9886",
        description = "Port used for the server2server replication server",
        tags = {DATANODE, MANAGEMENT})
    private int port;

    public int getPort() {
      return port;
    }

    public ReplicationConfig setPort(int portParam) {
      this.port = portParam;
      return this;
    }

    public int getReplicationMaxStreams() {
      return replicationMaxStreams;
    }

    public void setReplicationMaxStreams(int replicationMaxStreams) {
      this.replicationMaxStreams = replicationMaxStreams;
    }

    @PostConstruct
    public void validate() {
      if (replicationMaxStreams < 1) {
        LOG.warn(REPLICATION_STREAMS_LIMIT_KEY + " must be greater than zero " +
                "and was set to {}. Defaulting to {}",
            replicationMaxStreams, REPLICATION_MAX_STREAMS_DEFAULT);
        replicationMaxStreams = REPLICATION_MAX_STREAMS_DEFAULT;
      }
    }

  }

}
