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

package org.apache.hadoop.hdds.scm.ha;

import static org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder.forServer;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.ServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.ClientAuth;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to serve SCM DB checkpoints available for SCM HA.
 * Ideally should only be run on a ratis leader.
 */
public class InterSCMGrpcProtocolService {
  private static final Logger LOG =
      LoggerFactory.getLogger(InterSCMGrpcService.class);

  private final int port;
  private Server server;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  InterSCMGrpcProtocolService(final ConfigurationSource conf,
      final StorageContainerManager scm) throws IOException {
    Objects.requireNonNull(conf, "conf == null");
    this.port = conf.getInt(ScmConfigKeys.OZONE_SCM_GRPC_PORT_KEY,
        ScmConfigKeys.OZONE_SCM_GRPC_PORT_DEFAULT);

    NettyServerBuilder nettyServerBuilder =
        ((NettyServerBuilder) ServerBuilder.forPort(port))
            .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE);

    InterSCMGrpcService service = new InterSCMGrpcService(scm);
    ServerBuilder b = nettyServerBuilder.addService(service);

    SecurityConfig securityConfig = new SecurityConfig(conf);
    if (securityConfig.isSecurityEnabled()
        && securityConfig.isGrpcTlsEnabled()) {
      try {
        CertificateClient certClient = scm.getScmCertificateClient();
        SslContextBuilder sslServerContextBuilder =
            forServer(certClient.getKeyManager())
                .trustManager(certClient.getTrustManager());
        SslContextBuilder sslContextBuilder = GrpcSslContexts.configure(
            sslServerContextBuilder, securityConfig.getGrpcSslProvider());
        sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
        nettyServerBuilder.sslContext(sslContextBuilder.build());
      } catch (Exception ex) {
        LOG.error("Unable to setup TLS for secure " +
            "InterSCMGrpcProtocolService GRPC endpoint.", ex);
        throw new RuntimeException("Unable to setup TLS for secure " +
            "InterSCMGrpcProtocolService GRPC endpoint.");
      }
    }
    Objects.requireNonNull(b, "b == null");
    server = nettyServerBuilder.build();
  }

  public int getPort() {
    return this.port;
  }

  public void start() throws IOException {
    if (!isStarted.compareAndSet(false, true)) {
      LOG.info("Ignore. already started.");
      return;
    } else {
      LOG.info("Starting SCM Grpc Service at port {}", port);
      server.start();
    }
  }

  public void stop() {
    if (isStarted.get()) {
      server.shutdown();
      try {
        server.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.error("failed to shutdown XceiverServerGrpc", e);
        Thread.currentThread().interrupt();
      }
      isStarted.set(false);
    }
  }
}
