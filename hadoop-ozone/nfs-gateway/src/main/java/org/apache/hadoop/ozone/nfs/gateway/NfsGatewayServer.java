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

package org.apache.hadoop.ozone.nfs.gateway;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Loopback-only backend listener; mutual TLS is mandatory even on the local host. */
public final class NfsGatewayServer implements Closeable {
  public static final int MAX_MESSAGE_BYTES = 4 * 1024 * 1024;
  private final Server server;
  private final ThreadPoolExecutor executor;
  private final NfsExportLeaseManager leaseManager;

  public NfsGatewayServer(InetSocketAddress address, File certificate, File key, File trustedCa,
      Map<String, Set<UUID>> allowedPeers, BindableService service) throws IOException {
    this(address, certificate, key, trustedCa, allowedPeers, service, null);
  }

  NfsGatewayServer(InetSocketAddress address, File certificate, File key, File trustedCa,
      Map<String, Set<UUID>> allowedPeers, BindableService service, NfsExportLeaseManager leaseManager)
      throws IOException {
    if (address.getAddress() == null || !address.getAddress().isLoopbackAddress()) {
      throw new IllegalArgumentException("The initial NFS backend must bind to loopback");
    }
    this.leaseManager = leaseManager;
    NfsTlsPeerInterceptor interceptor = new NfsTlsPeerInterceptor(allowedPeers);
    SslContext ssl = GrpcSslContexts.configure(SslContextBuilder.forServer(certificate, key), SslProvider.JDK)
        .trustManager(trustedCa).clientAuth(ClientAuth.REQUIRE).protocols("TLSv1.3", "TLSv1.2").build();
    executor = new ThreadPoolExecutor(4, 4, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(64),
        new ThreadFactoryBuilder().setNameFormat("NfsGateway-%d").setDaemon(true).build());
    server = NettyServerBuilder.forAddress(address).sslContext(ssl).executor(executor)
        .handshakeTimeout(10, TimeUnit.SECONDS).maxInboundMessageSize(MAX_MESSAGE_BYTES)
        .maxConcurrentCallsPerConnection(64).addService(ServerInterceptors.intercept(service, interceptor)).build();
    try {
      server.start();
    } catch (IOException | RuntimeException ex) {
      executor.shutdownNow();
      throw ex;
    }
  }

  public int getPort() {
    return server.getPort();
  }

  public void awaitTermination() throws InterruptedException {
    server.awaitTermination();
  }

  @Override
  public void close() {
    server.shutdown();
    try {
      if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
        server.shutdownNow();
      }
    } catch (InterruptedException ex) {
      server.shutdownNow();
      Thread.currentThread().interrupt();
    } finally {
      executor.shutdownNow();
      if (leaseManager != null) {
        leaseManager.close();
      }
    }
  }
}
