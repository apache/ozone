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

package org.apache.hadoop.ozone.om.protocolPB;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.ozone.om.protocolPB.grpc.OzoneManagerServiceGrpcBinding;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.ServerCalls;

/**
 * OM gRPC test servers on localhost ephemeral ports, and plaintext channels to them; {@link #close()} shuts all down.
 * Ratis's shaded gRPC has no in-process transport, so tests use real loopback connections.
 */
final class GrpcTestServers implements AutoCloseable {
  private final List<Server> servers = new ArrayList<>();
  private final List<ManagedChannel> channels = new ArrayList<>();

  /** Starts a server for {@code handler} and returns its port. */
  int start(ServerCalls.UnaryMethod<OMRequest, OMResponse> handler) throws IOException {
    Server server = NettyServerBuilder.forAddress(new InetSocketAddress("localhost", 0))
        .directExecutor()
        .addService(OzoneManagerServiceGrpcBinding.bindService(handler))
        .build()
        .start();
    servers.add(server);
    return server.getPort();
  }

  /** Starts a server for {@code handler} and returns a channel to it. */
  ManagedChannel startAndConnect(ServerCalls.UnaryMethod<OMRequest, OMResponse> handler) throws IOException {
    return connect(NettyChannelBuilder.forAddress("localhost", start(handler)));
  }

  ManagedChannel connect(NettyChannelBuilder builder) {
    ManagedChannel channel = builder.usePlaintext().directExecutor().build();
    channels.add(channel);
    return channel;
  }

  @Override
  public void close() throws InterruptedException {
    for (ManagedChannel channel : channels) {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
    for (Server server : servers) {
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
    channels.clear();
    servers.clear();
  }
}
