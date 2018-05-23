/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.transport.server;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.ratis.shaded.io.grpc.Server;
import org.apache.ratis.shaded.io.grpc.ServerBuilder;
import org.apache.ratis.shaded.io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;

/**
 * Creates a Grpc server endpoint that acts as the communication layer for
 * Ozone containers.
 */
public final class XceiverServerGrpc implements XceiverServerSpi {
  private static final Logger
      LOG = LoggerFactory.getLogger(XceiverServerGrpc.class);
  private int port;
  private Server server;

  /**
   * Constructs a Grpc server class.
   *
   * @param conf - Configuration
   */
  public XceiverServerGrpc(DatanodeDetails datanodeDetails, Configuration conf,
                       ContainerDispatcher dispatcher) {
    Preconditions.checkNotNull(conf);

    this.port = conf.getInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
        OzoneConfigKeys.DFS_CONTAINER_IPC_PORT_DEFAULT);
    // Get an available port on current node and
    // use that as the container port
    if (conf.getBoolean(OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT,
        OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT_DEFAULT)) {
      try (ServerSocket socket = new ServerSocket()) {
        socket.setReuseAddress(true);
        SocketAddress address = new InetSocketAddress(0);
        socket.bind(address);
        this.port = socket.getLocalPort();
        LOG.info("Found a free port for the server : {}", this.port);
      } catch (IOException e) {
        LOG.error("Unable find a random free port for the server, "
            + "fallback to use default port {}", this.port, e);
      }
    }
    datanodeDetails.setContainerPort(port);
    server = ((NettyServerBuilder) ServerBuilder.forPort(port))
        .maxMessageSize(OzoneConfigKeys.DFS_CONTAINER_CHUNK_MAX_SIZE)
        .addService(new GrpcXceiverService(dispatcher))
        .build();
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
    server.start();
  }

  @Override
  public void stop() {
    server.shutdown();
  }
}
