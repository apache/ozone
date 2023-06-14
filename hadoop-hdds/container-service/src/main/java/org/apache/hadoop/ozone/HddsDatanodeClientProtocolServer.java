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

package org.apache.hadoop.ozone;

import com.google.protobuf.BlockingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos;
import org.apache.hadoop.hdds.protocolPB.ReconfigureProtocolPB;
import org.apache.hadoop.hdds.protocolPB.ReconfigureProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.server.ServiceRuntimeInfoImpl;
import org.apache.hadoop.hdds.utils.VersionInfo;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdds.HddsUtils.preserveThreadName;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.CLIENT_RPC;

/**
 * The RPC server that listens to requests from clients.
 */
public class HddsDatanodeClientProtocolServer extends ServiceRuntimeInfoImpl {
  private static final Logger LOG =
      LoggerFactory.getLogger(HddsDatanodeClientProtocolServer.class);
  private final RPC.Server rpcServer;
  private final InetSocketAddress clientRpcAddress;
  private final OzoneConfiguration conf;

  protected HddsDatanodeClientProtocolServer(
      DatanodeDetails datanodeDetails, OzoneConfiguration conf,
      VersionInfo versionInfo, ReconfigurationHandler reconfigurationHandler
  ) throws IOException {
    super(versionInfo);
    this.conf = conf;

    rpcServer = getRpcServer(conf, reconfigurationHandler);
    clientRpcAddress = ServerUtils.updateRPCListenAddress(this.conf,
        HDDS_DATANODE_CLIENT_ADDRESS_KEY,
        HddsUtils.getDatanodeRpcAddress(conf), rpcServer);
    datanodeDetails.setPort(CLIENT_RPC, clientRpcAddress.getPort());
  }

  public void start() {
    LOG.info("RPC server for Client " + getClientRpcAddress());
    rpcServer.start();
  }

  public void stop() {
    try {
      LOG.info("Stopping the RPC server for Client Protocol");
      getClientRpcServer().stop();
    } catch (Exception ex) {
      LOG.error("Client Protocol RPC stop failed.", ex);
    }
  }

  public void join() throws InterruptedException {
    LOG.trace("Join RPC server for Client Protocol");
    getClientRpcServer().join();
  }

  /**
   * Creates a new instance of rpc server. If an earlier instance is already
   * running then returns the same.
   */
  private RPC.Server getRpcServer(OzoneConfiguration configuration,
      ReconfigurationHandler reconfigurationHandler)
      throws IOException {
    InetSocketAddress rpcAddress = HddsUtils.getDatanodeRpcAddress(conf);
    // Add reconfigureProtocolService.
    RPC.setProtocolEngine(
        configuration, ReconfigureProtocolPB.class, ProtobufRpcEngine.class);

    final int handlerCount = conf.getInt(HDDS_DATANODE_HANDLER_COUNT_KEY,
        HDDS_DATANODE_HANDLER_COUNT_DEFAULT);
    ReconfigureProtocolServerSideTranslatorPB reconfigureServerProtocol
        = new ReconfigureProtocolServerSideTranslatorPB(reconfigurationHandler);
    BlockingService reconfigureService = ReconfigureProtocolProtos
        .ReconfigureProtocolService.newReflectiveBlockingService(
            reconfigureServerProtocol);

    return preserveThreadName(() -> startRpcServer(configuration, rpcAddress,
        ReconfigureProtocolPB.class, reconfigureService, handlerCount));
  }

  /**
   * Starts an RPC server, if configured.
   *
   * @param configuration configuration
   * @param addr          configured address of RPC server
   * @param protocol      RPC protocol provided by RPC server
   * @param instance      RPC protocol implementation instance
   * @param handlerCount  RPC server handler count
   * @return RPC server
   * @throws IOException if there is an I/O error while creating RPC server
   */
  private RPC.Server startRpcServer(
      Configuration configuration, InetSocketAddress addr,
      Class<?> protocol, BlockingService instance,
      int handlerCount)
      throws IOException {
    return new RPC.Builder(configuration)
        .setProtocol(protocol)
        .setInstance(instance)
        .setBindAddress(addr.getHostString())
        .setPort(addr.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .setSecretManager(null)
        .build();
  }

  private RPC.Server getClientRpcServer() {
    return rpcServer;
  }

  public InetSocketAddress getClientRpcAddress() {
    return clientRpcAddress;
  }
}
